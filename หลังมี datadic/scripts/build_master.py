
import sys, re, unicodedata, yaml, os
from pathlib import Path
import pandas as pd

def normalize_th(s):
    if pd.isna(s): return ""
    s = str(s)
    s = unicodedata.normalize("NFKC", s)
    s = re.sub(r"[\u200B-\u200D\uFEFF]", "", s)  # zero-width
    s = re.sub(r"\s+", " ", s.strip())
    return s

def contains_any(text, keywords):
    t = normalize_th(text)
    for kw in keywords:
        if kw in t:
            return True
    return False

def apply_rules(df, rules, fallback):
    # Ensure target columns exist
    for col in ["budget_group","plan_group","expenditure_type"]:
        if col not in df.columns:
            df[col] = None

    for idx, row in df.iterrows():
        applied = False
        # evaluate each rule in order (priority)
        for rule in rules:
            setvals = rule.get("set", {})
            ok = False

            if "when" in rule:
                w = rule["when"]
                field = w.get("field")
                equals = w.get("equals")
                equals_any = w.get("equals_any", [])
                contains_any_list = w.get("contains_any", [])

                v = normalize_th(row.get(field, ""))

                if equals is not None and v == normalize_th(equals):
                    ok = True
                elif equals_any and normalize_th(v) in [normalize_th(x) for x in equals_any]:
                    ok = True
                elif contains_any_list and contains_any(v, contains_any_list):
                    ok = True

            elif "when_any" in rule:
                ok_any = False
                for clause in rule["when_any"]:
                    field = clause.get("field")
                    equals = clause.get("equals")
                    equals_any = clause.get("equals_any", [])
                    contains_any_list = clause.get("contains_any", [])
                    v = normalize_th(row.get(field, ""))

                    cond_ok = False
                    if equals is not None and v == normalize_th(equals):
                        cond_ok = True
                    elif equals_any and normalize_th(v) in [normalize_th(x) for x in equals_any]:
                        cond_ok = True
                    elif contains_any_list and contains_any(v, contains_any_list):
                        cond_ok = True

                    if cond_ok:
                        ok_any = True
                        break
                ok = ok_any

            if ok:
                for k, val in setvals.items():
                    df.at[idx, k] = val
                applied = True
                # continue to next rule or break? priority -> stop at first match that sets budget_group
                # but allow later rules to set expenditure_type if not set yet.
                # We'll not break to allow later rules fill missing pieces:
                # break

        # apply fallback if still not mapped
        if pd.isna(df.at[idx, "budget_group"]) or df.at[idx, "budget_group"] in ("", None):
            for k, v in fallback.items():
                if pd.isna(df.at[idx, k]) or df.at[idx, k] in ("", None):
                    df.at[idx, k] = v

    return df

def main():
    import argparse
    ap = argparse.ArgumentParser(description="Build MasterData from BB 'data' sheet using YAML mapping.")
    ap.add_argument("--input_xlsx", required=True, help="Path to Excel file (e.g., 1748328157_5318.xlsx)")
    ap.add_argument("--sheet", default="data", help="Sheet name (default: data)")
    ap.add_argument("--config_yml", required=True, help="Path to budget_mapping.yml")
    ap.add_argument("--out_dir", default="output", help="Output directory (default: output)")
    args = ap.parse_args()

    cfg = yaml.safe_load(open(args.config_yml, "r", encoding="utf-8"))

    # Load data
    df = pd.read_excel(args.input_xlsx, sheet_name=args.sheet, dtype=str)
    # Normalize key text columns early
    for c in df.columns:
        df[c] = df[c].map(lambda x: normalize_th(x))

    # Make sure required columns exist (for future years with partial fields)
    must_have = ["min","min_name","agc","agc_name","group_budget","plan_name","output_name","act_name","objc","objc_8","cap_ncap","item_name","p_total_bud"]
    for c in must_have:
        if c not in df.columns:
            df[c] = None

    # Apply rules
    rules = cfg.get("rules", [])
    fallback = cfg.get("fallback", {"budget_group":"ไม่ทราบหมวด","plan_group":"ไม่ทราบแผน","expenditure_type":"ประจำ"})
    df = apply_rules(df, rules, fallback)

    # Convert amount to numeric
    def to_float(v):
        if v in (None, "", "None"): return None
        s = str(v).replace(",", "")
        try:
            return float(s)
        except:
            return None
    df["p_total_bud"] = df["p_total_bud"].map(to_float)

    # Reorder columns
    out_cols = cfg.get("output_columns", [])
    cols = [c for c in out_cols if c in df.columns] + [c for c in df.columns if c not in out_cols]
    master = df[cols].copy()


    # ----------------- Quality Gates -----------------
    total_amt = master["p_total_bud"].fillna(0).sum()
    unknown_amt = master.loc[master["budget_group"]=="ไม่ทราบหมวด", "p_total_bud"].fillna(0).sum()
    unknown_ratio = (unknown_amt / total_amt) if total_amt else 0.0

    # Gate 1: Unknown budget group by amount must be <= 2%
    UNKNOWN_MAX = float(os.environ.get("UNKNOWN_MAX", "0.02"))
    # Gate 2: Non-null amount coverage >= 95%
    nonnull_ratio = master["p_total_bud"].notna().mean()
    NONNULL_MIN = float(os.environ.get("NONNULL_MIN", "0.95"))

    # Gate 3: Negative amounts must be 0
    negative_count = (master["p_total_bud"].fillna(0) < 0).sum()

    qc_errors = []
    if unknown_ratio > UNKNOWN_MAX:
        qc_errors.append(f"[QC] Unknown budget group ratio {unknown_ratio:.2%} > {UNKNOWN_MAX:.2%}")
    if nonnull_ratio < NONNULL_MIN:
        qc_errors.append(f"[QC] Non-null amount coverage {nonnull_ratio:.2%} < {NONNULL_MIN:.2%}")
    if negative_count > 0:
        qc_errors.append(f"[QC] Negative amount rows detected: {negative_count}")

    if qc_errors:
        print("\\n".join(qc_errors))
        print("QC FAILED: stop without writing outputs. Adjust YAML rules or headers and re-run.")
        sys.exit(2)
    else:
        print("QC PASSED: unknown_ratio={:.2%}, nonnull_ratio={:.2%}, negatives={}".format(unknown_ratio, nonnull_ratio, negative_count))
    # --------------------------------------------------

    # Issues report
    issues = master[(master["budget_group"]=="ไม่ทราบหมวด") | (master["p_total_bud"].isna())].copy()

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    with pd.ExcelWriter(out_dir / "MasterData.xlsx", engine="openpyxl") as w:
        master.to_excel(w, index=False, sheet_name="MasterData")

    with pd.ExcelWriter(out_dir / "DataIssues.xlsx", engine="openpyxl") as w:
        issues.to_excel(w, index=False, sheet_name="Issues")

    print("✓ MasterData rows:", len(master), "Issues:", len(issues))
    print("Saved to:", out_dir / "MasterData.xlsx", "and", out_dir / "DataIssues.xlsx")

if __name__ == "__main__":
    main()
from pathlib import Path

p = Path(r"C:\Users\netthip\OneDrive\เรียน ป.โท\IS\หลังมี datadic\scripts\build_master.py")
txt = p.read_text(encoding="utf-8")

if "import os" not in txt:
    # แทรก os เข้าแถว import หลัก
    txt = txt.replace("import sys, re, unicodedata, yaml",
                      "import sys, re, unicodedata, yaml, os")
    p.write_text(txt, encoding="utf-8")
    print("✅ injected: import os")
else:
    print("ℹ️ already has: import os")

import sys, re, unicodedata, yaml, os
