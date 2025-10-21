
import argparse
import re
import sys
from pathlib import Path
import pandas as pd
import yaml

def load_yaml(path):
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)

def normalize(s):
    s = str(s or "").strip().lower()
    # remove spaces and special chars common in Thai headers
    s = re.sub(r"[\s\(\)\/\-_:]+", "", s)
    return s

def pick_header_by_synonyms(headers, synonyms):
    """Return mapping {std_name: actual_header} using synonym lists."""
    norm_map = {h: normalize(h) for h in headers}
    inv = {}
    for std, synlist in synonyms.items():
        inv[std] = None
        syn_norm = [normalize(x) for x in synlist]
        # exact norm match first
        for h, n in norm_map.items():
            if n in syn_norm:
                inv[std] = h
                break
        if not inv[std]:
            # fallback contains
            for h, n in norm_map.items():
                if any(n.find(s) >= 0 for s in syn_norm):
                    inv[std] = h
                    break
    return inv

def detect_province(top_rows, pattern):
    for _, row in top_rows.iterrows():
        for cell in row.astype(str).tolist():
            m = re.search(pattern, str(cell).strip())
            if m:
                return m.group(1), m.group(2).strip()
    return None, None

def read_sheet(path, sheet_name, header_row):
    raw = pd.read_excel(path, sheet_name=sheet_name, header=None, dtype=str)
    top = raw.iloc[:max(0, header_row-1)].fillna("")
    headers = raw.iloc[header_row-1].fillna("")
    df = raw.iloc[header_row:].copy()
    df.columns = headers
    df = df.reset_index(drop=True)
    return df, top

def coalesce_cols(df, columns_map, header_synonyms):
    # try exact first
    out = pd.DataFrame()
    used = set()
    for src, dst in columns_map.items():
        if src in df.columns:
            out[dst] = df[src]
            used.add(dst)
    # auto by synonyms for missing std fields
    missing = [x for x in ["item_text","unit","quantity","amount"] if x not in out.columns]
    if missing and header_synonyms:
        mapping = pick_header_by_synonyms(df.columns, header_synonyms)
        for std in missing:
            actual = mapping.get(std)
            if actual and actual in df.columns:
                out[std] = df[actual]
                used.add(std)
    # ensure exists
    for must in ["item_text","unit","quantity","amount"]:
        if must not in out.columns:
            out[must] = None
    return out

def apply_objc_mapping(row, objc_map):
    text = (row.get("item_text") or "")
    # direct by keyword for objc_8 lookup
    for key, payload in objc_map.get("lookup_by_objc8", {}).items():
        if key and key in text:
            row["objc_8"] = key
            row["objc_code"] = payload.get("objc_code")
            row["obj_12"] = payload.get("obj_12")
            row["objc_code_5g"] = payload.get("objc_code_5g")
            row["objc_5"] = payload.get("objc_5")
            return row
    # otherwise try detect group 5 by presence of group keyword in text
    for gname, code5 in objc_map.get("lookup_by_objc5", {}).items():
        if gname and gname in text:
            row["objc_code_5g"] = code5
            row["objc_5"] = gname
            break
    return row

def set_plan_and_stg(row, common_kw):
    it = (row.get("item_text") or "").strip()
    plan_prefix = common_kw.get("plan_prefix", "แผนงาน")
    exclusions = set(common_kw.get("plan_exclusions", []))
    if it.startswith(plan_prefix) and not any(it.startswith(e) for e in exclusions):
        row["plan_name"] = it
        row["stg_name"] = it
    # activity name
    act_pat = common_kw.get("activity_keyword")
    if act_pat and re.match(act_pat, it):
        row["activity_name"] = re.sub(r"^\s*กิจกรรม\s*[:：]?\s*", "", it)
    # output name
    if it.startswith("โครงการ"):
        row["output_name"] = it.replace("โครงการ :", "").strip()
    return row

def process_file(path, config, objc_map):
    common = config["common"]
    df, top = read_sheet(path, common["sheet_name"], common["header_row"])
    pcode, pname = detect_province(top, common["province_detection"]["pattern"])
    base = coalesce_cols(df, common["columns_map"], common.get("header_synonyms"))
    base["source_file"] = Path(path).name
    base["source_row"] = base.index + common["header_row"] + 1
    base["province_code"] = pcode
    base["province_name"] = pname

    out_rows = []
    for _, r in base.iterrows():
        row = r.to_dict()
        row = set_plan_and_stg(row, common.get("keywords", {}))
        row = apply_objc_mapping(row, objc_map)
        row["item_name1"] = row.get("item_text")
        # normalize numerics
        for c in ("quantity","amount"):
            val = row.get(c)
            if isinstance(val, str):
                val2 = re.sub(r"[,\s]", "", val)
                try:
                    row[c] = float(val2)
                except:
                    pass
        out_rows.append(row)
    out = pd.DataFrame(out_rows)
    return out

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", default="ingest_headers.yml")
    ap.add_argument("--mapping", default="objc_mapping.yml")
    ap.add_argument("--inputs", nargs="+", required=True)
    ap.add_argument("--output", default="clean_data.xlsx")
    args = ap.parse_args()

    cfg = load_yaml(args.config)
    objc_map = load_yaml(args.mapping)

    all_df = []
    for inp in args.inputs:
        try:
            df = process_file(inp, cfg, objc_map)
            print(f"[OK] processed {inp} -> {len(df)} rows")
            all_df.append(df)
        except Exception as e:
            print(f"[ERROR] {inp}: {e}", file=sys.stderr)
    if not all_df:
        print("[ERROR] no data processed", file=sys.stderr); sys.exit(2)

    out = pd.concat(all_df, ignore_index=True)
    cols = cfg["outputs"]["columns"]
    for c in cols:
        if c not in out.columns:
            out[c] = None
    out = out[cols]
    out.to_excel(args.output, index=False)
    print(f"[OK] wrote {args.output} with {len(out)} rows")

if __name__ == "__main__":
    main()
