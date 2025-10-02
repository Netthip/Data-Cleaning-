# clean_excel.py
import os, re, json, glob, sys
import pandas as pd

# ---------- ปรับค่าตามเครื่องกิ๊ฟ ----------
INPUT_FOLDER  = r"inputs"     # โฟลเดอร์ไฟล์ต้นฉบับ
OUTPUT_FOLDER = r"outputs"    # โฟลเดอร์ไฟล์ผลลัพธ์
HEADER_JSON   = r"header_dict.json"
TEST_FILE     = r""  # ถ้าจะทดสอบไฟล์เดียว ใส่พาธเต็ม/สัมพัทธ์ได้ เช่น r"inputs\21101_70MasterData.xlsx"

# ถ้า STRICT=True แล้วพบ issues > 0 จะ "ไม่" export ไฟล์ เพื่อกันงานไม่สมบูรณ์
STRICT = False
# ------------------------------------------

os.makedirs(OUTPUT_FOLDER, exist_ok=True)

# โหลดพจนานุกรมหัว
with open(HEADER_JSON, "r", encoding="utf-8") as f:
    HEADER_MAP = json.load(f)

# ---------- ตัวช่วย ----------
def normalize_header_names(cols, header_map):
    """คืนค่า (new_cols, unknown_list) — รีเนมหัวตาม dict + จับ case ที่สะกดคล้าย (ตัดช่องว่าง/วงเล็บ)"""
    new_cols, unknown = [], []
    for c in cols:
        c0 = str(c).strip()
        if c0 in header_map:
            new_cols.append(header_map[c0]); continue
        c_norm = re.sub(r"[()\s]", "", c0)
        matched = None
        for k, v in header_map.items():
            k_norm = re.sub(r"[()\s]", "", str(k))
            if c_norm.lower() == k_norm.lower():
                matched = v; break
        if matched:
            new_cols.append(matched)
        else:
            new_cols.append(c0)
            unknown.append(c0)
    return new_cols, unknown

def to_numeric_safe(s):
    return pd.to_numeric(s, errors="coerce")

# ---------- กำหนดกฎ Validation ที่ “กันงานไม่สมบูรณ์” ----------
VALIDATION_CFG = {
    "decimal_places": {     # ต้องเป็นทศนิยมตามจำนวนที่กำหนด (ตัวอย่าง: 4 ตำแหน่ง)
        "FY67_adjusted": 4, "FY68": 4, "FY69": 4, "FY70": 4
    },
    "non_negative": [       # ต้องไม่ติดลบ
        "FY67_adjusted", "FY68", "FY69", "FY70"
    ],
    "not_null": [           # ห้ามว่าง
        "activity_code", "item_code"
    ]
}

def validate_df(df):
    """ตรวจ dataframe ต่อชีต → คืน (issues:list[dict], passed:bool)"""
    issues = []
    # 1) ทศนิยมตามกำหนด
    for col, d in VALIDATION_CFG.get("decimal_places", {}).items():
        if col in df.columns:
            ser = to_numeric_safe(df[col])
            bad_idx = df[~ser.isna() & (ser.round(d) != ser)].index.tolist()
            for i in bad_idx:
                issues.append({"row": int(i)+1, "column": col, "rule": f"decimals_{d}", "value": df.at[i, col]})
    # 2) ห้ามติดลบ
    for col in VALIDATION_CFG.get("non_negative", []):
        if col in df.columns:
            ser = to_numeric_safe(df[col])
            bad_idx = df[ser < 0].index.tolist()
            for i in bad_idx:
                issues.append({"row": int(i)+1, "column": col, "rule": "non_negative", "value": df.at[i, col]})
    # 3) ห้ามว่าง
    for col in VALIDATION_CFG.get("not_null", []):
        if col in df.columns:
            bad_idx = df[df[col].isna() | (df[col].astype(str).str.strip() == "")].index.tolist()
            for i in bad_idx:
                issues.append({"row": int(i)+1, "column": col, "rule": "not_null", "value": df.at[i, col]})
    return issues, len(issues) == 0

# ---------- ทำความสะอาด “หนึ่งไฟล์” ----------
def process_one_file(path):
    xls = pd.ExcelFile(path)
    clean_sheets = {}
    all_unknown_headers = set()

    # รีเนมหัวทุกชีต
    for sheet in xls.sheet_names:
        df = xls.parse(sheet, header=0)
        new_cols, unknown = normalize_header_names(df.columns, HEADER_MAP)
        df.columns = new_cols
        clean_sheets[sheet] = df
        all_unknown_headers.update(unknown)

    # รวม issues จากทุกชีต
    issues_all = []
    for sheet, df in clean_sheets.items():
        issues, ok = validate_df(df)
        for it in issues:
            it["sheet"] = sheet
        issues_all.extend(issues)

    base = os.path.splitext(os.path.basename(path))[0]
    out_file = os.path.join(OUTPUT_FOLDER, base + "_Clean.xlsx")
    log_file = os.path.join(OUTPUT_FOLDER, base + "_clean_log.csv")

    # เขียน log เสมอ
    log_df = pd.DataFrame(issues_all) if issues_all else pd.DataFrame(columns=["row","column","rule","value","sheet"])
    log_df.to_csv(log_file, index=False, encoding="utf-8-sig")

    # โหมดยอม/ไม่ยอม export เมื่อมี issues
    if STRICT and len(issues_all) > 0:
        # กัน “งานไม่สมบูรณ์” โดยไม่ยอมสร้างไฟล์ Clean
        return {
            "file": os.path.basename(path),
            "exported": False,
            "reason": f"STRICT mode: พบ {len(issues_all)} issues",
            "unknown_headers": "|".join(sorted(all_unknown_headers)) if all_unknown_headers else "",
            "issues_count": len(issues_all),
            "out_file": "",
            "log_file": log_file
        }

    # export ได้ (ถึงจะมี issues ก็ยอม แต่มี log แนบ)
    with pd.ExcelWriter(out_file, engine="openpyxl") as writer:
        for sheet, df in clean_sheets.items():
            df.to_excel(writer, sheet_name=sheet[:31], index=False)

    return {
        "file": os.path.basename(path),
        "exported": True,
        "reason": "OK",
        "unknown_headers": "|".join(sorted(all_unknown_headers)) if all_unknown_headers else "",
        "issues_count": len(issues_all),
        "out_file": out_file,
        "log_file": log_file
    }

# ---------- main ----------
def main():
    summary = []

    # โหมดไฟล์เดียว (ตั้ง TEST_FILE)
    if TEST_FILE:
        if not os.path.exists(TEST_FILE):
            print(f"❌ ไม่พบไฟล์: {TEST_FILE}")
            return
        print(f"▶ Processing single file: {TEST_FILE}")
        res = process_one_file(TEST_FILE)
        summary.append(res)
        print(res)
    else:
        # โหมดทั้งโฟลเดอร์
        paths = glob.glob(os.path.join(INPUT_FOLDER, "*.xlsx"))
        if not paths:
            print(f"⚠️ ไม่พบไฟล์ .xlsx ในโฟลเดอร์: {INPUT_FOLDER}")
            return
        for p in paths:
            try:
                print(f"▶ {os.path.basename(p)} ...")
                res = process_one_file(p)
                summary.append(res)
                print(f"   → exported={res['exported']}, issues={res['issues_count']}")
            except Exception as e:
                summary.append({
                    "file": os.path.basename(p),
                    "exported": False,
                    "reason": f"ERROR: {e}",
                    "unknown_headers": "ERROR",
                    "issues_count": -1,
                    "out_file": "",
                    "log_file": ""
                })
                print(f"   ❌ Error: {e}")

    # เขียนสรุปทั้งชุด
    sum_df = pd.DataFrame(summary)
    sum_path = os.path.join(OUTPUT_FOLDER, "batch_summary.csv")
    sum_df.to_csv(sum_path, index=False, encoding="utf-8-sig")
    print(f"\n🧾 Summary → {sum_path}")
    print("เสร็จแล้วจ้า ✨")

if __name__ == "__main__":
    main()
# ลิสต์หัวมาตรฐานที่อยากให้เรียงเสมอ (Master Schema)
MASTER_SCHEMA = [
    "program",
    "project",
    "activity_code",
    "activity_name",
    "budget_category",
    "expense_nature",
    "expense_group",
    "single_or_multi_year",
    "opex_or_capex",
    "item_code",
    "item_name",
    "item_sub",
    "FY67_adjusted",
    "FY68",
    "FY69",
    "FY70"
]

