@echo off
chcp 65001 >nul
setlocal

if not exist .venv (
  py -3 -m venv .venv
)

".venv\Scripts\python.exe" -m pip install --upgrade pip
".venv\Scripts\pip.exe" install -r requirements.txt

if not exist out mkdir out

".venv\Scripts\python.exe" budget_ingest.py ^
  --config config\ingest_headers.yml ^
  --mapping config\objc_mapping.yml ^
  --inputs "data\กรุงเทพ.xlsx" "data\นนทบุรี.xlsx" ^
  --output out\clean_data.xlsx

echo.
echo ✔ Done. Output: out\clean_data.xlsx
pause
