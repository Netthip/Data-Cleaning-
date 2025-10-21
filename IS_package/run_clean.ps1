# PowerShell runner
python -m venv .venv
. .\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
python budget_ingest.py --config ingest_headers.yml --mapping objc_mapping.yml --inputs "กรุงเทพ.xlsx" "นนทบุรี.xlsx" --output clean_data.xlsx
Write-Host "Done. Output: clean_data.xlsx"
