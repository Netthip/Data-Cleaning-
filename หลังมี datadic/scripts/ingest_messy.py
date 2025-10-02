from pathlib import Path
import os, sys, subprocess

PROJECT = Path(r"C:\Users\netthip\OneDrive\เรียน ป.โท\IS\หลังมี datadic").resolve()
SCRIPTS = PROJECT / "scripts"
CONFIG  = PROJECT / "config"
DATA    = PROJECT / "data"
OUT     = PROJECT / "output"
OUT.mkdir(exist_ok=True)

env = os.environ.copy()
env["PYTHONIOENCODING"] = "utf-8"

cmd = [
    sys.executable, "-X", "utf8",
    str(SCRIPTS / "ingest_messy.py"),
    "--glob", str(DATA / "*.xlsx"),
    "--headers_yml", str(CONFIG / "headers.yml"),
    "--mapping_yml", str(CONFIG / "budget_mapping.yml"),
    "--out_dir", str(OUT),
]
print("CMD:", " ".join(cmd))

res = subprocess.run(
    cmd, cwd=str(PROJECT), env=env,
    stdout=subprocess.PIPE, stderr=subprocess.PIPE,
    text=True, encoding="utf-8", errors="replace"
)
print("---- STDOUT ----\n", res.stdout)
print("---- STDERR ----\n", res.stderr)
print("Exit code:", res.returncode)
