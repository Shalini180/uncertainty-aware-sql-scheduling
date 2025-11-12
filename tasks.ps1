param([string]\)

switch (\) {
  'setup' { pip install -r requirements.txt; break }
  'run'   { streamlit run src/energy_ml/main.py; break }
  'test'  { pytest -q; break }
  'fmt'   { black src tests; break }
  default { Write-Host "Use: .\tasks.ps1 [setup|run|test|fmt]"; }
}
