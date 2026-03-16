param(
  [string]$VenvPath = ".venv"
)

$ErrorActionPreference = "Stop"

if (-not (Get-Command python -ErrorAction SilentlyContinue)) {
  throw "Python not found in PATH. Install Python 3.10+ first."
}

if (-not (Test-Path $VenvPath)) {
  python -m venv $VenvPath
}

& "$VenvPath\Scripts\python.exe" -m pip install --upgrade pip
& "$VenvPath\Scripts\python.exe" -m pip install -r requirements.txt

Write-Host "Setup complete."
