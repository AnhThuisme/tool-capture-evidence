param(
  [string]$VenvPath = ".venv",
  [string]$Host = "0.0.0.0",
  [int]$Port = 8000
)

$ErrorActionPreference = "Stop"

if (-not (Test-Path "$VenvPath\Scripts\python.exe")) {
  throw "Virtual env not found. Run .\setup_windows.ps1 first."
}

if (Test-Path ".env") {
  Get-Content .env | ForEach-Object {
    if ($_ -match '^\s*#' -or $_ -match '^\s*$') { return }
    $parts = $_ -split '=', 2
    if ($parts.Count -eq 2) {
      [Environment]::SetEnvironmentVariable($parts[0].Trim(), $parts[1].Trim())
    }
  }
}

function Test-PortBusy([int]$P) {
  try {
    $conn = Get-NetTCPConnection -LocalPort $P -State Listen -ErrorAction SilentlyContinue
    return ($null -ne $conn)
  } catch {
    return $false
  }
}

$selectedPort = $Port
if (Test-PortBusy $selectedPort) {
  if (-not (Test-PortBusy 8012)) {
    $selectedPort = 8012
  } else {
    throw "Port $Port is busy and fallback port 8012 is also busy. Please free one port or pass -Port."
  }
}

Write-Host "Starting web app at http://127.0.0.1:$selectedPort"
& "$VenvPath\Scripts\python.exe" -m uvicorn web_ui:app --host $Host --port $selectedPort
