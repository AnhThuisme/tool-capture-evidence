param(
  [string]$VenvPath = ".venv",
  [string]$BindHost = "127.0.0.1",
  [int]$Port = 8765
)

$ErrorActionPreference = "Stop"
$ScriptRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
Set-Location $ScriptRoot

if (-not (Test-Path "$VenvPath\Scripts\python.exe")) {
  throw "Virtual env not found. Run .\setup_windows.ps1 first."
}

foreach ($envFile in @(".env", "local_agent.env")) {
  if (Test-Path $envFile) {
    Get-Content $envFile | ForEach-Object {
      if ($_ -match '^\s*#' -or $_ -match '^\s*$') { return }
      $parts = $_ -split '=', 2
      if ($parts.Count -eq 2) {
        [Environment]::SetEnvironmentVariable($parts[0].Trim(), $parts[1].Trim())
      }
    }
  }
}

if (-not $PSBoundParameters.ContainsKey('Port')) {
  $envPortRaw = [Environment]::GetEnvironmentVariable("LOCAL_AGENT_PORT")
  $envPort = 0
  if ($envPortRaw -and [int]::TryParse($envPortRaw, [ref]$envPort) -and $envPort -gt 0) {
    $Port = $envPort
  }
}

Write-Host "Starting local agent at http://$BindHost:$Port"
& "$VenvPath\Scripts\python.exe" -m uvicorn local_agent:app --host $BindHost --port $Port
