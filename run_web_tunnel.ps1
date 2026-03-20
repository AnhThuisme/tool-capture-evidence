param(
  [string]$VenvPath = ".venv",
  [int]$Port = 8000,
  [string]$NgrokPath = "ngrok.exe"
)

$ErrorActionPreference = "Stop"

$root = Split-Path -Parent $MyInvocation.MyCommand.Path
$pythonExe = Join-Path $root "$VenvPath\Scripts\python.exe"
if (-not (Test-Path $pythonExe)) {
  throw "Virtual env not found. Run .\setup_windows.ps1 first."
}

$runWebScript = Join-Path $root "run_web.ps1"
if (-not (Test-Path $runWebScript)) {
  throw "run_web.ps1 not found at $runWebScript"
}

$ngrokCmd = (Get-Command $NgrokPath -ErrorAction SilentlyContinue | Select-Object -ExpandProperty Source -First 1)
if (-not $ngrokCmd) {
  throw "ngrok not found in PATH. Install ngrok or pass -NgrokPath."
}

function Test-PortBusy([int]$P) {
  try {
    $conn = Get-NetTCPConnection -LocalPort $P -State Listen -ErrorAction SilentlyContinue
    return ($null -ne $conn)
  } catch {
    return $false
  }
}

function Wait-HttpOk([string]$Url, [int]$TimeoutSec = 30) {
  $deadline = (Get-Date).AddSeconds($TimeoutSec)
  while ((Get-Date) -lt $deadline) {
    try {
      $resp = Invoke-WebRequest -UseBasicParsing -Uri $Url -TimeoutSec 3
      if ($resp.StatusCode -ge 200 -and $resp.StatusCode -lt 500) {
        return $true
      }
    } catch {
      Start-Sleep -Milliseconds 600
    }
  }
  return $false
}

function Find-HealthyLocalPort([int[]]$Ports, [int]$TimeoutSec = 35) {
  $deadline = (Get-Date).AddSeconds($TimeoutSec)
  $candidates = $Ports | Select-Object -Unique
  while ((Get-Date) -lt $deadline) {
    foreach ($candidate in $candidates) {
      try {
        $resp = Invoke-WebRequest -UseBasicParsing -Uri "http://127.0.0.1:$candidate/health" -TimeoutSec 3
        if ($resp.StatusCode -ge 200 -and $resp.StatusCode -lt 500) {
          return $candidate
        }
      } catch {
      }
    }
    Start-Sleep -Milliseconds 600
  }
  return $null
}

function Wait-NgrokUrl([int]$TimeoutSec = 30) {
  $deadline = (Get-Date).AddSeconds($TimeoutSec)
  while ((Get-Date) -lt $deadline) {
    try {
      $resp = Invoke-RestMethod -Uri "http://127.0.0.1:4040/api/tunnels" -TimeoutSec 3
      $tunnel = $resp.tunnels | Where-Object { $_.public_url -like 'https://*' } | Select-Object -First 1
      if ($tunnel.public_url) {
        return $tunnel.public_url
      }
    } catch {
      Start-Sleep -Milliseconds 600
    }
  }
  return $null
}

if (Test-PortBusy 4040) {
  Write-Host "ngrok API port 4040 is already busy. Closing old ngrok is recommended." -ForegroundColor Yellow
}

$selectedPort = $Port
if (Test-PortBusy $selectedPort) {
  if (-not (Test-PortBusy 8012)) {
    $selectedPort = 8012
  } else {
    throw "Port $Port is busy and fallback port 8012 is also busy. Please free one port or pass -Port."
  }
}

$webProc = Start-Process -FilePath "powershell" -ArgumentList @(
  "-NoProfile",
  "-ExecutionPolicy", "Bypass",
  "-File", $runWebScript,
  "-VenvPath", $VenvPath,
  "-Port", $selectedPort
) -WorkingDirectory $root -PassThru

$resolvedPort = Find-HealthyLocalPort -Ports @($selectedPort, 8012, 8000) -TimeoutSec 35
if (-not $resolvedPort) {
  try { Stop-Process -Id $webProc.Id -Force -ErrorAction SilentlyContinue } catch {}
  throw "Local web did not become healthy on ports $($(@($selectedPort, 8012, 8000) | Select-Object -Unique) -join ', ')"
}

$ngrokProc = Start-Process -FilePath $ngrokCmd -ArgumentList @("http", "$resolvedPort") -WorkingDirectory $root -PassThru
$publicUrl = Wait-NgrokUrl -TimeoutSec 30
if (-not $publicUrl) {
  try { Stop-Process -Id $ngrokProc.Id -Force -ErrorAction SilentlyContinue } catch {}
  throw "ngrok did not expose a public URL."
}

Write-Host ""
Write-Host "Local web is running:" -ForegroundColor Green
Write-Host "  http://127.0.0.1:$resolvedPort"
Write-Host "Public tunnel is running:" -ForegroundColor Green
Write-Host "  $publicUrl"
Write-Host ""
Write-Host "Important:" -ForegroundColor Cyan
Write-Host "- Keep this PowerShell window open while using the tunnel."
Write-Host "- Open Chrome/login from the same machine that runs this script."
Write-Host "- Start/Replay from the same tunnel URL so jobs stay on your local machine."
Write-Host ""
Write-Host "Background PIDs:" -ForegroundColor Cyan
Write-Host "- web:   $($webProc.Id)"
Write-Host "- ngrok: $($ngrokProc.Id)"
Write-Host ""
Write-Host "To stop later:" -ForegroundColor Yellow
Write-Host "  Stop-Process -Id $($webProc.Id),$($ngrokProc.Id)"

try {
  Start-Process $publicUrl | Out-Null
} catch {
}
