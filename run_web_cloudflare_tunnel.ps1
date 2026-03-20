param(
  [string]$VenvPath = ".venv",
  [int]$Port = 8012,
  [string]$CloudflaredPath = "cloudflared.exe",
  [string]$TunnelToken = "",
  [string]$Hostname = "",
  [string]$EnvFile = "cloudflare_tunnel.env"
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

function Import-SimpleEnv([string]$Path) {
  Get-Content $Path | ForEach-Object {
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

function Wait-HttpOk([string]$Url, [int]$TimeoutSec = 35) {
  $deadline = (Get-Date).AddSeconds($TimeoutSec)
  while ((Get-Date) -lt $deadline) {
    try {
      $resp = Invoke-WebRequest -UseBasicParsing -Uri $Url -TimeoutSec 3
      if ($resp.StatusCode -ge 200 -and $resp.StatusCode -lt 500) {
        return $true
      }
    } catch {
    }
    Start-Sleep -Milliseconds 700
  }
  return $false
}

$envPath = Join-Path $root $EnvFile
if (Test-Path $envPath) {
  Import-SimpleEnv -Path $envPath
}

if (-not $TunnelToken) {
  $TunnelToken = $env:CLOUDFLARE_TUNNEL_TOKEN
}
if (-not $Hostname) {
  $Hostname = $env:CLOUDFLARE_PUBLIC_HOSTNAME
}
if (-not $TunnelToken) {
  throw "Missing CLOUDFLARE_TUNNEL_TOKEN. Put it in cloudflare_tunnel.env or pass -TunnelToken."
}

$cloudflaredCmd = (Get-Command $CloudflaredPath -ErrorAction SilentlyContinue | Select-Object -ExpandProperty Source -First 1)
if (-not $cloudflaredCmd) {
  throw "cloudflared not found in PATH. Install it first or pass -CloudflaredPath."
}

if (Test-PortBusy $Port) {
  throw "Local port $Port is busy. Free it or pass -Port."
}

$webProc = Start-Process -FilePath "powershell" -ArgumentList @(
  "-NoProfile",
  "-ExecutionPolicy", "Bypass",
  "-File", $runWebScript,
  "-VenvPath", $VenvPath,
  "-Port", $Port
) -WorkingDirectory $root -PassThru

$localHealth = "http://127.0.0.1:$Port/health"
if (-not (Wait-HttpOk -Url $localHealth -TimeoutSec 40)) {
  try { Stop-Process -Id $webProc.Id -Force -ErrorAction SilentlyContinue } catch {}
  throw "Local web did not become healthy on $localHealth"
}

$cloudflaredProc = Start-Process -FilePath $cloudflaredCmd -ArgumentList @(
  "tunnel",
  "run",
  "--token",
  $TunnelToken
) -WorkingDirectory $root -PassThru

Start-Sleep -Seconds 4
if ($cloudflaredProc.HasExited) {
  throw "cloudflared exited early. Check your tunnel token and Cloudflare dashboard tunnel config."
}

Write-Host ""
Write-Host "Local web is running:" -ForegroundColor Green
Write-Host "  http://127.0.0.1:$Port"
Write-Host "Cloudflare tunnel is running:" -ForegroundColor Green
if ($Hostname) {
  Write-Host "  https://$Hostname"
} else {
  Write-Host "  Tunnel is up. Add CLOUDFLARE_PUBLIC_HOSTNAME to cloudflare_tunnel.env for a clickable URL."
}
Write-Host ""
Write-Host "Important:" -ForegroundColor Cyan
Write-Host "- Keep this PowerShell window open while using the tunnel."
Write-Host "- Open Chrome/login from the same machine that runs this script."
Write-Host "- Start/Replay from the same Cloudflare URL so jobs stay on your local machine."
Write-Host "- In Cloudflare Tunnel public hostname, point your domain to http://localhost:$Port."
Write-Host ""
Write-Host "Background PIDs:" -ForegroundColor Cyan
Write-Host "- web:         $($webProc.Id)"
Write-Host "- cloudflared: $($cloudflaredProc.Id)"
Write-Host ""
Write-Host "To stop later:" -ForegroundColor Yellow
Write-Host "  Stop-Process -Id $($webProc.Id),$($cloudflaredProc.Id)"

if ($Hostname) {
  try {
    Start-Process "https://$Hostname" | Out-Null
  } catch {
  }
}
