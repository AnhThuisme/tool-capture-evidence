$ErrorActionPreference = "Stop"

$root = Split-Path -Parent $MyInvocation.MyCommand.Path
$scriptPath = Join-Path $root "tool_evidence_protocol.py"
if (-not (Test-Path $scriptPath)) {
    throw "Kh?ng t?m th?y tool_evidence_protocol.py t?i $scriptPath"
}

$pythonCmd = (Get-Command python -ErrorAction Stop).Source
$pythonDir = Split-Path -Parent $pythonCmd
$pythonwCmd = Join-Path $pythonDir "pythonw.exe"
if (-not (Test-Path $pythonwCmd)) {
    $pythonwCmd = $pythonCmd
}

$base = "HKCU:\Software\Classes\tool-evidence"
New-Item -Path $base -Force | Out-Null
Set-ItemProperty -Path $base -Name "(default)" -Value "URL:Tool Evidence Protocol"
New-ItemProperty -Path $base -Name "URL Protocol" -Value "" -PropertyType String -Force | Out-Null

$iconKey = Join-Path $base "DefaultIcon"
New-Item -Path $iconKey -Force | Out-Null
Set-ItemProperty -Path $iconKey -Name "(default)" -Value ('"{0}",0' -f $pythonwCmd)

$cmdKey = Join-Path $base "shell\open\command"
New-Item -Path $cmdKey -Force | Out-Null
$command = '"{0}" "{1}" "%1"' -f $pythonwCmd, $scriptPath
Set-ItemProperty -Path $cmdKey -Name "(default)" -Value $command

Write-Host "Installed tool-evidence:// protocol"
Write-Host "Command: $command"
Write-Host "Test URL: tool-evidence://launch?mode=seeding&block=0&port=9223"
