$ErrorActionPreference = "Stop"

$env:OTP_SENDER_PORT = if ($env:OTP_SENDER_PORT) { $env:OTP_SENDER_PORT } else { "8031" }

Write-Host "Starting OTP sender API on port $env:OTP_SENDER_PORT"
python -m uvicorn otp_sender_api:app --host 0.0.0.0 --port $env:OTP_SENDER_PORT
