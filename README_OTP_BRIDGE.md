# OTP Bridge

## Mục tiêu

- `Railway`: tạo OTP và gọi HTTP sang bridge
- `Máy Windows của bạn`: chạy `otp_sender_api.py` và gửi mail bằng Gmail SMTP

## 1. Chạy bridge local

Điền vào `.env` hoặc `otp_sender.env` trên máy local:

```env
GMAIL_SMTP_EMAIL=yourgmail@gmail.com
GMAIL_SMTP_APP_PASSWORD=your_16_char_app_password
GMAIL_SMTP_FROM_EMAIL=yourgmail@gmail.com
OTP_BRIDGE_TOKEN=mot-token-bi-mat
```

Chạy:

```powershell
.\run_otp_sender.ps1
```

Health:

```text
http://127.0.0.1:8031/health
```

## 2. Mở public URL cho máy local

Bạn cần một URL public trỏ về `http://127.0.0.1:8031`

Ví dụ:

- Cloudflare Tunnel
- ngrok

## 3. Cấu hình Railway

Thêm env trên Railway:

```env
OTP_BRIDGE_URL=https://your-public-bridge-url
OTP_BRIDGE_TOKEN=mot-token-bi-mat
```

Khi có `OTP_BRIDGE_URL`, backend Railway sẽ gửi OTP qua bridge thay vì tự SMTP.
