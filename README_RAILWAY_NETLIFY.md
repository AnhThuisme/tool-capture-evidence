# Railway + Netlify

## Mục tiêu

- `Railway`: chạy backend Python/FastAPI thật của app này
- `Netlify`: host frontend static và proxy `/api/*` sang Railway

## 1. Deploy backend lên Railway

Repo này đã có sẵn:

- `Dockerfile`
- `railway.toml`
- `requirements.txt`

### Trên Railway

1. Tạo project mới
2. Chọn `Deploy from GitHub repo`
3. Chọn repo này
4. Railway sẽ build bằng `Dockerfile`
5. Vào service settings:
   - `Healthcheck path`: `/health`
6. Tạo `Volume` và mount vào:
   - `/data`

### Biến môi trường khuyến nghị trên Railway

```env
EVIDENCE_BASE_DIR=/data
EVIDENCE_WDM_DIR=/data/.wdm
HOST=0.0.0.0
```

Nếu dùng OTP mail:

```env
WEB_SESSION_SECRET=doi-secret-rat-dai
GMAIL_SMTP_EMAIL=yourgmail@gmail.com
GMAIL_SMTP_APP_PASSWORD=your_app_password
GMAIL_SMTP_FROM_EMAIL=yourgmail@gmail.com
```

Nếu muốn giới hạn mail được login:

```env
WEB_LOGIN_ALLOWED_EMAILS=user1@gmail.com,user2@gmail.com
```

Sau khi deploy xong, lấy domain backend, ví dụ:

```env
https://tool-evidence-production.up.railway.app
```

## 2. Deploy frontend lên Netlify

Repo này đã có:

- `netlify.toml`
- `scripts/build_netlify_static.py`
- `netlify_src/index.html`
- `netlify_src/login.html`

### Trên Netlify

1. Tạo site từ GitHub repo này
2. Vào `Site configuration -> Environment variables`
3. Thêm:

```env
NETLIFY_BACKEND_ORIGIN=https://your-railway-domain.up.railway.app
```

4. Redeploy site

## 3. Kết quả

- `https://your-site.netlify.app/` -> frontend
- `https://your-site.netlify.app/api/*` -> proxy sang Railway

## 4. Lưu ý

- Nếu Railway chưa có domain public đúng, Netlify frontend sẽ không gọi API được.
- Dữ liệu local như `web_job_history.json`, `web_auth_policy.json`, `credentials.inline.json` sẽ được giữ trên Railway volume `/data`.
