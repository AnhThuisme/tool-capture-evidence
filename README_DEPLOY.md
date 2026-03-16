# Deploy Evidence Tool Web (FastAPI) - End To End

## 1) Chạy local trên Windows (khuyên dùng để kiểm tra trước)

```bash
.\setup_windows.ps1
.\run_web.ps1
```

Mở: `http://127.0.0.1:8000`

Nếu muốn set biến môi trường qua file:

1. Copy `.env.example` thành `.env`
2. Cập nhật `GOOGLE_CREDENTIALS_PATH`
3. Chạy lại `./run_web.ps1`

## 2) Biến môi trường khuyến nghị

- `GOOGLE_CREDENTIALS_PATH`: đường dẫn đến `credentials.json`
- `EVIDENCE_BASE_DIR`: thư mục data/log nếu muốn override
- `PORT`: cổng chạy web (mặc định 8000)
- `HOST`: host bind (mặc định `0.0.0.0`)
- `EVIDENCE_WDM_DIR`: thư mục cache webdriver

## 3) Deploy bằng Docker (khuyến nghị cho Linux server)

```bash
docker compose up --build -d
```

Mở: `http://127.0.0.1:8000`

Xem log:

```bash
docker compose logs -f evidence-web
```

Lưu ý:
- Mặc định request start job để `auto_launch_chrome=false` cho môi trường server/headless.
- `credentials.json` nên mount vào container và set `GOOGLE_CREDENTIALS_PATH`.

Ví dụ:

```bash
docker run -d --name evidence-web -p 8000:8000 \
  -e GOOGLE_CREDENTIALS_PATH=/app/credentials.json \
  -v $(pwd)/credentials.json:/app/credentials.json \
  evidence-web
```

## 4) Deploy thực tế không Docker

Ứng dụng dùng Selenium + Chrome, nên server phải có Chromium/Chrome tương thích.

### Option A: VPS/Windows Server

1. Cài Python 3.10+
2. Cài Google Chrome
3. Upload source + `credentials.json`
4. `pip install -r requirements.txt`
5. Chạy: `uvicorn web_ui:app --host 0.0.0.0 --port 8000`
6. Đặt reverse proxy (Nginx/Caddy/IIS) + domain

### Option B: PaaS hỗ trợ container

Dùng `Procfile` để chạy web process. Runtime phải có Chrome để Selenium hoạt động ổn định.

## 5) Deploy VPS Ubuntu (systemd + nginx)

Đã có sẵn:
- `deploy/evidence-web.service`
- `deploy/evidence-web.nginx`
- `deploy/install_ubuntu.sh`

Trên VPS Ubuntu:

```bash
cd /path/to/source
sudo bash deploy/install_ubuntu.sh your-domain.com
```

Sau đó bật SSL:

```bash
sudo apt-get install -y certbot python3-certbot-nginx
sudo certbot --nginx -d your-domain.com
```

## 6) API chính

- `GET /health`
- `GET /api/default-config`
- `POST /api/chrome/launch`
- `POST /api/jobs/start`
- `POST /api/jobs/{job_id}/stop`
- `GET /api/jobs/{job_id}`
- `GET /api/jobs/{job_id}/logs`

## 7) Lưu ý vận hành

- Hiện tại giới hạn 1 job chạy cùng lúc để tránh xung đột profile/browser.
- Log runtime vẫn ghi vào `log.txt` theo core `evidence.py`.
- Nếu đổi `credentials_input` trong web UI, server sẽ cập nhật `evidence.JSON_PATH` theo input đó.
