# Tool Evidence Local Agent

Mục tiêu:
- web vẫn mở từ domain online
- nhưng `Chrome / job / log / settings runtime` chạy local trên máy Windows của bạn

## 1. Chạy agent local

```powershell
.\setup_windows.ps1
.\run_local_agent.ps1
```

Health check:

```text
http://127.0.0.1:8765/health
```

## 2. Nếu cần cấu hình origin/port

Tạo `local_agent.env` từ `local_agent.env.example`

Ví dụ:

```env
LOCAL_AGENT_PORT=8765
LOCAL_AGENT_ALLOWED_ORIGINS=https://evidence-capture.netlify.app,https://tool.example.com
```

Hoặc để `*` nếu bạn chỉ dùng agent trên máy cá nhân.

## 3. Cách web online hoạt động

- auth/admin vẫn gọi server online
- các API runtime sau sẽ tự gọi agent local nếu agent đang bật:
  - `/api/settings`
  - `/api/sheets/names`
  - `/api/activity`
  - `/api/chrome/*`
  - `/api/jobs*`

## 4. Đổi địa chỉ agent nếu không dùng port mặc định

Trong browser console:

```js
toolEvidenceSetLocalAgentOrigin('http://127.0.0.1:8765')
```

Rồi reload trang.
