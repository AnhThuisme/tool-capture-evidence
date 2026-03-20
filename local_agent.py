from __future__ import annotations

import json
import os
from datetime import datetime
from typing import Any

from fastapi import FastAPI, HTTPException, Request, Response
from fastapi.responses import FileResponse

import evidence
import web_ui


def _allowed_origins() -> list[str]:
    raw = str(os.getenv("LOCAL_AGENT_ALLOWED_ORIGINS", "*") or "*").strip()
    if not raw:
        return ["*"]
    items = [item.strip() for item in raw.split(",") if item.strip()]
    return items or ["*"]


ALLOWED_ORIGINS = _allowed_origins()
ALLOW_ALL_ORIGINS = "*" in ALLOWED_ORIGINS
AGENT_PORT = max(1, int(os.getenv("LOCAL_AGENT_PORT", "8765") or 8765))

app = FastAPI(title="Tool Evidence Local Agent", version="1.0.0")


def _apply_cors(request: Request, response: Response) -> Response:
    origin = str(request.headers.get("origin", "") or "").strip()
    if ALLOW_ALL_ORIGINS:
        response.headers["Access-Control-Allow-Origin"] = origin or "*"
        response.headers["Vary"] = "Origin"
    elif origin and origin in ALLOWED_ORIGINS:
        response.headers["Access-Control-Allow-Origin"] = origin
        response.headers["Vary"] = "Origin"
    response.headers["Access-Control-Allow-Headers"] = "Content-Type, X-Tool-Evidence-User"
    response.headers["Access-Control-Allow-Methods"] = "GET, POST, DELETE, OPTIONS"
    response.headers["Access-Control-Allow-Private-Network"] = "true"
    return response


@app.middleware("http")
async def local_agent_cors(request: Request, call_next):
    if request.method == "OPTIONS":
        return _apply_cors(request, Response(status_code=204))
    response = await call_next(request)
    return _apply_cors(request, response)


def _require_local_user(request: Request) -> str:
    raw = (
        request.headers.get("X-Tool-Evidence-User")
        or request.query_params.get("user_email")
        or ""
    )
    email = web_ui._normalize_email(raw)
    if not web_ui._is_valid_email(email):
        raise HTTPException(status_code=401, detail="Thiếu user_email hợp lệ cho local agent")
    return email


def _list_owned_jobs(owner_email: str) -> list[dict[str, Any]]:
    out = []
    with web_ui.JOBS_LOCK:
        for job in web_ui.JOBS.values():
            if web_ui._job_owner_email(job) != owner_email:
                continue
            out.append(
                {
                    "id": job["id"],
                    "mode": web_ui._get_job_mode(job),
                    "status": job["status"],
                    "created_at": job["created_at"],
                    "started_at": job["started_at"],
                    "finished_at": job["finished_at"],
                    "summary": job.get("summary"),
                    "detail": job.get("detail"),
                    "request": job.get("request"),
                    "completion": job.get("completion"),
                    "error_rows": job.get("error_rows"),
                    "error": job.get("error"),
                    "recent_logs": list(job.get("logs", []))[-20:],
                }
            )
    out.sort(key=lambda x: x["created_at"], reverse=True)
    return out


@app.get("/health")
def health():
    return {
        "ok": True,
        "service": "tool-evidence-local-agent",
        "base_dir": evidence.BASE_DIR,
        "settings_path": evidence.SETTINGS_PATH,
        "allowed_origins": ALLOWED_ORIGINS,
    }


@app.get("/api/settings")
def get_settings(request: Request):
    user_email = _require_local_user(request)
    return web_ui._build_settings_payload(web_ui._read_saved_settings(user_email))


@app.post("/api/settings")
def save_settings(request: Request, payload: web_ui.SettingsUpdateRequest):
    user_email = _require_local_user(request)
    credentials_path = str(payload.credentials_path or "").strip()
    inline_json = str(payload.service_account_json or "").strip()
    if inline_json:
        try:
            parsed = json.loads(inline_json)
        except Exception as exc:
            raise HTTPException(status_code=400, detail=f"Service account JSON không hợp lệ: {exc}") from exc
        out_path = web_ui._user_service_account_path(user_email)
        with open(out_path, "w", encoding="utf-8") as handle:
            json.dump(parsed, handle, ensure_ascii=False, indent=2)
        credentials_path = out_path

    patch = {
        "credentials_path": credentials_path,
        "sheet_url": str(payload.sheet_url or "").strip(),
        "sheet_name": str(payload.sheet_name or "").strip(),
        "drive_id": str(payload.drive_id or "").strip(),
        "viewport_width": max(320, int(payload.viewport_width or 1920)),
        "viewport_height": max(320, int(payload.viewport_height or 1400)),
        "page_timeout_ms": max(500, int(payload.page_timeout_ms or 3000)),
        "ready_state": str(payload.ready_state or "interactive").strip() or "interactive",
        "full_page_capture": bool(payload.full_page_capture),
    }
    data = web_ui._build_settings_payload(web_ui._write_saved_settings(user_email, patch))
    return {"ok": True, "settings": data}


@app.get("/api/sheets/names")
def list_sheet_names(request: Request, sheet_url: str, credentials_path: str = ""):
    user_email = _require_local_user(request)
    saved = web_ui._read_saved_settings(user_email)
    cred_path = str(credentials_path or "").strip() or str(saved.get("credentials_path", "")).strip()
    spreadsheet = web_ui._open_spreadsheet(sheet_url, cred_path)
    titles = []
    for ws in spreadsheet.worksheets():
        title = str(getattr(ws, "title", "")).strip()
        if title:
            titles.append(title)
    return {
        "ok": True,
        "sheet_url": evidence.normalize_sheet_input(sheet_url),
        "titles": titles,
    }


@app.get("/api/activity")
def list_activity(request: Request, limit: int = 50):
    owner_email = _require_local_user(request)
    return {"ok": True, "items": web_ui._list_activity_events(owner_email, limit=limit)}


@app.post("/api/activity")
def save_activity(request: Request, payload: web_ui.ActivityEventRequest):
    owner_email = _require_local_user(request)
    event = web_ui._append_activity_event(
        owner_email,
        kind=payload.kind,
        message=payload.message,
        level=payload.level,
        run_mode=payload.run_mode,
        block_name=payload.block_name,
        browser_port=payload.browser_port,
        job_id=payload.job_id,
        row=payload.row,
    )
    return {"ok": True, "item": event}


@app.post("/api/chrome/launch-block/{block_index}")
def launch_chrome_block(block_index: int, request: Request, run_mode: str = "seeding", browser_port: int | None = None):
    owner_email = _require_local_user(request)
    run_mode = web_ui._normalize_run_mode(run_mode)
    with web_ui.JOBS_LOCK:
        running_id = web_ui._any_running_job_for_mode(run_mode, owner_email=owner_email)
    if running_id:
        raise HTTPException(status_code=409, detail=f"Mode {run_mode} đang có job chạy: {running_id}. Không thể mở lại Chrome block lúc này.")
    idx = int(block_index)
    base_port = web_ui._get_mode_base_port(run_mode)
    port = int(browser_port or evidence.get_post_port(idx, base_port))
    profile = web_ui._get_mode_profile(run_mode, idx)
    ok, info = evidence.launch_chrome_for_login(browser_port=port, profile_path=profile)
    if not ok:
        raise HTTPException(status_code=500, detail=info)
    block_name = f"Post {idx + 1}"
    event = web_ui._append_activity_event(
        owner_email,
        kind="login",
        message=f"{(run_mode or 'seeding').title()} · {block_name} · đã mở Chrome {port}",
        level="info",
        run_mode=run_mode,
        block_name=block_name,
        browser_port=port,
    )
    return {"ok": True, "message": info, "port": port, "profile_path": profile, "activity": event}


@app.post("/api/jobs/start")
def start_job(request: Request, payload: web_ui.JobStartRequest):
    owner_email = _require_local_user(request)
    run_mode = web_ui._normalize_run_mode(payload.run_mode)
    saved_settings = web_ui._read_saved_settings(owner_email)
    with web_ui.JOBS_LOCK:
        running_id = web_ui._any_running_job_for_mode(run_mode, owner_email=owner_email)
        if running_id:
            raise HTTPException(status_code=409, detail=f"Mode {run_mode} đang có job chạy: {running_id}")

    credentials_input = str(payload.credentials_input or "").strip() or str(saved_settings.get("credentials_path", "")).strip()
    credentials_path = web_ui._resolve_credentials_input(credentials_input, owner_email)

    sheet_url = evidence.normalize_sheet_input(payload.sheet_url)
    drive_id = evidence.normalize_drive_folder_input(payload.drive_id)
    merged_settings = web_ui._build_settings_payload(saved_settings)
    runtime_settings = {
        "credentials_path": credentials_path,
        "viewport_width": int(merged_settings.get("viewport_width", 1920) or 1920),
        "viewport_height": int(merged_settings.get("viewport_height", 1400) or 1400),
        "page_timeout_ms": int(merged_settings.get("page_timeout_ms", 3000) or 3000),
        "ready_state": str(merged_settings.get("ready_state", "interactive") or "interactive"),
        "full_page_capture": bool(merged_settings.get("full_page_capture", False)),
    }
    web_ui._write_saved_settings(
        owner_email,
        {
            "credentials_path": credentials_path,
            "sheet_url": sheet_url,
            "sheet_name": payload.sheet_name.strip(),
            "drive_id": drive_id,
        },
    )

    mapping_payload = [m.model_dump() for m in payload.mappings] or [web_ui._default_mapping(payload.start_line, payload.run_mode)]
    run_mode = web_ui._infer_job_mode(mapping_payload, fallback=run_mode)
    browser_port = web_ui._get_mode_base_port(run_mode)
    profile_path = web_ui._get_mode_profile(run_mode, 0)

    if payload.auto_launch_chrome and run_mode != "scan":
        for idx, mapping in enumerate(mapping_payload):
            block_mode = web_ui._normalize_run_mode(str((mapping or {}).get("mode", run_mode)))
            if block_mode == "scan":
                continue
            block_port = evidence.get_post_port(idx, web_ui._get_mode_base_port(block_mode))
            block_profile = web_ui._get_mode_profile(block_mode, idx)
            ok, info = evidence.launch_chrome_for_login(browser_port=block_port, profile_path=block_profile)
            if not ok:
                web_ui._append_activity_event(
                    owner_email,
                    kind="chrome",
                    message=f"{(mapping or {}).get('name') or f'Post {idx + 1}'} · mở Chrome thất bại: {info}",
                    level="warning",
                    run_mode=block_mode,
                    block_name=str((mapping or {}).get("name") or f"Post {idx + 1}"),
                    browser_port=block_port,
                )

    request_snapshot = {
        "mode": run_mode,
        "browser_port": browser_port,
        "profile_path": profile_path,
        "sheet_url": sheet_url,
        "sheet_name": payload.sheet_name.strip(),
        "drive_id": drive_id,
        "runtime_settings": runtime_settings,
        "force_run_all": bool(payload.force_run_all),
        "only_run_error_rows": bool(payload.only_run_error_rows),
        "capture_five_per_link": bool(payload.capture_five_per_link),
        "target_rows": [],
        "target_block_name": "",
        "mappings": mapping_payload,
    }
    return web_ui._enqueue_job(
        owner_email=owner_email,
        request_snapshot=request_snapshot,
        run_mode=run_mode,
        start_line=int(payload.start_line),
        force_run_all=bool(payload.force_run_all),
        only_run_error_rows=bool(payload.only_run_error_rows),
        capture_five_per_link=bool(payload.capture_five_per_link),
        detail="Chờ chạy",
    )


@app.post("/api/jobs/{job_id}/replay-row")
def replay_job_row(job_id: str, request: Request, payload: web_ui.ReplayRowRequest):
    owner_email = _require_local_user(request)
    row = int(payload.row)
    if row < 1:
        raise HTTPException(status_code=400, detail="Row không hợp lệ")

    with web_ui.JOBS_LOCK:
        source_job = web_ui.JOBS.get(job_id)
        if not source_job or web_ui._job_owner_email(source_job) != owner_email:
            raise HTTPException(status_code=404, detail="Không tìm thấy job nguồn")
        run_mode = web_ui._get_job_mode(source_job)
        running_id = web_ui._any_running_job_for_mode(run_mode, owner_email=owner_email)
        if running_id:
            raise HTTPException(status_code=409, detail=f"Mode {run_mode} đang có job chạy: {running_id}")
        source_request = json.loads(json.dumps(source_job.get("request") or {}))

    mappings = list(source_request.get("mappings") or [])
    block_name = str(payload.block_name or "").strip()
    if block_name:
        matched = [m for m in mappings if str((m or {}).get("name", "")).strip() == block_name]
        if matched:
            mappings = matched
    if not mappings:
        raise HTTPException(status_code=400, detail="Không tìm thấy mapping để replay dòng này")

    replay_start_line = row
    for item in mappings:
        try:
            item["start_line"] = min(int(str(item.get("start_line", row)).strip() or row), row)
        except Exception:
            item["start_line"] = row
        replay_start_line = min(replay_start_line, int(item.get("start_line", row) or row))

    source_request["mappings"] = mappings
    source_request["mode"] = run_mode
    source_request["start_line"] = int(replay_start_line)
    source_request["target_rows"] = [row]
    source_request["target_block_name"] = block_name
    source_request["owner_email"] = owner_email

    detail = f"Replay dòng {row}"
    if block_name:
        detail += f" · {block_name}"

    return web_ui._enqueue_job(
        owner_email=owner_email,
        request_snapshot=source_request,
        run_mode=run_mode,
        start_line=int(replay_start_line),
        force_run_all=True,
        only_run_error_rows=False,
        capture_five_per_link=bool(source_request.get("capture_five_per_link")),
        detail=detail,
    )


@app.post("/api/jobs/{job_id}/pause-toggle")
def pause_toggle_job(job_id: str, request: Request):
    owner_email = _require_local_user(request)
    with web_ui.JOBS_LOCK:
        job = web_ui.JOBS.get(job_id)
        if not job or web_ui._job_owner_email(job) != owner_email:
            raise HTTPException(status_code=404, detail="Không tìm thấy job")
        adapter: web_ui.WebAppAdapter = job.get("adapter")
        if not adapter:
            raise HTTPException(status_code=400, detail="Job này không còn hỗ trợ tạm dừng / tiếp tục")
        current_status = str(job.get("status") or "").strip().lower()
        if current_status not in {"running", "paused"}:
            raise HTTPException(status_code=400, detail="Chỉ có thể tạm dừng / tiếp tục job đang chạy")
        adapter.is_paused = not bool(getattr(adapter, "is_paused", False))
        if adapter.is_paused:
            job["status"] = "paused"
            job["detail"] = job.get("detail") or "Đã tạm dừng"
            job["ui_status"] = "TẠM DỪNG"
            job["ui_color"] = "#f59e0b"
        else:
            job["status"] = "running"
            job["ui_status"] = "ĐANG CHẠY"
            job["ui_color"] = "#1877F2"
        status = job["status"]
    web_ui._persist_jobs(force=True)
    return {"ok": True, "job_id": job_id, "status": status}


@app.post("/api/jobs/{job_id}/stop")
def stop_job(job_id: str, request: Request):
    owner_email = _require_local_user(request)
    with web_ui.JOBS_LOCK:
        job = web_ui.JOBS.get(job_id)
        if not job or web_ui._job_owner_email(job) != owner_email:
            raise HTTPException(status_code=404, detail="Không tìm thấy job")
        adapter: web_ui.WebAppAdapter = job["adapter"]
        if adapter:
            adapter.is_running = False
        job["status"] = "stopped"
        job["finished_at"] = web_ui._utc_now_iso()
    web_ui._persist_jobs(force=True)
    return {"ok": True, "job_id": job_id, "status": "stopped"}


@app.delete("/api/jobs/{job_id}")
def delete_job(job_id: str, request: Request):
    owner_email = _require_local_user(request)
    with web_ui.JOBS_LOCK:
        job = web_ui.JOBS.get(job_id)
        if not job or web_ui._job_owner_email(job) != owner_email:
            raise HTTPException(status_code=404, detail="Không tìm thấy job")
        if str(job.get("status") or "").strip().lower() in {"running", "paused"}:
            raise HTTPException(status_code=409, detail="Không thể xóa job đang chạy hoặc đang tạm dừng")
        web_ui.JOBS.pop(job_id, None)
    web_ui._persist_jobs(force=True)
    return {"ok": True, "job_id": job_id}


@app.get("/api/jobs/{job_id}/export-log")
def export_job_log(job_id: str, request: Request):
    owner_email = _require_local_user(request)
    with web_ui.JOBS_LOCK:
        job = web_ui.JOBS.get(job_id)
        if not job or web_ui._job_owner_email(job) != owner_email:
            raise HTTPException(status_code=404, detail="Không tìm thấy job")
        job_snapshot = web_ui._serialize_job(job)
    rows = web_ui._build_export_log_rows(job_snapshot)
    if not rows:
        raise HTTPException(status_code=400, detail="Chưa có log để xuất")
    export_dir = os.path.join(evidence.TEMP_DIR, "web_exports")
    os.makedirs(export_dir, exist_ok=True)
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    mode = web_ui._safe_filename_part(web_ui._get_job_mode(job_snapshot))
    sheet = web_ui._safe_filename_part((job_snapshot.get("request") or {}).get("sheet_name", ""))
    job_short = web_ui._safe_filename_part(str(job_snapshot.get("id", ""))[:8])
    filename = f"evidence_log_{mode}_{sheet or 'sheet'}_{job_short}_{stamp}.xlsx"
    out_path = os.path.join(export_dir, filename)
    headers = ["Time", "Post", "#", "Result", "Message"]
    evidence.write_colored_xlsx_builtin(out_path, headers, rows)
    return FileResponse(
        out_path,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        filename=filename,
    )


@app.get("/api/jobs")
def list_jobs(request: Request):
    owner_email = _require_local_user(request)
    return {"jobs": _list_owned_jobs(owner_email)}


@app.get("/api/jobs/{job_id}")
def get_job(job_id: str, request: Request):
    owner_email = _require_local_user(request)
    with web_ui.JOBS_LOCK:
        job = web_ui.JOBS.get(job_id)
        if not job or web_ui._job_owner_email(job) != owner_email:
            raise HTTPException(status_code=404, detail="Không tìm thấy job")
        return {
            "id": job["id"],
            "mode": web_ui._get_job_mode(job),
            "status": job["status"],
            "created_at": job["created_at"],
            "started_at": job["started_at"],
            "finished_at": job["finished_at"],
            "summary": job.get("summary"),
            "detail": job.get("detail"),
            "request": job.get("request"),
            "ui_status": job.get("ui_status"),
            "completion": job.get("completion"),
            "error_rows": job.get("error_rows"),
            "error": job.get("error"),
        }


@app.get("/api/jobs/{job_id}/logs")
def get_job_logs(job_id: str, request: Request, limit: int = 100):
    owner_email = _require_local_user(request)
    lim = max(1, min(int(limit), 1000))
    with web_ui.JOBS_LOCK:
        job = web_ui.JOBS.get(job_id)
        if not job or web_ui._job_owner_email(job) != owner_email:
            raise HTTPException(status_code=404, detail="Không tìm thấy job")
        logs = list(job.get("logs", []))
    return {"job_id": job_id, "logs": logs[-lim:]}


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("local_agent:app", host="127.0.0.1", port=AGENT_PORT, reload=False)
