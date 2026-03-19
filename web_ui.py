from __future__ import annotations

import base64
import html
import json
import os
import re
import secrets
import smtplib
import ssl
import subprocess
import threading
import time
import uuid
from datetime import datetime
from email.message import EmailMessage
from email.utils import formataddr, parseaddr
from typing import Any, Literal

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse, RedirectResponse
from pydantic import BaseModel, Field
from starlette.middleware.sessions import SessionMiddleware

import requests

import evidence


def _load_dotenv_file(path: str) -> None:
    if not os.path.exists(path):
        return
    try:
        with open(path, "r", encoding="utf-8") as f:
            for raw_line in f:
                line = raw_line.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                key, value = line.split("=", 1)
                key = key.strip()
                if not key or key in os.environ:
                    continue
                value = value.strip().strip('"').strip("'")
                os.environ[key] = value
    except Exception:
        return


_load_dotenv_file(os.path.join(os.path.dirname(__file__), ".env"))

BRAND_MASCOT_PATH = os.path.join(os.path.dirname(__file__), "Fanscom mascot-05.png")


def _utc_now_iso() -> str:
    return datetime.utcnow().replace(microsecond=0).isoformat() + "Z"


class _Flag:
    def __init__(self, value: bool = False):
        self._value = bool(value)

    def get(self) -> bool:
        return self._value


class _LabelProxy:
    def __init__(self, callback):
        self._callback = callback

    def config(self, **kwargs):
        self._callback(**kwargs)


class WebAppAdapter:
    """Adapter object that mimics methods/attrs used by evidence.main_logic."""

    def __init__(
        self,
        *,
        start_line: int,
        force_run_all: bool,
        only_run_error_rows: bool,
        capture_five_per_link: bool,
        job_store: dict[str, Any],
        persist_callback=None,
        log_limit: int = 2000,
    ):
        self.is_running = True
        self.is_paused = False
        self.driver = None
        self.start_line = int(start_line)
        self.force_run_all = _Flag(force_run_all)
        self.only_run_error_rows = _Flag(only_run_error_rows)
        self.capture_five_per_link = _Flag(capture_five_per_link)

        self.progress = {"value": 0}
        self._job_store = job_store
        self._log_limit = int(log_limit)
        self._persist_callback = persist_callback or (lambda force=False: None)

        self.label_detail = _LabelProxy(self._on_detail)
        self.label_status = _LabelProxy(self._on_status)

    def _persist(self, force: bool = False):
        try:
            self._persist_callback(force=force)
        except TypeError:
            self._persist_callback()

    def _on_detail(self, **kwargs):
        text = str(kwargs.get("text", "")).strip()
        if text:
            self._job_store["detail"] = text
            self._persist()

    def _on_status(self, **kwargs):
        text = str(kwargs.get("text", "")).strip()
        fg = str(kwargs.get("fg", "")).strip()
        if text:
            self._job_store["ui_status"] = text
        if fg:
            self._job_store["ui_color"] = fg
        if text or fg:
            self._persist()

    def set_inputs_enabled(self, enabled: bool):
        self._job_store["inputs_enabled"] = bool(enabled)
        self._persist()

    def update_progress_summary(self, done: int, total: int, ok_count: int, fail_count: int, eta_text: str = "---"):
        self._job_store["summary"] = {
            "done": int(done),
            "total": int(total),
            "success": int(ok_count),
            "failed": int(fail_count),
            "eta": str(eta_text or "---"),
        }
        self._persist()

    def add_live_log(self, row: int, state_left: str, state_right: str, message: str, tag: str = ""):
        logs = self._job_store.setdefault("logs", [])
        logs.append(
            {
                "ts": _utc_now_iso(),
                "row": int(row),
                "state": str(state_left),
                "result": str(state_right),
                "message": str(message),
                "tag": str(tag or ""),
            }
        )
        overflow = len(logs) - self._log_limit
        if overflow > 0:
            del logs[:overflow]
        self._persist()

    def update_error_row_live(self, row: int, message: str, is_error: bool):
        details = self._job_store.setdefault("error_rows", {})
        key = str(int(row))
        if is_error:
            details[key] = str(message or "").strip()
        else:
            details.pop(key, None)
        self._persist()

    def refresh_error_history_ui(self):
        return

    def _render_error_history_card(self, details: dict[int, str]):
        compact: dict[str, str] = {}
        for k, v in (details or {}).items():
            try:
                compact[str(int(k))] = str(v)
            except Exception:
                continue
        self._job_store["error_rows"] = compact
        self._persist()

    def show_completion_popup(self, title: str, summary_text: str, severity: str = "info"):
        self._job_store["completion"] = {
            "title": str(title),
            "summary": str(summary_text),
            "severity": str(severity),
        }
        self._persist(force=True)


class MappingBlock(BaseModel):
    name: str = "Post 1"
    start_line: int = 4
    col_url: str = ""
    col_profile: str = ""
    col_content: str = ""
    col_screenshot: str = ""
    col_drive: str = ""
    col_air_date: str = ""
    fixed_air_date: str = ""
    mode: Literal["seeding", "booking", "scan"] = "seeding"


class JobStartRequest(BaseModel):
    drive_id: str = Field(default=evidence.DEFAULT_DRIVE_FOLDER_ID)
    sheet_url: str = Field(default=evidence.DEFAULT_SHEET_URL)
    sheet_name: str = Field(default=evidence.DEFAULT_SHEET_NAME_TARGET)
    run_mode: Literal["seeding", "booking", "scan"] = "seeding"
    browser_port: int = 9223
    start_line: int = 4
    force_run_all: bool = False
    only_run_error_rows: bool = False
    capture_five_per_link: bool = False
    credentials_input: str = ""
    auto_launch_chrome: bool = False
    mappings: list[MappingBlock] = Field(default_factory=list)


class ReplayRowRequest(BaseModel):
    row: int
    block_name: str = ""


class LaunchChromeRequest(BaseModel):
    run_mode: Literal["seeding", "booking", "scan"] = "seeding"
    browser_port: int = 9223
    profile_path: str = ""


class AuthRequestCodeRequest(BaseModel):
    email: str = ""


class AuthVerifyCodeRequest(BaseModel):
    email: str = ""
    code: str = ""


class SettingsUpdateRequest(BaseModel):
    credentials_path: str = ""
    service_account_json: str = ""
    sheet_url: str = ""
    sheet_name: str = ""
    drive_id: str = ""
    viewport_width: int = 1920
    viewport_height: int = 1400
    page_timeout_ms: int = 3000
    ready_state: str = "interactive"
    full_page_capture: bool = False


class AccessPolicyUpdateRequest(BaseModel):
    allowed_emails: str = ""
    admin_emails: str = ""
    managed_emails: list[str] = Field(default_factory=list)
    email_types: dict[str, str] = Field(default_factory=dict)


class MailConfigUpdateRequest(BaseModel):
    sender_email: str = ""
    from_email: str = ""
    app_password: str = ""


app = FastAPI(title="Tool Evidence", version="1.0.0")
app.add_middleware(
    SessionMiddleware,
    secret_key=os.getenv("WEB_SESSION_SECRET", secrets.token_urlsafe(32)),
    same_site="lax",
    https_only=False,
    max_age=int(os.getenv("WEB_SESSION_MAX_AGE_SEC", "43200") or 43200),
)

JOBS_LOCK = threading.Lock()
JOBS: dict[str, dict[str, Any]] = {}
JOB_HISTORY_PATH = os.path.join(evidence.BASE_DIR, "web_job_history.json")
AUTH_POLICY_PATH = os.path.join(evidence.BASE_DIR, "web_auth_policy.json")
MAIL_CONFIG_PATH = os.path.join(evidence.BASE_DIR, "web_mail_config.json")
JOB_PERSIST_MIN_INTERVAL_SEC = 0.5
_LAST_JOB_PERSIST_TS = 0.0
RUN_MODES = ("seeding", "booking", "scan")
OTP_STORE_LOCK = threading.Lock()
OTP_STORE: dict[str, dict[str, Any]] = {}
OTP_TTL_SEC = max(60, int(os.getenv("WEB_AUTH_OTP_TTL_SEC", "600") or 600))
OTP_RESEND_COOLDOWN_SEC = max(10, int(os.getenv("WEB_AUTH_RESEND_COOLDOWN_SEC", "45") or 45))
OTP_MAX_ATTEMPTS = max(1, int(os.getenv("WEB_AUTH_MAX_ATTEMPTS", "6") or 6))
PUBLIC_PATHS = {
    "/login",
    "/health",
    "/api/auth/request-code",
    "/api/auth/verify-code",
    "/api/auth/logout",
}
MODE_BROWSER_PORTS = {
    "seeding": 9223,
    "booking": 9423,
    "scan": 9623,
}

print(
    f"[startup-config] base_dir={evidence.BASE_DIR} settings={evidence.SETTINGS_PATH} "
    f"port_env={os.getenv('PORT', '') or 'unset'}"
)


SETTINGS_USER_KEYS = {
    "credentials_path",
    "sheet_url",
    "sheet_name",
    "drive_id",
    "viewport_width",
    "viewport_height",
    "page_timeout_ms",
    "ready_state",
    "full_page_capture",
}


def _read_saved_settings_root() -> dict[str, Any]:
    if not os.path.exists(evidence.SETTINGS_PATH):
        return {}
    try:
        with open(evidence.SETTINGS_PATH, "r", encoding="utf-8") as f:
            data = json.load(f) or {}
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def _filter_settings_payload(data: Any) -> dict[str, Any]:
    if not isinstance(data, dict):
        return {}
    return {key: data.get(key) for key in SETTINGS_USER_KEYS if key in data}


def _normalize_email(value: str) -> str:
    return str(value or "").strip().lower()


def _is_valid_email(value: str) -> bool:
    return bool(re.fullmatch(r"[^@\s]+@[^@\s]+\.[^@\s]+", _normalize_email(value)))


def _clean_header_email(value: str, label: str = "Email") -> str:
    raw = str(value or "").replace("\r", " ").replace("\n", " ").strip()
    _display, addr = parseaddr(raw)
    email = _normalize_email(addr or raw)
    if not _is_valid_email(email):
        raise HTTPException(status_code=500, detail=f"{label} header không hợp lệ: {raw or '(trống)'}")
    return email


def _settings_user_slug(email: str) -> str:
    normalized = _normalize_email(email)
    if not normalized:
        return "default"
    slug = re.sub(r"[^a-z0-9._-]+", "_", normalized)
    return slug.strip("._-") or "default"


def _user_service_account_path(email: str) -> str:
    cred_dir = os.path.join(evidence.APP_DIR, "service_accounts")
    os.makedirs(cred_dir, exist_ok=True)
    return os.path.join(cred_dir, f"{_settings_user_slug(email)}.json")


def _parse_email_list(value: Any) -> list[str]:
    if isinstance(value, (list, tuple, set)):
        parts = value
    else:
        parts = re.split(r"[\n,;]+", str(value or ""))
    out: list[str] = []
    seen: set[str] = set()
    for part in parts:
        email = _normalize_email(part)
        if not email or email in seen or not _is_valid_email(email):
            continue
        seen.add(email)
        out.append(email)
    return out


def _system_admin_emails() -> list[str]:
    return _parse_email_list(os.getenv("WEB_SYSTEM_ADMIN_EMAILS", "thu.phannguyenanh@fanscom.vn"))


def _internal_email_domains() -> set[str]:
    configured = _parse_email_list(os.getenv("WEB_INTERNAL_EMAIL_DOMAINS", ""))
    domains = {item.split("@", 1)[1].lower() for item in configured if "@" in item}
    if domains:
        return domains
    inferred: set[str] = set()
    for email in _system_admin_emails():
        if "@" in email:
            inferred.add(email.split("@", 1)[1].lower())
    return inferred


def _normalize_email_type(value: str, email: str = "") -> str:
    raw = str(value or "").strip().lower()
    if raw in {"internal", "noi-bo", "noi_bo", "nội-bộ", "nội bộ"}:
        return "internal"
    if raw in {"external", "ben-ngoai", "ben_ngoai", "bên-ngoài", "bên ngoài"}:
        return "external"
    normalized_email = _normalize_email(email)
    domain = normalized_email.split("@", 1)[1].lower() if "@" in normalized_email else ""
    return "internal" if domain and domain in _internal_email_domains() else "external"


def _normalize_email_types_map(value: Any, managed: list[str]) -> dict[str, str]:
    raw = value if isinstance(value, dict) else {}
    managed_list = _parse_email_list(managed)
    out: dict[str, str] = {}
    for email in managed_list:
        out[email] = _normalize_email_type(raw.get(email, ""), email)
    return out


def _normalize_auth_policy_payload(data: dict[str, Any] | None = None) -> dict[str, Any]:
    raw = data or {}
    allowed = _parse_email_list(raw.get("allowed_emails"))
    admins = _parse_email_list(raw.get("admin_emails"))
    managed = _parse_email_list(raw.get("managed_emails"))
    if allowed:
        for email in admins:
            if email not in allowed:
                allowed.append(email)
    for email in [*admins, *allowed]:
        if email not in managed:
            managed.append(email)
    return {
        "allowed_emails": allowed,
        "admin_emails": admins,
        "managed_emails": managed,
        "email_types": _normalize_email_types_map(raw.get("email_types"), managed),
        "updated_at": raw.get("updated_at"),
    }


def _auth_policy_defaults() -> dict[str, Any]:
    system_admins = _system_admin_emails()
    return _normalize_auth_policy_payload(
        {
            "allowed_emails": [*_parse_email_list(os.getenv("WEB_LOGIN_ALLOWED_EMAILS", "")), *system_admins],
            "admin_emails": [*_parse_email_list(os.getenv("WEB_ADMIN_EMAILS", "")), *system_admins],
            "managed_emails": system_admins,
        }
    )


def _read_auth_policy() -> dict[str, Any]:
    defaults = _auth_policy_defaults()
    if not os.path.exists(AUTH_POLICY_PATH):
        return defaults
    try:
        with open(AUTH_POLICY_PATH, "r", encoding="utf-8") as f:
            raw = json.load(f) or {}
    except Exception:
        return defaults
    if not isinstance(raw, dict):
        return defaults
    return _normalize_auth_policy_payload(
        {
            "allowed_emails": [*(defaults.get("allowed_emails", []) or []), *(raw.get("allowed_emails") or [])],
            "admin_emails": [*(defaults.get("admin_emails", []) or []), *(raw.get("admin_emails") or [])],
            "managed_emails": [*(defaults.get("managed_emails", []) or []), *(raw.get("managed_emails") or [])],
            "email_types": {**(defaults.get("email_types") or {}), **((raw.get("email_types") or {}) if isinstance(raw.get("email_types"), dict) else {})},
            "updated_at": raw.get("updated_at"),
        }
    )


def _write_auth_policy(patch: dict[str, Any]) -> dict[str, Any]:
    current = _read_auth_policy()
    payload = _normalize_auth_policy_payload({**current, **(patch or {})})
    payload["updated_at"] = _utc_now_iso()
    with open(AUTH_POLICY_PATH, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
    return payload


def _ensure_bootstrap_admin(email: str) -> dict[str, Any]:
    normalized = _normalize_email(email)
    policy = _read_auth_policy()
    if policy.get("admin_emails"):
        return policy
    return _write_auth_policy({"allowed_emails": policy.get("allowed_emails", []), "admin_emails": [normalized]})


def _allowed_login_emails() -> set[str]:
    policy = _read_auth_policy()
    allowed = set(policy.get("allowed_emails") or [])
    allowed.update(policy.get("admin_emails") or [])
    allowed.update(policy.get("managed_emails") or [])
    return {email for email in allowed if email}


def _assert_email_allowed(email: str) -> str:
    normalized = _normalize_email(email)
    if not _is_valid_email(normalized):
        raise HTTPException(status_code=400, detail="Email không hợp lệ")
    allowed = _allowed_login_emails()
    if normalized not in allowed:
        raise HTTPException(status_code=403, detail="Email này chưa được cấp quyền đăng nhập")
    return normalized


def _effective_access_emails(policy: dict[str, Any] | None) -> set[str]:
    data = policy or {}
    emails: set[str] = set()
    emails.update(_parse_email_list(data.get("allowed_emails")))
    emails.update(_parse_email_list(data.get("admin_emails")))
    emails.update(_parse_email_list(data.get("managed_emails")))
    return {item for item in emails if item}


def _notify_access_policy_changes(previous: dict[str, Any], current: dict[str, Any]) -> dict[str, Any]:
    old_access = _effective_access_emails(previous)
    new_access = _effective_access_emails(current)
    old_admins = set(_parse_email_list((previous or {}).get("admin_emails")))
    new_admins = set(_parse_email_list((current or {}).get("admin_emails")))
    new_access_only = sorted(new_access - old_access)
    promoted_admins = sorted(new_admins - old_admins)
    targets = sorted(set(new_access_only) | set(promoted_admins))
    result = {
        "sent": [],
        "failed": [],
        "promoted_admins": promoted_admins,
        "new_access": new_access_only,
    }
    for email in targets:
        try:
            subject, plain_body, html_body = _build_access_granted_email(email, email in new_admins)
            _send_platform_email(email, subject, plain_body, html_body, "Tool Evidence")
            result["sent"].append(email)
        except Exception as exc:
            detail = getattr(exc, "detail", None) if isinstance(exc, HTTPException) else str(exc)
            result["failed"].append({"email": email, "detail": str(detail or "Gửi mail thông báo thất bại")})
    return result
    return normalized


def _cleanup_otp_store() -> None:
    now = time.time()
    with OTP_STORE_LOCK:
        expired = [email for email, item in OTP_STORE.items() if float(item.get("expires_at", 0) or 0) <= now]
        for email in expired:
            OTP_STORE.pop(email, None)


def _mail_config_defaults() -> dict[str, Any]:
    gmail_email = _normalize_email(str(os.getenv("GMAIL_SMTP_EMAIL", "") or os.getenv("GMAIL_EMAIL", "")))
    gmail_password = str(os.getenv("GMAIL_SMTP_APP_PASSWORD", "") or os.getenv("GMAIL_APP_PASSWORD", "")).strip().replace(" ", "")
    gmail_from = _normalize_email(str(os.getenv("GMAIL_SMTP_FROM_EMAIL", "")).strip() or gmail_email)
    return {
        "sender_email": gmail_email,
        "from_email": gmail_from or gmail_email,
        "app_password": gmail_password,
        "updated_at": None,
        "source": "env",
    }


def _normalize_mail_config_payload(data: dict[str, Any] | None = None) -> dict[str, Any]:
    raw = data or {}
    sender_email = _normalize_email(raw.get("sender_email"))
    from_email = _normalize_email(raw.get("from_email")) or sender_email
    app_password = str(raw.get("app_password", "") or "").strip().replace(" ", "")
    return {
        "sender_email": sender_email,
        "from_email": from_email,
        "app_password": app_password,
        "updated_at": raw.get("updated_at"),
        "source": raw.get("source") or "file",
    }


def _read_mail_config(secret: bool = False) -> dict[str, Any]:
    defaults = _mail_config_defaults()
    current = defaults
    if os.path.exists(MAIL_CONFIG_PATH):
        try:
            with open(MAIL_CONFIG_PATH, "r", encoding="utf-8") as f:
                raw = json.load(f) or {}
            if isinstance(raw, dict):
                merged = {
                    "sender_email": raw.get("sender_email", defaults.get("sender_email", "")),
                    "from_email": raw.get("from_email", defaults.get("from_email", "")),
                    "app_password": raw.get("app_password", defaults.get("app_password", "")),
                    "updated_at": raw.get("updated_at"),
                    "source": "file",
                }
                current = _normalize_mail_config_payload(merged)
        except Exception:
            current = defaults
    result = dict(current)
    result["has_password"] = bool(result.get("app_password"))
    if not secret:
        result.pop("app_password", None)
    return result


def _write_mail_config(patch: dict[str, Any]) -> dict[str, Any]:
    current = _read_mail_config(secret=True)
    sender_email = _normalize_email(patch.get("sender_email", current.get("sender_email", "")))
    from_email = _normalize_email(patch.get("from_email", current.get("from_email", ""))) or sender_email
    incoming_password = str(patch.get("app_password", "") or "").strip().replace(" ", "")
    password = incoming_password or str(current.get("app_password", "") or "").strip().replace(" ", "")
    if sender_email and not _is_valid_email(sender_email):
        raise HTTPException(status_code=400, detail="Email gửi OTP không hợp lệ")
    if from_email and not _is_valid_email(from_email):
        raise HTTPException(status_code=400, detail="Email From không hợp lệ")
    if sender_email and current.get("sender_email") and sender_email != current.get("sender_email") and not incoming_password:
        raise HTTPException(status_code=400, detail="Đổi Gmail gửi OTP thì cần nhập app password mới")
    if sender_email and not password:
        raise HTTPException(status_code=400, detail="Thiếu app password cho Gmail gửi OTP")
    payload = _normalize_mail_config_payload(
        {
            "sender_email": sender_email,
            "from_email": from_email,
            "app_password": password,
            "updated_at": _utc_now_iso(),
            "source": "file",
        }
    )
    with open(MAIL_CONFIG_PATH, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
    return _read_mail_config(secret=False)


def _smtp_config() -> dict[str, Any]:
    mail_config = _read_mail_config(secret=True)
    gmail_email = str(mail_config.get("sender_email", "") or "").strip()
    gmail_password = str(mail_config.get("app_password", "") or "").strip().replace(" ", "")
    gmail_from = str(mail_config.get("from_email", "") or "").strip() or gmail_email
    if gmail_email and gmail_password:
        return {
            "host": "smtp.gmail.com",
            "port": 587,
            "username": gmail_email,
            "password": gmail_password,
            "from_email": gmail_from,
            "use_ssl": False,
            "use_tls": True,
        }
    host = str(os.getenv("SMTP_HOST", "")).strip()
    username = str(os.getenv("SMTP_USERNAME", "")).strip()
    password = str(os.getenv("SMTP_PASSWORD", "")).strip()
    from_email = str(os.getenv("SMTP_FROM_EMAIL", "")).strip() or username
    port = int(os.getenv("SMTP_PORT", "587") or 587)
    use_ssl = str(os.getenv("SMTP_USE_SSL", "")).strip().lower() in {"1", "true", "yes", "on"}
    use_tls = str(os.getenv("SMTP_USE_TLS", "1")).strip().lower() in {"1", "true", "yes", "on"}
    if not host or not from_email:
        raise HTTPException(status_code=500, detail="Chưa cấu hình Gmail SMTP. Hãy thêm GMAIL_SMTP_EMAIL và GMAIL_SMTP_APP_PASSWORD")
    return {
        "host": host,
        "port": port,
        "username": username,
        "password": password,
        "from_email": from_email,
        "use_ssl": use_ssl,
        "use_tls": use_tls,
    }


def _otp_bridge_config() -> dict[str, Any]:
    url = str(os.getenv("OTP_BRIDGE_URL", "")).strip().rstrip("/")
    token = str(os.getenv("OTP_BRIDGE_TOKEN", "")).strip()
    timeout_sec = max(5, int(os.getenv("OTP_BRIDGE_TIMEOUT_SEC", "20") or 20))
    if not url:
        return {}
    return {"url": url, "token": token, "timeout_sec": timeout_sec}


def _outlook_auth_enabled() -> bool:
    return str(os.getenv("WEB_AUTH_USE_OUTLOOK", "0")).strip().lower() in {"1", "true", "yes", "on"}


def _gmail_api_config() -> dict[str, Any]:
    client_id = str(os.getenv("GMAIL_API_CLIENT_ID", "") or os.getenv("GOOGLE_CLIENT_ID", "")).strip()
    client_secret = str(os.getenv("GMAIL_API_CLIENT_SECRET", "") or os.getenv("GOOGLE_CLIENT_SECRET", "")).strip()
    refresh_token = str(os.getenv("GMAIL_API_REFRESH_TOKEN", "") or os.getenv("GOOGLE_REFRESH_TOKEN", "")).strip()
    from_email = (
        str(os.getenv("GMAIL_API_FROM_EMAIL", "")).strip()
        or str(os.getenv("GMAIL_SMTP_FROM_EMAIL", "")).strip()
        or str(os.getenv("GMAIL_SMTP_EMAIL", "")).strip()
    )
    timeout_sec = max(5, int(os.getenv("GMAIL_API_TIMEOUT_SEC", "20") or 20))
    if not client_id or not client_secret or not refresh_token or not from_email:
        return {}
    return {
        "client_id": client_id,
        "client_secret": client_secret,
        "refresh_token": refresh_token,
        "from_email": from_email,
        "timeout_sec": timeout_sec,
    }


def _ps_quote(value: str) -> str:
    return str(value or "").replace("'", "''")


def _build_login_code_email(email: str, code: str) -> tuple[str, str, str]:
    recipient = html.escape(_normalize_email(email))
    safe_code = html.escape(str(code or "").strip())
    ttl_minutes = max(1, OTP_TTL_SEC // 60)
    subject = "Evidence | Mã OTP đăng nhập"
    plain = "\n".join(
        [
            "Mã xác nhận đăng nhập Evidence",
            "",
            f"Email nhận mã: {email}",
            f"Mã của bạn: {code}",
            f"Mã có hiệu lực trong {ttl_minutes} phút.",
            "",
            "Nếu bạn không yêu cầu đăng nhập, hãy bỏ qua email này.",
        ]
    )
    html_body = f"""<!doctype html>
<html lang="vi">
  <body style="margin:0;padding:0;background:#f4f7fb;font-family:Segoe UI,Arial,sans-serif;color:#0f172a;">
    <table role="presentation" cellspacing="0" cellpadding="0" border="0" width="100%" style="background:#f4f7fb;padding:28px 12px;">
      <tr>
        <td align="center">
          <table role="presentation" cellspacing="0" cellpadding="0" border="0" width="100%" style="max-width:640px;background:#ffffff;border:1px solid #dbe4f0;border-radius:24px;overflow:hidden;">
            <tr>
              <td style="padding:24px 28px;background:linear-gradient(135deg,#0f172a 0%,#1f3355 100%);">
                <table role="presentation" cellspacing="0" cellpadding="0" border="0" width="100%">
                  <tr>
                    <td align="left">
                      <div style="display:inline-block;padding:10px 14px;border-radius:999px;background:rgba(255,255,255,0.12);color:#dbeafe;font-size:12px;letter-spacing:1.8px;text-transform:uppercase;">
                        Evidence OTP
                      </div>
                      <div style="margin-top:18px;font-size:30px;line-height:1.2;font-weight:700;color:#ffffff;">
                        Xác nhận đăng nhập
                      </div>
                      <div style="margin-top:8px;font-size:15px;line-height:1.7;color:#cbd5e1;max-width:440px;">
                        Hệ thống vừa nhận yêu cầu đăng nhập vào dashboard Evidence. Dùng mã bên dưới để hoàn tất xác thực.
                      </div>
                    </td>
                  </tr>
                </table>
              </td>
            </tr>
            <tr>
              <td style="padding:28px;">
                <table role="presentation" cellspacing="0" cellpadding="0" border="0" width="100%">
                  <tr>
                    <td style="padding:18px 20px;border:1px solid #dbe4f0;border-radius:18px;background:#f8fbff;">
                      <div style="font-size:12px;letter-spacing:1.8px;text-transform:uppercase;color:#64748b;">Email nhan ma</div>
                      <div style="margin-top:8px;font-size:18px;font-weight:600;color:#0f172a;">{recipient}</div>
                    </td>
                  </tr>
                  <tr><td style="height:18px;"></td></tr>
                  <tr>
                    <td align="center" style="padding:24px 20px;border-radius:22px;background:linear-gradient(135deg,#eff6ff 0%,#eef2ff 100%);border:1px solid #c7d2fe;">
                      <div style="font-size:12px;letter-spacing:2px;text-transform:uppercase;color:#6366f1;">Ma xac nhan</div>
                      <div style="margin-top:14px;font-size:42px;line-height:1;font-weight:800;letter-spacing:10px;color:#111827;">{safe_code}</div>
                      <div style="margin-top:14px;font-size:14px;color:#475569;">
                        Mã có hiệu lực trong <strong>{ttl_minutes} phút</strong>
                      </div>
                    </td>
                  </tr>
                  <tr><td style="height:18px;"></td></tr>
                  <tr>
                    <td style="padding:18px 20px;border:1px solid #e2e8f0;border-radius:18px;background:#ffffff;">
                      <div style="font-size:13px;line-height:1.8;color:#334155;">
                        Nếu bạn không thực hiện yêu cầu này, hãy bỏ qua email. Không chia sẻ mã này với người khác.
                      </div>
                    </td>
                  </tr>
                </table>
              </td>
            </tr>
          </table>
        </td>
      </tr>
    </table>
  </body>
</html>"""
    return subject, plain, html_body


def _build_access_granted_email(email: str, is_admin: bool = False) -> tuple[str, str, str]:
    recipient = html.escape(_normalize_email(email))
    role_line = "quyền quản trị" if is_admin else "quyền truy cập"
    subject = "Tool Evidence | Quyền truy cập đã được cấp"
    plain_lines = [
        "Thông báo cấp quyền Tool Evidence",
        "",
        f"Email: {email}",
        f"Trạng thái mới: {'Admin' if is_admin else 'User'}",
        f"Bạn đã được cấp {role_line} vào Tool Evidence.",
        "",
        "Bạn có thể vào màn hình đăng nhập để nhận OTP và truy cập hệ thống.",
    ]
    plain = "\n".join(plain_lines)
    title = "Bạn đã được cấp quyền admin" if is_admin else "Bạn đã được cấp quyền truy cập"
    subtitle = (
        "Bạn có thể quản lý người dùng và cài đặt trong hệ thống."
        if is_admin
        else "Bạn có thể đăng nhập và sử dụng các chức năng đã được cấp."
    )
    badge = "ADMIN ACCESS" if is_admin else "USER ACCESS"
    html_body = f"""<!doctype html>
<html lang="vi">
  <body style="margin:0;padding:0;background:#f4f7fb;font-family:Segoe UI,Arial,sans-serif;color:#0f172a;">
    <table role="presentation" cellspacing="0" cellpadding="0" border="0" width="100%" style="background:#f4f7fb;padding:28px 12px;">
      <tr>
        <td align="center">
          <table role="presentation" cellspacing="0" cellpadding="0" border="0" width="100%" style="max-width:640px;background:#ffffff;border:1px solid #dbe4f0;border-radius:24px;overflow:hidden;">
            <tr>
              <td style="padding:24px 28px;background:linear-gradient(135deg,#0f172a 0%,#1f3355 100%);">
                <div style="display:inline-block;padding:10px 14px;border-radius:999px;background:rgba(255,255,255,0.12);color:#dbeafe;font-size:12px;letter-spacing:1.8px;text-transform:uppercase;">
                  {badge}
                </div>
                <div style="margin-top:18px;font-size:30px;line-height:1.2;font-weight:700;color:#ffffff;">
                  {title}
                </div>
                <div style="margin-top:8px;font-size:15px;line-height:1.7;color:#cbd5e1;max-width:460px;">
                  {subtitle}
                </div>
              </td>
            </tr>
            <tr>
              <td style="padding:28px;">
                <div style="padding:18px 20px;border:1px solid #dbe4f0;border-radius:18px;background:#f8fbff;">
                  <div style="font-size:12px;letter-spacing:1.8px;text-transform:uppercase;color:#64748b;">Email được cấp</div>
                  <div style="margin-top:8px;font-size:18px;font-weight:600;color:#0f172a;">{recipient}</div>
                </div>
                <div style="margin-top:18px;padding:18px 20px;border:1px solid #e2e8f0;border-radius:18px;background:#ffffff;">
                  <div style="font-size:14px;line-height:1.8;color:#334155;">
                    Quyền hiện tại: <strong>{'Admin' if is_admin else 'User'}</strong><br/>
                    Bạn có thể truy cập Tool Evidence bằng mã OTP được gửi tới email này.
                  </div>
                </div>
              </td>
            </tr>
          </table>
        </td>
      </tr>
    </table>
  </body>
</html>"""
    return subject, plain, html_body


def _send_email_via_gmail_api(to_email: str, subject: str, plain_body: str, html_body: str, from_name: str = "Evidence Security") -> None:
    config = _gmail_api_config()
    if not config:
        raise HTTPException(status_code=500, detail="Chưa cấu hình Gmail API")
    safe_to = _clean_header_email(to_email, "To")
    safe_from = _clean_header_email(str(config["from_email"]), "From")
    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"] = formataddr((from_name, safe_from))
    msg["To"] = safe_to
    msg.set_content(plain_body)
    msg.add_alternative(html_body, subtype="html")
    raw = base64.urlsafe_b64encode(msg.as_bytes()).decode("utf-8")
    access_token = _gmail_api_access_token(config)
    try:
        resp = requests.post(
            "https://gmail.googleapis.com/gmail/v1/users/me/messages/send",
            headers={
                "Authorization": f"Bearer {access_token}",
                "Content-Type": "application/json",
            },
            json={"raw": raw},
            timeout=float(config.get("timeout_sec") or 20),
        )
    except requests.RequestException as exc:
        raise HTTPException(status_code=500, detail=f"Gmail API unreachable: {exc}") from exc
    try:
        data = resp.json()
    except Exception:
        data = {}
    if not (200 <= resp.status_code < 300):
        detail = data.get("error", {}).get("message") or data.get("message") or resp.text or f"HTTP {resp.status_code}"
        raise HTTPException(status_code=500, detail=f"Gmail API gửi thất bại: {detail}")


def _send_email_via_smtp(to_email: str, subject: str, plain_body: str, html_body: str, from_name: str = "Evidence Security") -> None:
    config = _smtp_config()
    safe_to = _clean_header_email(to_email, "To")
    safe_from = _clean_header_email(str(config["from_email"]), "From")
    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"] = formataddr((from_name, safe_from))
    msg["To"] = safe_to
    msg.set_content(plain_body)
    msg.add_alternative(html_body, subtype="html")
    context = ssl.create_default_context()
    if config["use_ssl"]:
        with smtplib.SMTP_SSL(config["host"], config["port"], timeout=20, context=context) as server:
            if config["username"]:
                server.login(config["username"], config["password"])
            server.send_message(msg)
    else:
        with smtplib.SMTP(config["host"], config["port"], timeout=20) as server:
            server.ehlo()
            if config["use_tls"]:
                server.starttls(context=context)
                server.ehlo()
            if config["username"]:
                server.login(config["username"], config["password"])
            server.send_message(msg)


def _send_email_via_outlook(to_email: str, subject: str, plain_body: str) -> None:
    if not _outlook_auth_enabled():
        raise HTTPException(status_code=500, detail="Chưa cấu hình Outlook để gửi mail")
    safe_to = _clean_header_email(to_email, "To")
    script = f"""
$ErrorActionPreference = 'Stop'
$outlook = New-Object -ComObject Outlook.Application
$mail = $outlook.CreateItem(0)
$mail.To = '{_ps_quote(safe_to)}'
$mail.Subject = '{_ps_quote(subject)}'
$mail.Body = '{_ps_quote(plain_body)}'
$mail.Send()
Write-Output 'OK'
"""
    try:
        result = subprocess.run(
            ["powershell", "-NoProfile", "-NonInteractive", "-Command", script],
            capture_output=True,
            text=True,
            timeout=45,
            check=False,
        )
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Không mở được Outlook để gửi mail: {exc}") from exc
    stdout = str(result.stdout or "").strip()
    stderr = str(result.stderr or "").strip()
    if result.returncode != 0 or "OK" not in stdout:
        detail = stderr or stdout or "Outlook không gửi được mail"
        raise HTTPException(status_code=500, detail=f"Không gửi được mail qua Outlook: {detail}")


def _send_platform_email(to_email: str, subject: str, plain_body: str, html_body: str, from_name: str = "Evidence Security") -> None:
    gmail_api = _gmail_api_config()
    if gmail_api:
        _send_email_via_gmail_api(to_email, subject, plain_body, html_body, from_name)
        return
    try:
        _send_email_via_smtp(to_email, subject, plain_body, html_body, from_name)
        return
    except Exception as smtp_exc:
        if not _outlook_auth_enabled():
            detail = getattr(smtp_exc, "detail", None) if isinstance(smtp_exc, HTTPException) else str(smtp_exc)
            raise HTTPException(status_code=500, detail=str(detail or "Gửi mail thất bại")) from smtp_exc
        try:
            _send_email_via_outlook(to_email, subject, plain_body)
            return
        except HTTPException as outlook_exc:
            smtp_detail = getattr(smtp_exc, "detail", None) if isinstance(smtp_exc, HTTPException) else str(smtp_exc)
            outlook_detail = outlook_exc.detail if isinstance(outlook_exc, HTTPException) else str(outlook_exc)
            raise HTTPException(
                status_code=500,
                detail=f"Gửi mail thất bại. SMTP: {smtp_detail}. Outlook: {outlook_detail}",
            ) from smtp_exc


def _gmail_api_access_token(config: dict[str, Any]) -> str:
    try:
        resp = requests.post(
            "https://oauth2.googleapis.com/token",
            data={
                "client_id": config["client_id"],
                "client_secret": config["client_secret"],
                "refresh_token": config["refresh_token"],
                "grant_type": "refresh_token",
            },
            timeout=float(config.get("timeout_sec") or 20),
        )
    except requests.RequestException as exc:
        raise HTTPException(status_code=500, detail=f"Kh?ng l?y ???c access token Gmail API: {exc}") from exc
    try:
        data = resp.json()
    except Exception:
        data = {}
    if not (200 <= resp.status_code < 300):
        detail = data.get("error_description") or data.get("error") or resp.text or f"HTTP {resp.status_code}"
        raise HTTPException(status_code=500, detail=f"Google OAuth th?t b?i: {detail}")
    token = str(data.get("access_token") or "").strip()
    if not token:
        raise HTTPException(status_code=500, detail="Google OAuth kh?ng tr? access token")
    return token


def _send_login_code_via_gmail_api(email: str, code: str) -> None:
    subject, plain_body, html_body = _build_login_code_email(email, code)
    _send_email_via_gmail_api(email, subject, plain_body, html_body, "Evidence Security")


def _send_login_code_via_outlook(email: str, code: str) -> None:
    if not _outlook_auth_enabled():
        raise HTTPException(status_code=500, detail="Chưa cấu hình SMTP để gửi mã xác nhận")
    subject, body, _html_body = _build_login_code_email(email, code)
    script = f"""
$ErrorActionPreference = 'Stop'
$outlook = New-Object -ComObject Outlook.Application
$mail = $outlook.CreateItem(0)
$mail.To = '{_ps_quote(email)}'
$mail.Subject = '{_ps_quote(subject)}'
$mail.Body = '{_ps_quote(body)}'
$mail.Send()
Write-Output 'OK'
"""
    try:
        result = subprocess.run(
            ["powershell", "-NoProfile", "-NonInteractive", "-Command", script],
            capture_output=True,
            text=True,
            timeout=45,
            check=False,
        )
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Không mở được Outlook để gửi mã: {exc}") from exc
    stdout = str(result.stdout or "").strip()
    stderr = str(result.stderr or "").strip()
    if result.returncode != 0 or "OK" not in stdout:
        detail = stderr or stdout or "Outlook không gửi được mail xác nhận"
        raise HTTPException(status_code=500, detail=f"Không gửi được mã qua Outlook: {detail}")


def _send_login_code_via_bridge(email: str, code: str) -> None:
    bridge = _otp_bridge_config()
    if not bridge:
        raise HTTPException(status_code=500, detail="Chưa cấu hình OTP bridge")
    subject, plain_body, html_body = _build_login_code_email(email, code)
    payload = {
        "token": bridge.get("token", ""),
        "to_email": email,
        "subject": subject,
        "text_body": plain_body,
        "html_body": html_body,
    }
    try:
        resp = requests.post(
            f'{bridge["url"]}/send-otp',
            json=payload,
            timeout=float(bridge["timeout_sec"]),
        )
    except requests.RequestException as exc:
        raise HTTPException(status_code=500, detail=f"OTP bridge unreachable: {exc}") from exc
    if 200 <= resp.status_code < 300:
        return
    try:
        data = resp.json()
    except Exception:
        data = {}
    detail = data.get("detail") or data.get("message") or resp.text or f"HTTP {resp.status_code}"
    raise HTTPException(status_code=500, detail=f"OTP bridge gửi thất bại: {detail}")


def _send_login_code(email: str, code: str) -> None:
    subject, plain_body, html_body = _build_login_code_email(email, code)
    try:
        _send_platform_email(email, subject, plain_body, html_body, "Evidence Security")
    except HTTPException as exc:
        raise HTTPException(status_code=500, detail=str(exc.detail or "Gửi OTP thất bại")) from exc


def _issue_login_code(email: str) -> None:
    normalized = _assert_email_allowed(email)
    _cleanup_otp_store()
    now = time.time()
    code = ""
    with OTP_STORE_LOCK:
        current = OTP_STORE.get(normalized) or {}
        resend_after = float(current.get("resend_after", 0) or 0)
        if resend_after > now:
            wait_sec = int(resend_after - now) + 1
            raise HTTPException(status_code=429, detail=f"Vui lòng chờ {wait_sec}s rồi gửi lại mã")
        code = f"{secrets.randbelow(900000) + 100000:06d}"
        OTP_STORE[normalized] = {
            "code": code,
            "expires_at": now + OTP_TTL_SEC,
            "resend_after": now + OTP_RESEND_COOLDOWN_SEC,
            "attempts_left": OTP_MAX_ATTEMPTS,
        }
    try:
        _send_login_code(normalized, code)
    except Exception:
        with OTP_STORE_LOCK:
            OTP_STORE.pop(normalized, None)
        raise


def _verify_login_code(email: str, code: str) -> str:
    normalized = _assert_email_allowed(email)
    raw_code = re.sub(r"\D", "", str(code or ""))
    if len(raw_code) != 6:
        raise HTTPException(status_code=400, detail="Mã xác nhận phải có 6 số")
    _cleanup_otp_store()
    now = time.time()
    with OTP_STORE_LOCK:
        item = OTP_STORE.get(normalized)
        if not item:
            raise HTTPException(status_code=400, detail="Mã đã hết hạn hoặc chưa được gửi")
        if float(item.get("expires_at", 0) or 0) <= now:
            OTP_STORE.pop(normalized, None)
            raise HTTPException(status_code=400, detail="Mã xác nhận đã hết hạn")
        if raw_code != str(item.get("code", "")):
            attempts_left = max(0, int(item.get("attempts_left", OTP_MAX_ATTEMPTS) or OTP_MAX_ATTEMPTS) - 1)
            if attempts_left <= 0:
                OTP_STORE.pop(normalized, None)
                raise HTTPException(status_code=400, detail="Sai mã quá số lần cho phép, vui lòng gửi lại mã mới")
            item["attempts_left"] = attempts_left
            OTP_STORE[normalized] = item
            raise HTTPException(status_code=400, detail=f"Mã xác nhận không đúng, còn {attempts_left} lần thử")
        OTP_STORE.pop(normalized, None)
    return normalized


def _is_authenticated(request: Request) -> bool:
    return bool((request.session or {}).get("auth_email"))


def _auth_email_from_request(request: Request) -> str:
    try:
        session_data = request.session or {}
    except AssertionError:
        session_data = request.scope.get("session") or {}
    return _normalize_email(session_data.get("auth_email", ""))


def _get_user_role(email: str) -> str:
    normalized = _normalize_email(email)
    if not normalized:
        return ""
    admins = set(_read_auth_policy().get("admin_emails") or [])
    return "admin" if normalized in admins else "user"


def _auth_role_from_request(request: Request) -> str:
    return _get_user_role(_auth_email_from_request(request))


def _is_railway_healthcheck(request: Request) -> bool:
    headers = request.headers or {}
    host = (headers.get("host") or "").lower()
    forwarded_host = (headers.get("x-forwarded-host") or "").lower()
    user_agent = (headers.get("user-agent") or "").lower()
    if "healthcheck.railway.app" in host or "healthcheck.railway.app" in forwarded_host:
        return True
    return "railway-healthcheck" in user_agent


def _require_api_auth(request: Request) -> str:
    email = _auth_email_from_request(request)
    if not email:
        raise HTTPException(status_code=401, detail="Authentication required")
    return email


def _require_admin(request: Request) -> str:
    email = _require_api_auth(request)
    if not (_read_auth_policy().get("admin_emails") or []):
        _ensure_bootstrap_admin(email)
    if _get_user_role(email) != "admin":
        raise HTTPException(status_code=403, detail="Chỉ admin mới được dùng tính năng này")
    return email


def _read_saved_settings(user_email: str | None = None) -> dict[str, Any]:
    root = _read_saved_settings_root()
    if "users" not in root or not isinstance(root.get("users"), dict):
        return _filter_settings_payload(root)
    legacy_defaults = _filter_settings_payload(root.get("_legacy_defaults"))
    if not user_email:
        return legacy_defaults
    bucket = root["users"].get(_normalize_email(user_email), {})
    data = dict(legacy_defaults)
    data.update(_filter_settings_payload(bucket))
    return data


def _write_saved_settings(user_email: str, patch: dict[str, Any]) -> dict[str, Any]:
    normalized_email = _normalize_email(user_email)
    if not normalized_email:
        raise HTTPException(status_code=400, detail="Không xác định được người dùng để lưu cài đặt")
    root = _read_saved_settings_root()
    if "users" in root and isinstance(root.get("users"), dict):
        users = {
            _normalize_email(key): _filter_settings_payload(value)
            for key, value in dict(root.get("users") or {}).items()
            if _normalize_email(key)
        }
        legacy_defaults = _filter_settings_payload(root.get("_legacy_defaults"))
    else:
        users = {}
        legacy_defaults = _filter_settings_payload(root)
    current = dict(legacy_defaults)
    current.update(_filter_settings_payload(users.get(normalized_email)))
    current.update(_filter_settings_payload(patch or {}))
    users[normalized_email] = current
    data: dict[str, Any] = {"users": users}
    if legacy_defaults:
        data["_legacy_defaults"] = legacy_defaults
    with open(evidence.SETTINGS_PATH, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    return current


LOGIN_PAGE_HTML = """<!doctype html>
<html lang="vi">
<head>
<meta charset="utf-8" />
<meta name="viewport" content="width=device-width, initial-scale=1" />
<title>Tool Evidence Login</title>
<style>
:root{--bg:#0e1525;--bg-2:#121b2f;--panel:#121b2b;--soft:#162033;--line:#263247;--text:#dbe6f5;--muted:#91a0b8;--blue:#5b93d3;--blue-dark:#3b6fb0;--green:#34c38f;--red:#ef4444}
*{box-sizing:border-box}
body{margin:0;min-height:100vh;display:grid;place-items:center;background:linear-gradient(180deg,var(--bg),var(--bg-2));font-family:Segoe UI,Arial,sans-serif;color:var(--text)}
.wrap{width:min(460px,calc(100vw - 32px))}
.card{background:rgba(18,27,43,.96);border:1px solid var(--line);border-radius:22px;padding:24px;box-shadow:0 20px 60px rgba(0,0,0,.35)}
.brand{display:flex;align-items:center;gap:12px;margin-bottom:20px}
.dot{position:relative;width:54px;height:54px;border-radius:16px;background:#ffffff url('/assets/brand-mascot') center/88% no-repeat;box-shadow:0 12px 24px rgba(36,72,143,.24);border:1px solid rgba(191,219,254,.34);overflow:hidden;flex:0 0 auto}
.dot::before,.dot::after{display:none}
.brand strong{display:block;font-size:18px}
.brand span{display:block;font-size:12px;color:var(--muted);margin-top:3px}
h1{margin:0 0 10px;font-size:28px;letter-spacing:-.02em}
p{margin:0 0 18px;font-size:14px;color:var(--muted);line-height:1.55}
label{display:block;font-size:12px;color:var(--muted);margin-bottom:6px}
input{width:100%;height:46px;border:1px solid var(--line);border-radius:12px;background:#0b1322;color:var(--text);padding:0 14px;font-size:14px;outline:none}
input:focus{border-color:var(--blue)}
.row{display:grid;gap:14px}
.actions{display:flex;gap:10px;flex-wrap:wrap;margin-top:18px}
button{height:44px;padding:0 16px;border-radius:12px;border:1px solid var(--line);background:var(--soft);color:var(--text);font-size:14px;font-weight:700;cursor:pointer}
button.primary{background:linear-gradient(135deg,var(--blue),var(--blue-dark));border-color:var(--blue);color:#fff}
button:disabled{opacity:.55;cursor:not-allowed}
.step{display:none}
.step.active{display:block}
.note{margin-top:14px;min-height:20px;font-size:13px;color:var(--muted)}
.note.error{color:#fca5a5}
.note.ok{color:#86efac}
.hint{margin-top:8px;font-size:12px;color:var(--muted)}
</style>
</head>
<body>
  <div class="wrap">
    <div class="card">
      <div class="brand">
        <div class="dot"></div>
        <div>
          <strong>Tool Evidence</strong>
          <span>Secure access login</span>
        </div>
      </div>
      <h1>Đăng nhập bằng mail</h1>
      <p>Nhập email của bạn. Hệ thống sẽ gửi mã xác nhận 6 số qua email trước khi vào dashboard.</p>
      <div id="stepEmail" class="step active">
        <div class="row">
          <div>
            <label for="login_email">Email</label>
            <input id="login_email" type="email" placeholder="you@example.com" autocomplete="email" />
            <div class="hint">Chỉ mail đã được thêm trong danh sách người dùng mới có quyền nhập OTP.</div>
          </div>
        </div>
        <div class="actions">
          <button id="requestBtn" class="primary" type="button" onclick="requestCode()">Gửi mã xác nhận</button>
        </div>
      </div>
      <div id="stepVerify" class="step">
        <div class="row">
          <div>
            <label for="verify_email">Email</label>
            <input id="verify_email" type="email" readonly />
          </div>
          <div>
            <label for="verify_code">Mã xác nhận</label>
            <input id="verify_code" type="text" inputmode="numeric" maxlength="6" placeholder="123456" />
          </div>
        </div>
        <div class="actions">
          <button id="verifyBtn" class="primary" type="button" onclick="verifyCode()">Xác nhận và vào web</button>
          <button id="resendBtn" type="button" onclick="requestCode(true)">Gửi lại mã</button>
        </div>
      </div>
      <div id="loginNote" class="note"></div>
    </div>
  </div>
<script>
async function api(url, payload) {
  const res = await fetch(url, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(payload || {}),
  });
  const data = await res.json().catch(() => ({}));
  if (!res.ok) throw new Error(data.detail || ('HTTP ' + res.status));
  return data;
}

function setNote(text, kind = '') {
  const node = document.getElementById('loginNote');
  node.textContent = text || '';
  node.className = 'note' + (kind ? ' ' + kind : '');
}

function showVerifyStep(email) {
  document.getElementById('stepEmail').classList.remove('active');
  document.getElementById('stepVerify').classList.add('active');
  document.getElementById('verify_email').value = email;
  document.getElementById('verify_code').focus();
}

async function requestCode(force = false) {
  const emailInput = force ? document.getElementById('verify_email') : document.getElementById('login_email');
  const email = String(emailInput.value || '').trim();
  if (!email) {
    setNote('Vui lòng nhập email trước', 'error');
    return;
  }
  const button = document.getElementById(force ? 'resendBtn' : 'requestBtn');
  button.disabled = true;
  setNote('Đang gửi mã xác nhận...');
  try {
    const out = await api('/api/auth/request-code', { email });
    showVerifyStep(out.email || email);
    setNote(out.message || 'Đã gửi mã xác nhận vào mail của bạn', 'ok');
  } catch (e) {
    setNote(e.message, 'error');
  } finally {
    button.disabled = false;
  }
}

async function verifyCode() {
  const email = String(document.getElementById('verify_email').value || '').trim();
  const code = String(document.getElementById('verify_code').value || '').trim();
  const button = document.getElementById('verifyBtn');
  button.disabled = true;
  setNote('Đang xác nhận mã...');
  try {
    await api('/api/auth/verify-code', { email, code });
    window.location.href = '/';
  } catch (e) {
    setNote(e.message, 'error');
  } finally {
    button.disabled = false;
  }
}

document.getElementById('login_email').addEventListener('keydown', e => {
  if (e.key === 'Enter') requestCode();
});
document.getElementById('verify_code').addEventListener('keydown', e => {
  if (e.key === 'Enter') verifyCode();
});
</script>
</body>
</html>
"""


def _window_size_parts(value: str) -> tuple[int, int]:
    raw = str(value or "").strip()
    try:
        width_s, height_s = raw.split(",", 1)
        return max(320, int(width_s)), max(320, int(height_s))
    except Exception:
        return 1920, 1400


def _settings_defaults() -> dict[str, Any]:
    width, height = _window_size_parts(getattr(evidence, "CAPTURE_WINDOW_SIZE", "1920,1400"))
    return {
        "credentials_path": "",
        "sheet_url": str(getattr(evidence, "DEFAULT_SHEET_URL", "")),
        "sheet_name": str(getattr(evidence, "DEFAULT_SHEET_NAME_TARGET", "")),
        "drive_id": str(getattr(evidence, "DEFAULT_DRIVE_FOLDER_ID", "")),
        "viewport_width": width,
        "viewport_height": height,
        "page_timeout_ms": int(float(getattr(evidence, "PAGE_READY_TIMEOUT", 3)) * 1000),
        "ready_state": "interactive",
        "full_page_capture": False,
    }


def _apply_runtime_settings(data: dict[str, Any]) -> None:
    width = max(320, int(data.get("viewport_width", 1920) or 1920))
    height = max(320, int(data.get("viewport_height", 1400) or 1400))
    timeout_ms = max(500, int(data.get("page_timeout_ms", 3000) or 3000))
    evidence.CAPTURE_WINDOW_SIZE = f"{width},{height}"
    evidence.PAGE_READY_TIMEOUT = max(1, int(round(timeout_ms / 1000)))
    cred_path = str(data.get("credentials_path", "")).strip()
    evidence.JSON_PATH = cred_path


def _capture_runtime_settings() -> dict[str, Any]:
    width, height = _window_size_parts(getattr(evidence, "CAPTURE_WINDOW_SIZE", "1920,1400"))
    return {
        "credentials_path": str(getattr(evidence, "JSON_PATH", "")),
        "viewport_width": width,
        "viewport_height": height,
        "page_timeout_ms": int(float(getattr(evidence, "PAGE_READY_TIMEOUT", 3)) * 1000),
    }


def _build_settings_payload(data: dict[str, Any] | None = None) -> dict[str, Any]:
    merged = dict(_settings_defaults())
    merged.update(data or {})
    cred_path = str(merged.get("credentials_path", "")).strip()
    merged["credentials_path"] = cred_path
    merged["service_account_email"] = evidence.get_service_account_email(cred_path) if cred_path else ""
    merged["service_account_saved"] = bool(cred_path and os.path.exists(cred_path))
    merged["service_account_fixed"] = bool(
        merged["service_account_saved"] and evidence.is_fixed_credentials_path(cred_path)
    )
    merged["service_account_status"] = (
        "Fixed credentials" if merged["service_account_fixed"]
        else ("Saved" if merged["service_account_saved"] else "Not saved")
    )
    return merged


def _resolve_credentials_input(credentials_input: str, user_email: str | None = None) -> str:
    raw = str(credentials_input or "").strip()
    if not raw:
        return ""

    if raw.startswith("{"):
        try:
            data = json.loads(raw)
        except Exception as exc:
            raise HTTPException(status_code=400, detail=f"credentials_input JSON không hợp lệ: {exc}") from exc
        out_path = _user_service_account_path(user_email or "")
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        return out_path

    path = os.path.normpath(raw)
    if not os.path.exists(path):
        raise HTTPException(status_code=400, detail=f"Không tìm thấy credentials file: {path}")
    return path


def _open_spreadsheet(sheet_url: str, credentials_path: str):
    norm_url = evidence.normalize_sheet_input(sheet_url)
    if not norm_url:
        raise HTTPException(status_code=400, detail="Thiếu Sheet URL")
    cred_path = str(credentials_path or "").strip()
    if not cred_path or not os.path.exists(cred_path):
        raise HTTPException(status_code=400, detail="Chưa có credentials để đọc Google Sheets")
    try:
        creds = evidence.ServiceAccountCredentials.from_json_keyfile_name(
            cred_path,
            [
                "https://spreadsheets.google.com/feeds",
                "https://www.googleapis.com/auth/drive",
            ],
        )
        client = evidence.gspread.authorize(creds)
        return client.open_by_url(norm_url)
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=400, detail=f"Không đọc được Google Sheets: {exc}") from exc


def _any_running_job() -> str | None:
    for jid, data in JOBS.items():
        if data.get("status") in {"running", "paused"}:
            return jid
    return None


def _normalize_run_mode(mode: str | None) -> str:
    raw = str(mode or "seeding").strip().lower()
    return raw if raw in RUN_MODES else "seeding"


def _infer_job_mode(mappings: list[dict[str, Any]] | None = None, fallback: str = "seeding") -> str:
    for item in mappings or []:
        if isinstance(item, dict):
            mode = _normalize_run_mode(item.get("mode"))
            if mode in RUN_MODES:
                return mode
    return _normalize_run_mode(fallback)


def _get_job_mode(job: dict[str, Any]) -> str:
    request = dict(job.get("request") or {})
    if request.get("mode"):
        return _normalize_run_mode(request.get("mode"))
    if job.get("mode"):
        return _normalize_run_mode(job.get("mode"))
    return _infer_job_mode(request.get("mappings"), fallback="seeding")


def _any_running_job_for_mode(run_mode: str | None = None, owner_email: str | None = None) -> str | None:
    target_mode = _normalize_run_mode(run_mode)
    target_owner = _normalize_email(owner_email)
    for jid, data in JOBS.items():
        if data.get("status") in {"running", "paused"} and _get_job_mode(data) == target_mode:
            if target_owner and _job_owner_email(data) != target_owner:
                continue
            return jid
    return None


def _get_mode_base_port(run_mode: str | None) -> int:
    return int(MODE_BROWSER_PORTS.get(_normalize_run_mode(run_mode), MODE_BROWSER_PORTS["seeding"]))


def _get_mode_profile(run_mode: str | None, block_index: int = 0) -> str:
    mode = _normalize_run_mode(run_mode)
    idx = int(block_index or 0)
    if mode == "seeding":
        return evidence.LOCAL_PROFILE_PATH if idx <= 0 else os.path.join(evidence.TEMP_DIR, f"chrome_profile_worker_{idx}")
    suffix = f"{mode}_{idx}" if idx > 0 else f"{mode}_main"
    return os.path.join(evidence.TEMP_DIR, f"chrome_profile_{suffix}")


def _safe_filename_part(value: str) -> str:
    text = str(value or "").strip()
    if not text:
        return "log"
    cleaned = "".join(ch if ch.isalnum() or ch in {"-", "_"} else "_" for ch in text)
    cleaned = cleaned.strip("._")
    return cleaned[:80] or "log"


def _default_job_owner_email() -> str:
    admins = _system_admin_emails()
    if admins:
        return admins[0]
    return "thu.phannguyenanh@fanscom.vn"


def _job_owner_email(job: dict[str, Any] | None) -> str:
    data = job or {}
    request_data = dict(data.get("request") or {})
    return _normalize_email(data.get("owner_email") or request_data.get("owner_email") or "")


def _get_owned_job(job_id: str, owner_email: str) -> dict[str, Any]:
    with JOBS_LOCK:
        job = JOBS.get(job_id)
        if not job or _job_owner_email(job) != _normalize_email(owner_email):
            raise HTTPException(status_code=404, detail="Không tìm thấy job")
        return job


def _extract_log_block_name_py(log: dict[str, Any] | None) -> str:
    text = str((log or {}).get("message", "") or "").strip()
    if not text:
        return ""
    if ":" not in text:
        return ""
    head = text.split(":", 1)[0].strip()
    return head[:80]


def _build_export_log_rows(job: dict[str, Any]) -> list[tuple[list[Any], list[str]]]:
    rows_with_tags: list[tuple[list[Any], list[str]]] = []
    mode = _get_job_mode(job)
    for log in list(job.get("logs") or []):
        post_name = _extract_log_block_name_py(log) or ("Scan" if mode == "scan" else "Post")
        result_text = str(log.get("result", "") or log.get("state", "") or "").strip()
        message = str(log.get("message", "") or "").strip()
        row_vals = [
            str(log.get("ts", "") or ""),
            post_name,
            str(log.get("row", "") or ""),
            result_text,
            message,
        ]
        tags = []
        tag = str(log.get("tag", "") or "").strip().lower()
        if tag:
            tags.append(tag)
        raw = f"{tag} {log.get('state', '')} {log.get('result', '')} {message}".lower()
        if "fail" in raw or "error" in raw:
            tags.append("fail")
        elif "unavailable" in raw or "không khả dụng" in raw or "khong kha dung" in raw:
            tags.append("unavailable")
        elif "ok" in raw or "success" in raw:
            tags.append("ok")
        rows_with_tags.append((row_vals, tags))
    return rows_with_tags


def _serialize_job(job: dict[str, Any]) -> dict[str, Any]:
    return {
        "id": str(job.get("id", "")),
        "owner_email": _job_owner_email(job),
        "mode": _get_job_mode(job),
        "status": str(job.get("status", "queued")),
        "created_at": job.get("created_at"),
        "started_at": job.get("started_at"),
        "finished_at": job.get("finished_at"),
        "request": dict(job.get("request") or {}),
        "summary": dict(job.get("summary") or {}),
        "detail": str(job.get("detail", "") or ""),
        "ui_status": str(job.get("ui_status", "") or ""),
        "ui_color": str(job.get("ui_color", "") or ""),
        "inputs_enabled": bool(job.get("inputs_enabled", True)),
        "logs": list(job.get("logs") or []),
        "error_rows": dict(job.get("error_rows") or {}),
        "completion": dict(job.get("completion") or {}) if job.get("completion") else None,
        "error": job.get("error"),
    }


def _persist_jobs(force: bool = False) -> None:
    global _LAST_JOB_PERSIST_TS
    now = time.time()
    if not force and (now - _LAST_JOB_PERSIST_TS) < JOB_PERSIST_MIN_INTERVAL_SEC:
        return
    with JOBS_LOCK:
        payload = [_serialize_job(job) for job in JOBS.values()]
    payload.sort(key=lambda x: str(x.get("created_at") or ""), reverse=True)
    temp_path = JOB_HISTORY_PATH + ".tmp"
    with open(temp_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
    os.replace(temp_path, JOB_HISTORY_PATH)
    _LAST_JOB_PERSIST_TS = now


def _load_persisted_jobs() -> None:
    if not os.path.exists(JOB_HISTORY_PATH):
        return
    try:
        with open(JOB_HISTORY_PATH, "r", encoding="utf-8") as f:
            raw = json.load(f) or []
    except Exception:
        return
    if not isinstance(raw, list):
        return
    restored: dict[str, dict[str, Any]] = {}
    for item in raw:
        if not isinstance(item, dict):
            continue
        job_id = str(item.get("id", "")).strip()
        if not job_id:
            continue
        status = str(item.get("status", "completed") or "completed").strip().lower()
        detail = str(item.get("detail", "") or "")
        finished_at = item.get("finished_at")
        if status in {"queued", "running", "paused"}:
            status = "stopped"
            detail = detail or "Web server restarted before the job finished."
            finished_at = finished_at or _utc_now_iso()
        restored[job_id] = {
            "id": job_id,
            "owner_email": _normalize_email(item.get("owner_email")) or _default_job_owner_email(),
            "mode": _normalize_run_mode(item.get("mode")),
            "status": status,
            "created_at": item.get("created_at"),
            "started_at": item.get("started_at"),
            "finished_at": finished_at,
            "request": dict(item.get("request") or {}),
            "adapter": None,
            "summary": dict(item.get("summary") or {"done": 0, "total": 0, "success": 0, "failed": 0, "eta": "---"}),
            "detail": detail,
            "ui_status": str(item.get("ui_status", "") or ""),
            "ui_color": str(item.get("ui_color", "") or ""),
            "inputs_enabled": bool(item.get("inputs_enabled", True)),
            "logs": list(item.get("logs") or []),
            "error_rows": dict(item.get("error_rows") or {}),
            "completion": dict(item.get("completion") or {}) if item.get("completion") else None,
            "error": item.get("error"),
        }
    with JOBS_LOCK:
        JOBS.clear()
        JOBS.update(restored)


def _default_mapping(start_line: int, run_mode: str = "seeding") -> dict[str, Any]:
    mode = _normalize_run_mode(run_mode)
    if mode == "scan":
        return {
            "name": "Scan 1",
            "start_line": int(start_line),
            "col_url": "F",
            "col_profile": "",
            "col_content": "E",
            "col_screenshot": "",
            "col_drive": "G",
            "col_air_date": "",
            "fixed_air_date": "",
            "mode": "scan",
        }
    if mode == "booking":
        return {
            "name": "Post 1",
            "start_line": int(start_line),
            "col_url": "K",
            "col_profile": "B",
            "col_content": "I",
            "col_screenshot": "J",
            "col_drive": "L",
            "col_air_date": "",
            "fixed_air_date": "",
            "mode": "booking",
        }
    return {
        "name": "Post 1",
        "start_line": int(start_line),
        "col_url": "K",
        "col_profile": "",
        "col_content": "",
        "col_screenshot": "J",
        "col_drive": "L",
        "col_air_date": "",
        "fixed_air_date": "",
        "mode": "seeding",
    }


_load_persisted_jobs()


def _enqueue_job(
    *,
    owner_email: str,
    request_snapshot: dict[str, Any],
    run_mode: str,
    start_line: int,
    force_run_all: bool,
    only_run_error_rows: bool,
    capture_five_per_link: bool,
    detail: str = "Chờ chạy",
) -> dict[str, Any]:
    job_id = str(uuid.uuid4())
    adapter = WebAppAdapter(
        start_line=int(start_line),
        force_run_all=force_run_all,
        only_run_error_rows=only_run_error_rows,
        capture_five_per_link=capture_five_per_link,
        job_store={},
        persist_callback=_persist_jobs,
    )

    job = {
        "id": job_id,
        "owner_email": _normalize_email(owner_email),
        "mode": _normalize_run_mode(run_mode),
        "status": "queued",
        "created_at": _utc_now_iso(),
        "started_at": None,
        "finished_at": None,
        "request": dict(request_snapshot or {}),
        "adapter": adapter,
        "summary": {"done": 0, "total": 0, "success": 0, "failed": 0, "eta": "---"},
        "detail": str(detail or "Chờ chạy"),
        "ui_status": "READY",
        "ui_color": "",
        "inputs_enabled": True,
        "logs": [],
        "error_rows": {},
        "completion": None,
        "error": None,
    }
    adapter._job_store = job

    t = threading.Thread(target=_run_job, args=(job_id,), daemon=True)
    job["thread"] = t

    with JOBS_LOCK:
        JOBS[job_id] = job
    _persist_jobs(force=True)

    t.start()
    return {"ok": True, "job_id": job_id, "status": "queued"}


def _run_job(job_id: str):
    with JOBS_LOCK:
        job = JOBS.get(job_id)
        if not job:
            return
        req = dict(job["request"])
        app_adapter: WebAppAdapter = job["adapter"]
        job["status"] = "running"
        job["started_at"] = _utc_now_iso()
    _persist_jobs(force=True)

    previous_runtime = _capture_runtime_settings()
    try:
        runtime_settings = dict(req.get("runtime_settings") or {})
        if runtime_settings:
            _apply_runtime_settings(runtime_settings)
        evidence.main_logic(
            app_adapter,
            req["drive_id"],
            req["sheet_url"],
            req["sheet_name"],
            start_line=req["start_line"],
            browser_port=req["browser_port"],
            mappings=req["mappings"],
            primary_profile_path=req.get("profile_path"),
            target_rows=req.get("target_rows"),
            target_block_name=req.get("target_block_name"),
        )
        with JOBS_LOCK:
            job = JOBS.get(job_id)
            if job:
                current_status = str(job.get("status") or "").strip().lower()
                summary = dict(job.get("summary") or {})
                try:
                    done = int(summary.get("done") or 0)
                except Exception:
                    done = 0
                try:
                    total = int(summary.get("total") or 0)
                except Exception:
                    total = 0
                if current_status not in {"stopped", "failed"}:
                    if total <= 0 or done >= total:
                        job["status"] = "completed"
                    else:
                        job["status"] = "stopped"
                        if not str(job.get("detail") or "").strip():
                            job["detail"] = "Tiến trình kết thúc trước khi xử lý hết dữ liệu."
                if summary:
                    summary["eta"] = "---"
                    job["summary"] = summary
                if job.get("status") in {"completed", "stopped", "failed"}:
                    job["finished_at"] = _utc_now_iso()
        _persist_jobs(force=True)
    except Exception as exc:
        with JOBS_LOCK:
            job = JOBS.get(job_id)
            if job:
                job["status"] = "failed"
                job["error"] = str(exc)
                job["finished_at"] = _utc_now_iso()
        _persist_jobs(force=True)
    finally:
        with JOBS_LOCK:
            job = JOBS.get(job_id)
            if job:
                job.pop("thread", None)
        _persist_jobs(force=True)
        _apply_runtime_settings(previous_runtime)


@app.middleware("http")
async def _auth_guard(request: Request, call_next):
    return await call_next(request)


@app.get("/login", response_class=HTMLResponse)
def login_page(request: Request):
    if _auth_email_from_request(request):
        return RedirectResponse(url="/", status_code=302)
    return HTMLResponse(LOGIN_PAGE_HTML)


@app.post("/api/auth/request-code")
def auth_request_code(payload: AuthRequestCodeRequest):
    _issue_login_code(payload.email)
    return {
        "ok": True,
        "email": _normalize_email(payload.email),
        "message": "Đã gửi mã xác nhận. Kiểm tra mail của bạn.",
    }


@app.post("/api/auth/verify-code")
def auth_verify_code(request: Request, payload: AuthVerifyCodeRequest):
    email = _verify_login_code(payload.email, payload.code)
    _ensure_bootstrap_admin(email)
    role = _get_user_role(email)
    request.session["auth_email"] = email
    request.session["auth_role"] = role
    request.session["auth_at"] = _utc_now_iso()
    return {"ok": True, "email": email, "role": role}


@app.get("/api/auth/me")
def auth_me(request: Request):
    email = _require_api_auth(request)
    role = _auth_role_from_request(request)
    request.session["auth_role"] = role
    return {"ok": True, "email": email, "role": role, "is_admin": role == "admin"}


@app.post("/api/auth/logout")
def auth_logout(request: Request):
    request.session.clear()
    return {"ok": True}


@app.get("/assets/brand-mascot")
def brand_mascot():
    if not os.path.exists(BRAND_MASCOT_PATH):
        raise HTTPException(status_code=404, detail="Brand mascot not found")
    return FileResponse(BRAND_MASCOT_PATH, media_type="image/png")


@app.get("/", response_class=HTMLResponse)
def home_page(request: Request):
    if _is_railway_healthcheck(request):
        return HTMLResponse("ok", status_code=200)
    auth_email_raw = _auth_email_from_request(request)
    if not auth_email_raw:
        return RedirectResponse(url="/login", status_code=302)
    _ensure_bootstrap_admin(auth_email_raw)
    auth_role_raw = _get_user_role(auth_email_raw) or "user"
    request.session["auth_role"] = auth_role_raw
    auth_email = html.escape(auth_email_raw, quote=True)
    auth_role = html.escape(auth_role_raw, quote=True)
    auth_role_display = "Admin" if auth_role_raw == "admin" else "User"
    return HTMLResponse(
        """<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8" />
<meta name="viewport" content="width=device-width, initial-scale=1" />
<title>Tool Evidence</title>
<style>
:root{--bg:#f3f4f6;--bg-grad-1:#f3f4f6;--bg-grad-2:#f8fafc;--panel:#ffffff;--panel-soft:#fbfcff;--line:#e4e7ec;--text:#101828;--muted:#667085;--soft:#f8fafc;--blue:#2f80ed;--blue-soft:#e8f1ff;--green:#16a34a;--red:#dc2626;--shadow:0 12px 36px rgba(16,24,40,.06);--input-bg:#ffffff;--input-fg:#102033;--danger-bg:#fff7f7;--danger-line:#fecaca;--danger-text:#be123c;--log-bg:#0b1322}
[data-theme="dark"]{--bg:#0e1525;--bg-grad-1:#0e1525;--bg-grad-2:#121b2f;--panel:#121b2b;--panel-soft:#162033;--line:#263247;--text:#dbe6f5;--muted:#91a0b8;--soft:#182338;--blue:#5b93d3;--blue-soft:#1a2940;--green:#34c38f;--red:#f08aa0;--shadow:0 18px 40px rgba(0,0,0,.28);--input-bg:#0b1322;--input-fg:#dbe6f5;--danger-bg:#2a1920;--danger-line:#5f2e3a;--danger-text:#f1b3c1;--log-bg:#0c1424}
*{box-sizing:border-box}
body{margin:0;min-height:100vh;background:linear-gradient(180deg,var(--bg-grad-1),var(--bg-grad-2));font-family:Segoe UI,Arial,sans-serif;color:var(--text)}
.shell{width:100%;min-height:100vh;padding:10px}
.board{width:100%;min-height:calc(100vh - 20px);background:var(--panel);border:1px solid var(--line);border-radius:18px;box-shadow:var(--shadow);display:grid;grid-template-columns:236px 1fr;overflow:hidden}
.sidebar{background:var(--panel-soft);border-right:1px solid var(--line);padding:20px 16px;display:flex;flex-direction:column;gap:16px}
.dot{position:relative;width:56px;height:56px;border-radius:18px;background:#ffffff url('/assets/brand-mascot') center/88% no-repeat;box-shadow:0 14px 30px rgba(59,130,246,.2);border:1px solid rgba(191,219,254,.34);flex:0 0 auto;overflow:hidden}
.dot::before,.dot::after{display:none}
.brand-row{position:relative;display:flex;align-items:center;gap:14px;min-height:94px;padding:16px 16px;border:1px solid rgba(123,168,255,.14);border-radius:20px;background:linear-gradient(135deg,rgba(76,110,196,.18),rgba(255,255,255,.02) 48%,rgba(37,99,235,.08));overflow:hidden;box-shadow:inset 0 1px 0 rgba(255,255,255,.05)}
.brand-row::after{content:"";position:absolute;right:-28px;top:-24px;width:108px;height:108px;border-radius:50%;background:rgba(96,139,255,.12);filter:blur(6px)}
.brand-copy{position:relative;z-index:1;display:flex;flex-direction:column;gap:4px;min-width:0}
.brand-copy strong{font-size:17px;line-height:1.08;letter-spacing:-.03em;color:#fff}
.brand-copy span{font-size:10px;color:#a9bddc;letter-spacing:.18em;text-transform:uppercase;font-weight:700}
[data-theme="light"] .brand-row{background:linear-gradient(135deg,rgba(91,147,211,.12),rgba(255,255,255,.92) 48%,rgba(239,244,255,.86));border-color:rgba(91,147,211,.18)}
[data-theme="light"] .brand-copy strong{color:#0f172a}
[data-theme="light"] .brand-copy span{color:#51627f}
.side-nav{display:flex;flex-direction:column;gap:8px;margin-top:4px}
.side-group{display:flex;flex-direction:column;gap:8px}
.side-btn{width:100%;min-height:42px;border-radius:14px;border:1px solid transparent;display:flex;align-items:center;gap:10px;color:var(--muted);font-size:13px;background:var(--panel);padding:0 14px;cursor:pointer;text-align:left}
.side-icon{display:inline-grid;place-items:center;width:22px;height:22px;border-radius:8px;background:var(--soft);color:var(--muted)}
.side-icon svg{width:14px;height:14px;stroke:currentColor;fill:none;stroke-width:1.8;stroke-linecap:round;stroke-linejoin:round}
.side-btn.active{border-color:#dbeafe;background:#eef4ff;color:#12315f}
.side-btn.active .side-icon{background:#2f80ed;color:#fff}
[data-theme="dark"] .side-btn.active{border-color:#355072;background:#1a2940;color:#dbe6f5}
[data-theme="dark"] .side-btn.active .side-icon{background:#5b93d3;color:#fff}
.side-subnav{display:none;flex-direction:column;gap:6px;margin:-2px 0 2px 34px}
.side-group.open .side-subnav{display:flex}
.side-subbtn{border:1px solid transparent;border-radius:10px;background:transparent;color:var(--muted);padding:8px 10px;font-size:12px;text-align:left;cursor:pointer}
.side-subbtn:hover{background:var(--soft)}
.side-subbtn.active{background:var(--blue-soft);border-color:#bfdbfe;color:var(--blue);font-weight:600}
[data-theme="dark"] .side-subbtn.active{border-color:#355072;color:#dbe6f5}
.settings-note{font-size:11px;color:#98a2b3;min-height:16px;margin-top:6px}
.admin-access-grid{display:grid;grid-template-columns:1fr 1fr;gap:12px}
.access-textarea{width:100%;min-height:176px;padding:14px 16px;border:1px solid var(--line);border-radius:16px;font-size:13px;line-height:1.6;resize:vertical;font-family:Consolas,monospace;background:linear-gradient(180deg,rgba(255,255,255,.04),rgba(255,255,255,.01)),var(--input-bg);color:var(--input-fg);box-shadow:inset 0 1px 0 rgba(255,255,255,.03)}
.access-textarea::placeholder{color:var(--muted)}
.access-kicker{font-size:11px;font-weight:800;letter-spacing:.22em;text-transform:uppercase;color:#7b8aa5;margin-bottom:8px}
.access-headline .state{display:inline-flex}
.access-layout{display:grid;grid-template-columns:1.45fr .95fr;gap:12px;margin-top:12px}
.access-editor,.access-summary-card{background:linear-gradient(180deg,rgba(255,255,255,.025),rgba(255,255,255,.01)),var(--panel)}
.access-section-head{display:flex;justify-content:space-between;align-items:flex-start;gap:12px;margin-bottom:18px}
.access-section-title{font-size:20px;font-weight:700;letter-spacing:-.02em}
.access-section-sub{font-size:12px;color:var(--muted);line-height:1.6;max-width:560px}
.access-badge{display:inline-flex;align-items:center;min-height:32px;padding:0 12px;border-radius:999px;border:1px solid rgba(91,147,211,.22);background:var(--blue-soft);color:var(--blue);font-size:12px;font-weight:700}
.access-editor-grid .field label{font-size:11px;font-weight:800;letter-spacing:.14em;text-transform:uppercase;color:#8ea0bf;margin-bottom:8px}
.access-editor-grid .settings-note{display:block;min-height:auto;margin-top:10px;font-size:12px;color:var(--muted);line-height:1.5}
.access-actions{display:flex;gap:10px;align-items:center;flex-wrap:wrap;margin-top:16px}
.access-actions .btn{min-height:40px;border-radius:12px;padding:0 14px;font-weight:700}
.access-summary-stack{display:flex;flex-direction:column;gap:12px}
.access-summary-block{padding:14px 16px;border:1px solid var(--line);border-radius:18px;background:linear-gradient(180deg,rgba(255,255,255,.03),rgba(255,255,255,.01)),var(--panel-soft)}
.access-summary-label{font-size:11px;font-weight:800;letter-spacing:.16em;text-transform:uppercase;color:#7b8aa5}
.access-summary-main{font-size:16px;font-weight:700;line-height:1.5;margin-top:10px;word-break:break-word}
.access-summary-main.dim{font-size:14px;font-weight:600;color:var(--muted)}
.access-role-pill{display:inline-flex;align-items:center;min-height:34px;padding:0 14px;border-radius:999px;font-size:12px;font-weight:800;letter-spacing:.08em;text-transform:uppercase;border:1px solid transparent}
.access-role-pill.admin{background:rgba(52,195,143,.14);border-color:rgba(52,195,143,.26);color:var(--green)}
.access-role-pill.user{background:var(--blue-soft);border-color:rgba(91,147,211,.26);color:var(--blue)}
.access-role-pill.otp{background:rgba(245,158,11,.14);border-color:rgba(245,158,11,.28);color:#ffcd73}
.access-chip-list{display:flex;flex-wrap:wrap;gap:8px;margin-top:10px}
.access-chip{display:inline-flex;align-items:center;min-height:30px;padding:0 12px;border-radius:999px;border:1px solid var(--line);background:var(--panel);font-size:12px;font-weight:600;color:var(--text)}
.access-chip.empty{background:transparent;color:var(--muted);border-style:dashed}
.access-directory{margin-top:12px}
.access-directory-head{align-items:center;margin-bottom:14px}
.access-directory-title-wrap{display:flex;align-items:center;gap:10px;flex-wrap:wrap}
.access-mini-pill{display:inline-flex;align-items:center;justify-content:center;min-width:34px;height:28px;padding:0 10px;border-radius:999px;background:rgba(255,255,255,.05);border:1px solid rgba(255,255,255,.08);font-size:12px;font-weight:800;color:var(--text)}
.access-directory-actions{display:flex;align-items:center;gap:10px;flex-wrap:wrap}
.access-mail-card{display:none;margin-top:12px;background:linear-gradient(180deg,rgba(255,255,255,.025),rgba(255,255,255,.01)),var(--panel)}
.access-mail-card.open{display:block}
.access-mail-grid{display:grid;grid-template-columns:repeat(3,minmax(0,1fr));gap:12px}
.access-mail-grid .field label{display:block;font-size:11px;font-weight:800;letter-spacing:.14em;text-transform:uppercase;color:#8ea0bf;margin-bottom:8px}
.access-mail-meta{display:flex;align-items:center;gap:8px;flex-wrap:wrap;margin-top:12px}
.access-mail-pill{display:inline-flex;align-items:center;min-height:30px;padding:0 12px;border-radius:999px;border:1px solid var(--line);background:var(--panel-soft);font-size:12px;font-weight:700;color:var(--text)}
.access-mail-pill.ok{background:rgba(52,195,143,.12);border-color:rgba(52,195,143,.24);color:#7df0ba}
.access-mail-pill.warn{background:rgba(245,158,11,.14);border-color:rgba(245,158,11,.28);color:#ffcd73}
.access-mail-foot{display:flex;align-items:center;justify-content:space-between;gap:12px;flex-wrap:wrap;margin-top:16px}
.access-mail-foot .settings-note{margin:0;min-height:auto;flex:1}
.access-entry-editor{display:none;margin-top:12px;background:linear-gradient(180deg,rgba(255,255,255,.025),rgba(255,255,255,.01)),var(--panel)}
.access-entry-editor.open{display:grid;grid-template-columns:minmax(190px,.7fr) minmax(420px,1.35fr) auto;grid-template-areas:"head head head" "meta form actions";gap:14px;align-items:end}
.access-entry-editor.open>.access-section-head{grid-area:head;margin-bottom:0}
.access-entry-editor.open>.access-entry-grid{grid-area:form;margin-top:0}
.access-entry-editor.open>.access-entry-meta{grid-area:meta;margin-top:0;align-self:end}
.access-entry-editor.open>.access-entry-foot{grid-area:actions;margin-top:0;align-self:end;justify-content:flex-end;flex-direction:column;align-items:flex-end}
.access-entry-editor.open>.access-entry-foot .settings-note{text-align:right}
.access-entry-grid{display:grid;grid-template-columns:minmax(240px,1fr) minmax(150px,200px) minmax(150px,200px);gap:12px}
.access-entry-grid .field label{display:block;font-size:11px;font-weight:800;letter-spacing:.14em;text-transform:uppercase;color:#8ea0bf;margin-bottom:8px}
.access-entry-meta{display:flex;align-items:center;gap:8px;flex-wrap:wrap;margin-top:12px}
.access-entry-foot{display:flex;align-items:center;justify-content:space-between;gap:12px;flex-wrap:wrap;margin-top:16px}
.access-entry-foot .settings-note{margin:0;min-height:auto;flex:1}
.access-row-btn.edit{background:rgba(245,158,11,.12);border-color:rgba(245,158,11,.26);color:#ffcd73}
.access-search{display:flex;align-items:center;gap:10px;min-width:280px;max-width:380px;flex:1;padding:0 12px;height:42px;border-radius:14px;border:1px solid var(--line);background:linear-gradient(180deg,rgba(255,255,255,.03),rgba(255,255,255,.015)),var(--panel-soft)}
.access-search svg{width:16px;height:16px;stroke:var(--muted);fill:none;stroke-width:1.9;stroke-linecap:round;stroke-linejoin:round;flex:0 0 auto}
.access-search input{width:100%;border:0;outline:0;background:transparent;color:var(--text);font-size:13px}
.access-search input::placeholder{color:var(--muted)}
.access-add-btn{min-height:42px;border-radius:14px;padding:0 16px;font-weight:700;white-space:nowrap}
.access-filter-row{display:flex;justify-content:flex-start;align-items:flex-end;gap:12px;flex-wrap:wrap;margin-bottom:14px}
.access-filter-item{display:flex;flex-direction:column;gap:6px;min-width:140px}
.access-filter-label{font-size:11px;font-weight:800;letter-spacing:.16em;text-transform:uppercase;color:#7b8aa5}
.access-filter-select{width:100%;min-height:38px;padding:0 12px;border:1px solid var(--line);border-radius:12px;background:linear-gradient(180deg,rgba(255,255,255,.03),rgba(255,255,255,.015)),var(--panel-soft);color:var(--text);font-size:12px;font-weight:700;outline:0}
.access-filter-select:focus{border-color:rgba(91,147,211,.35);box-shadow:0 0 0 3px rgba(91,147,211,.12)}
.access-table-wrap{overflow:auto;border:1px solid var(--line);border-radius:18px;background:linear-gradient(180deg,rgba(255,255,255,.02),rgba(255,255,255,.008)),var(--panel-soft)}
.access-table{width:100%;border-collapse:separate;border-spacing:0;min-width:860px}
.access-table thead th{position:sticky;top:0;background:rgba(11,18,32,.96);backdrop-filter:blur(6px);z-index:2;padding:14px 16px;border-bottom:1px solid var(--line);text-align:left;font-size:11px;font-weight:800;letter-spacing:.12em;text-transform:uppercase;color:#7b8aa5}
[data-theme="light"] .access-table thead th{background:rgba(255,255,255,.96)}
.access-table tbody td{padding:16px;border-bottom:1px solid rgba(255,255,255,.04);vertical-align:middle}
[data-theme="light"] .access-table tbody td{border-bottom:1px solid rgba(15,23,42,.06)}
.access-table tbody tr:last-child td{border-bottom:0}
.access-table tbody tr:hover td{background:rgba(91,147,211,.04)}
.access-person{display:flex;align-items:center;gap:12px;min-width:0}
.access-avatar{width:42px;height:42px;border-radius:50%;display:grid;place-items:center;font-size:13px;font-weight:800;color:#fff;flex:0 0 auto;box-shadow:inset 0 1px 0 rgba(255,255,255,.12)}
.access-avatar.admin{background:linear-gradient(135deg,#27c281,#1d8f63)}
.access-avatar.user{background:linear-gradient(135deg,#5f8bff,#3f67db)}
.access-avatar.open{background:linear-gradient(135deg,#f59e0b,#ea580c)}
.access-person-meta{min-width:0}
.access-person-name{font-size:14px;font-weight:700;line-height:1.3;word-break:break-word}
.access-person-sub{font-size:12px;color:var(--muted);margin-top:4px}
.access-cell-stack{display:flex;flex-direction:column;gap:6px}
.access-table-pill{display:inline-flex;align-items:center;min-height:28px;padding:0 10px;border-radius:999px;border:1px solid transparent;font-size:12px;font-weight:700;white-space:nowrap}
.access-table-pill.allowed{background:rgba(91,147,211,.12);border-color:rgba(91,147,211,.22);color:#8bbdff}
.access-table-pill.admin{background:rgba(52,195,143,.14);border-color:rgba(52,195,143,.26);color:#7df0ba}
.access-table-pill.open{background:rgba(245,158,11,.14);border-color:rgba(245,158,11,.28);color:#ffcd73}
.access-type-pill{display:inline-flex;align-items:center;min-height:28px;padding:0 10px;border-radius:999px;border:1px solid transparent;font-size:12px;font-weight:700;white-space:nowrap}
.access-type-pill.internal{background:rgba(52,211,153,.12);border-color:rgba(52,211,153,.22);color:#7df0ba}
.access-type-pill.external{background:rgba(245,158,11,.14);border-color:rgba(245,158,11,.28);color:#ffcd73}
.access-you-tag{display:inline-flex;align-items:center;margin-left:8px;padding:2px 8px;border-radius:999px;border:1px solid rgba(123,168,255,.24);background:rgba(123,168,255,.1);color:#9cc3ff;font-size:10px;font-weight:800;letter-spacing:.08em;text-transform:uppercase;vertical-align:middle}
.access-status{display:inline-flex;align-items:center;gap:8px;font-size:12px;font-weight:700;color:var(--text)}
.access-status::before{content:"";width:8px;height:8px;border-radius:50%;background:#93c5fd;box-shadow:0 0 0 4px rgba(147,197,253,.08)}
.access-status.admin::before{background:#34d399;box-shadow:0 0 0 4px rgba(52,211,153,.08)}
.access-status.open::before{background:#f59e0b;box-shadow:0 0 0 4px rgba(245,158,11,.08)}
.access-row-actions{display:flex;align-items:center;justify-content:flex-end;gap:8px;flex-wrap:wrap}
.access-row-btn{min-height:32px;padding:0 10px;border-radius:10px;border:1px solid var(--line);background:var(--panel);color:var(--text);font-size:12px;font-weight:700;cursor:pointer}
.access-row-btn.admin{background:rgba(52,195,143,.12);border-color:rgba(52,195,143,.24);color:#7df0ba}
.access-row-btn.user{background:rgba(91,147,211,.12);border-color:rgba(91,147,211,.24);color:#8bbdff}
.access-row-btn.remove{background:#fff1f2;border-color:#fecdd3;color:#be123c}
[data-theme="dark"] .access-row-btn.remove{background:#2a1620;border-color:#5b2435;color:#fda4af}
.access-empty{padding:26px 18px;text-align:center;color:var(--muted);font-size:13px}
.access-directory-foot{display:flex;align-items:center;justify-content:space-between;gap:12px;flex-wrap:wrap;margin-top:14px}
.access-directory-foot .settings-note{margin:0;min-height:auto;flex:1}
.access-layout{margin-top:12px}
.main{padding:14px 18px 18px}
.jobs-wrap::-webkit-scrollbar,.monitor-table-wrap::-webkit-scrollbar{width:10px;height:10px}
.jobs-wrap::-webkit-scrollbar-thumb,.monitor-table-wrap::-webkit-scrollbar-thumb{background:#cbd5e1;border-radius:999px;border:2px solid transparent;background-clip:padding-box}
.jobs-wrap::-webkit-scrollbar-track,.monitor-table-wrap::-webkit-scrollbar-track{background:transparent}
[data-theme="dark"] .jobs-wrap::-webkit-scrollbar-thumb,[data-theme="dark"] .monitor-table-wrap::-webkit-scrollbar-thumb{background:#41516d;border-radius:999px;border:2px solid transparent;background-clip:padding-box}
.topbar{display:flex;justify-content:flex-end;align-items:center;border-bottom:1px solid var(--line);padding-bottom:10px}
.actions{display:flex;gap:8px;align-items:center}
.auth-box{display:inline-flex;align-items:center;gap:8px;padding:4px 6px 4px 10px;border:1px solid var(--line);border-radius:999px;background:var(--panel-soft);max-width:380px}
.auth-role{display:inline-flex;align-items:center;justify-content:center;min-width:64px;height:28px;padding:0 10px;border-radius:999px;font-size:11px;font-weight:800;letter-spacing:.08em;text-transform:uppercase;border:1px solid transparent}
.auth-role.auth-role-admin{background:rgba(22,163,74,.14);border-color:rgba(22,163,74,.22);color:var(--green)}
.auth-role.auth-role-user{background:var(--blue-soft);border-color:rgba(47,128,237,.2);color:var(--blue)}
.auth-email{font-size:12px;font-weight:600;color:var(--text);white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
.auth-logout{border:1px solid var(--line);border-radius:999px;background:var(--panel);color:var(--text);padding:6px 12px;font-size:12px;font-weight:700;cursor:pointer}
.auth-logout:hover{border-color:rgba(47,128,237,.35);color:var(--blue)}
.lang-switch{display:flex;align-items:center;margin-right:4px}
.theme-switch{display:flex;align-items:center;margin-right:4px}
.lang-toggle{min-width:54px;height:38px;border:1px solid var(--line);border-radius:999px;background:var(--panel-soft);display:inline-flex;align-items:center;justify-content:center;padding:0 14px;cursor:pointer;color:var(--text);font-size:12px;font-weight:700;letter-spacing:.08em}
.theme-toggle{position:relative;width:72px;height:38px;border:1px solid var(--line);border-radius:999px;background:var(--panel-soft);display:flex;align-items:center;justify-content:space-between;padding:0 10px;cursor:pointer;color:var(--muted)}
.theme-toggle svg{width:15px;height:15px;stroke:currentColor;fill:none;stroke-width:1.8;stroke-linecap:round;stroke-linejoin:round;position:relative;z-index:2}
.theme-toggle .thumb{position:absolute;top:4px;left:4px;width:28px;height:28px;border-radius:50%;background:var(--blue);box-shadow:0 6px 18px rgba(47,128,237,.35);transition:left .22s ease}
[data-theme="dark"] .theme-toggle .thumb{left:38px}
.search{width:240px;background:var(--input-bg);color:var(--input-fg);border:1px solid var(--line);border-radius:9px;padding:7px 10px;font-size:12px}
.btn{border:1px solid var(--line);border-radius:8px;background:var(--panel);color:var(--text);padding:8px 10px;font-size:12px;cursor:pointer}
.btn.dark{background:var(--soft);color:var(--text);border-color:var(--line)}
.btn.blue{background:var(--blue);border-color:var(--blue);color:#fff}
.btn.red{background:#fff1f2;border-color:#fecdd3;color:#be123c}
[data-theme="dark"] .btn.red{background:#2a1620;border-color:#5b2435;color:#fda4af}
.headline{display:flex;justify-content:space-between;align-items:center;padding:14px 0 10px}
.h1{font-size:34px;font-weight:700;letter-spacing:-.01em}
.state{font-size:12px;padding:6px 10px;border-radius:999px;background:var(--soft);color:var(--text)}
.headline .state,.s{display:none}
#view-overview aside .right-top > div:nth-child(2),
#view-settings .settings-layout > .card > .muted,
#view-settings .list-row .muted,
#view-settings .settings-layout > .card .card > .muted,
#view-settings aside .timeline-item:last-child{display:none}
.layout{display:grid;grid-template-columns:2fr 1.15fr;gap:12px}
.card{background:var(--panel);border:1px solid var(--line);border-radius:14px}
.stats{display:grid;grid-template-columns:repeat(3,minmax(0,1fr))}
.stat{padding:14px;border-right:1px solid var(--line)}.stat:last-child{border-right:0}
.cards-4{display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:12px}
.k{font-size:12px;color:var(--muted)}.v{font-size:34px;font-weight:700;margin-top:2px}.s{font-size:11px;color:#98a2b3}
.chart{padding:16px;border-top:1px solid var(--line);overflow:hidden}
.bars{height:172px;display:flex;gap:8px;align-items:flex-end}
.bar{flex:1;display:flex;flex-direction:column;align-items:center;justify-content:flex-end;gap:8px;min-width:0}
.bar-val{font-size:11px;font-weight:700;color:var(--text);line-height:1}
.col{width:100%;max-width:58px;background:#dbeafe;border-radius:10px;transition:height .22s ease}
.col.mid{background:#bfdbfe}.col.active{background:#2f80ed}
.day{font-size:11px;color:var(--muted)}
.subgrid{display:grid;grid-template-columns:1fr 1fr;gap:10px;padding:12px;border-top:1px solid var(--line)}
.field label{display:block;font-size:11px;color:var(--muted);margin-bottom:5px}
.field input,.field select,.field textarea{width:100%;padding:7px 9px;border:1px solid var(--line);border-radius:8px;font-size:12px;background:var(--input-bg);color:var(--input-fg)}
.right-top{padding:14px;display:flex;flex-direction:column;height:100%}
.stack{display:flex;flex-direction:column;gap:8px;margin-top:10px}
.item{display:flex;justify-content:space-between;align-items:center;gap:12px;padding:12px 14px;border:1px solid var(--line);border-radius:14px;background:linear-gradient(180deg,rgba(255,255,255,.02),rgba(255,255,255,.01)),var(--soft)}
.item .t{font-size:12px;font-weight:700;color:var(--text)}
.item .d{font-size:11px;color:var(--muted);margin-top:4px}
.item-copy{min-width:0;display:flex;flex-direction:column}
.summary-action{display:inline-flex;align-items:center;gap:8px;min-height:36px;padding:0 12px;border-radius:12px;border:1px solid rgba(91,147,211,.18);background:linear-gradient(180deg,rgba(91,147,211,.1),rgba(91,147,211,.04));color:#dbe6f5;font-size:12px;font-weight:700;cursor:pointer;white-space:nowrap;transition:transform .15s ease,border-color .15s ease,background .15s ease}
.summary-action:hover{transform:translateY(-1px);border-color:rgba(91,147,211,.34);background:linear-gradient(180deg,rgba(91,147,211,.16),rgba(91,147,211,.08))}
.summary-action svg{width:14px;height:14px;stroke:currentColor;fill:none;stroke-width:1.9;stroke-linecap:round;stroke-linejoin:round;flex:0 0 auto}
.summary-action.sync svg{stroke-width:2}
.summary-action.is-loading{pointer-events:none;opacity:.92}
.summary-action.is-loading svg{animation:summary-spin .85s linear infinite}
.summary-action.is-done{border-color:rgba(52,195,143,.26);background:linear-gradient(180deg,rgba(52,195,143,.16),rgba(52,195,143,.06));color:#7df0ba}
.summary-action.is-error{border-color:rgba(239,68,68,.26);background:linear-gradient(180deg,rgba(239,68,68,.16),rgba(239,68,68,.06));color:#fda4af}
[data-theme="light"] .summary-action.is-done{color:#166534}
[data-theme="light"] .summary-action.is-error{color:#b91c1c}
@keyframes summary-spin{from{transform:rotate(0deg)}to{transform:rotate(360deg)}}
[data-theme="light"] .summary-action{color:#17315c;background:linear-gradient(180deg,rgba(91,147,211,.08),rgba(91,147,211,.03))}
.mini{padding:12px;border-top:1px solid var(--line)}
.mini-card{padding:12px 14px;border-top:0}
.progress{height:8px;background:#e5e7eb;border-radius:999px;overflow:hidden}.progress > span{display:block;height:100%;width:0%;background:#2f80ed;transition:width .35s ease}
.jobs-wrap{max-height:248px;overflow:auto;margin-top:8px}
.jobs{width:100%;border-collapse:collapse;margin-top:0}
.jobs th,.jobs td{font-size:12px;padding:8px;border-bottom:1px solid #eef2f7;text-align:left}
.jobs-wrap thead th{position:sticky;top:0;background:var(--panel);z-index:1}
.jobs tr.active{background:var(--blue-soft)}.jobs tr:hover{background:#f8fbff;cursor:pointer}
[data-theme="dark"] .jobs tr:hover{background:#162235}
[data-theme="dark"] .jobs-wrap thead th{background:var(--panel)}
.bottom{display:grid;grid-template-columns:1.5fr 1fr;gap:12px;margin-top:12px}
.logs{height:250px;overflow:auto;background:var(--log-bg);color:#dbe6f5;border:1px solid var(--line);border-radius:12px;padding:10px;font-size:12px;white-space:pre-wrap}
.errors{max-height:250px;overflow:auto;background:var(--danger-bg);border:1px solid var(--danger-line);color:var(--danger-text);border-radius:12px;padding:10px;font-size:12px}
.meta{font-size:11px;color:var(--muted);margin-top:6px}
.view{display:none}
.view.active{display:block}
.runs-head{display:flex;justify-content:space-between;align-items:flex-start;gap:16px;margin-bottom:14px}
.runs-head .headline{flex:1;justify-content:flex-start;padding:14px 0 0}
.run-config-head{font-size:18px;font-weight:700}
.run-layout{display:grid;grid-template-columns:minmax(420px,.9fr) minmax(760px,1.1fr);gap:12px;align-items:start}
.run-form{padding:12px 14px}
.run-grid{display:grid;grid-template-columns:1fr;gap:10px}
.run-share-note{margin-top:14px;padding:12px 14px;border:1px solid var(--line);border-radius:12px;background:var(--panel-soft);display:grid;grid-template-columns:max-content minmax(0,1fr);align-items:center;gap:12px}
.run-share-top{margin:0 0 0 auto;max-width:720px;min-width:520px}
.run-share-title{font-size:12px;font-weight:700;color:#2d6df6;letter-spacing:.02em;white-space:nowrap}
.run-share-email{margin-top:0;padding:10px 12px;border:1px solid var(--line);border-radius:8px;background:var(--input-bg);font-size:13px;color:var(--input-fg);word-break:break-all}
.run-form .run-actions{justify-content:space-between;align-items:center}
.mapping-panel{border:1px solid var(--line);border-radius:16px;background:var(--panel-soft);overflow:hidden}
.mapping-panel-body{padding:16px 18px}
.mapping-blocks{display:flex;flex-direction:column;gap:12px}
.mapping-block{border:1px solid var(--line);border-radius:12px;background:var(--panel);padding:12px}
.mapping-block-head{display:flex;justify-content:space-between;align-items:center;gap:10px;margin-bottom:10px}
.mapping-block-title{font-size:13px;font-weight:700}
.mapping-block-grid{display:grid;grid-template-columns:130px 1fr;gap:8px 12px;align-items:center}
.mapping-seeding-row{display:grid;grid-auto-flow:column;grid-auto-columns:minmax(340px,390px);gap:14px;overflow-x:auto;align-items:start;padding-bottom:4px;scroll-behavior:smooth}
.mapping-seeding-row .mapping-block{height:100%}
.mapping-block-new{animation:mappingSlideIn .28s ease}
@keyframes mappingSlideIn{
  from{opacity:0;transform:translateX(22px)}
  to{opacity:1;transform:translateX(0)}
}
.mapping-scan-grid{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:12px}
.mapping-matrix{border:1px solid var(--line);border-radius:12px;background:var(--panel);padding:10px 12px}
.mapping-matrix-grid{display:grid;gap:8px 12px;align-items:center}
.mapping-matrix-label{font-size:12px;color:var(--text);font-weight:500}
.mapping-matrix-name{display:flex;align-items:center;gap:6px}
.mapping-remove{min-width:28px;height:28px;padding:0}
.mapping-label{font-size:12px;color:var(--text)}
.mapping-input{width:100%;padding:7px 9px;border:1px solid var(--line);border-radius:8px;background:var(--input-bg);color:var(--input-fg);font-size:12px}
.mapping-field-combo{display:flex;align-items:center;gap:6px}
.mapping-icon-btn{min-width:34px;height:34px;padding:0}
.mapping-chrome-btn{justify-self:start}
.mapping-add-row{display:flex;justify-content:flex-start;align-items:center;gap:12px;margin-top:12px;flex-wrap:wrap}
.mapping-add-row.booking{justify-content:space-between;align-items:flex-start}
.mapping-check{display:inline-flex;align-items:center;gap:8px;font-size:12px;color:var(--text)}
.mapping-check input{width:16px;height:16px}
.mapping-toggle-card{display:grid;grid-template-columns:minmax(0,1fr) auto;align-items:center;gap:14px;min-width:280px;max-width:360px;padding:12px 14px;border:1px solid var(--line);border-radius:14px;background:linear-gradient(180deg,rgba(255,255,255,.03),rgba(255,255,255,.01)),var(--panel-soft);cursor:pointer;flex:0 0 auto}
.mapping-toggle-copy{display:flex;flex-direction:column;gap:4px;min-width:0}
.mapping-toggle-title{font-size:13px;font-weight:700;color:var(--text)}
.mapping-toggle-help{font-size:12px;color:var(--muted);line-height:1.45}
.mapping-toggle-switch{position:relative;display:inline-flex;align-items:center;justify-content:center;width:52px;height:30px;flex:0 0 auto}
.mapping-toggle-switch input{position:absolute;inset:0;opacity:0;cursor:pointer}
.mapping-toggle-slider{position:relative;display:block;width:52px;height:30px;border-radius:999px;background:#cbd5e1;border:1px solid rgba(148,163,184,.28);transition:background .2s ease,border-color .2s ease,box-shadow .2s ease}
.mapping-toggle-slider::after{content:"";position:absolute;top:3px;left:3px;width:22px;height:22px;border-radius:50%;background:#fff;box-shadow:0 4px 12px rgba(15,23,42,.16);transition:left .2s ease}
.mapping-toggle-switch input:checked + .mapping-toggle-slider{background:linear-gradient(135deg,#f59e0b,#f97316);border-color:#f97316;box-shadow:0 0 0 4px rgba(249,115,22,.12)}
.mapping-toggle-switch input:checked + .mapping-toggle-slider::after{left:25px}
[data-theme="dark"] .mapping-toggle-card{background:linear-gradient(180deg,rgba(255,255,255,.025),rgba(255,255,255,.01)),var(--panel-soft)}
[data-theme="dark"] .mapping-toggle-slider{background:#334155;border-color:#475569}
[data-theme="dark"] .mapping-toggle-slider::after{background:#f8fafc}
.run-actions{display:flex;gap:8px;flex-wrap:wrap;margin-top:12px}
.action-btn{min-height:44px;padding:0 16px;border-radius:14px;display:inline-flex;align-items:center;gap:10px;font-weight:700;letter-spacing:-.01em;box-shadow:0 10px 24px rgba(15,23,42,.08);transition:transform .15s ease,box-shadow .15s ease,filter .15s ease}
.action-btn:hover{transform:translateY(-1px);box-shadow:0 14px 30px rgba(15,23,42,.12);filter:saturate(1.03)}
.action-btn .action-icon{width:22px;height:22px;display:inline-flex;align-items:center;justify-content:center;border-radius:999px;flex:0 0 auto}
.action-btn svg{width:14px;height:14px;stroke:currentColor;fill:none;stroke-width:2;stroke-linecap:round;stroke-linejoin:round}
.action-btn .action-label{white-space:nowrap}
.action-btn.start{background:linear-gradient(135deg,#3b82f6,#2563eb);border-color:#2563eb;color:#fff}
.action-btn.start .action-icon{background:rgba(255,255,255,.18);color:#fff}
.action-btn.resume{background:linear-gradient(135deg,#22c55e,#16a34a);border-color:#1f9a4a;color:#fff}
.action-btn.resume .action-icon{background:rgba(255,255,255,.18);color:#fff}
.action-btn.red{background:linear-gradient(135deg,#fff1f2,#ffe4e6);border-color:#fecdd3;color:#be123c}
.action-btn.red .action-icon{background:rgba(190,24,93,.1);color:#be123c}
[data-theme="dark"] .action-btn{box-shadow:none}
[data-theme="dark"] .action-btn.start{background:linear-gradient(135deg,#3b82f6,#1d4ed8);border-color:#2563eb}
[data-theme="dark"] .action-btn.resume{background:linear-gradient(135deg,#22c55e,#15803d);border-color:#1d8d46}
[data-theme="dark"] .action-btn.red{background:linear-gradient(135deg,#2b1822,#351a24);border-color:#5b2435;color:#fda4af}
[data-theme="dark"] .action-btn.red .action-icon{background:rgba(253,164,175,.12);color:#fda4af}
.run-overwrite-card{display:grid;grid-template-columns:minmax(0,1fr) auto;align-items:center;gap:14px;width:100%;padding:12px 14px;border:1px solid var(--line);border-radius:14px;background:linear-gradient(180deg,rgba(255,255,255,.03),rgba(255,255,255,.01)),var(--panel-soft);cursor:pointer}
.run-overwrite-copy{display:flex;flex-direction:column;gap:4px;min-width:0}
.run-overwrite-title{font-size:13px;font-weight:700;color:var(--text)}
.run-overwrite-help{font-size:12px;color:var(--muted);line-height:1.45}
.run-overwrite-switch{position:relative;display:inline-flex;align-items:center;justify-content:center;width:52px;height:30px;flex:0 0 auto}
.run-overwrite-switch input{position:absolute;inset:0;opacity:0;cursor:pointer}
.run-overwrite-slider{position:relative;display:block;width:52px;height:30px;border-radius:999px;background:#cbd5e1;border:1px solid rgba(148,163,184,.28);transition:background .2s ease,border-color .2s ease,box-shadow .2s ease}
.run-overwrite-slider::after{content:"";position:absolute;top:3px;left:3px;width:22px;height:22px;border-radius:50%;background:#fff;box-shadow:0 4px 12px rgba(15,23,42,.16);transition:left .2s ease}
.run-overwrite-switch input:checked + .run-overwrite-slider{background:linear-gradient(135deg,#3b82f6,#2563eb);border-color:#2563eb;box-shadow:0 0 0 4px rgba(59,130,246,.12)}
.run-overwrite-switch input:checked + .run-overwrite-slider::after{left:25px}
[data-theme="dark"] .run-overwrite-card{background:linear-gradient(180deg,rgba(255,255,255,.025),rgba(255,255,255,.01)),var(--panel-soft)}
[data-theme="dark"] .run-overwrite-slider{background:#334155;border-color:#475569}
[data-theme="dark"] .run-overwrite-slider::after{background:#f8fafc}
.run-actions-main{display:flex;align-items:center;gap:8px;flex-wrap:wrap}
.monitor-card{margin-top:12px;padding:16px}
.monitor-head{display:flex;justify-content:space-between;align-items:flex-start;gap:12px}
.monitor-kicker{font-size:12px;font-weight:700;letter-spacing:.22em;text-transform:uppercase;color:#7b8aa5}
.monitor-title{font-size:18px;font-weight:700;margin-top:8px}
.monitor-badge{display:inline-flex;align-items:center;border-radius:999px;padding:6px 12px;font-size:12px;font-weight:600;background:var(--blue-soft);color:var(--blue);border:1px solid rgba(91,147,211,.25)}
.monitor-grid{display:grid;grid-template-columns:repeat(3,minmax(0,1fr));gap:12px;margin-top:16px}
.monitor-mini{padding:16px;border:1px solid var(--line);border-radius:18px;background:var(--panel-soft)}
.monitor-mini-label{font-size:12px;letter-spacing:.18em;text-transform:uppercase;color:#7b8aa5}
.monitor-mini-title{font-size:15px;font-weight:700;margin-top:10px;line-height:1.45}
.monitor-mini-sub{font-size:12px;color:var(--muted);margin-top:8px}
.monitor-progress-row{display:flex;justify-content:space-between;align-items:center;gap:10px;margin-top:10px}
.monitor-progress-value{font-size:24px;font-weight:700}
.monitor-progress-track{height:12px;border-radius:999px;background:#dbe4f0;overflow:hidden;margin-top:14px}
.monitor-progress-track span{display:block;height:100%;width:0;background:linear-gradient(90deg,#6b63ff,#7d77ff);transition:width .35s ease}
.monitor-progress-detail{font-size:12px;color:var(--muted);margin-top:10px;line-height:1.45;min-height:18px}
.monitor-error-main{font-size:24px;font-weight:700;margin-top:10px}
.monitor-table-card{margin-top:14px;border:1px solid var(--line);border-radius:22px;overflow:hidden;background:var(--panel-soft)}
.monitor-table-head{display:flex;justify-content:space-between;align-items:center;gap:12px;padding:16px 18px;border-bottom:1px solid var(--line)}
.monitor-table-title{font-size:16px;font-weight:700}
.monitor-export-btn{display:inline-flex;align-items:center;gap:8px;padding:8px 12px;border:1px solid var(--line);border-radius:999px;background:var(--panel);color:var(--text);font-size:12px;font-weight:700;cursor:pointer}
.monitor-export-btn svg{width:14px;height:14px;stroke:currentColor;fill:none;stroke-width:1.9;stroke-linecap:round;stroke-linejoin:round}
.monitor-export-btn:hover{border-color:rgba(47,128,237,.35);color:var(--blue)}
.monitor-table-wrap{max-height:360px;overflow:auto}
.monitor-table{width:100%;border-collapse:collapse}
.monitor-table th,.monitor-table td{padding:12px 16px;font-size:13px;text-align:left;border-bottom:1px solid var(--line);vertical-align:top}
.monitor-table thead th{position:sticky;top:0;background:var(--panel-soft);z-index:1;color:#71819d}
.monitor-table tbody tr:hover{background:rgba(91,147,211,.06)}
.monitor-replay-cell{text-align:right;white-space:nowrap}
.monitor-replay-btn{display:inline-flex;align-items:center;gap:6px;padding:7px 10px;border:1px solid var(--line);border-radius:999px;background:var(--panel);color:var(--text);font-size:12px;font-weight:600;cursor:pointer}
.monitor-replay-btn svg{width:13px;height:13px;fill:currentColor}
.monitor-replay-btn:hover{border-color:rgba(47,128,237,.35);color:var(--blue)}
.monitor-replay-btn:disabled{opacity:.45;cursor:not-allowed;color:var(--muted)}
.result-pill{display:inline-flex;align-items:center;border-radius:999px;padding:4px 10px;font-size:12px;font-weight:600;border:1px solid transparent}
.result-pill.success{background:#dcfce7;color:#15803d;border-color:#bbf7d0}
.result-pill.failed{background:#fee2e2;color:#b91c1c;border-color:#fecaca}
.result-pill.running,.result-pill.info{background:#dbeafe;color:#1d4ed8;border-color:#bfdbfe}
.result-pill.warning{background:#ffedd5;color:#c2410c;border-color:#fed7aa}
[data-theme="dark"] .monitor-kicker,[data-theme="dark"] .monitor-mini-label,[data-theme="dark"] .monitor-table thead th{color:#8ea0bf}
[data-theme="dark"] .monitor-export-btn{background:#121b2b;color:#dbe6f5}
[data-theme="dark"] .monitor-progress-track{background:#223149}
[data-theme="dark"] .monitor-table tbody tr:hover{background:rgba(91,147,211,.1)}
[data-theme="dark"] .result-pill.success{background:#153527;color:#9be6be;border-color:#25573d}
[data-theme="dark"] .result-pill.failed{background:#3a1b24;color:#f5a7b6;border-color:#6c3140}
[data-theme="dark"] .result-pill.running,[data-theme="dark"] .result-pill.info{background:#1a2940;color:#b7d2f3;border-color:#355072}
[data-theme="dark"] .result-pill.warning{background:#3a2a18;color:#f3c58e;border-color:#6f502e}
.muted{font-size:12px;color:var(--muted)}
.overview-stats-grid{display:flex;flex-direction:column;gap:12px}
.overview-stat-card{min-height:118px;display:flex;flex-direction:column;justify-content:space-between}
.overview-note-card{padding:14px 16px}
.overview-note{display:flex;justify-content:space-between;align-items:center;gap:16px;font-size:12px;color:var(--muted)}
.overview-note span{flex:1;min-width:0}
.overview-cta{display:inline-flex;align-items:center;gap:10px;min-height:42px;padding:0 16px;border:1px solid rgba(123,168,255,.24);border-radius:14px;background:linear-gradient(135deg,#4f8df7,#2f6fe4);color:#fff;font-size:12px;font-weight:800;letter-spacing:.02em;cursor:pointer;box-shadow:0 10px 24px rgba(47,111,228,.24);transition:transform .15s ease,box-shadow .15s ease,filter .15s ease}
.overview-cta:hover{transform:translateY(-1px);box-shadow:0 14px 28px rgba(47,111,228,.28);filter:saturate(1.05)}
.overview-cta svg{width:15px;height:15px;stroke:currentColor;fill:none;stroke-width:2;stroke-linecap:round;stroke-linejoin:round}
[data-theme="dark"] .overview-cta{background:linear-gradient(135deg,#548ff8,#2563eb);border-color:rgba(123,168,255,.22)}
.overview-top-card{margin-bottom:12px;padding:18px}
.overview-top-card .chart{padding:0;border-top:0}
.overview-top-grid{display:grid;grid-template-columns:minmax(0,1.82fr) minmax(260px,.7fr);gap:16px;align-items:stretch}
.overview-history-chart{display:flex;flex-direction:column;gap:12px}
.overview-history-head{display:flex;justify-content:space-between;align-items:center;gap:12px;flex-wrap:wrap}
.overview-history-title{font-size:18px;font-weight:700;color:var(--text)}
.overview-history-meta{display:flex;align-items:center;gap:12px;flex-wrap:wrap}
.overview-history-legend{display:flex;gap:10px;flex-wrap:wrap}
.overview-history-legend-item{display:inline-flex;align-items:center;gap:8px;font-size:12px;color:var(--muted);font-weight:600}
.overview-history-legend-dot{width:10px;height:10px;border-radius:999px;display:inline-block}
.overview-history-legend-dot.success{background:#5b8def}
.overview-history-legend-dot.failed{background:#ef4444}
.overview-history-legend-dot.unavailable{background:#f59e0b}
.overview-history-badges{display:flex;gap:8px;flex-wrap:wrap}
.overview-history-badge{display:inline-flex;align-items:center;justify-content:center;min-height:32px;padding:0 12px;border:1px solid var(--line);border-radius:999px;background:var(--panel-soft);font-size:12px;font-weight:600;color:var(--text)}
.overview-history-bars{margin-top:6px;position:relative;display:flex;align-items:flex-end;gap:14px;min-height:320px;padding:24px 12px 12px;border-radius:18px;background:
linear-gradient(to top, rgba(148,163,184,.06) 0, rgba(148,163,184,.06) 1px, transparent 1px) 0 100%/100% 25%,
linear-gradient(to right, transparent, transparent)}
.overview-history-group{flex:1;min-width:0;display:flex;flex-direction:column;align-items:center;justify-content:flex-end;gap:10px}
.overview-history-columns{height:240px;width:100%;display:flex;align-items:flex-end;justify-content:center;gap:10px}
.overview-history-col-wrap{display:flex;flex-direction:column;align-items:center;justify-content:flex-end;gap:8px;flex:1;max-width:34px}
.overview-history-col-value{font-size:12px;font-weight:700;color:var(--text);line-height:1}
.overview-history-col{width:100%;min-height:14px;border-radius:10px 10px 6px 6px;transition:height .22s ease, filter .18s ease}
.overview-history-col.success{background:linear-gradient(180deg,#7ba8ff,#4d7ff0)}
.overview-history-col.failed{background:linear-gradient(180deg,#f87171,#dc2626)}
.overview-history-col.unavailable{background:linear-gradient(180deg,#f7b14f,#f28c22)}
.overview-history-col.is-latest{filter:saturate(1.12)}
.overview-history-day{font-size:12px;color:var(--muted)}
.overview-history-empty{width:100%;padding:16px;border:1px dashed var(--line);border-radius:12px;background:var(--panel-soft);font-size:12px;color:var(--muted);text-align:center}
.overview-greeting-card{position:relative;overflow:hidden;padding:20px 18px;border:1px solid var(--line);border-radius:24px;background:radial-gradient(circle at 100% 0%,rgba(123,168,255,.18),transparent 34%),linear-gradient(180deg,rgba(255,255,255,.04),rgba(255,255,255,.01)),var(--panel-soft);display:flex;flex-direction:column;justify-content:space-between;min-height:100%}
.overview-greeting-card::after{content:"";position:absolute;right:-28px;bottom:-38px;width:132px;height:132px;border-radius:50%;background:rgba(91,147,211,.08);filter:blur(4px)}
.overview-greeting-head{position:relative;z-index:1;display:flex;justify-content:space-between;align-items:center;gap:12px}
.overview-greeting-kicker{font-size:12px;font-weight:700;letter-spacing:.18em;text-transform:uppercase;color:#7b8aa5}
.overview-greeting-visual{position:relative;z-index:1;display:flex;justify-content:center;align-items:center;padding:12px 0 8px}
.overview-greeting-orbit{position:relative;width:144px;height:144px;border-radius:50%;display:grid;place-items:center;background:conic-gradient(from 210deg,#6b63ff 0 118deg,rgba(107,99,255,.08) 118deg 360deg);box-shadow:0 18px 36px rgba(27,40,72,.12)}
.overview-greeting-orbit::before{content:"";position:absolute;inset:10px;border-radius:50%;background:var(--panel);border:1px solid rgba(123,168,255,.14)}
.overview-greeting-avatar{position:relative;z-index:1;width:102px;height:102px;border-radius:50%;display:grid;place-items:center;background:linear-gradient(135deg,#ffe6ef,#eef4ff);color:#111827;font-size:34px;font-weight:800;letter-spacing:.04em;border:6px solid rgba(255,255,255,.85)}
.overview-greeting-role{position:absolute;top:16px;right:22px;z-index:2;box-shadow:0 6px 16px rgba(15,23,42,.12)}
.overview-greeting-title{position:relative;z-index:1;margin-top:8px;font-size:26px;font-weight:800;line-height:1.18;color:var(--text);text-align:center;letter-spacing:-.03em}
.overview-greeting-sub{position:relative;z-index:1;margin-top:10px;font-size:13px;line-height:1.6;color:var(--muted);text-align:center}
.overview-greeting-footer{position:relative;z-index:1;display:flex;justify-content:center;margin-top:16px}
.overview-greeting-email{display:inline-flex;align-items:center;max-width:100%;padding:10px 14px;border-radius:999px;border:1px solid rgba(123,168,255,.22);background:rgba(123,168,255,.08);font-size:12px;font-weight:700;color:var(--text);white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
[data-theme="dark"] .overview-greeting-avatar{background:linear-gradient(135deg,#f7d4e2,#dde9ff);border-color:rgba(255,255,255,.7)}
[data-theme="dark"] .overview-greeting-email{background:rgba(91,147,211,.12)}
.overview-side-panels{display:flex;flex-direction:column;gap:12px;margin-top:12px}
.overview-side-card{padding:14px;border:1px solid var(--line);border-radius:16px;background:linear-gradient(180deg,rgba(255,255,255,.03),rgba(255,255,255,.01)),var(--panel-soft)}
.overview-top-side{display:flex;align-items:stretch}
.overview-top-side .overview-side-card{width:100%;padding:16px;min-height:0}
.overview-side-title{font-size:13px;font-weight:700;letter-spacing:.03em;color:var(--text)}
.overview-side-sub{margin-top:4px;font-size:12px;color:var(--muted)}
.overview-mode-list{display:flex;flex-direction:column;gap:12px;margin-top:14px}
.overview-mode-row{display:grid;gap:8px}
.overview-mode-head{display:flex;justify-content:space-between;align-items:center;gap:8px}
.overview-mode-value{font-size:12px;font-weight:800;color:var(--text)}
.overview-mode-track{height:10px;border-radius:999px;background:rgba(148,163,184,.18);overflow:hidden}
.overview-mode-fill{display:block;height:100%;border-radius:999px}
.overview-mode-fill.mode-seeding{background:linear-gradient(90deg,#34d399,#10b981)}
.overview-mode-fill.mode-booking{background:linear-gradient(90deg,#f59e0b,#f97316)}
.overview-mode-fill.mode-scan{background:linear-gradient(90deg,#60a5fa,#2563eb)}
.overview-mode-meta{font-size:11px;color:var(--muted)}
.overview-side-empty{padding:12px;border:1px dashed var(--line);border-radius:12px;background:var(--panel);font-size:12px;color:var(--muted);text-align:center}
.overview-side-panels.single{display:flex;flex:1;min-height:0}
.overview-side-panels.single .overview-side-card{min-height:0;flex:1}
.cards-3{display:grid;grid-template-columns:repeat(3,minmax(0,1fr));gap:12px}
.pad{padding:14px}
.big-number{font-size:30px;font-weight:700}
.list{display:flex;flex-direction:column;gap:8px}
.list-row{display:flex;justify-content:space-between;align-items:center;padding:10px 12px;border:1px solid var(--line);border-radius:10px;background:var(--panel-soft);font-size:12px}
.project-item{width:100%;text-align:left;cursor:pointer;gap:12px;background:var(--panel-soft)}
.project-item.active{border-color:#bfdbfe;background:var(--blue-soft)}
[data-theme="dark"] .project-item.active{border-color:#355072;background:#1a2940}
.project-list-head{display:flex;justify-content:space-between;align-items:center;gap:12px;flex-wrap:wrap}
.project-mode-filters{display:flex;gap:8px;flex-wrap:wrap}
.project-mode-filter{display:inline-flex;align-items:center;gap:8px;min-height:34px;padding:0 12px;border:1px solid var(--line);border-radius:999px;background:var(--panel-soft);color:var(--muted);font-size:12px;font-weight:600;cursor:pointer}
.project-mode-filter span{display:inline-flex;align-items:center;justify-content:center;min-width:20px;height:20px;padding:0 6px;border-radius:999px;background:var(--panel);color:var(--text);font-size:11px;font-weight:700}
.project-mode-filter.active{border-color:#bfdbfe;background:#eef4ff;color:#12315f}
.project-mode-filter.active span{background:#dbeafe;color:#1d4ed8}
.project-mode-filter.mode-all{border-color:#d0d5dd;background:#f8fafc;color:#344054}
.project-mode-filter.mode-all span{background:#ffffff;color:#101828}
.project-mode-filter.mode-seeding{border-color:#bbf7d0;background:#f0fdf4;color:#166534}
.project-mode-filter.mode-seeding span{background:#dcfce7;color:#166534}
.project-mode-filter.mode-booking{border-color:#fed7aa;background:#fff7ed;color:#c2410c}
.project-mode-filter.mode-booking span{background:#ffedd5;color:#c2410c}
.project-mode-filter.mode-scan{border-color:#bfdbfe;background:#eef4ff;color:#1d4ed8}
.project-mode-filter.mode-scan span{background:#dbeafe;color:#1d4ed8}
.project-mode-filter.mode-all.active{border-color:#98a2b3;background:#eef2f6;color:#1f2937}
.project-mode-filter.mode-all.active span{background:#d0d5dd;color:#101828}
.project-mode-filter.mode-seeding.active{border-color:#86efac;background:#dcfce7;color:#166534}
.project-mode-filter.mode-seeding.active span{background:#bbf7d0;color:#166534}
.project-mode-filter.mode-booking.active{border-color:#fdba74;background:#ffedd5;color:#9a3412}
.project-mode-filter.mode-booking.active span{background:#fed7aa;color:#9a3412}
.project-mode-filter.mode-scan.active{border-color:#93c5fd;background:#dbeafe;color:#1d4ed8}
.project-mode-filter.mode-scan.active span{background:#bfdbfe;color:#1d4ed8}
[data-theme="dark"] .project-mode-filter{background:#162033}
[data-theme="dark"] .project-mode-filter span{background:#121b2b;color:#dbe6f5}
[data-theme="dark"] .project-mode-filter.active{border-color:#355072;background:#1a2940;color:#dbe6f5}
[data-theme="dark"] .project-mode-filter.active span{background:#223149;color:#dbe6f5}
[data-theme="dark"] .project-mode-filter.mode-all{border-color:#475467;background:#182338;color:#dbe6f5}
[data-theme="dark"] .project-mode-filter.mode-all span{background:#101828;color:#dbe6f5}
[data-theme="dark"] .project-mode-filter.mode-seeding{border-color:#25573d;background:#153527;color:#9be6be}
[data-theme="dark"] .project-mode-filter.mode-seeding span{background:#1b4631;color:#9be6be}
[data-theme="dark"] .project-mode-filter.mode-booking{border-color:#6f502e;background:#3a2a18;color:#f3c58e}
[data-theme="dark"] .project-mode-filter.mode-booking span{background:#4a3520;color:#f3c58e}
[data-theme="dark"] .project-mode-filter.mode-scan{border-color:#355072;background:#1a2940;color:#dbe6f5}
[data-theme="dark"] .project-mode-filter.mode-scan span{background:#223149;color:#dbe6f5}
.project-item-main{display:flex;flex-direction:column;gap:4px;min-width:0}
.project-item-title{font-size:13px;font-weight:700;line-height:1.35}
.project-item-meta{font-size:11px;color:var(--muted);display:flex;align-items:center;gap:6px;flex-wrap:wrap}
.mode-pill{display:inline-flex;align-items:center;justify-content:center;min-height:20px;padding:0 8px;border-radius:999px;border:1px solid transparent;font-size:10px;font-weight:700;letter-spacing:.04em;text-transform:uppercase}
.mode-pill.mode-seeding{background:#ecfdf3;color:#166534;border-color:#bbf7d0}
.mode-pill.mode-booking{background:#fff7ed;color:#c2410c;border-color:#fed7aa}
.mode-pill.mode-scan{background:#eef4ff;color:#1d4ed8;border-color:#bfdbfe}
[data-theme="dark"] .mode-pill.mode-seeding{background:#153527;color:#9be6be;border-color:#25573d}
[data-theme="dark"] .mode-pill.mode-booking{background:#3a2a18;color:#f3c58e;border-color:#6f502e}
[data-theme="dark"] .mode-pill.mode-scan{background:#1a2940;color:#dbe6f5;border-color:#355072}
.project-item-side{display:flex;align-items:center;gap:10px;flex-shrink:0}
.project-delete-btn{display:inline-flex;align-items:center;justify-content:center;width:32px;height:32px;border:1px solid #fecaca;border-radius:10px;background:#fff1f2;color:#be123c;cursor:pointer}
.project-delete-btn svg{width:14px;height:14px;stroke:currentColor;fill:none;stroke-width:1.8;stroke-linecap:round;stroke-linejoin:round}
.project-delete-btn:hover{background:#ffe4e6}
[data-theme="dark"] .project-delete-btn{background:#2a1620;border-color:#5b2435;color:#fda4af}
[data-theme="dark"] .project-delete-btn:hover{background:#351a24}
.project-card-head{display:flex;justify-content:space-between;align-items:center;gap:12px}
.project-detail-actions{margin-top:10px;display:flex;justify-content:flex-end}
.project-card-head .project-detail-actions{margin-top:0}
.project-nav-btn{display:inline-flex;align-items:center;justify-content:center;width:40px;height:40px;border:1px solid #bfdbfe;border-radius:999px;background:#eef4ff;color:#1d4ed8;cursor:pointer}
.project-nav-btn svg{width:18px;height:18px;stroke:currentColor;fill:none;stroke-width:2;stroke-linecap:round;stroke-linejoin:round}
.project-nav-btn:hover{background:#dbeafe}
[data-theme="dark"] .project-nav-btn{background:#1a2940;border-color:#355072;color:#dbe6f5}
[data-theme="dark"] .project-nav-btn:hover{background:#223149}
.timeline{display:flex;flex-direction:column;gap:10px}
.timeline-item{padding:10px 12px;border-left:3px solid var(--blue);background:var(--panel-soft);border-radius:0 10px 10px 0}
.settings-layout{display:grid;grid-template-columns:1.1fr .9fr;gap:12px}
.badge{display:inline-flex;align-items:center;border-radius:999px;padding:3px 8px;font-size:11px;border:1px solid transparent}
.badge.info{background:#e8f1ff;color:#1d4ed8;border-color:#bfdbfe}
.badge.warning{background:#fff7ed;color:#c2410c;border-color:#fed7aa}
.badge.error{background:#fff1f2;color:#be123c;border-color:#fecdd3}
.badge.ok{background:#ecfdf3;color:#166534;border-color:#bbf7d0}
.mini-bars{display:flex;align-items:flex-end;gap:8px;height:180px;padding:10px 0}
.mini-bar{flex:1;display:flex;flex-direction:column;align-items:center;gap:6px}
.mini-bar-fill{width:100%;max-width:44px;background:#cfe0fb;border-radius:10px 10px 6px 6px;min-height:18px}
.mini-bar-fill.active{background:#2f80ed}
.mini-bar-label{font-size:11px;color:var(--muted)}
.mini-bar-value{font-size:11px;color:#344054}
.toast-host{position:fixed;top:18px;right:18px;display:flex;flex-direction:column;gap:10px;z-index:9999;pointer-events:none;max-width:min(360px,calc(100vw - 32px))}
.toast{pointer-events:auto;display:flex;gap:12px;align-items:flex-start;padding:14px 16px;border-radius:16px;border:1px solid var(--line);background:linear-gradient(180deg,rgba(18,27,43,.98),rgba(15,22,36,.98));box-shadow:0 18px 48px rgba(0,0,0,.34);color:var(--text);transform:translateY(-8px);opacity:0;animation:toast-in .18s ease forwards}
.toast.success{border-color:rgba(52,195,143,.34);box-shadow:0 18px 48px rgba(0,0,0,.34),0 0 0 1px rgba(52,195,143,.08)}
.toast.failed{border-color:rgba(239,68,68,.3)}
.toast-icon{width:34px;height:34px;border-radius:12px;display:grid;place-items:center;flex:0 0 auto;background:rgba(91,147,211,.12);border:1px solid rgba(91,147,211,.2);color:#9cc3ff;font-size:16px;font-weight:900}
.toast.success .toast-icon{background:rgba(52,195,143,.14);border-color:rgba(52,195,143,.26);color:#7df0ba}
.toast.failed .toast-icon{background:rgba(239,68,68,.14);border-color:rgba(239,68,68,.26);color:#fda4af}
.toast-copy{min-width:0;flex:1}
.toast-title{font-size:13px;font-weight:800;line-height:1.2}
.toast-message{margin-top:4px;font-size:12px;line-height:1.45;color:var(--muted)}
.toast-close{flex:0 0 auto;width:28px;height:28px;border-radius:10px;border:1px solid var(--line);background:transparent;color:var(--muted);cursor:pointer}
.toast-close:hover{color:var(--text);border-color:rgba(91,147,211,.3)}
@keyframes toast-in{from{opacity:0;transform:translateY(-8px)}to{opacity:1;transform:translateY(0)}}
@media (max-width:980px){.board{grid-template-columns:1fr}.layout,.bottom{grid-template-columns:1fr}.search{display:none}}
@media (max-width:980px){.run-layout,.run-grid,.cards-3,.cards-4,.settings-layout,.monitor-grid,.mapping-scan-grid,.admin-access-grid,.access-layout,.access-mail-grid,.access-entry-grid,.overview-top-grid{grid-template-columns:1fr}.access-entry-editor.open{grid-template-columns:1fr;grid-template-areas:"head" "meta" "form" "actions"}.sidebar{border-right:0;border-bottom:1px solid var(--line)}.runs-head{flex-direction:column;align-items:stretch}.runs-head .headline{padding:14px 0 0}.run-share-top{max-width:none;min-width:0;margin:0}.run-share-note{grid-template-columns:1fr;align-items:stretch}.run-share-title{white-space:normal}.access-directory-actions,.access-filter-row,.access-filter-group,.access-mail-foot,.access-entry-foot{align-items:stretch}.access-search{min-width:0;max-width:none;width:100%}.access-row-actions{justify-content:flex-start}.access-entry-editor.open>.access-entry-foot{align-items:stretch}.access-entry-editor.open>.access-entry-foot .settings-note{text-align:left}.overview-note{flex-direction:column;align-items:stretch}.overview-cta{justify-content:center}}
</style>
</head>
<body>
  <div class="shell">
    <div class="board">
      <aside class="sidebar">
        <div class="brand-row">
          <div class="dot"></div>
          <div class="brand-copy">
            <strong>Tool Evidence</strong>
            <span>Automation Suite</span>
          </div>
        </div>
        <div class="side-nav">
          <button class="side-btn active" data-view="overview" onclick="switchView('overview', this)"><span class="side-icon"><svg viewBox="0 0 24 24"><path d="M3 11.5 12 4l9 7.5"/><path d="M5 10.5V20h14v-9.5"/><path d="M10 20v-5h4v5"/></svg></span><span>Overview</span></button>
          <div id="runs_group" class="side-group">
            <button class="side-btn" data-view="runs" onclick="switchView('runs', this)"><span class="side-icon"><svg viewBox="0 0 24 24"><path d="M6 4h12"/><path d="M9 4v4l-3 5a4 4 0 0 0 3.4 6h5.2A4 4 0 0 0 18 13l-3-5V4"/><path d="M8 14h8"/></svg></span><span>Runs</span></button>
            <div class="side-subnav">
              <button id="run_mode_seeding" class="side-subbtn" type="button" onclick="openRunMode('seeding')">Seeding</button>
              <button id="run_mode_booking" class="side-subbtn" type="button" onclick="openRunMode('booking')">Booking</button>
              <button id="run_mode_scan" class="side-subbtn" type="button" onclick="openRunMode('scan')">Scan</button>
            </div>
          </div>
          <button class="side-btn" data-view="projects" onclick="switchView('projects', this)"><span class="side-icon"><svg viewBox="0 0 24 24"><rect x="3" y="4" width="18" height="14" rx="2"/><path d="M3 10h18"/><path d="M8 20h8"/></svg></span><span>Projects</span></button>
          <button class="side-btn" data-view="activities" onclick="switchView('activities', this)"><span class="side-icon"><svg viewBox="0 0 24 24"><path d="M4 12h4l2-5 4 10 2-5h4"/><path d="M4 19h16"/></svg></span><span>Activities</span></button>
          <button id="access_nav_button" class="side-btn" data-view="access" onclick="switchView('access', this)" style="__ADMIN_NAV_STYLE__"><span class="side-icon"><svg viewBox="0 0 24 24"><path d="M12 12a4 4 0 1 0-4-4 4 4 0 0 0 4 4Z"></path><path d="M5 20a7 7 0 0 1 14 0"></path><path d="M18 7h3"></path><path d="M19.5 5.5v3"></path></svg></span><span>Access</span></button>
          <button id="settings_nav_button" class="side-btn" data-view="settings" onclick="switchView('settings', this)" style="__SETTINGS_NAV_STYLE__"><span class="side-icon"><svg viewBox="0 0 24 24"><path d="M12 3v3"/><path d="M12 18v3"/><path d="m4.9 4.9 2.1 2.1"/><path d="m17 17 2.1 2.1"/><path d="M3 12h3"/><path d="M18 12h3"/><path d="m4.9 19.1 2.1-2.1"/><path d="m17 7 2.1-2.1"/><circle cx="12" cy="12" r="3.5"/></svg></span><span>Settings</span></button>
        </div>
      </aside>
      <main class="main">
        <div class="topbar">
          <div class="actions">
            <div class="auth-box">
              <span id="authRoleBadge" class="auth-role auth-role-__AUTH_ROLE_CLASS__">__AUTH_ROLE_DISPLAY__</span>
              <span class="auth-email" title="__AUTH_EMAIL_TITLE__">__AUTH_EMAIL_DISPLAY__</span>
              <button class="auth-logout" type="button" onclick="logoutAuth()"><span id="logoutLabel">Đăng xuất</span></button>
            </div>
            <div class="lang-switch">
              <button id="lang_toggle" class="lang-toggle" type="button" onclick="toggleLanguage()" aria-label="Toggle language" title="VI / EN">
                VN
              </button>
            </div>
            <div class="theme-switch">
              <button id="theme_toggle" class="theme-toggle" type="button" onclick="toggleTheme()" aria-label="Toggle theme" title="Toggle theme">
                <svg viewBox="0 0 24 24" aria-hidden="true">
                  <circle cx="12" cy="12" r="4"></circle>
                  <path d="M12 2v2"></path>
                  <path d="M12 20v2"></path>
                  <path d="m4.93 4.93 1.41 1.41"></path>
                  <path d="m17.66 17.66 1.41 1.41"></path>
                  <path d="M2 12h2"></path>
                  <path d="M20 12h2"></path>
                  <path d="m6.34 17.66-1.41 1.41"></path>
                  <path d="m19.07 4.93-1.41 1.41"></path>
                </svg>
                <span class="thumb"></span>
                <svg viewBox="0 0 24 24" aria-hidden="true">
                  <path d="M21 12.8A9 9 0 1 1 11.2 3a7 7 0 1 0 9.8 9.8Z"></path>
                </svg>
              </button>
            </div>
          </div>
        </div>

        <section id="view-overview" class="view active">
          <div class="headline">
            <div class="h1">Overview</div>
            <div id="envChip" class="state">Trạng thái: Sẵn sàng</div>
          </div>

          <section class="card overview-top-card">
            <div class="overview-top-grid">
              <div class="overview-history-chart">
                <div class="overview-history-head">
                  <div id="ovHistoryTitle" class="overview-history-title">Results by Date</div>
                  <div class="overview-history-meta">
                    <div class="overview-history-legend">
                      <div class="overview-history-legend-item"><span class="overview-history-legend-dot success"></span><span id="ovLegendSuccess">Completed</span></div>
                      <div class="overview-history-legend-item"><span class="overview-history-legend-dot failed"></span><span id="ovLegendFailed">Failed</span></div>
                      <div class="overview-history-legend-item"><span class="overview-history-legend-dot unavailable"></span><span id="ovLegendUnavailable">Unavailable</span></div>
                    </div>
                    <div id="ovHistoryBadges" class="overview-history-badges"></div>
                  </div>
                </div>
                <div id="ovHistoryBars" class="overview-history-bars"></div>
              </div>
              <aside class="overview-top-side">
                <section class="overview-side-card">
                  <div id="ovModeSplitTitle" class="overview-side-title">Mode split</div>
                  <div id="ovModeSplitSub" class="overview-side-sub">Distribution of tracked jobs by mode.</div>
                  <div id="ovModeSplit" class="overview-mode-list"></div>
                </section>
              </aside>
            </div>
          </section>

          <div class="layout">
            <section class="overview-stats-grid">
              <div class="cards-3 overview-stat-cards">
                <section class="card pad overview-stat-card">
                  <div id="ovSavedProjectsLabel" class="k">Saved Projects</div>
                  <div id="ovSavedProjects" class="big-number">0</div>
                </section>
                <section class="card pad overview-stat-card">
                  <div id="ovSavedSheetsLabel" class="k">Saved Sheets</div>
                  <div id="ovSavedSheets" class="big-number">0</div>
                </section>
                <section class="card pad overview-stat-card">
                  <div id="ovSelectedProjectLabel" class="k">Selected Project</div>
                  <div id="ovSelectedProject" class="big-number">-</div>
                </section>
              </div>
              <section class="card overview-note-card">
                <div class="overview-note">
                  <span id="overviewText">No run selected.</span>
                  <button class="overview-cta" onclick="switchView('runs')">
                    <span id="overviewRunCtaLabel">Open Run Center</span>
                    <svg viewBox="0 0 24 24" aria-hidden="true"><path d="M5 12h12"></path><path d="m13 6 6 6-6 6"></path></svg>
                  </button>
                </div>
              </section>
              <section class="card mini mini-card">
                <div style="display:flex;justify-content:space-between;font-size:12px;color:var(--muted)"><span>Overall progress</span><span id="pctText">0%</span></div>
                <div class="progress" style="margin-top:6px"><span id="pfill"></span></div>
                <div class="jobs-wrap">
                  <table class="jobs">
                    <thead><tr><th>Status</th><th>ID</th><th>Done</th></tr></thead>
                    <tbody id="jobsBody"></tbody>
                  </table>
                </div>
              </section>
            </section>

            <aside class="card">
              <div class="right-top">
                <div id="runSummaryTitle" style="font-size:20px;font-weight:700">Run Summary</div>
                <div id="runSummarySub" style="font-size:12px;color:var(--muted);margin-top:3px">Overview stays clean. Running tools live in the Runs tab.</div>
                <div class="stack">
                  <div class="item">
                    <div class="item-copy"><div class="t">Selected job</div><div id="kpiJob" class="d">-</div></div>
                    <button class="summary-action" onclick="switchView('runs')"><svg viewBox="0 0 24 24"><path d="M5 12h12"></path><path d="m13 6 6 6-6 6"></path></svg><span>Open Runs</span></button>
                  </div>
                  <div class="item">
                    <div class="item-copy"><div class="t">Stored jobs</div><div id="jobCountText" class="d">0 jobs loaded</div></div>
                    <button id="overviewSyncButton" class="summary-action sync" onclick="refreshJobsWithFeedback(this)"><svg viewBox="0 0 24 24"><path d="M21 12a9 9 0 0 1-15.36 6.36"></path><path d="M3 12A9 9 0 0 1 18.36 5.64"></path><path d="M3 16v-4h4"></path><path d="M21 8v4h-4"></path></svg><span id="overviewSyncLabel">Sync</span></button>
                  </div>
                  <div class="item">
                    <div class="item-copy"><div class="t">Success / Failed</div><div id="kpiSF" class="d">0 / 0</div></div>
                    <button class="summary-action" onclick="switchView('activities')"><svg viewBox="0 0 24 24"><path d="M2 12s3.5-6 10-6 10 6 10 6-3.5 6-10 6S2 12 2 12Z"></path><circle cx="12" cy="12" r="2.5"></circle></svg><span>View</span></button>
                  </div>
                </div>
              </div>
            </aside>
          </div>

        </section>

        <section id="view-runs" class="view">
          <div class="runs-head">
            <div class="headline">
              <div id="runTitleText" class="h1">Seeding</div>
            </div>
            <div class="run-share-note run-share-top">
              <div id="runShareLabel" class="run-share-title">Chia sẻ Sheet & Drive folder cho (quyền Editor):</div>
              <div id="runShareEmail" class="run-share-email">Chưa có email service account</div>
            </div>
          </div>
          <div class="run-layout">
            <section class="card run-form">
              <div class="run-grid">
                <div class="field"><label>Sheet URL</label><input id="sheet_url" /></div>
                <div class="field"><label>Sheet Name</label><input id="sheet_name" list="sheet_name_suggestions" autocomplete="off" /><datalist id="sheet_name_suggestions"></datalist><div id="sheet_name_hint" class="settings-note"></div></div>
                <div class="field"><label>Drive Folder ID</label><input id="drive_id" /></div>
              </div>
              <div class="run-actions">
                <label class="run-overwrite-card">
                  <span class="run-overwrite-copy">
                    <span id="overwriteRunLabel" class="run-overwrite-title">Overwrite</span>
                    <span id="overwriteRunHelp" class="run-overwrite-help">Always rerun and replace previous results</span>
                  </span>
                  <span class="run-overwrite-switch">
                    <input id="force_run_all" type="checkbox" checked />
                    <span class="run-overwrite-slider"></span>
                  </span>
                </label>
                <div class="run-actions-main">
                  <button class="btn action-btn start" onclick="startJob()">
                    <span class="action-icon" aria-hidden="true">
                      <svg viewBox="0 0 24 24"><path d="M8 6.5v11l9-5.5-9-5.5Z"></path></svg>
                    </span>
                    <span id="startJobLabel" class="action-label">Start Job</span>
                  </button>
                  <button class="btn red action-btn" onclick="stopJob()">
                    <span id="stopJobIconWrap" class="action-icon" aria-hidden="true">
                      <svg id="stopJobIcon" viewBox="0 0 24 24"><rect x="7" y="7" width="10" height="10" rx="1.5"></rect></svg>
                    </span>
                    <span id="stopJobLabel" class="action-label">Stop Job</span>
                  </button>
                </div>
              </div>
            </section>
            <aside class="mapping-panel">
              <div class="mapping-panel-body">
                <div id="mappingBlocks" class="mapping-blocks"></div>
                <div class="mapping-add-row">
                  <button id="mappingAddButton" class="btn" type="button" onclick="addMappingBlock()">+ Thêm Block</button>
                </div>
              </div>
            </aside>
          </div>
          <section class="card monitor-card">
            <div class="monitor-head">
              <div>
                <div id="runMonitorKicker" class="monitor-kicker">4. Result & Monitor</div>
              </div>
              <div id="runMonitorStatus" class="monitor-badge">Sẵn sàng</div>
            </div>
            <div class="monitor-grid">
              <section class="monitor-mini">
                <div id="runMonitorJobLabel" class="monitor-mini-label">Job</div>
                <div id="runMonitorJobTitle" class="monitor-mini-title">Chua chon job</div>
                <div id="runMonitorJobMeta" class="monitor-mini-sub">-</div>
              </section>
              <section class="monitor-mini">
                <div id="runMonitorProgressLabel" class="monitor-mini-label">Progress</div>
                <div class="monitor-progress-row">
                  <div id="runMonitorProgressMain" class="monitor-mini-title">0 / 0</div>
                  <div id="runMonitorPercent" class="monitor-progress-value">0%</div>
                </div>
                <div class="monitor-progress-track"><span id="runMonitorBar"></span></div>
                <div id="runMonitorProgressMeta" class="monitor-progress-detail">-</div>
              </section>
              <section class="monitor-mini">
                <div id="runMonitorErrorLabel" class="monitor-mini-label">Loi theo link sheet</div>
                <div id="runMonitorErrorMain" class="monitor-error-main">Khong co loi</div>
                <div id="runMonitorErrorMeta" class="monitor-mini-sub">Success 0 - Failed 0</div>
              </section>
            </div>
            <div class="monitor-table-card">
              <div class="monitor-table-head">
                <div id="runMonitorTableTitle" class="monitor-table-title">Bang log xu ly</div>
                <button id="exportLogBtn" class="monitor-export-btn" type="button" onclick="exportCurrentLog()">
                  <svg viewBox="0 0 24 24" aria-hidden="true"><path d="M12 3v12"></path><path d="m7 10 5 5 5-5"></path><path d="M5 19h14"></path></svg>
                  <span id="exportLogLabel">Xuất log Excel</span>
                </button>
              </div>
              <div class="monitor-table-wrap">
                <table class="monitor-table">
                  <thead>
                    <tr>
                      <th id="runMonitorHeadTime">Time</th>
                      <th id="runMonitorHeadPost">Post</th>
                      <th id="runMonitorHeadRow">#</th>
                      <th id="runMonitorHeadResult">Result</th>
                      <th id="runMonitorHeadMessage">Message</th>
                      <th id="runMonitorHeadReplay">Replay</th>
                    </tr>
                  </thead>
                  <tbody id="runMonitorRows">
                    <tr><td colspan="6">No data</td></tr>
                  </tbody>
                </table>
              </div>
            </div>
          </section>
        </section>

        <section id="view-projects" class="view">
          <div class="headline">
            <div class="h1">Projects</div>
            <div class="state">Portfolio of stored runs</div>
          </div>
          <div class="bottom">
            <section class="card pad">
              <div class="project-list-head">
                <div id="projectsListTitle" style="font-size:15px;font-weight:600">Grouped Registry</div>
                <div id="projectsModeFilters" class="project-mode-filters"></div>
              </div>
              <div id="projectsList" class="list" style="margin-top:10px"></div>
            </section>
            <section class="card pad">
              <div class="project-card-head">
                <div id="projectsSnapshotTitle" style="font-size:15px;font-weight:600">Group Snapshot</div>
                <div id="projectsSnapshotAction"></div>
              </div>
              <div id="projectsSnapshot" class="timeline" style="margin-top:10px"></div>
            </section>
          </div>
        </section>

        <section id="view-activities" class="view">
          <div class="headline">
            <div class="h1">Activities</div>
            <div class="state">Latest runtime events with severity</div>
          </div>
          <div class="card pad">
            <div style="font-size:15px;font-weight:600">Recent Timeline</div>
            <div id="activitiesTimeline" class="timeline" style="margin-top:10px"></div>
          </div>
        </section>

        <section id="view-access" class="view" style="__ADMIN_SECTION_STYLE__">
          <div class="headline access-headline">
            <div>
              <div class="access-kicker">Admin Control</div>
              <div class="h1">Access</div>
            </div>
            <div class="state">Admin manages user access</div>
          </div>
          <section class="card pad access-mail-card">
            <div class="access-section-head">
              <div>
                <div id="accessMailTitle" class="access-section-title">Mail gửi OTP</div>
                <div id="accessMailHelp" class="access-section-sub">Đổi Gmail gửi mã xác nhận ngay trên giao diện admin. App password cũ sẽ được giữ kín và chỉ thay khi bạn nhập mới.</div>
              </div>
              <div style="display:flex;align-items:center;gap:8px;flex-wrap:wrap">
                <button class="btn" type="button" onclick="setAccessMailEditorOpen(false)" id="hideMailConfigButton">Ẩn</button>
              </div>
            </div>
            <div class="access-mail-grid">
              <div class="field">
                <label for="access_mail_sender_email" id="accessMailSenderLabel">Gmail gửi OTP</label>
                <input id="access_mail_sender_email" type="email" placeholder="yourgmail@gmail.com" />
              </div>
              <div class="field">
                <label for="access_mail_from_email" id="accessMailFromLabel">From email</label>
                <input id="access_mail_from_email" type="email" placeholder="yourgmail@gmail.com" />
              </div>
              <div class="field">
                <label for="access_mail_app_password" id="accessMailPasswordLabel">App password mới</label>
                <input id="access_mail_app_password" type="password" placeholder="abcd efgh ijkl mnop" />
              </div>
            </div>
            <div class="access-mail-meta">
              <span id="accessMailCurrentPill" class="access-mail-pill">Đang dùng: Chưa cấu hình</span>
              <span id="accessMailPasswordPill" class="access-mail-pill warn">Chưa có app password</span>
            </div>
            <div class="access-mail-foot">
              <div id="access_mail_note" class="settings-note"></div>
              <button class="btn blue" type="button" onclick="saveMailConfig()" id="saveMailConfigButton">Lưu mail OTP</button>
            </div>
          </section>
          <section class="card pad access-entry-editor">
            <div class="access-section-head">
              <div>
                <div id="accessEntryTitle" class="access-section-title">Chỉnh sửa Gmail</div>
                <div id="accessEntryHelp" class="access-section-sub">Đổi địa chỉ Gmail hoặc role của dòng đang chọn rồi lưu lại.</div>
              </div>
              <button class="btn" type="button" onclick="setAccessEntryEditorOpen(false)" id="accessEntryCancelTop">Hủy</button>
            </div>
            <div class="access-entry-grid">
              <div class="field">
                <label for="access_entry_email" id="accessEntryEmailLabel">Địa chỉ Gmail</label>
                <input id="access_entry_email" type="email" placeholder="user@example.com" />
              </div>
                <div class="field">
                  <label for="access_entry_role" id="accessEntryRoleLabel">Role</label>
                  <select id="access_entry_role">
                    <option value="user">User</option>
                    <option value="admin">Admin</option>
                  </select>
                </div>
                <div class="field">
                  <label for="access_entry_type" id="accessEntryTypeLabel">Type</label>
                  <select id="access_entry_type">
                    <option value="internal">Internal</option>
                    <option value="external">Ngoại bộ</option>
                  </select>
                </div>
              </div>
            <div class="access-entry-meta">
              <span id="accessEntryCurrentPill" class="access-mail-pill">Đang sửa: -</span>
            </div>
            <div class="access-entry-foot">
              <div id="access_entry_note" class="settings-note"></div>
              <div style="display:flex;align-items:center;gap:8px;flex-wrap:wrap">
                <button class="btn" type="button" onclick="setAccessEntryEditorOpen(false)" id="accessEntryCancelButton">Hủy</button>
                <button class="btn blue" type="button" onclick="saveAccessEntryEditor()" id="accessEntrySaveButton">Lưu chỉnh sửa</button>
              </div>
            </div>
          </section>
          <section class="card pad access-directory">
            <div class="access-section-head access-directory-head">
              <div>
                <div class="access-directory-title-wrap">
                  <div id="accessDirectoryTitle" class="access-section-title">Danh sách người dùng</div>
                  <span id="accessDirectoryCount" class="access-mini-pill">0</span>
                </div>
                <div id="accessDirectoryHelp" class="access-section-sub">Lọc nhanh mail theo quyền, trạng thái truy cập và chỉnh role trực tiếp trên từng dòng.</div>
              </div>
              <div class="access-directory-actions">
                <div class="access-search">
                  <svg viewBox="0 0 24 24" aria-hidden="true"><circle cx="11" cy="11" r="7"></circle><path d="m20 20-3.5-3.5"></path></svg>
                  <input id="accessDirectorySearch" type="text" placeholder="Search Gmail" oninput="setAccessDirectoryQuery(this.value)" />
                </div>
                <button class="btn blue access-add-btn" type="button" onclick="addAccessEmailFromSearch()" id="accessQuickAddButton">+ Add Gmail</button>
              </div>
            </div>
            <div class="access-filter-row">
              <div class="access-filter-item">
                <label id="accessFilterRoleLabel" class="access-filter-label" for="accessRoleFilterSelect">Role</label>
                <select id="accessRoleFilterSelect" class="access-filter-select" onchange="setAccessDirectoryRole(this.value)">
                  <option id="accessRoleFilterAll" value="all">All</option>
                  <option id="accessRoleFilterAdmin" value="admin">Admin</option>
                  <option id="accessRoleFilterUser" value="user">User</option>
                </select>
              </div>
              <div class="access-filter-item">
                <label id="accessFilterScopeLabel" class="access-filter-label" for="accessScopeFilterSelect">Access</label>
                <select id="accessScopeFilterSelect" class="access-filter-select" onchange="setAccessDirectoryScope(this.value)">
                  <option id="accessScopeFilterAll" value="all">All</option>
                  <option id="accessScopeFilterAllowed" value="allowed">Allowed</option>
                  <option id="accessScopeFilterAdmin" value="admin">Admin</option>
                  <option id="accessScopeFilterOpen" value="open">Open OTP</option>
                </select>
              </div>
              <div class="access-filter-item">
                <label id="accessFilterTypeLabel" class="access-filter-label" for="accessTypeFilterSelect">Type</label>
                <select id="accessTypeFilterSelect" class="access-filter-select" onchange="setAccessDirectoryType(this.value)">
                  <option id="accessTypeFilterAll" value="all">All</option>
                  <option id="accessTypeFilterInternal" value="internal">Internal</option>
                  <option id="accessTypeFilterExternal" value="external">Ngoại bộ</option>
                </select>
              </div>
            </div>
            <div class="access-table-wrap">
              <table class="access-table">
                <thead>
                  <tr>
                    <th id="accessTableHeadEmail">Gmail</th>
                    <th id="accessTableHeadRole">Role</th>
                    <th id="accessTableHeadType">Type</th>
                    <th id="accessTableHeadStatus">Status</th>
                    <th id="accessTableHeadUpdated">Updated</th>
                    <th id="accessTableHeadActions">Actions</th>
                  </tr>
                </thead>
                <tbody id="accessDirectoryBody"></tbody>
              </table>
            </div>
            <div class="access-directory-foot">
              <div id="access_policy_note" class="settings-note"></div>
              <button class="btn" onclick="reloadAccessAdminPanel()" id="reloadAccessButton">Reload Access</button>
            </div>
          </section>
        </section>

        <section id="view-settings" class="view" style="__SETTINGS_SECTION_STYLE__">
          <div class="headline">
            <div class="h1">Settings</div>
            <div class="state">Saved configuration</div>
          </div>
          <div class="settings-layout">
            <section class="card pad">
              <div style="font-size:18px;font-weight:700">Screenshot & credentials</div>
              <div class="muted" style="margin-top:4px">These values are reused by future jobs. You can also paste service account JSON here and save it once.</div>
              <div class="run-grid" style="margin-top:14px">
                <div class="field">
                  <label for="settings_viewport_width">Viewport width</label>
                  <input id="settings_viewport_width" type="number" min="320" step="1" />
                </div>
                <div class="field">
                  <label for="settings_viewport_height">Viewport height</label>
                  <input id="settings_viewport_height" type="number" min="320" step="1" />
                </div>
              </div>
              <div class="field" style="margin-top:12px">
                <label for="settings_page_timeout_ms">Page timeout (ms)</label>
                <input id="settings_page_timeout_ms" type="number" min="500" step="100" />
              </div>
              <div class="list-row" style="margin-top:12px">
                <div>
                  <div style="font-weight:600">Full page capture</div>
                  <div class="muted">Store this preference for future screenshot modes.</div>
                </div>
                <input id="settings_full_page_capture" type="checkbox" style="width:18px;height:18px" />
              </div>
              <div id="settings_service_card" class="card pad" style="margin-top:14px;background:var(--panel-soft)">
                <div style="font-size:15px;font-weight:700">JSON service account</div>
                <div class="muted" style="margin-top:4px">Chọn file .json hoặc dán JSON trực tiếp để lưu credentials và tự cập nhật credentials path.</div>
                <div id="settings_service_status" class="badge info" style="margin-top:10px">Not saved</div>
                <div class="field" style="margin-top:12px">
                  <label for="settings_service_account_file" id="settingsServiceAccountFileLabel">Chọn file JSON</label>
                  <input id="settings_service_account_file" type="file" accept=".json,application/json" onchange="handleServiceAccountFileChange(event)" />
                  <div id="settings_service_account_file_hint" class="muted" style="margin-top:8px">Chưa chọn file</div>
                </div>
                <div class="field" style="margin-top:14px">
                  <label for="settings_service_account_json" id="settingsServiceAccountJsonLabel">Hoặc dán JSON trực tiếp</label>
                  <textarea id="settings_service_account_json" placeholder='{"type":"service_account","project_id":"..."}'></textarea>
                </div>
              </div>
              <div class="run-actions">
                <button id="saveSettingsButton" class="btn blue" onclick="saveSidebarSettings()">Save Settings</button>
              </div>
              <div id="settings_note" class="settings-note"></div>
            </section>
            <aside class="card pad">
              <div style="font-size:18px;font-weight:700">Current config summary</div>
              <div class="timeline" style="margin-top:12px">
                <div class="timeline-item"><strong>Viewport</strong><div id="settings_summary_viewport">-</div></div>
                <div class="timeline-item"><strong>Timeout</strong><div id="settings_summary_timeout">-</div></div>
                <div class="timeline-item"><strong>Output</strong><div id="settings_summary_full_page">-</div></div>
                <div class="timeline-item"><strong>Service account</strong><div id="settings_summary_service_account">Not saved</div><div id="settings_summary_service_email" class="muted"></div></div>
                <div class="timeline-item"><strong>Sharing note</strong><div>Share Google Sheets and Drive folder with the service account email above using Editor permission.</div></div>
              </div>
            </aside>
          </div>
        </section>
      </main>
    </div>
  </div>
<div id="toastHost" class="toast-host" aria-live="polite" aria-atomic="false"></div>
<script>
let currentJobId = null;
let pollTimer = null;
let jobsTimer = null;
let syncFeedbackTimer = null;
let jobsCache = [];
let currentJobSnapshot = null;
let currentLogsCache = [];
let currentJobIdByMode = { seeding: null, booking: null, scan: null };
let currentProjectJobId = null;
let currentProjectModeFilter = 'all';
let currentSettingsCache = {};
let currentRunMode = 'seeding';
let currentMappingBlocksByMode = {};
let captureFivePerLink = false;
let sheetNameSuggestTimer = null;
let sheetNameSuggestKey = '';
let sheetNameSuggestCache = {};
let pendingMappingScrollMode = '';
let pendingMappingHighlightIndex = -1;
let currentAccessPolicy = { allowed_emails: [], admin_emails: [], managed_emails: [], email_types: {}, updated_at: null };
let currentMailConfig = { sender_email: '', from_email: '', has_password: false, updated_at: null, source: 'env' };
let accessDirectoryQuery = '';
let accessDirectoryRole = 'all';
let accessDirectoryScope = 'all';
let accessDirectoryType = 'all';
let accessMailEditorOpen = false;
let accessEntryEditorState = { open: false, originalEmail: '', email: '', role: 'user', type: 'internal' };
let jobStatusMemory = {};
let notifiedCompletedJobKeys = new Set();
const BROWSER_PORT_BY_MODE = { seeding: 9223, booking: 9423, scan: 9623 };
const DEFAULT_AUTO_LAUNCH_CHROME = false;
let currentLang = localStorage.getItem('ui_lang') || 'vi';
let currentTheme = localStorage.getItem('ui_theme') || 'light';
const authState = {
  email: '__AUTH_EMAIL__',
  role: '__AUTH_ROLE__',
  isAdmin: __AUTH_IS_ADMIN__,
};

const I18N = {
  vi: {
    searchPlaceholder: 'Tìm job hoặc trạng thái...',
    launchChrome: 'Mở Chrome',
    refresh: 'Làm mới',
    light: 'Sáng',
    dark: 'Tối',
    logout: 'Đăng xuất',
    roleAdmin: 'Admin',
    roleUser: 'User',
    adminOnly: 'Chỉ admin mới dùng được phần này',
    overview: 'Tổng quan',
    runs: 'Chạy tác vụ',
    projects: 'Dự án',
    tasks: 'Tác vụ',
    activities: 'Hoạt động',
    access: 'Quản lý người dùng',
    settings: 'Cài đặt',
    state: 'Trạng thái',
    readyState: 'Sẵn sàng',
    openRuns: 'Mở Runs',
    view: 'Xem',
    sync: 'Đồng bộ',
    syncing: 'Đang đồng bộ',
    synced: 'Đã đồng bộ',
    syncFailed: 'Lỗi',
    goToRuns: 'Mở Run Center',
    selectedJob: 'Job đang chọn',
    storedJobs: 'Job đã lưu',
    successFailed: 'Thành công / Lỗi',
    overallProgress: 'Tiến độ tổng',
    overviewModeSplit: 'Tỉ lệ theo mode',
    overviewModeSplitSub: 'Phân bổ job đang theo dõi theo từng mode.',
    overviewModeShareFmt: (count, pct) => `${count} job · ${pct}%`,
    overviewModeSplitEmpty: 'Chưa có dữ liệu mode để thống kê.',
    overviewGreetingLabel: 'Lời chào hôm nay',
    overviewGreetingMorning: 'Chào buổi sáng',
    overviewGreetingAfternoon: 'Chào buổi chiều',
    overviewGreetingEvening: 'Chào buổi tối',
    overviewGreetingFallbackName: 'bạn',
    overviewGreetingSub: 'Tiếp tục theo dõi job và giữ nhịp công việc hôm nay.',
    jobsToday: 'Tổng số job hôm nay',
    avgSuccess: 'Tỉ lệ success trung bình',
    latestJob: 'Job chạy gần nhất',
    topError: 'Top lỗi gặp nhiều nhất',
    overviewTimeline: 'Kết quả theo ngày',
    overviewTimelineEmpty: 'Chưa có lịch sử chạy theo ngày',
    overviewDateFmt: label => `Ngày ${label}`,
    overviewTimelineJobsBadgeFmt: count => `${count} job`,
    overviewTimelineSuccessBadgeFmt: count => `${count} ok`,
    overviewTimelineFailedBadgeFmt: count => `${count} lỗi`,
    overviewTimelineUnavailableBadgeFmt: count => `${count} không khả dụng`,
    overviewCompletedLegend: 'Hoàn thành',
    overviewFailedLegend: 'Lỗi',
    overviewUnavailableLegend: 'Không khả dụng',
    createdLast24h: 'được tạo trong 24h gần nhất',
    acrossTracked: 'trên toàn bộ job đã theo dõi',
    noRecentRun: 'chưa có job gần đây',
    noRecurring: 'chưa có lỗi lặp lại',
    runSummary: 'Tóm tắt job',
    overviewClean: 'Overview chỉ để xem số liệu. Khu chạy nằm ở tab Runs.',
    runConfig: 'Cấu hình chạy',
    runConfigHelp: 'Chia sẻ quyền Editor cho Sheet và Drive trước khi chạy.',
    runShareLabel: 'Chia sẻ Sheet & Drive folder cho (quyền Editor):',
    overwriteRunHelp: 'Luôn chạy lại và ghi đè kết quả cũ.',
    runMode: 'Chế độ chạy',
    columnMapping: 'Column Mapping',
    seeding: 'Seeding',
    booking: 'Booking',
    scan: 'Scan',
    runModeSeedingHelp: 'Seeding dùng luồng chụp và upload ảnh tiêu chuẩn cho bài đăng.',
    runModeBookingHelp: 'Booking phù hợp cho job cần multi-capture và theo dõi lịch booking.',
    runModeScanHelp: 'Scan bỏ qua Chrome nếu chỉ quét dữ liệu và dùng bộ cột scan mặc định.',
    addBlock: '+ Thêm Block',
    captureFive: 'Chụp 5 tấm / 1 link',
    captureFiveHelp: 'Bật để mỗi link chụp đủ 5 ảnh và giữ nhịp booking ổn định.',
    chrome: 'Chrome',
    postName: 'Tên Post',
    textColumn: 'Text Column',
    imageColumn: 'Image Column',
    resultColumn: 'Result Column',
    profileColumn: 'Profile',
    contentColumn: 'Content',
    linkUrl: 'Link URL',
    driveUrl: 'Drive URL',
    screenshotColumn: 'Screenshot',
    airDate: 'Air Date',
    sheetUrl: 'Sheet URL',
    sheetName: 'Tên Sheet',
    driveFolder: 'Drive Folder ID',
    sheetNameHintLoading: 'Đang tải danh sách sheet...',
    sheetNameHintEmpty: 'Không tìm thấy sheet nào trong file này',
    sheetNameHintCountFmt: count => `Tìm thấy ${count} sheet`,
    browserPort: 'Browser Port',
    startLine: 'Dòng bắt đầu',
    autoLaunchChrome: 'Tự mở Chrome',
    startJob: 'Chạy job',
    overwriteRun: 'Chạy đè',
    stopJob: 'Dừng',
    resumeJob: 'Tiếp tục',
    refreshJobs: 'Làm mới job',
    runQueue: 'Hàng đợi job',
    runQueueHelp: 'Chọn job để theo dõi. Mỗi mode được chạy 1 job cùng lúc.',
    liveLogs: 'Live log',
    errorRows: 'Dòng lỗi',
    selectedJobMeta: 'Job đang chọn',
    monitorKicker: '4. Kết quả & Theo dõi',
    monitorTitle: 'Theo dõi tiến độ và lỗi',
    monitorJob: 'Job',
    monitorProgress: 'Tiến độ',
    monitorErrors: 'Lỗi theo link sheet',
    monitorTable: 'Bảng log xử lý',
    monitorNoJob: 'Chưa chọn job',
    monitorNoErrors: 'Không có lỗi',
    jobFinishedTitle: 'Hoàn tất',
    jobFinishedToastFmt: (name, done, total) => `${name} đã chạy xong ${done}/${total} dòng.`,
    monitorNoLogs: 'Chưa có dữ liệu',
    monitorSuccessFailedFmt: (ok, fail, unavailable = 0) => `Success ${ok} · Failed ${fail} · Không khả dụng ${unavailable}`,
    unavailableLabel: 'Không khả dụng',
    time: 'Time',
    post: 'Post',
    result: 'Kết quả',
    message: 'Thông điệp',
    replay: 'Replay',
    exportLog: 'Xuất log Excel',
    noLogsToExport: 'Chưa có log để xuất',
    replayStartedFmt: row => `Đã tạo replay cho dòng ${row}`,
    noData: 'Chưa có dữ liệu',
    projectsState: 'Lưu các run hoàn tất và xem lại chi tiết',
    groupedProjects: 'Dự án đã lưu',
    completedGroups: 'Sheet đã lưu',
    largestGroup: 'Dự án đang chọn',
    groupedRegistry: 'Thư viện dự án',
    groupSnapshot: 'Chi tiết dự án',
    allProjects: 'Tất cả',
    noProjectsInFilter: 'Chưa có dự án trong nhóm này',
    tasksState: 'Phân rã khối lượng xử lý',
    done: 'Hoàn thành',
    pending: 'Chờ xử lý',
    success: 'Thành công',
    failed: 'Lỗi',
    rowsProcessed: 'số dòng đã xử lý',
    rowsRemaining: 'số dòng còn lại',
    rowsPassed: 'số dòng thành công',
    rowsNeedRetry: 'số dòng cần chạy lại',
    taskDistribution: 'Phân bố tác vụ',
    progressOverTime: 'Tiến độ theo thời gian',
    errorQueue: 'Hàng đợi lỗi',
    currentProgress: 'Tiến độ hiện tại',
    activitiesState: 'Dòng thời gian runtime có phân loại',
    recentTimeline: 'Dòng thời gian gần nhất',
    accessState: 'Admin quản lý người dùng được đăng nhập và mail admin',
    accessMailTitle: 'Mail gửi OTP',
    accessMailHelp: 'Đổi Gmail gửi mã xác nhận ngay trên giao diện admin. App password cũ sẽ được giữ kín và chỉ thay khi bạn nhập mới.',
    accessMailSenderLabel: 'Gmail gửi OTP',
    accessMailFromLabel: 'From email',
    accessMailPasswordLabel: 'App password mới',
    accessMailSave: 'Lưu mail OTP',
    accessMailEdit: 'Chỉnh sửa',
    accessMailHide: 'Ẩn',
    accessMailCurrentFmt: email => `Đang dùng: ${email || 'Chưa cấu hình'}`,
    accessMailPasswordSaved: 'Đã có app password',
    accessMailPasswordMissing: 'Chưa có app password',
    accessMailSourceEnv: 'Đang lấy từ .env',
    accessMailSourceFile: 'Đang lấy từ giao diện',
    accessMailSaved: 'Đã lưu mail gửi OTP',
    accessMailReloaded: 'Đã tải lại cấu hình mail OTP',
    accessEntryTitle: 'Chỉnh sửa Gmail',
    accessEntryHelp: 'Đổi địa chỉ Gmail hoặc role của dòng đang chọn rồi lưu lại.',
    accessEntryEmailLabel: 'Địa chỉ Gmail',
    accessEntryRoleLabel: 'Role',
    accessEntryTypeLabel: 'Loại',
    accessEntryCurrentFmt: email => `Đang sửa: ${email || '-'}`,
    accessEntrySave: 'Lưu chỉnh sửa',
    accessEntryCancel: 'Hủy',
    accessEntrySaved: 'Đã lưu chỉnh sửa Gmail',
    accessEntryInvalid: 'Nhập đúng địa chỉ Gmail hợp lệ',
    accessDirectoryTitle: 'Danh sách người dùng',
    accessDirectoryHelp: 'Lọc nhanh mail theo quyền, trạng thái truy cập và chỉnh role trực tiếp trên từng dòng.',
    accessDirectorySearchPlaceholder: 'Tìm Gmail hoặc trạng thái...',
    accessQuickAdd: '+ Thêm Gmail',
    accessFilterRole: 'Role',
    accessFilterScope: 'Truy cập',
    accessFilterType: 'Loại',
    accessFilterAll: 'Tất cả',
    accessFilterAdmin: 'Admin',
    accessFilterUser: 'User',
    accessFilterInternal: 'Nội bộ',
    accessFilterExternal: 'Ngoại bộ',
    accessYouTag: 'You',
    accessScopeAllowed: 'Được phép',
    accessScopeAdmin: 'Admin',
    accessScopeOpen: 'OTP',
    accessTableEmail: 'Gmail',
    accessTableAccess: 'Truy cập',
    accessTableRole: 'Quyền',
    accessTableType: 'Loại',
    accessTableStatus: 'Trạng thái',
    accessTableUpdated: 'Cập nhật',
    accessTableActions: 'Thao tác',
    accessDirectoryNoMatch: 'Không có mail nào khớp bộ lọc hiện tại',
    accessOpenEntryTitle: 'Cấu hình OTP',
    accessOpenEntrySub: 'Chỉ mail trong danh sách mới được nhập OTP',
    accessOpenEntryMailFmt: email => `Mail gửi OTP: ${email || 'Chưa cấu hình'}`,
    accessAllowedEntrySub: 'Được phép nhập OTP',
    accessAdminEntrySub: 'Giữ quyền quản trị',
    accessStatusActive: 'Đang được phép',
    accessStatusAdmin: 'Toàn quyền quản trị',
    accessStatusOpen: 'OTP giới hạn theo danh sách',
    accessTypeInternal: 'Nội bộ',
    accessTypeExternal: 'Ngoại bộ',
    accessMakeAdmin: 'Lên admin',
    accessMakeUser: 'Hạ user',
    accessRemove: 'Gỡ',
    accessQuickAddInvalid: 'Nhập đúng địa chỉ Gmail để thêm nhanh',
    accessQuickAddDoneFmt: email => `Đã thêm ${email} vào danh sách người dùng`,
    accessSummaryTitle: 'Tóm tắt phân quyền',
    accessSummaryAllowed: 'Danh sách được phép',
    accessSummaryAdmins: 'Danh sách admin',
    accessSummaryUpdated: 'Cập nhật gần nhất',
    accessSummaryCurrentMail: 'Mail đang đăng nhập',
    accessSummaryCurrentRole: 'Role hiện tại',
    accessSummaryOpen: 'Chưa có mail nào trong danh sách',
    accessSummaryEmptyAdmins: 'Chưa có admin nào',
    settingsState: 'Cấu hình đã lưu',
    settingsTitle: 'Thông số screenshot & credentials',
    settingsHelp: 'Các giá trị này sẽ được áp dụng cho các job mới. Bạn cũng có thể dán JSON service account để lưu một lần.',
    accessPolicyTitle: 'Phân quyền truy cập',
    accessPolicyHelp: 'Admin quản lý mail nào được đăng nhập và mail nào có quyền admin.',
    accessAllowedLabel: 'Mail được phép đăng nhập',
    accessAllowedHelp: 'Chỉ mail nằm trong danh sách mới được nhập OTP.',
    accessAdminLabel: 'Mail admin',
    accessAdminHelp: 'Mail admin luôn giữ quyền quản trị và cũng có quyền nhập OTP.',
    saveAccessPolicy: 'Lưu phân quyền',
    reloadAccessPolicy: 'Tải lại phân quyền',
    accessPolicySaved: 'Đã lưu phân quyền',
    accessNotifySentFmt: count => `Đã gửi mail thông báo cho ${count} người dùng`,
    accessNotifyPartialFmt: (sent, failed) => `Đã lưu phân quyền. Gửi mail thành công ${sent}, lỗi ${failed}`,
    accessPolicySelfProtect: 'Không thể tự gỡ quyền admin của chính bạn trong phiên này',
    viewportWidth: 'Viewport width',
    viewportHeight: 'Viewport height',
    pageTimeout: 'Timeout tải trang (ms)',
    waitReadyState: 'Chờ trang ở trạng thái',
    fullPageCapture: 'Chụp full page',
    fullPageHelp: 'Bật nếu bạn muốn giữ toàn bộ chiều dài trang thay vì chỉ phần đang thấy.',
    jsonServiceAccount: 'JSON service account',
    jsonHelp: 'Chọn file service account .json hoặc dán JSON trực tiếp để lưu cục bộ và tự cập nhật credentials path.',
    serviceJsonLabel: 'Chọn file JSON',
    serviceJsonPasteLabel: 'Hoặc dán JSON trực tiếp',
    serviceJsonNoFile: 'Chưa chọn file',
    serviceJsonSelectedFmt: name => `Đã chọn: ${name}`,
    serviceJsonReadError: 'Không đọc được file JSON đã chọn',
    saveSettings: 'Lưu cài đặt',
    reloadSettings: 'Tải lại cài đặt',
    currentConfigSummary: 'Tóm tắt cấu hình hiện tại',
    viewport: 'Viewport',
    timeout: 'Timeout',
    waitMode: 'Chế độ chờ',
    output: 'Ảnh đầu ra',
    serviceAccount: 'Service account',
    sharingNote: 'Cách share quyền',
    sharingHelp: 'Share Google Sheets và thư mục Google Drive cho email service account ở trên với quyền Editor.',
    notSaved: 'Chưa lưu',
    saved: 'Đã lưu',
    fullPage: 'Chụp toàn bộ trang',
    viewportOnly: 'Chỉ chụp phần nhìn thấy',
    noServiceEmail: 'Chưa có email service account',
    fixedCredentials: 'Đã dùng credentials cố định',
    persistent: 'Lưu bền',
    noRunSelected: 'Chưa có job được chọn.',
    noGroupsYet: 'Chưa có dự án nào được lưu',
    noProjectGroup: 'Chưa chọn dự án',
    noErrors: 'Không có lỗi',
    clear: 'sạch',
    noProgressHistory: 'Chưa có lịch sử tiến độ',
    noActivity: 'Chưa có hoạt động nào',
    startOrSelect: 'Hãy chạy hoặc chọn một job để xem sự kiện.',
    latestUpdate: 'Cập nhật gần nhất',
    jobs: 'Jobs',
    detailLabel: 'Chi tiết',
    summaryLabel: 'Tóm tắt',
    openProjectRun: 'Mở trong chạy tác vụ',
    openProjectRunDone: 'Đã mở dự án trong Chạy tác vụ',
    deleteLabel: 'Xóa',
    deleteProjectConfirm: 'Xóa dự án đã lưu này?',
    deleteProjectDone: 'Đã xóa dự án',
    totalScope: 'Tổng phạm vi',
    processed: 'Đã xử lý',
    succeeded: 'Thành công',
    failedLabel: 'Thất bại',
    pendingFailed: 'Chờ / Lỗi',
    eta: 'ETA',
    group: 'Nhóm',
    latestJobMetaFmt: (status, stamp) => `${status} · ${stamp}`,
    overviewTextFmt: (id, done, total) => `Job ${id} đang theo dõi ${done}/${total} tác vụ.`,
    jobsLoadedFmt: count => `${count} job đã tải`,
    rowFmt: row => `Dòng ${row}`,
    jobsCountFmt: count => `${count} jobs`,
  },
  en: {
    searchPlaceholder: 'Search jobs or status...',
    launchChrome: 'Launch Chrome',
    refresh: 'Refresh',
    light: 'Light',
    dark: 'Dark',
    logout: 'Logout',
    roleAdmin: 'Admin',
    roleUser: 'User',
    adminOnly: 'Only admins can use this section',
    overview: 'Overview',
    runs: 'Runs',
    projects: 'Projects',
    tasks: 'Tasks',
    activities: 'Activities',
    access: 'User Management',
    settings: 'Settings',
    state: 'State',
    readyState: 'Ready',
    openRuns: 'Open Runs',
    view: 'View',
    sync: 'Sync',
    syncing: 'Syncing',
    synced: 'Synced',
    syncFailed: 'Failed',
    goToRuns: 'Open Run Center',
    selectedJob: 'Selected job',
    storedJobs: 'Stored jobs',
    successFailed: 'Success / Failed',
    overallProgress: 'Overall progress',
    overviewModeSplit: 'Mode split',
    overviewModeSplitSub: 'Tracked job distribution by mode.',
    overviewModeShareFmt: (count, pct) => `${count} jobs · ${pct}%`,
    overviewModeSplitEmpty: 'No mode data available yet.',
    overviewGreetingLabel: 'Daily greeting',
    overviewGreetingMorning: 'Good morning',
    overviewGreetingAfternoon: 'Good afternoon',
    overviewGreetingEvening: 'Good evening',
    overviewGreetingFallbackName: 'there',
    overviewGreetingSub: 'Keep your runs on track and continue today’s workflow.',
    jobsToday: 'Jobs today',
    avgSuccess: 'Average success rate',
    latestJob: 'Latest job',
    topError: 'Top error',
    overviewTimeline: 'Results by Date',
    overviewTimelineEmpty: 'No date-based run history yet',
    overviewDateFmt: label => `Date ${label}`,
    overviewTimelineJobsBadgeFmt: count => `${count} jobs`,
    overviewTimelineSuccessBadgeFmt: count => `${count} success`,
    overviewTimelineFailedBadgeFmt: count => `${count} failed`,
    overviewTimelineUnavailableBadgeFmt: count => `${count} unavailable`,
    overviewCompletedLegend: 'Completed',
    overviewFailedLegend: 'Errors',
    overviewUnavailableLegend: 'Unavailable',
    createdLast24h: 'created in the last 24h',
    acrossTracked: 'across tracked jobs',
    noRecentRun: 'no recent run',
    noRecurring: 'no recurring issues',
    runSummary: 'Run Summary',
    overviewClean: 'Overview stays clean. Running tools live in the Runs tab.',
    runConfig: 'Run Config',
    runConfigHelp: 'Share Editor access for the Sheet and Drive folder before running.',
    runShareLabel: 'Share Sheet & Drive folder with (Editor permission):',
    overwriteRunHelp: 'Always rerun and replace previous results.',
    runMode: 'Run mode',
    columnMapping: 'Column Mapping',
    seeding: 'Seeding',
    booking: 'Booking',
    scan: 'Scan',
    runModeSeedingHelp: 'Seeding uses the standard posting flow and screenshot upload columns.',
    runModeBookingHelp: 'Booking is tuned for booking runs and repeated capture workflows.',
    runModeScanHelp: 'Scan skips Chrome when possible and uses the default scan columns.',
    addBlock: '+ Add Block',
    captureFive: 'Capture 5 images / link',
    captureFiveHelp: 'Enable this to capture all 5 images per link for booking runs.',
    chrome: 'Chrome',
    postName: 'Post Name',
    textColumn: 'Text Column',
    imageColumn: 'Image Column',
    resultColumn: 'Result Column',
    profileColumn: 'Profile',
    contentColumn: 'Content',
    linkUrl: 'Link URL',
    driveUrl: 'Drive URL',
    screenshotColumn: 'Screenshot',
    airDate: 'Air Date',
    sheetUrl: 'Sheet URL',
    sheetName: 'Sheet Name',
    driveFolder: 'Drive Folder ID',
    sheetNameHintLoading: 'Loading sheet names...',
    sheetNameHintEmpty: 'No sheets found in this spreadsheet',
    sheetNameHintCountFmt: count => `${count} sheets found`,
    browserPort: 'Browser Port',
    startLine: 'Start Line',
    autoLaunchChrome: 'Auto Launch Chrome',
    startJob: 'Start Job',
    overwriteRun: 'Overwrite',
    stopJob: 'Pause',
    resumeJob: 'Resume',
    refreshJobs: 'Refresh Jobs',
    runQueue: 'Run Queue',
    runQueueHelp: 'Select a job to monitor. One active job is allowed per mode.',
    liveLogs: 'Live Logs',
    errorRows: 'Error Rows',
    selectedJobMeta: 'Selected Job',
    monitorKicker: '4. Result & Monitor',
    monitorTitle: 'Track progress and errors',
    monitorJob: 'Job',
    monitorProgress: 'Progress',
    monitorErrors: 'Errors by sheet link',
    monitorTable: 'Processing log table',
    monitorNoJob: 'No job selected',
    monitorNoErrors: 'No errors',
    jobFinishedTitle: 'Completed',
    jobFinishedToastFmt: (name, done, total) => `${name} finished ${done}/${total} rows.`,
    monitorNoLogs: 'No data yet',
    monitorSuccessFailedFmt: (ok, fail, unavailable = 0) => `Success ${ok} · Failed ${fail} · Unavailable ${unavailable}`,
    unavailableLabel: 'Unavailable',
    time: 'Time',
    post: 'Post',
    result: 'Result',
    message: 'Message',
    replay: 'Replay',
    exportLog: 'Export Excel Log',
    noLogsToExport: 'No logs to export',
    replayStartedFmt: row => `Replay job queued for row ${row}`,
    noData: 'No data',
    projectsState: 'Store completed runs and reopen their details',
    groupedProjects: 'Saved Projects',
    completedGroups: 'Saved Sheets',
    largestGroup: 'Selected Project',
    groupedRegistry: 'Project Library',
    groupSnapshot: 'Project Detail',
    allProjects: 'All',
    noProjectsInFilter: 'No projects in this category',
    tasksState: 'Workload breakdown',
    done: 'Done',
    pending: 'Pending',
    success: 'Success',
    failed: 'Failed',
    rowsProcessed: 'rows processed',
    rowsRemaining: 'remaining rows',
    rowsPassed: 'rows passed',
    rowsNeedRetry: 'rows need retry',
    taskDistribution: 'Task Distribution',
    progressOverTime: 'Progress Over Time',
    errorQueue: 'Error Queue',
    currentProgress: 'Current Progress',
    activitiesState: 'Latest runtime events with severity',
    recentTimeline: 'Recent Timeline',
    accessState: 'Admins manage user access and admin emails',
    accessMailTitle: 'OTP Sender',
    accessMailHelp: 'Change the Gmail account that sends login codes from the admin UI. The old app password stays hidden and is only replaced when you enter a new one.',
    accessMailSenderLabel: 'Gmail sender',
    accessMailFromLabel: 'From email',
    accessMailPasswordLabel: 'New app password',
    accessMailSave: 'Save OTP Mail',
    accessMailEdit: 'Edit',
    accessMailHide: 'Hide',
    accessMailCurrentFmt: email => `Current sender: ${email || 'Not configured'}`,
    accessMailPasswordSaved: 'App password saved',
    accessMailPasswordMissing: 'App password missing',
    accessMailSourceEnv: 'Using .env source',
    accessMailSourceFile: 'Using UI override',
    accessMailSaved: 'OTP sender saved',
    accessMailReloaded: 'OTP sender reloaded',
    accessEntryTitle: 'Edit Gmail',
    accessEntryHelp: 'Change the selected Gmail address or role, then save it.',
    accessEntryEmailLabel: 'Gmail address',
    accessEntryRoleLabel: 'Role',
    accessEntryTypeLabel: 'Type',
    accessEntryCurrentFmt: email => `Editing: ${email || '-'}`,
    accessEntrySave: 'Save changes',
    accessEntryCancel: 'Cancel',
    accessEntrySaved: 'Gmail changes saved',
    accessEntryInvalid: 'Enter a valid Gmail address',
    accessDirectoryTitle: 'User Directory',
    accessDirectoryHelp: 'Filter Gmail accounts by role and access state, then change permission per row.',
    accessDirectorySearchPlaceholder: 'Search Gmail or state...',
    accessQuickAdd: '+ Add Gmail',
    accessFilterRole: 'Role',
    accessFilterScope: 'Access',
    accessFilterType: 'Type',
    accessFilterAll: 'All',
    accessFilterAdmin: 'Admin',
    accessFilterUser: 'User',
    accessFilterInternal: 'Internal',
    accessFilterExternal: 'External',
    accessYouTag: 'You',
    accessScopeAllowed: 'Allowed',
    accessScopeAdmin: 'Admin',
    accessScopeOpen: 'OTP',
    accessTableEmail: 'Gmail',
    accessTableAccess: 'Access',
    accessTableRole: 'Permission',
    accessTableType: 'Type',
    accessTableStatus: 'Status',
    accessTableUpdated: 'Updated',
    accessTableActions: 'Actions',
    accessDirectoryNoMatch: 'No Gmail matches the current filters',
    accessOpenEntryTitle: 'OTP Settings',
    accessOpenEntrySub: 'Only listed emails can request OTP',
    accessOpenEntryMailFmt: email => `OTP sender: ${email || 'Not configured'}`,
    accessAllowedEntrySub: 'Can request OTP',
    accessAdminEntrySub: 'Keeps admin control',
    accessStatusActive: 'Allowed',
    accessStatusAdmin: 'Admin control',
    accessStatusOpen: 'OTP restricted by list',
    accessTypeInternal: 'Internal',
    accessTypeExternal: 'External',
    accessMakeAdmin: 'Make admin',
    accessMakeUser: 'Make user',
    accessRemove: 'Remove',
    accessQuickAddInvalid: 'Enter a valid Gmail address to quick-add',
    accessQuickAddDoneFmt: email => `Added ${email} to the user list`,
    accessSummaryTitle: 'Access summary',
    accessSummaryAllowed: 'Allowed list',
    accessSummaryAdmins: 'Admin list',
    accessSummaryUpdated: 'Last updated',
    accessSummaryCurrentMail: 'Current signed-in email',
    accessSummaryCurrentRole: 'Current role',
    accessSummaryOpen: 'No email has been added yet',
    accessSummaryEmptyAdmins: 'No admin email yet',
    settingsState: 'Saved configuration',
    settingsTitle: 'Screenshot & credentials',
    settingsHelp: 'These values are reused by future jobs. You can also paste service account JSON here and save it once.',
    accessPolicyTitle: 'Access control',
    accessPolicyHelp: 'Admins manage which emails can log in and which emails keep admin permission.',
    accessAllowedLabel: 'Allowed emails',
    accessAllowedHelp: 'Only emails in the list can request OTP.',
    accessAdminLabel: 'Admin emails',
    accessAdminHelp: 'Admin emails always keep admin permission and can request OTP.',
    saveAccessPolicy: 'Save Access',
    reloadAccessPolicy: 'Reload Access',
    accessPolicySaved: 'Access control saved',
    accessNotifySentFmt: count => `Notification email sent to ${count} users`,
    accessNotifyPartialFmt: (sent, failed) => `Access control saved. Email sent: ${sent}, failed: ${failed}`,
    accessPolicySelfProtect: 'You cannot remove your own admin right in this session',
    viewportWidth: 'Viewport width',
    viewportHeight: 'Viewport height',
    pageTimeout: 'Page timeout (ms)',
    waitReadyState: 'Wait ready state',
    fullPageCapture: 'Full page capture',
    fullPageHelp: 'Enable this if you want to keep the entire page length instead of only the visible area.',
    jsonServiceAccount: 'JSON service account',
    jsonHelp: 'Upload a service account .json file or paste the JSON directly to save it locally and update the credentials path automatically.',
    serviceJsonLabel: 'Choose JSON file',
    serviceJsonPasteLabel: 'Or paste JSON directly',
    serviceJsonNoFile: 'No file selected',
    serviceJsonSelectedFmt: name => `Selected: ${name}`,
    serviceJsonReadError: 'Unable to read the selected JSON file',
    saveSettings: 'Save Settings',
    reloadSettings: 'Reload Settings',
    currentConfigSummary: 'Current config summary',
    viewport: 'Viewport',
    timeout: 'Timeout',
    waitMode: 'Wait mode',
    output: 'Output',
    serviceAccount: 'Service account',
    sharingNote: 'Sharing note',
    sharingHelp: 'Share Google Sheets and Drive folder with the service account email above using Editor permission.',
    notSaved: 'Not saved',
    saved: 'Saved',
    fullPage: 'Full page',
    viewportOnly: 'Viewport only',
    noServiceEmail: 'No service account email',
    fixedCredentials: 'Using fixed credentials',
    persistent: 'Persistent',
    noRunSelected: 'No run selected.',
    noGroupsYet: 'No saved projects yet',
    noProjectGroup: 'No project selected',
    noErrors: 'No errors',
    clear: 'clear',
    noProgressHistory: 'No progress history yet',
    noActivity: 'No activity yet',
    startOrSelect: 'Start or select a job to see events.',
    latestUpdate: 'Latest update',
    jobs: 'Jobs',
    detailLabel: 'Detail',
    summaryLabel: 'Summary',
    openProjectRun: 'Open in Runs',
    openProjectRunDone: 'Project opened in Runs',
    deleteLabel: 'Delete',
    deleteProjectConfirm: 'Delete this saved project?',
    deleteProjectDone: 'Project deleted',
    totalScope: 'Total scope',
    processed: 'Processed',
    succeeded: 'Succeeded',
    failedLabel: 'Failed',
    pendingFailed: 'Pending / Failed',
    eta: 'ETA',
    group: 'Group',
    latestJobMetaFmt: (status, stamp) => `${status} · ${stamp}`,
    overviewTextFmt: (id, done, total) => `Job ${id} is tracking ${done}/${total} tasks.`,
    jobsLoadedFmt: count => `${count} jobs loaded`,
    rowFmt: row => `Row ${row}`,
    jobsCountFmt: count => `${count} jobs`,
  }
};

function t(key) {
  return (I18N[currentLang] && I18N[currentLang][key]) || (I18N.en[key] ?? key);
}

function getRoleLabel(role = authState.role) {
  return String(role || '').toLowerCase() === 'admin' ? t('roleAdmin') : t('roleUser');
}

function deriveGreetingName(email = authState.email) {
  const local = String(email || '').split('@')[0] || '';
  const parts = local.split(/[._-]+/).map(part => part.replace(/\\d+/g, '').trim()).filter(Boolean);
  const base = parts[0] || '';
  if (!base) return t('overviewGreetingFallbackName');
  return base.charAt(0).toUpperCase() + base.slice(1);
}

function deriveGreetingInitials(email = authState.email) {
  const local = String(email || '').split('@')[0] || '';
  const parts = local.split(/[._-]+/).map(part => part.replace(/\\d+/g, '').trim()).filter(Boolean);
  const initials = (parts.slice(0, 2).map(part => part.charAt(0).toUpperCase()).join('') || 'EV').slice(0, 2);
  return initials || 'EV';
}

function getGreetingTextByHour(date = new Date()) {
  const hour = Number(date.getHours());
  if (hour < 12) return t('overviewGreetingMorning');
  if (hour < 18) return t('overviewGreetingAfternoon');
  return t('overviewGreetingEvening');
}

function renderOverviewGreeting() {
  const kicker = document.getElementById('ovGreetingKicker');
  if (kicker) kicker.textContent = t('overviewGreetingLabel');
  const title = document.getElementById('ovGreetingTitle');
  if (title) title.textContent = `${getGreetingTextByHour()}, ${deriveGreetingName()}`;
  const sub = document.getElementById('ovGreetingSub');
  if (sub) sub.textContent = t('overviewGreetingSub');
  const avatar = document.getElementById('ovGreetingAvatar');
  if (avatar) avatar.textContent = deriveGreetingInitials();
  const email = document.getElementById('ovGreetingEmail');
  if (email) email.textContent = authState.email || '-';
  const role = document.getElementById('ovGreetingRole');
  if (role) {
    role.textContent = getRoleLabel();
    role.className = `auth-role auth-role-${authState.role || 'user'} overview-greeting-role`;
  }
}

function isAdminUser() {
  return !!authState.isAdmin;
}

function getRunModeLabel(mode) {
  return t(String(mode || 'seeding').toLowerCase());
}

function formatRunTitle(mode = currentRunMode) {
  return getRunModeLabel(mode);
}

function formatRunConfigTitle(mode = currentRunMode) {
  return t('runConfig');
}

function sanitizeMappingBlockForMode(mode, block, index = 1) {
  const key = String(mode || 'seeding').toLowerCase();
  const next = {
    ...defaultMappingBlock(key, index),
    ...(block || {}),
    start_line: Number(block?.start_line || 4),
    mode: key,
  };
  if (key === 'seeding') {
    next.col_profile = '';
    next.col_content = '';
  } else if (key === 'scan') {
    next.col_profile = '';
    next.col_screenshot = '';
    next.col_air_date = '';
  }
  return next;
}

function getRunModeHelp(mode) {
  if (mode === 'booking') return t('runModeBookingHelp');
  if (mode === 'scan') return t('runModeScanHelp');
  return t('runModeSeedingHelp');
}

function defaultMappingBlock(mode, index = 1) {
  const blockIndex = Number(index || 1);
  if (mode === 'scan') {
    return {
      name: `Scan ${blockIndex}`,
      start_line: 4,
      col_profile: '',
      col_content: 'E',
      col_url: 'F',
      col_drive: 'G',
      col_screenshot: '',
      col_air_date: '',
      fixed_air_date: '',
      manual_link: '',
      mode: 'scan'
    };
  }
  const isBooking = mode === 'booking';
  return {
    name: `Post ${blockIndex}`,
    start_line: 4,
    col_profile: isBooking ? 'B' : '',
    col_content: isBooking ? 'I' : '',
    col_url: 'K',
    col_drive: 'L',
    col_screenshot: 'J',
    col_air_date: '',
    fixed_air_date: '',
    manual_link: '',
    mode: isBooking ? 'booking' : 'seeding'
  };
}

function ensureMappingBlocks(mode) {
  const key = String(mode || 'seeding').toLowerCase();
  if (!Array.isArray(currentMappingBlocksByMode[key]) || !currentMappingBlocksByMode[key].length) {
    currentMappingBlocksByMode[key] = [defaultMappingBlock(key, 1)];
  } else {
    currentMappingBlocksByMode[key] = currentMappingBlocksByMode[key].map((block, index) => sanitizeMappingBlockForMode(key, block, index + 1));
  }
  return currentMappingBlocksByMode[key];
}

function mappingFieldsForMode(mode) {
  if (mode === 'scan') {
    return [
      { key: 'name', label: t('postName') },
      { key: 'col_content', label: t('textColumn') },
      { key: 'col_url', label: t('imageColumn') },
      { key: 'col_drive', label: t('resultColumn') },
      { key: 'start_line', label: t('startLine'), type: 'number' },
    ];
  }
  if (mode === 'seeding') {
    return [
      { key: 'name', label: t('postName') },
      { key: 'col_air_date', label: t('airDate') },
      { key: 'col_url', label: t('linkUrl') },
      { key: 'col_drive', label: t('driveUrl') },
      { key: 'col_screenshot', label: t('screenshotColumn') },
      { key: 'start_line', label: t('startLine'), type: 'number' },
    ];
  }
  return [
    { key: 'name', label: t('postName') },
    { key: 'col_air_date', label: t('airDate') },
    { key: 'col_profile', label: t('profileColumn') },
    { key: 'col_content', label: t('contentColumn') },
    { key: 'col_url', label: t('linkUrl') },
    { key: 'col_drive', label: t('driveUrl') },
    { key: 'col_screenshot', label: t('screenshotColumn') },
    { key: 'start_line', label: t('startLine'), type: 'number' },
  ];
}

function updateMappingBlock(mode, index, key, value) {
  const blocks = ensureMappingBlocks(mode);
  if (!blocks[index]) return;
  blocks[index][key] = key === 'start_line' ? Number(value || 4) : String(value || '');
}

function removeMappingBlock(index) {
  const blocks = ensureMappingBlocks(currentRunMode);
  if (blocks.length <= 1) return;
  blocks.splice(index, 1);
  renderMappingEditor();
}

function addMappingBlock() {
  const blocks = ensureMappingBlocks(currentRunMode);
  blocks.push(defaultMappingBlock(currentRunMode, blocks.length + 1));
  pendingMappingScrollMode = currentRunMode;
  pendingMappingHighlightIndex = blocks.length - 1;
  renderMappingEditor();
}

function toggleCaptureFivePerLink(checked) {
  captureFivePerLink = !!checked;
}

function getModeBasePort(mode = currentRunMode) {
  return Number(BROWSER_PORT_BY_MODE[String(mode || 'seeding').toLowerCase()] || BROWSER_PORT_BY_MODE.seeding);
}

function getChromePortForBlock(index, mode = currentRunMode) {
  const basePort = getModeBasePort(mode);
  return Number(index) <= 0 ? basePort : basePort + 100 + Number(index);
}

function openAirDatePicker(mode, index) {
  const picker = document.getElementById(`air_date_picker_${mode}_${index}`);
  if (!picker) return;
  if (typeof picker.showPicker === 'function') picker.showPicker();
  else picker.click();
}

function applyAirDate(mode, index, value) {
  updateMappingBlock(mode, index, 'col_air_date', value || '');
  renderMappingEditor();
}

function isLocalWebHost() {
  const host = String(window.location.hostname || '').toLowerCase();
  return host === '127.0.0.1' || host === 'localhost';
}

function launchChromeViaLocalProtocol(index) {
  const blockIndex = Number(index) || 0;
  const port = getChromePortForBlock(blockIndex, currentRunMode);
  const href = `tool-evidence://launch?mode=${encodeURIComponent(currentRunMode)}&block=${blockIndex}&port=${port}`;
  const frame = document.createElement('iframe');
  frame.style.display = 'none';
  frame.src = href;
  document.body.appendChild(frame);
  window.setTimeout(() => frame.remove(), 1500);
  return { href, port };
}

async function launchChromeBlock(index) {
  try {
    if (!isLocalWebHost()) {
      const local = launchChromeViaLocalProtocol(index);
      setStatus(`?? g?i l?nh m? Chrome ${local.port} t?i m?y c?a b?n`, 'running');
      return;
    }
    const out = await req(`/api/chrome/launch-block/${Number(index)}?run_mode=${encodeURIComponent(currentRunMode)}`, { method: 'POST' });
    setStatus(out.message || 'Chrome launch requested', 'running');
  } catch (e) {
    alert(e.message);
  }
}

function renderMappingEditor() {
  const blocks = ensureMappingBlocks(currentRunMode);
  const fields = mappingFieldsForMode(currentRunMode);
  const host = document.getElementById('mappingBlocks');
  const addButton = document.getElementById('mappingAddButton');
  if (addButton) addButton.textContent = t('addBlock');
  if (!host) return;
  if (currentRunMode === 'scan') {
    host.innerHTML = `<div class="mapping-scan-grid">${blocks.map((block, index) => {
      const title = block.name || `Scan ${index + 1}`;
      const rows = fields.map(field => {
        const value = block[field.key] ?? '';
        const inputType = field.type === 'number' ? 'number' : 'text';
        return `<div class="mapping-label">${esc(field.label)}</div><div><input class="mapping-input" type="${inputType}" value="${esc(value)}" oninput="updateMappingBlock('${currentRunMode}', ${index}, '${field.key}', this.value)" /></div>`;
      }).join('');
      return `<section class="mapping-block">
        <div class="mapping-block-head">
          <div class="mapping-block-title">${esc(title)}</div>
          ${blocks.length > 1 ? `<button class="btn red mapping-remove" type="button" onclick="removeMappingBlock(${index})">x</button>` : ''}
        </div>
        <div class="mapping-block-grid">${rows}</div>
      </section>`;
    }).join('')}</div>`;
  } else if (currentRunMode === 'seeding') {
    host.innerHTML = `<div class="mapping-seeding-row">${blocks.map((block, index) => {
      const blockClass = pendingMappingScrollMode === currentRunMode && pendingMappingHighlightIndex === index
        ? 'mapping-block mapping-block-new'
        : 'mapping-block';
      const rows = fields.map(field => {
        const value = block[field.key] ?? '';
        if (field.key === 'col_air_date') {
          return `<div class="mapping-label">${esc(field.label)}</div><div class="mapping-field-combo"><input class="mapping-input" type="text" value="${esc(value)}" oninput="updateMappingBlock('${currentRunMode}', ${index}, '${field.key}', this.value)" /><button class="btn mapping-icon-btn" type="button" onclick="openAirDatePicker('${currentRunMode}', ${index})">...</button><input id="air_date_picker_${currentRunMode}_${index}" type="date" style="position:absolute;opacity:0;pointer-events:none;width:1px;height:1px" onchange="applyAirDate('${currentRunMode}', ${index}, this.value)" /></div>`;
        }
        const inputType = field.type === 'number' ? 'number' : 'text';
        if (field.key === 'name') {
          return `<div class="mapping-label">${esc(field.label)}</div><div class="mapping-field-combo"><input class="mapping-input" type="${inputType}" value="${esc(value)}" oninput="updateMappingBlock('${currentRunMode}', ${index}, '${field.key}', this.value)" />${blocks.length > 1 ? `<button class="btn red mapping-remove" type="button" onclick="removeMappingBlock(${index})">x</button>` : ''}</div>`;
        }
        return `<div class="mapping-label">${esc(field.label)}</div><div><input class="mapping-input" type="${inputType}" value="${esc(value)}" oninput="updateMappingBlock('${currentRunMode}', ${index}, '${field.key}', this.value)" /></div>`;
      }).join('');
      const chromeRow = `<div class="mapping-label">${esc(t('chrome'))}</div><div><button class="btn mapping-chrome-btn" type="button" onclick="launchChromeBlock(${index})">${esc(`${t('chrome')} ${getChromePortForBlock(index, currentRunMode)}`)}</button></div>`;
      return `<section class="${blockClass}"><div class="mapping-block-grid">${rows}${chromeRow}</div></section>`;
    }).join('')}</div>`;
  } else {
    const colTemplate = `132px repeat(${blocks.length}, minmax(110px, 1fr))`;
    const nameRow = [
      `<div class="mapping-matrix-label">${esc(t('postName'))}</div>`,
      ...blocks.map((block, index) => {
        const title = block.name || `Post ${index + 1}`;
        return `<div class="mapping-matrix-name">
          <input class="mapping-input" type="text" value="${esc(title)}" oninput="updateMappingBlock('${currentRunMode}', ${index}, 'name', this.value)" />
          ${blocks.length > 1 ? `<button class="btn red mapping-remove" type="button" onclick="removeMappingBlock(${index})">x</button>` : ''}
        </div>`;
      })
    ].join('');
    const rows = fields
      .filter(field => field.key !== 'name')
      .map(field => {
        const cells = blocks.map((block, index) => {
          const value = block[field.key] ?? '';
          const inputType = field.type === 'number' ? 'number' : 'text';
          if (field.key === 'col_air_date') {
            return `<div class="mapping-field-combo"><input class="mapping-input" type="text" value="${esc(value)}" oninput="updateMappingBlock('${currentRunMode}', ${index}, '${field.key}', this.value)" /><button class="btn mapping-icon-btn" type="button" onclick="openAirDatePicker('${currentRunMode}', ${index})">...</button><input id="air_date_picker_${currentRunMode}_${index}" type="date" style="position:absolute;opacity:0;pointer-events:none;width:1px;height:1px" onchange="applyAirDate('${currentRunMode}', ${index}, this.value)" /></div>`;
          }
          return `<div><input class="mapping-input" type="${inputType}" value="${esc(value)}" oninput="updateMappingBlock('${currentRunMode}', ${index}, '${field.key}', this.value)" /></div>`;
        }).join('');
        return `<div class="mapping-matrix-label">${esc(field.label)}</div>${cells}`;
      }).join('');
    const chromeRow = [
      `<div class="mapping-matrix-label">${esc(t('chrome'))}</div>`,
      ...blocks.map((_, index) => `<div><button class="btn mapping-chrome-btn" type="button" onclick="launchChromeBlock(${index})">${esc(`${t('chrome')} ${getChromePortForBlock(index, currentRunMode)}`)}</button></div>`)
    ].join('');
    host.innerHTML = `<section class="mapping-matrix"><div class="mapping-matrix-grid" style="grid-template-columns:${colTemplate}">${nameRow}${rows}${chromeRow}</div></section>`;
  }
  const addRow = document.querySelector('.mapping-add-row');
  if (addRow) {
    addRow.classList.toggle('booking', currentRunMode === 'booking');
    const bookingExtra = currentRunMode === 'booking'
      ? `<label class="mapping-toggle-card">
          <span class="mapping-toggle-copy">
            <span class="mapping-toggle-title">${esc(t('captureFive'))}</span>
            <span class="mapping-toggle-help">${esc(t('captureFiveHelp'))}</span>
          </span>
          <span class="mapping-toggle-switch">
            <input type="checkbox" ${captureFivePerLink ? 'checked' : ''} onchange="toggleCaptureFivePerLink(this.checked)" />
            <span class="mapping-toggle-slider"></span>
          </span>
        </label>`
      : '';
    addRow.innerHTML = `<button id="mappingAddButton" class="btn" type="button" onclick="addMappingBlock()">${esc(t('addBlock'))}</button>${bookingExtra}`;
  }
  if (pendingMappingScrollMode === currentRunMode && pendingMappingHighlightIndex >= 0) {
    const row = host.querySelector('.mapping-seeding-row');
    const target = row && row.children ? row.children[pendingMappingHighlightIndex] : null;
    requestAnimationFrame(() => {
      if (row && target) {
        row.scrollTo({ left: target.offsetLeft - 8, behavior: 'smooth' });
      }
      pendingMappingScrollMode = '';
      pendingMappingHighlightIndex = -1;
    });
  } else {
    pendingMappingScrollMode = '';
    pendingMappingHighlightIndex = -1;
  }
}

function applyRunModeUI() {
  ['seeding', 'booking', 'scan'].forEach(mode => {
    const node = document.getElementById('run_mode_' + mode);
    if (node) {
      node.classList.toggle('active', currentRunMode === mode);
      node.textContent = t(mode);
    }
  });
  const runTitle = document.getElementById('runTitleText');
  if (runTitle) runTitle.textContent = formatRunTitle(currentRunMode);
  const runsGroup = document.getElementById('runs_group');
  if (runsGroup) runsGroup.classList.toggle('open', document.getElementById('view-runs')?.classList.contains('active'));
  renderMappingEditor();
}

function applyLanguage() {
  document.documentElement.lang = currentLang === 'vi' ? 'vi' : 'en';
  const langToggle = document.getElementById('lang_toggle');
  if (langToggle) {
    langToggle.textContent = currentLang === 'vi' ? 'VN' : 'EN';
    langToggle.title = currentLang === 'vi' ? 'Switch to English' : 'Chuyen sang tieng Viet';
    langToggle.setAttribute('aria-label', currentLang === 'vi' ? 'Switch to English' : 'Chuyen sang tieng Viet');
  }
  const themeToggle = document.getElementById('theme_toggle');
  if (themeToggle) {
    const nextLabel = currentTheme === 'dark' ? t('light') : t('dark');
    themeToggle.title = `${t('light')} / ${t('dark')}`;
    themeToggle.setAttribute('aria-label', `${t('light')} / ${t('dark')} (${nextLabel})`);
  }
  const topSearch = document.getElementById('top_search');
  if (topSearch) topSearch.placeholder = t('searchPlaceholder');
  const launchChromeBtn = document.getElementById('btn_launch_chrome');
  if (launchChromeBtn) launchChromeBtn.textContent = t('launchChrome');
  const refreshJobsBtn = document.getElementById('btn_refresh_jobs');
  if (refreshJobsBtn) refreshJobsBtn.textContent = t('refresh');

  const menuMap = { overview: 'overview', runs: 'runs', projects: 'projects', tasks: 'tasks', activities: 'activities', access: 'access', settings: 'settings' };
  Object.entries(menuMap).forEach(([view, key]) => {
    const node = document.querySelector(`.side-btn[data-view="${view}"] span:last-child`);
    if (node) node.textContent = t(key);
  });

  const setText = (selector, value) => {
    const el = document.querySelector(selector);
    if (el) el.textContent = value;
  };
  const setNthText = (selector, index, value) => {
    const nodes = document.querySelectorAll(selector);
    if (nodes[index]) nodes[index].textContent = value;
  };
  const setFirstChildText = (selector, value) => {
    const el = document.querySelector(selector);
    if (el && el.childNodes && el.childNodes[0]) el.childNodes[0].textContent = value;
  };
  setText('#logoutLabel', t('logout'));
  setText('#authRoleBadge', getRoleLabel());
  setText('#view-overview .h1', t('overview'));
  setText('#runTitleText', formatRunTitle());
  setText('#view-projects .h1', t('projects'));
  setText('#view-activities .h1', t('activities'));
  setText('#view-access .h1', t('access'));
  setText('#view-settings .h1', t('settings'));
  setText('#view-projects .state', t('projectsState'));
  setText('#view-activities .state', t('activitiesState'));
  setText('#view-access .state', t('accessState'));
  setText('#view-settings .state', t('settingsState'));
  setText('#view-runs .state', t('runConfigHelp'));

  setText('#ovSavedProjectsLabel', t('groupedProjects'));
  setText('#ovSavedSheetsLabel', t('completedGroups'));
  setText('#ovSelectedProjectLabel', t('largestGroup'));
  setText('#ovHistoryTitle', t('overviewTimeline'));
  setText('#ovLegendSuccess', t('overviewCompletedLegend'));
  setText('#ovLegendFailed', t('overviewFailedLegend'));
  setText('#ovLegendUnavailable', t('overviewUnavailableLegend'));
  setText('#ovModeSplitTitle', t('overviewModeSplit'));
  setText('#ovModeSplitSub', t('overviewModeSplitSub'));
  setText('#overviewRunCtaLabel', t('goToRuns'));
  setText('#runSummaryTitle', t('runSummary'));
  setText('#runSummarySub', t('overviewClean'));
  setText('#view-overview .item:nth-child(1) .t', t('selectedJob'));
  setText('#view-overview .item:nth-child(1) .btn', t('openRuns'));
  setText('#view-overview .item:nth-child(2) .t', t('storedJobs'));
  setText('#overviewSyncLabel', t('sync'));
  setText('#view-overview .item:nth-child(3) .t', t('successFailed'));
  setText('#view-overview .item:nth-child(3) .btn', t('view'));
  setText('#view-overview .mini > div span:first-child', t('overallProgress'));
  renderOverviewGreeting();
  setNthText('#view-overview .day', 0, t('totalScope'));
  setNthText('#view-overview .day', 1, t('done'));
  setNthText('#view-overview .day', 2, t('success'));
  setNthText('#view-overview .day', 3, t('failed'));
  setNthText('#view-overview .day', 4, t('jobs'));

  setText('#view-runs .headline .state', t('runConfigHelp'));
  setText('#runShareLabel', t('runShareLabel'));
  applyRunModeUI();
  setText('label[for="sheet_url"]', t('sheetUrl'));
  setText('label[for="sheet_name"]', t('sheetName'));
  setText('label[for="drive_id"]', t('driveFolder'));
  setText('#startJobLabel', t('startJob'));
  setText('#overwriteRunLabel', t('overwriteRun'));
  setText('#overwriteRunHelp', t('overwriteRunHelp'));
  setText('#runMonitorKicker', t('monitorKicker'));
  setText('#runMonitorJobLabel', t('monitorJob'));
  setText('#runMonitorProgressLabel', t('monitorProgress'));
  setText('#runMonitorErrorLabel', t('monitorErrors'));
  setText('#runMonitorTableTitle', t('monitorTable'));
  setText('#runMonitorHeadTime', t('time'));
  setText('#runMonitorHeadPost', t('post'));
  setText('#runMonitorHeadResult', t('result'));
  setText('#runMonitorHeadMessage', t('message'));
  setText('#runMonitorHeadReplay', t('replay'));
  setText('#exportLogLabel', t('exportLog'));
  updateRunActionButtons();

  setText('#view-projects .cards-3 .card:nth-child(1) .k', t('groupedProjects'));
  setText('#view-projects .cards-3 .card:nth-child(2) .k', t('completedGroups'));
  setText('#view-projects .cards-3 .card:nth-child(3) .k', t('largestGroup'));
  setText('#projectsListTitle', t('groupedRegistry'));
  setText('#projectsSnapshotTitle', t('groupSnapshot'));

  setText('#view-activities .card > div:first-child', t('recentTimeline'));

  setText('#accessMailTitle', t('accessMailTitle'));
  setText('#accessMailHelp', t('accessMailHelp'));
  setText('#accessMailSenderLabel', t('accessMailSenderLabel'));
  setText('#accessMailFromLabel', t('accessMailFromLabel'));
  setText('#accessMailPasswordLabel', t('accessMailPasswordLabel'));
  setText('#saveMailConfigButton', t('accessMailSave'));
  setText('#hideMailConfigButton', t('accessMailHide'));
  setText('#accessEntryTitle', t('accessEntryTitle'));
  setText('#accessEntryHelp', t('accessEntryHelp'));
  setText('#accessEntryEmailLabel', t('accessEntryEmailLabel'));
  setText('#accessEntryRoleLabel', t('accessEntryRoleLabel'));
  setText('#accessEntryTypeLabel', t('accessEntryTypeLabel'));
  setText('#accessEntryCancelTop', t('accessEntryCancel'));
  setText('#accessEntryCancelButton', t('accessEntryCancel'));
  setText('#accessEntrySaveButton', t('accessEntrySave'));
  const accessEntryRole = document.getElementById('access_entry_role');
  if (accessEntryRole?.options?.[0]) accessEntryRole.options[0].text = t('roleUser');
  if (accessEntryRole?.options?.[1]) accessEntryRole.options[1].text = t('roleAdmin');
  const accessEntryType = document.getElementById('access_entry_type');
  if (accessEntryType?.options?.[0]) accessEntryType.options[0].text = t('accessTypeInternal');
  if (accessEntryType?.options?.[1]) accessEntryType.options[1].text = t('accessTypeExternal');
  setText('#accessDirectoryTitle', t('accessDirectoryTitle'));
  setText('#accessDirectoryHelp', t('accessDirectoryHelp'));
  setText('#accessFilterRoleLabel', t('accessFilterRole'));
  setText('#accessFilterScopeLabel', t('accessFilterScope'));
  setText('#accessFilterTypeLabel', t('accessFilterType'));
  setText('#accessRoleFilterAll', t('accessFilterAll'));
  setText('#accessRoleFilterAdmin', t('accessFilterAdmin'));
  setText('#accessRoleFilterUser', t('accessFilterUser'));
  setText('#accessScopeFilterAll', t('accessFilterAll'));
  setText('#accessScopeFilterAllowed', t('accessScopeAllowed'));
  setText('#accessScopeFilterAdmin', t('accessScopeAdmin'));
  setText('#accessScopeFilterOpen', t('accessScopeOpen'));
  setText('#accessTypeFilterAll', t('accessFilterAll'));
  setText('#accessTypeFilterInternal', t('accessFilterInternal'));
  setText('#accessTypeFilterExternal', t('accessFilterExternal'));
  setText('#accessTableHeadEmail', t('accessTableEmail'));
  setText('#accessTableHeadAccess', t('accessTableAccess'));
  setText('#accessTableHeadRole', t('accessTableRole'));
  setText('#accessTableHeadType', t('accessTableType'));
  setText('#accessTableHeadStatus', t('accessTableStatus'));
  setText('#accessTableHeadUpdated', t('accessTableUpdated'));
  setText('#accessTableHeadActions', t('accessTableActions'));
  setText('#accessQuickAddButton', t('accessQuickAdd'));
  setText('#accessSummaryTitle', t('accessSummaryTitle'));
  const accessSearchInput = document.getElementById('accessDirectorySearch');
  if (accessSearchInput) accessSearchInput.placeholder = t('accessDirectorySearchPlaceholder');
  renderMailConfig(currentMailConfig);
  renderAccessEntryEditor();

  setText('#view-settings .settings-layout .card:first-child > div:first-child', t('settingsTitle'));
  setText('#view-settings .settings-layout .card:first-child > div:nth-child(2)', t('settingsHelp'));
  setText('label[for="settings_viewport_width"]', t('viewportWidth'));
  setText('label[for="settings_viewport_height"]', t('viewportHeight'));
  setText('label[for="settings_page_timeout_ms"]', t('pageTimeout'));
  setText('#view-settings .list-row div div:first-child', t('fullPageCapture'));
  setText('#view-settings .list-row .muted', t('fullPageHelp'));
  setText('#view-settings .settings-layout .card:first-child .card > div:first-child', t('jsonServiceAccount'));
  setText('#view-settings .settings-layout .card:first-child .card > div:nth-child(2)', t('jsonHelp'));
  setText('#settingsServiceAccountFileLabel', t('serviceJsonLabel'));
  setText('#settingsServiceAccountJsonLabel', t('serviceJsonPasteLabel'));
  const serviceFileHint = document.getElementById('settings_service_account_file_hint');
  if (serviceFileHint && !serviceFileHint.dataset.fileName) serviceFileHint.textContent = t('serviceJsonNoFile');
  setText('#saveSettingsButton', t('saveSettings'));
  setText('#accessPolicyTitle', t('accessPolicyTitle'));
  setText('#accessPolicyHelp', t('accessPolicyHelp'));
  setText('#accessAllowedLabel', t('accessAllowedLabel'));
  setText('#accessAllowedHelp', t('accessAllowedHelp'));
  setText('#accessAdminLabel', t('accessAdminLabel'));
  setText('#accessAdminHelp', t('accessAdminHelp'));
  setText('#saveAccessButton', t('saveAccessPolicy'));
  setText('#reloadAccessButton', t('reloadAccessPolicy'));
  setText('#view-settings .settings-layout aside > div:first-child', t('currentConfigSummary'));
  const summaryTitles = document.querySelectorAll('#view-settings .settings-layout aside .timeline-item strong');
  if (summaryTitles[0]) summaryTitles[0].textContent = t('viewport');
  if (summaryTitles[1]) summaryTitles[1].textContent = t('timeout');
  if (summaryTitles[2]) summaryTitles[2].textContent = t('output');
  if (summaryTitles[3]) summaryTitles[3].textContent = t('serviceAccount');
  if (summaryTitles[4]) summaryTitles[4].textContent = t('sharingNote');
  const shareHelp = document.querySelector('#view-settings .settings-layout aside .timeline-item:last-child div');
  if (shareHelp) shareHelp.textContent = t('sharingHelp');
  renderRunShareInfo(currentSettingsCache);
  renderAccessDirectory(currentAccessPolicy);
  renderAccessPolicySummary(currentAccessPolicy);
  syncAuthUI();
}

function applyTheme() {
  document.documentElement.setAttribute('data-theme', currentTheme);
  const themeToggle = document.getElementById('theme_toggle');
  if (themeToggle) {
    themeToggle.setAttribute('data-mode', currentTheme);
    const nextLabel = currentTheme === 'dark' ? t('light') : t('dark');
    themeToggle.title = `${t('light')} / ${t('dark')}`;
    themeToggle.setAttribute('aria-label', `${t('light')} / ${t('dark')} (${nextLabel})`);
  }
}

function setTheme(theme) {
  currentTheme = theme === 'dark' ? 'dark' : 'light';
  localStorage.setItem('ui_theme', currentTheme);
  applyTheme();
}

function toggleTheme() {
  setTheme(currentTheme === 'dark' ? 'light' : 'dark');
}

function setRunMode(mode) {
  const nextMode = String(mode || 'seeding').toLowerCase();
  currentRunMode = ['seeding', 'booking', 'scan'].includes(nextMode) ? nextMode : 'seeding';
  currentJobId = resolveModeJobId(currentRunMode);
  applyRunModeUI();
}

function openRunMode(mode) {
  switchView('runs');
  setRunMode(mode);
  if (currentJobId) {
    pollCurrent();
  } else {
    currentJobSnapshot = null;
    currentLogsCache = [];
    renderRunMonitor(null, []);
  }
}

function setLanguage(lang) {
  currentLang = lang === 'en' ? 'en' : 'vi';
  localStorage.setItem('ui_lang', currentLang);
  applyLanguage();
  renderOverview();
  renderProjects();
  renderActivities(currentLogsCache);
  renderRunMonitor(currentJobSnapshot, currentLogsCache);
  if (String(document.getElementById('sheet_url')?.value || '').trim()) scheduleSheetNameSuggestions(false);
}

function toggleLanguage() {
  setLanguage(currentLang === 'vi' ? 'en' : 'vi');
}

async function req(url, opts = {}) {
  const res = await fetch(url, { headers: { 'Content-Type': 'application/json' }, ...opts });
  const data = await res.json().catch(() => ({}));
  if (res.status === 401) {
    window.location.href = '/login';
    throw new Error(data.detail || 'Authentication required');
  }
  if (!res.ok) throw new Error(data.detail || ('HTTP ' + res.status));
  return data;
}

async function logoutAuth() {
  try {
    await fetch('/api/auth/logout', { method: 'POST' });
  } finally {
    window.location.href = '/login';
  }
}

function esc(s) {
  return String(s || '').replaceAll('&','&amp;').replaceAll('<','&lt;').replaceAll('>','&gt;');
}

function toLocalStamp(iso) {
  if (!iso) return '-';
  const d = new Date(iso);
  if (Number.isNaN(d.getTime())) return iso;
  return new Intl.DateTimeFormat('en-GB', {
    day: '2-digit',
    month: 'short',
    hour: '2-digit',
    minute: '2-digit'
  }).format(d);
}

function toCalendarDayKey(iso) {
  if (!iso) return '';
  const d = new Date(iso);
  if (Number.isNaN(d.getTime())) return '';
  const year = d.getFullYear();
  const month = String(d.getMonth() + 1).padStart(2, '0');
  const day = String(d.getDate()).padStart(2, '0');
  return `${year}-${month}-${day}`;
}

function toLocalDayLabel(value) {
  if (!value) return '-';
  let d = null;
  if (/^\\d{4}-\\d{2}-\\d{2}$/.test(String(value))) {
    const [year, month, day] = String(value).split('-').map(Number);
    d = new Date(year, month - 1, day);
  } else {
    d = new Date(value);
  }
  if (Number.isNaN(d.getTime())) return String(value);
  return new Intl.DateTimeFormat(currentLang === 'vi' ? 'vi-VN' : 'en-GB', {
    day: '2-digit',
    month: '2-digit'
  }).format(d);
}

function getJobTimelineStamp(job) {
  return job?.finished_at || job?.created_at || '';
}

function getTerminalLogStats(job) {
  const logs = Array.isArray(job?.logs) ? job.logs : [];
  if (!logs.length) {
    const summary = getJobSummary(job);
    return {
      success: Number(summary.success || 0),
      failed: Number(summary.failed || 0),
      unavailable: 0,
    };
  }
  let success = 0;
  let failed = 0;
  let unavailable = 0;
  logs.forEach(log => {
    const tag = String(log?.tag || '').toLowerCase();
    const state = String(log?.state || '').toLowerCase();
    const result = String(log?.result || '').toLowerCase();
    const raw = `${log?.tag || ''} ${log?.state || ''} ${log?.result || ''} ${log?.message || ''}`.toLowerCase();
    if (tag.includes('unavailable') || raw.includes('unavailable') || raw.includes('không khả dụng') || raw.includes('khong kha dung')) {
      unavailable += 1;
      return;
    }
    if (state === 'fail' || result === 'fail' || tag.includes('fail')) {
      failed += 1;
      return;
    }
    if (state === 'ok' || result === 'ok' || tag.includes('ok')) {
      success += 1;
    }
  });
  if (!success && !failed && !unavailable) {
    const summary = getJobSummary(job);
    success = Number(summary.success || 0);
    failed = Number(summary.failed || 0);
  }
  return { success, failed, unavailable };
}

function buildOverviewDateBuckets(jobs, limit = 7) {
  const buckets = new Map();
  (jobs || []).forEach(job => {
    const stamp = getJobTimelineStamp(job);
    const key = toCalendarDayKey(stamp);
    if (!key) return;
    const stats = getTerminalLogStats(job);
    const existing = buckets.get(key) || { key, jobs: 0, success: 0, failed: 0, unavailable: 0 };
    existing.jobs += 1;
    existing.success += Number(stats.success || 0);
    existing.failed += Number(stats.failed || 0);
    existing.unavailable += Number(stats.unavailable || 0);
    buckets.set(key, existing);
  });
  return [...buckets.values()].sort((a, b) => a.key.localeCompare(b.key)).slice(-limit);
}

function toDateKeyFromDate(date) {
  const year = date.getFullYear();
  const month = String(date.getMonth() + 1).padStart(2, '0');
  const day = String(date.getDate()).padStart(2, '0');
  return `${year}-${month}-${day}`;
}

function getJobSummary(job) {
  return job?.summary || { done: 0, total: 0, success: 0, failed: 0, eta: '---' };
}

function getJobSheetLabel(job) {
  const req = job?.request || {};
  return req.sheet_name || req.sheet_url || 'Unknown sheet';
}

function getJobMode(job) {
  return String(job?.mode || job?.request?.mode || job?.request?.mappings?.[0]?.mode || 'seeding').toLowerCase();
}

function getJobsByMode(mode) {
  const key = String(mode || 'seeding').toLowerCase();
  return (jobsCache || []).filter(job => getJobMode(job) === key);
}

function getSelectedJobIdForMode(mode) {
  const key = String(mode || 'seeding').toLowerCase();
  return currentJobIdByMode[key] || null;
}

function setSelectedJobIdForMode(mode, jobId) {
  const key = String(mode || 'seeding').toLowerCase();
  currentJobIdByMode[key] = jobId || null;
}

function resolveModeJobId(mode) {
  const jobs = getJobsByMode(mode);
  if (!jobs.length) return null;
  const selected = getSelectedJobIdForMode(mode);
  const matched = selected ? jobs.find(job => job.id === selected) : null;
  return matched ? matched.id : jobs[0].id;
}

function syncModeSelections() {
  ['seeding', 'booking', 'scan'].forEach(mode => {
    setSelectedJobIdForMode(mode, resolveModeJobId(mode));
  });
}

function getSavedProjectJobs() {
  return (jobsCache || []).filter(job => job.status === 'completed');
}

function getFilteredProjectJobs() {
  const saved = getSavedProjectJobs();
  if (currentProjectModeFilter === 'all') return saved;
  return saved.filter(job => getJobMode(job) === currentProjectModeFilter);
}

function getSelectedProjectJob() {
  const saved = getFilteredProjectJobs();
  if (!saved.length) {
    currentProjectJobId = null;
    return null;
  }
  const matched = currentProjectJobId ? saved.find(job => job.id === currentProjectJobId) : null;
  if (matched) return matched;
  currentProjectJobId = saved[0].id;
  return saved[0];
}

function selectProject(jobId) {
  currentProjectJobId = jobId || null;
  renderProjects();
}

function setProjectModeFilter(mode) {
  currentProjectModeFilter = String(mode || 'all').toLowerCase();
  currentProjectJobId = null;
  renderProjects();
}

function getActivityLogsFromJobs() {
  const rows = [];
  (jobsCache || []).forEach(job => {
    const logs = Array.isArray(job?.recent_logs) ? job.recent_logs : [];
    logs.forEach(item => {
      rows.push({
        ...item,
        __job_id: String(job?.id || ''),
        __job_mode: getJobMode(job),
      });
    });
  });
  rows.sort((a, b) => {
    const left = new Date(a?.ts || 0).getTime();
    const right = new Date(b?.ts || 0).getTime();
    return right - left;
  });
  return rows.slice(0, 20);
}

function openProjectInRuns(jobId) {
  const job = (jobsCache || []).find(item => item.id === jobId);
  if (!job) return;
  const request = job.request || {};
  const mode = getJobMode(job);
  sheet_url.value = request.sheet_url || '';
  sheet_name.value = request.sheet_name || '';
  drive_id.value = request.drive_id || '';
  document.getElementById('force_run_all').checked = request.force_run_all !== false;
  currentMappingBlocksByMode[mode] = (request.mappings || []).length
    ? request.mappings.map((block, index) => sanitizeMappingBlockForMode(mode, block, index + 1))
    : [defaultMappingBlock(mode, 1)];
  captureFivePerLink = !!request.capture_five_per_link;
  setSelectedJobIdForMode(mode, job.id);
  currentJobId = job.id;
  switchView('runs');
  setRunMode(mode);
  currentJobId = job.id;
  pollCurrent();
  setStatus(t('openProjectRunDone'), String(job.status || 'idle').toLowerCase());
}

async function deleteProject(jobId, ev = null) {
  if (ev && typeof ev.stopPropagation === 'function') ev.stopPropagation();
  if (!jobId) return;
  if (!confirm(t('deleteProjectConfirm'))) return;
  try {
    await req('/api/jobs/' + jobId, { method: 'DELETE' });
    if (currentProjectJobId === jobId) currentProjectJobId = null;
    if (currentJobId === jobId) currentJobId = null;
    await refreshJobs();
    renderProjects();
    setStatus(t('deleteProjectDone'), 'stopped');
  } catch (e) {
    alert(e.message);
  }
}

function classifyLog(log) {
  const raw = `${log?.tag || ''} ${log?.state || ''} ${log?.result || ''} ${log?.message || ''}`.toLowerCase();
  if (raw.includes('fail') || raw.includes('error')) return 'error';
  if (raw.includes('unavailable') || raw.includes('không khả dụng') || raw.includes('khong kha dung')) return 'warning';
  if (raw.includes('warn') || raw.includes('quota')) return 'warning';
  return 'info';
}

function prettyWord(value) {
  const raw = String(value || '').trim();
  if (!raw) return '-';
  if (raw.toLowerCase() === 'idle') return t('readyState');
  return raw.charAt(0).toUpperCase() + raw.slice(1);
}

function showToast(message, type = 'info', title = '') {
  const host = document.getElementById('toastHost');
  if (!host) return;
  const toast = document.createElement('div');
  toast.className = `toast ${type}`;
  toast.innerHTML = `
    <div class="toast-icon">${type === 'success' ? '✓' : '!'}</div>
    <div class="toast-copy">
      <div class="toast-title">${esc(title || t('jobFinishedTitle'))}</div>
      <div class="toast-message">${esc(message)}</div>
    </div>
    <button type="button" class="toast-close" aria-label="Close">×</button>
  `;
  const closeToast = () => {
    toast.style.opacity = '0';
    toast.style.transform = 'translateY(-8px)';
    setTimeout(() => toast.remove(), 160);
  };
  toast.querySelector('.toast-close')?.addEventListener('click', closeToast);
  host.appendChild(toast);
  setTimeout(closeToast, 5200);
}

function processJobLifecycleNotifications(jobs) {
  const nextStatusMemory = {};
  (jobs || []).forEach(job => {
    const jobId = String(job?.id || '').trim();
    if (!jobId) return;
    const status = String(job?.status || '').trim().toLowerCase();
    const previousStatus = String(jobStatusMemory[jobId] || '').trim().toLowerCase();
    nextStatusMemory[jobId] = status;
    const summary = getJobSummary(job);
    const done = Number(summary.done || 0);
    const total = Number(summary.total || 0);
    const completionKey = `${jobId}:${String(job?.finished_at || '')}:${done}/${total}`;
    const isReallyCompleted = status === 'completed' && (total <= 0 || done >= total);
    if (isReallyCompleted && previousStatus && previousStatus !== 'completed' && !notifiedCompletedJobKeys.has(completionKey)) {
      notifiedCompletedJobKeys.add(completionKey);
      showToast(t('jobFinishedToastFmt')(getJobSheetLabel(job), done, total), 'success', t('jobFinishedTitle'));
    }
  });
  jobStatusMemory = nextStatusMemory;
}

function resultPill(result, state = '', tag = '', message = '') {
  const raw = `${tag || ''} ${result || ''} ${state || ''} ${message || ''}`.toLowerCase();
  let level = 'info';
  let label = prettyWord(result || state || level);
  if (raw.includes('unavailable') || raw.includes('không khả dụng') || raw.includes('khong kha dung')) {
    level = 'warning';
    label = t('unavailableLabel');
  } else if (raw.includes('success') || raw.includes('ok') || raw.includes('done')) level = 'success';
  else if (raw.includes('fail') || raw.includes('error')) level = 'failed';
  else if (raw.includes('warn')) level = 'warning';
  else if (raw.includes('running') || raw.includes('process')) level = 'running';
  return `<span class="result-pill ${level}">${esc(label)}</span>`;
}

function extractLogBlockName(log) {
  const text = String(log?.message || '').trim();
  const match = text.match(/^([^:]{1,80}):/);
  return match ? match[1].trim() : '';
}

function getLogPostLabel(log) {
  return extractLogBlockName(log) || (currentRunMode === 'scan' ? 'Scan' : 'Post');
}

function isUnavailableLog(log) {
  const raw = `${log?.tag || ''} ${log?.state || ''} ${log?.result || ''} ${log?.message || ''}`.toLowerCase();
  return raw.includes('unavailable') || raw.includes('không khả dụng') || raw.includes('khong kha dung');
}

function isFailedLog(log) {
  const raw = `${log?.tag || ''} ${log?.state || ''} ${log?.result || ''} ${log?.message || ''}`.toLowerCase();
  if (raw.includes('unavailable') || raw.includes('không khả dụng') || raw.includes('khong kha dung')) return false;
  return raw.includes('fail') || raw.includes('error');
}

function canReplayLog(log) {
  const row = Number(log?.row || 0);
  if (!Number.isFinite(row) || row < 1) return false;
  const raw = `${log?.tag || ''} ${log?.state || ''} ${log?.result || ''}`.toLowerCase();
  return raw.includes('ok') || raw.includes('fail') || raw.includes('unavailable');
}

function statusBadge(status) {
  const key = String(status || '').toLowerCase();
  if (key === 'completed') return '<span class="badge ok">completed</span>';
  if (key === 'failed') return '<span class="badge error">failed</span>';
  if (key === 'running') return '<span class="badge info">running</span>';
  if (key === 'paused') return '<span class="badge warning">paused</span>';
  if (key === 'stopped') return '<span class="badge warning">stopped</span>';
  return `<span class="badge info">${esc(key || 'idle')}</span>`;
}

function aggregateErrorCounts(jobs) {
  const map = new Map();
  (jobs || []).forEach(job => {
    const rows = job?.error_rows || {};
    Object.values(rows).forEach(msg => {
      const key = String(msg || '').trim() || 'Unknown error';
      map.set(key, (map.get(key) || 0) + 1);
    });
  });
  return [...map.entries()].sort((a, b) => b[1] - a[1]);
}

function groupJobsBySheet(jobs) {
  const groups = new Map();
  (jobs || []).forEach(job => {
    const label = getJobSheetLabel(job);
    if (!groups.has(label)) groups.set(label, []);
    groups.get(label).push(job);
  });
  return [...groups.entries()].map(([label, items]) => {
    const completed = items.filter(x => x.status === 'completed').length;
    const failed = items.filter(x => x.status === 'failed').length;
    return { label, items, count: items.length, completed, failed };
  }).sort((a, b) => b.count - a.count);
}

function renderOverview() {
  const savedProjects = getSavedProjectJobs();
  const savedSheets = new Set(savedProjects.map(job => getJobSheetLabel(job))).size;
  let selectedProject = currentProjectJobId ? savedProjects.find(job => job.id === currentProjectJobId) : null;
  if (!selectedProject && savedProjects.length) selectedProject = savedProjects[0];
  const selectedProjectSummary = getJobSummary(selectedProject);
  const modeCounts = ['seeding', 'booking', 'scan'].map(mode => ({
    mode,
    count: jobsCache.filter(job => getJobMode(job) === mode).length,
  }));
  const modeTotal = modeCounts.reduce((sum, item) => sum + item.count, 0);
  document.getElementById('ovSavedProjects').textContent = savedProjects.length;
  document.getElementById('ovSavedSheets').textContent = savedSheets;
  document.getElementById('ovSelectedProject').textContent = selectedProject
    ? `${selectedProjectSummary.done || 0}/${selectedProjectSummary.total || 0}`
    : '-';
  const modeSplitHost = document.getElementById('ovModeSplit');
  if (modeSplitHost) {
    if (!modeTotal) {
      modeSplitHost.innerHTML = `<div class="overview-side-empty">${esc(t('overviewModeSplitEmpty'))}</div>`;
    } else {
      modeSplitHost.innerHTML = modeCounts.map(item => {
        const pct = modeTotal ? Math.round((item.count / modeTotal) * 100) : 0;
        const width = item.count > 0 ? Math.max(8, Math.round((item.count / modeTotal) * 100)) : 0;
        return `<div class="overview-mode-row">
          <div class="overview-mode-head">
            <span class="mode-pill mode-${item.mode}">${esc(getRunModeLabel(item.mode))}</span>
            <span class="overview-mode-value">${item.count}</span>
          </div>
          <div class="overview-mode-track"><span class="overview-mode-fill mode-${item.mode}" style="width:${width}%"></span></div>
          <div class="overview-mode-meta">${esc(t('overviewModeShareFmt')(item.count, pct))}</div>
        </div>`;
      }).join('');
    }
  }

  const historyBars = document.getElementById('ovHistoryBars');
  const historyBadges = document.getElementById('ovHistoryBadges');
  const buckets = buildOverviewDateBuckets(jobsCache, 7);
  if (historyBars) {
    if (!buckets.length) {
      historyBars.innerHTML = `<div class="overview-history-empty">${esc(t('overviewTimelineEmpty'))}</div>`;
    } else {
      const maxSeries = Math.max(1, ...buckets.flatMap(bucket => [bucket.success, bucket.failed, bucket.unavailable]));
      historyBars.innerHTML = buckets.map((bucket, idx, arr) => {
        const latestClass = idx === arr.length - 1 ? ' is-latest' : '';
        const successHeight = bucket.success > 0 ? Math.max(18, Math.round((bucket.success / maxSeries) * 150)) : 8;
        const failedHeight = bucket.failed > 0 ? Math.max(18, Math.round((bucket.failed / maxSeries) * 150)) : 8;
        const unavailableHeight = bucket.unavailable > 0 ? Math.max(18, Math.round((bucket.unavailable / maxSeries) * 150)) : 8;
        return `<div class="overview-history-group">
          <div class="overview-history-columns">
            <div class="overview-history-col-wrap">
              <div class="overview-history-col-value">${bucket.success}</div>
              <div class="overview-history-col success${latestClass}" style="height:${successHeight}px" title="${esc(t('overviewCompletedLegend'))}: ${bucket.success}"></div>
            </div>
            <div class="overview-history-col-wrap">
              <div class="overview-history-col-value">${bucket.failed}</div>
              <div class="overview-history-col failed${latestClass}" style="height:${failedHeight}px" title="${esc(t('overviewFailedLegend'))}: ${bucket.failed}"></div>
            </div>
            <div class="overview-history-col-wrap">
              <div class="overview-history-col-value">${bucket.unavailable}</div>
              <div class="overview-history-col unavailable${latestClass}" style="height:${unavailableHeight}px" title="${esc(t('overviewUnavailableLegend'))}: ${bucket.unavailable}"></div>
            </div>
          </div>
          <div class="overview-history-day">${esc(toLocalDayLabel(bucket.key))}</div>
        </div>`;
      }).join('');
    }
  }
  if (historyBadges) {
    if (!buckets.length) {
      historyBadges.innerHTML = '';
    } else {
      const latestBucket = buckets[buckets.length - 1];
      historyBadges.innerHTML = [
        `<div class="overview-history-badge">${esc(t('overviewDateFmt')(toLocalDayLabel(latestBucket.key)))}</div>`,
        `<div class="overview-history-badge">${esc(t('overviewTimelineJobsBadgeFmt')(latestBucket.jobs))}</div>`,
        `<div class="overview-history-badge">${esc(t('overviewTimelineSuccessBadgeFmt')(latestBucket.success))}</div>`,
        `<div class="overview-history-badge">${esc(t('overviewTimelineFailedBadgeFmt')(latestBucket.failed))}</div>`,
        `<div class="overview-history-badge">${esc(t('overviewTimelineUnavailableBadgeFmt')(latestBucket.unavailable))}</div>`,
      ].join('');
    }
  }
}

function switchView(name, tabEl = null) {
  if ((name === 'settings' || name === 'access') && !isAdminUser()) {
    setStatus(t('adminOnly'), 'failed');
    name = 'overview';
    tabEl = document.querySelector('.side-btn[data-view="overview"]');
  }
  document.querySelectorAll('.view').forEach(node => node.classList.remove('active'));
  const view = document.getElementById('view-' + name);
  if (view) view.classList.add('active');
  document.querySelectorAll('.side-btn[data-view]').forEach(node => node.classList.remove('active'));
  const activeTab = tabEl || document.querySelector(`.side-btn[data-view="${name}"]`);
  if (activeTab) activeTab.classList.add('active');
  const runsGroup = document.getElementById('runs_group');
  if (runsGroup) runsGroup.classList.toggle('open', name === 'runs');
  if (name === 'access' && isAdminUser()) Promise.all([loadAccessPolicy(), loadMailConfig()]);
}

function setStatus(text, status) {
  const statusText = document.getElementById('statusText');
  if (statusText) statusText.textContent = text;
  const chip = document.getElementById('envChip');
  if (!chip) return;
  chip.style.background = '#eef2f6';
  chip.style.color = '#334155';
  if (status === 'running') { chip.style.background = '#dbeafe'; chip.style.color = '#1d4ed8'; }
  if (status === 'paused') { chip.style.background = '#fef3c7'; chip.style.color = '#b45309'; }
  if (status === 'completed') { chip.style.background = '#dcfce7'; chip.style.color = '#166534'; }
  if (status === 'failed') { chip.style.background = '#fee2e2'; chip.style.color = '#991b1b'; }
  if (status === 'stopped') { chip.style.background = '#ffedd5'; chip.style.color = '#9a3412'; }
  chip.textContent = `${t('state')}: ` + prettyWord(status || 'idle');
}

function setKPI(summary, jobId) {
  const s = summary || { done: 0, total: 0, success: 0, failed: 0, eta: '---' };
  const pct = s.total > 0 ? Math.min(100, Math.floor((s.done / s.total) * 100)) : 0;
  document.getElementById('kpiJob').textContent = jobId ? jobId.slice(0, 8) : '-';
  document.getElementById('kpiSF').textContent = s.success + ' / ' + s.failed;
  document.getElementById('pctText').textContent = pct + '%';
  document.getElementById('pfill').style.width = pct + '%';
  document.getElementById('overviewText').textContent = jobId
    ? t('overviewTextFmt')(jobId.slice(0, 8), s.done, s.total)
    : t('noRunSelected');
}

function renderProjects() {
  const allSaved = getSavedProjectJobs();
  const saved = getFilteredProjectJobs();
  const selected = getSelectedProjectJob();
  const uniqueSheets = new Set(saved.map(job => getJobSheetLabel(job))).size;
  const summary = getJobSummary(selected);
  const completionText = String(selected?.completion?.summary || '').trim();
  const request = selected?.request || {};
  const filterOptions = [
    { key: 'all', label: t('allProjects'), count: allSaved.length },
    { key: 'seeding', label: getRunModeLabel('seeding'), count: allSaved.filter(job => getJobMode(job) === 'seeding').length },
    { key: 'booking', label: getRunModeLabel('booking'), count: allSaved.filter(job => getJobMode(job) === 'booking').length },
    { key: 'scan', label: getRunModeLabel('scan'), count: allSaved.filter(job => getJobMode(job) === 'scan').length },
  ];
  const totalNode = document.getElementById('projectsTotalJobs');
  const sheetsNode = document.getElementById('projectsCompletedJobs');
  const selectedNode = document.getElementById('projectsSelectedJob');
  if (totalNode) totalNode.textContent = saved.length;
  if (sheetsNode) sheetsNode.textContent = uniqueSheets;
  if (selectedNode) selectedNode.textContent = selected ? `${summary.done || 0}/${summary.total || 0}` : '-';
  document.getElementById('projectsModeFilters').innerHTML = filterOptions.map(opt => {
    const active = currentProjectModeFilter === opt.key ? ' active' : '';
    return `<button type="button" class="project-mode-filter mode-${opt.key}${active}" onclick="setProjectModeFilter('${opt.key}')">${esc(opt.label)}<span>${opt.count}</span></button>`;
  }).join('');
  document.getElementById('projectsSnapshotAction').innerHTML = selected
    ? `<div class="project-detail-actions"><button type="button" class="project-nav-btn" title="${esc(t('openProjectRun'))}" aria-label="${esc(t('openProjectRun'))}" onclick="openProjectInRuns('${selected.id}')"><svg viewBox="0 0 24 24" aria-hidden="true"><path d="M5 12h12"></path><path d="m13 6 6 6-6 6"></path></svg></button></div>`
    : '';
  document.getElementById('projectsList').innerHTML = saved.length
    ? saved.map(job => {
        const jobSummary = getJobSummary(job);
        const active = currentProjectJobId === job.id ? ' active' : '';
        const mode = getJobMode(job);
        return `<div class="list-row project-item${active}" onclick="selectProject('${job.id}')">
          <div class="project-item-main">
            <div class="project-item-title">${esc(getJobSheetLabel(job))}</div>
            <div class="project-item-meta"><span class="mode-pill mode-${mode}">${esc(prettyWord(mode))}</span><span>${esc(toLocalStamp(job.finished_at || job.created_at))}</span><span>${esc(job.id.slice(0, 8))}</span></div>
          </div>
          <div class="project-item-side">
            <span>${jobSummary.success || 0}/${jobSummary.total || 0}</span>
            ${isAdminUser() ? `<button type="button" class="project-delete-btn" title="${esc(t('deleteLabel'))}" onclick="deleteProject('${job.id}', event)">
              <svg viewBox="0 0 24 24" aria-hidden="true"><path d="M4 7h16"></path><path d="M10 11v6"></path><path d="M14 11v6"></path><path d="M6 7l1 12h10l1-12"></path><path d="M9 7V4h6v3"></path></svg>
            </button>` : ''}
          </div>
        </div>`;
      }).join('')
    : `<div class="list-row"><span>${allSaved.length ? t('noProjectsInFilter') : t('noGroupsYet')}</span><span>-</span></div>`;
  document.getElementById('projectsSnapshot').innerHTML = selected
    ? [
        `<div class="timeline-item"><strong>${t('group')}</strong><div>${esc(getJobSheetLabel(selected))}</div></div>`,
        `<div class="timeline-item"><strong>${t('state')}</strong><div>${esc(prettyWord(selected.status))} · <span class="mode-pill mode-${getJobMode(selected)}">${esc(prettyWord(getJobMode(selected)))}</span></div></div>`,
        `<div class="timeline-item"><strong>${t('latestUpdate')}</strong><div>${esc(toLocalStamp(selected.finished_at || selected.created_at))}</div></div>`,
        `<div class="timeline-item"><strong>${t('jobs')}</strong><div>${summary.done || 0}/${summary.total || 0} · ${summary.success || 0} ok · ${summary.failed || 0} ${t('failedLabel').toLowerCase()}</div></div>`,
        `<div class="timeline-item"><strong>${t('driveFolder')}</strong><div>${esc(request.drive_id || '-')}</div></div>`,
        `<div class="timeline-item"><strong>${t('detailLabel')}</strong><div>${esc(selected.detail || '-')}</div></div>`,
        `<div class="timeline-item"><strong>${t('summaryLabel')}</strong><div style="white-space:pre-line">${esc(completionText || '-')}</div></div>`,
      ].join('')
    : `<div class="timeline-item"><strong>${t('noProjectGroup')}</strong><div>${t('startOrSelect')}</div></div>`;
}

function renderActivities(logs) {
  const items = (logs || []).slice(-10).reverse();
  document.getElementById('activitiesTimeline').innerHTML = items.length
    ? items.map(x => {
        const level = classifyLog(x);
        const jobMeta = x.__job_id ? `${String(x.__job_mode || '').trim() ? `${prettyWord(x.__job_mode)} · ` : ''}${String(x.__job_id || '').slice(0, 8)}` : '';
        return `<div class="timeline-item"><div style="display:flex;justify-content:space-between;gap:8px;align-items:center"><strong>#${x.row} ${esc(x.state)}/${esc(x.result)}</strong><span class="badge ${level}">${level}</span></div><div>${esc(x.message)}</div><div class="s">${jobMeta ? `${esc(jobMeta)} · ` : ''}${toLocalStamp(x.ts)}</div></div>`;
      }).join('')
    : `<div class="timeline-item"><strong>${t('noActivity')}</strong><div>${t('startOrSelect')}</div></div>`;
}

function renderRunMonitor(snapshot, logs) {
  const st = snapshot || {};
  const s = st.summary || { done: 0, total: 0, success: 0, failed: 0, eta: '---' };
  const pct = s.total ? Math.round((s.done / s.total) * 100) : 0;
  const errorRows = st.error_rows || {};
  const errorKeys = Object.keys(errorRows);
  const logItems = Array.isArray(logs) ? logs : [];
  const unavailableCount = logItems.filter(isUnavailableLog).length;
  const failedLogCount = logItems.filter(isFailedLog).length;
  const issueRows = new Set();
  errorKeys.forEach(key => {
    const row = Number(key);
    if (Number.isFinite(row) && row > 0) issueRows.add(row);
  });
  logItems.forEach(item => {
    if (!isUnavailableLog(item) && !isFailedLog(item)) return;
    const row = Number(item?.row || 0);
    if (Number.isFinite(row) && row > 0) issueRows.add(row);
  });
  const failedCount = Math.max(Number(s.failed || 0), failedLogCount, errorKeys.length);
  const derivedIssueCount = issueRows.size || failedCount + unavailableCount;
  const hasIssueState = derivedIssueCount > 0 || String(st.status || '').toLowerCase() === 'failed' || !!String(st.error || '').trim();
  const statusLabel = prettyWord(st.status || 'idle');
  const latestLog = (logs || []).length ? logs[logs.length - 1] : null;
  const detailText = String(st.detail || latestLog?.message || '').trim();
  const etaText = s.eta && s.eta !== '---' ? `${t('eta')}: ${s.eta}` : '';
  const title = st.request ? getJobSheetLabel(st) : t('monitorNoJob');
  const metaParts = [];
  if (st.mode || st.request?.mode) metaParts.push(prettyWord(getJobMode(st)));
  if (currentJobId) metaParts.push(currentJobId.slice(0, 8));
  if (st.created_at) metaParts.push(toLocalStamp(st.created_at));
  const statusNode = document.getElementById('runMonitorStatus');
  statusNode.textContent = statusLabel;
  statusNode.style.background = 'var(--blue-soft)';
  statusNode.style.color = 'var(--blue)';
  statusNode.style.borderColor = 'rgba(91,147,211,.25)';
  if (st.status === 'completed') {
    statusNode.style.background = 'rgba(52,195,143,.16)';
    statusNode.style.color = 'var(--green)';
    statusNode.style.borderColor = 'rgba(52,195,143,.35)';
  } else if (st.status === 'paused') {
    statusNode.style.background = 'rgba(245,158,11,.16)';
    statusNode.style.color = '#b45309';
    statusNode.style.borderColor = 'rgba(245,158,11,.35)';
  } else if (st.status === 'failed') {
    statusNode.style.background = 'rgba(240,138,160,.16)';
    statusNode.style.color = 'var(--red)';
    statusNode.style.borderColor = 'rgba(240,138,160,.35)';
  } else if (st.status === 'stopped') {
    statusNode.style.background = 'rgba(243,197,142,.16)';
    statusNode.style.color = '#b45309';
    statusNode.style.borderColor = 'rgba(243,197,142,.35)';
  }
  document.getElementById('runMonitorJobTitle').textContent = title;
  document.getElementById('runMonitorJobMeta').textContent = metaParts.join(' · ') || '-';
  document.getElementById('runMonitorProgressMain').textContent = `${s.done || 0} / ${s.total || 0}`;
  document.getElementById('runMonitorPercent').textContent = `${pct}%`;
  document.getElementById('runMonitorBar').style.width = `${pct}%`;
  document.getElementById('runMonitorProgressMeta').textContent = detailText
    ? `${detailText}${etaText ? ' · ' + etaText : ''}`
    : (etaText || '-');
  document.getElementById('runMonitorErrorMain').textContent = hasIssueState ? `${Math.max(derivedIssueCount, 1)}` : t('monitorNoErrors');
  document.getElementById('runMonitorErrorMeta').textContent = hasIssueState
    ? (issueRows.size
        ? `${Array.from(issueRows).sort((a, b) => a - b).slice(0, 5).map(x => `#${x}`).join(', ')} · ${t('monitorSuccessFailedFmt')(s.success || 0, failedCount, unavailableCount)}`
        : t('monitorSuccessFailedFmt')(s.success || 0, failedCount, unavailableCount))
    : t('monitorSuccessFailedFmt')(s.success || 0, 0, unavailableCount);

  const rows = (logs || []).slice().reverse();
  const replayLocked = ['running', 'paused'].includes(String(st.status || '').toLowerCase());
  document.getElementById('runMonitorRows').innerHTML = rows.length
    ? rows.map(x => {
        const postName = getLogPostLabel(x);
        const message = x.message || `${x.state}/${x.result}`;
        const replayBlockName = extractLogBlockName(x);
        const replayButton = canReplayLog(x)
          ? `<button class="monitor-replay-btn" type="button" ${replayLocked ? 'disabled title="Job đang chạy, chưa thể replay"' : `onclick="replayLogRow('${esc(st.id || currentJobId || '')}', ${Number(x.row || 0)}, '${esc(replayBlockName)}')"`}>
              <svg viewBox="0 0 24 24" aria-hidden="true"><path d="M12 5V1L7 6l5 5V7c3.309 0 6 2.691 6 6a6 6 0 0 1-6 6 6 6 0 0 1-5.657-4H4.263A8.001 8.001 0 0 0 12 21c4.411 0 8-3.589 8-8s-3.589-8-8-8Z"></path></svg>
              <span>${esc(t('replay'))}</span>
            </button>`
          : `<span class="muted">-</span>`;
        return `<tr>
          <td>${esc(toLocalStamp(x.ts))}</td>
          <td>${esc(postName)}</td>
          <td>${esc(x.row)}</td>
          <td>${resultPill(x.result, x.state, x.tag, message)}</td>
          <td>${esc(message)}</td>
          <td class="monitor-replay-cell">${replayButton}</td>
        </tr>`;
      }).join('')
    : `<tr><td colspan="6">${t('noData')}</td></tr>`;
}

function updateRunActionButtons(snapshot = currentJobSnapshot) {
  const stopLabel = document.getElementById('stopJobLabel');
  const stopIcon = document.getElementById('stopJobIcon');
  const stopButton = stopLabel ? stopLabel.closest('button') : null;
  if (!stopLabel || !stopIcon || !stopButton) return;
  const status = String(snapshot?.status || '').toLowerCase();
  const paused = status === 'paused';
  stopLabel.textContent = paused ? t('resumeJob') : t('stopJob');
  stopIcon.innerHTML = paused
    ? '<path d="M8 6.5v11l9-5.5-9-5.5Z"></path>'
    : '<rect x="7" y="7" width="10" height="10" rx="1.5"></rect>';
  stopButton.classList.remove('blue');
  stopButton.classList.toggle('resume', paused);
  stopButton.classList.toggle('red', !paused);
}

async function replayLogRow(jobId, row, blockName = '') {
  try {
    if (!jobId) throw new Error('No job selected');
    const payload = {
      row: Number(row || 0),
      block_name: String(blockName || ''),
    };
    const out = await req(`/api/jobs/${jobId}/replay-row`, {
      method: 'POST',
      body: JSON.stringify(payload),
    });
    await refreshJobs();
    currentJobId = out.job_id;
    setSelectedJobIdForMode(currentRunMode, out.job_id);
    await pollCurrent();
    setStatus(`${t('replayStartedFmt')(payload.row)} · ${String(out.job_id || '').slice(0, 8)}`, 'running');
  } catch (e) {
    alert(e.message);
  }
}

function exportCurrentLog() {
  const jobId = String(currentJobSnapshot?.id || currentJobId || '').trim();
  if (!jobId) {
    alert(t('monitorNoJob'));
    return;
  }
  if (!Array.isArray(currentLogsCache) || !currentLogsCache.length) {
    alert(t('noLogsToExport'));
    return;
  }
  const link = document.createElement('a');
  link.href = `/api/jobs/${encodeURIComponent(jobId)}/export-log?ts=${Date.now()}`;
  link.target = '_blank';
  link.rel = 'noopener';
  document.body.appendChild(link);
  link.click();
  link.remove();
}

function setSettingsNote(text, isError = false) {
  const node = document.getElementById('settings_note');
  node.textContent = text || '';
  node.style.color = isError ? '#be123c' : '#98a2b3';
}

function setMailConfigNote(text, isError = false) {
  const node = document.getElementById('access_mail_note');
  if (!node) return;
  node.textContent = text || '';
  node.style.color = isError ? '#be123c' : '#98a2b3';
}

function setAccessEntryNote(text, isError = false) {
  const node = document.getElementById('access_entry_note');
  if (!node) return;
  node.textContent = text || '';
  node.style.color = isError ? '#be123c' : '#98a2b3';
}

function normalizeAccessType(value, email = '') {
  const raw = String(value || '').trim().toLowerCase();
  if (raw === 'internal' || raw === 'external') return raw;
  const domain = String(email || '').trim().toLowerCase().split('@')[1] || '';
  return domain === 'fanscom.vn' ? 'internal' : 'external';
}

function getAccessEmailTypes(policy = currentAccessPolicy) {
  const data = policy || {};
  const raw = data.email_types && typeof data.email_types === 'object' ? data.email_types : {};
  const lists = getAccessPolicyLists(data);
  const union = Array.from(new Set([...(lists.managed || []), ...(lists.admins || []), ...(lists.allowed || [])]));
  const out = {};
  union.forEach(email => {
    out[email] = normalizeAccessType(raw[email], email);
  });
  return out;
}

function setAccessMailEditorOpen(open, shouldScroll = false) {
  accessMailEditorOpen = !!open;
  if (accessMailEditorOpen) {
    accessEntryEditorState.open = false;
  }
  const card = document.querySelector('.access-mail-card');
  if (card) {
    card.classList.toggle('open', accessMailEditorOpen);
    if (accessMailEditorOpen && shouldScroll) {
      requestAnimationFrame(() => {
        card.scrollIntoView({ behavior: 'smooth', block: 'nearest' });
      });
    }
  }
}

function setAccessEntryEditorOpen(open, shouldScroll = false) {
  accessEntryEditorState.open = !!open;
  const card = document.querySelector('.access-entry-editor');
  if (card) {
    card.classList.toggle('open', accessEntryEditorState.open);
    if (accessEntryEditorState.open && shouldScroll) {
      requestAnimationFrame(() => {
        card.scrollIntoView({ behavior: 'smooth', block: 'nearest' });
      });
    }
  }
}

function renderMailConfig(config = currentMailConfig) {
  const data = config || { sender_email: '', from_email: '', has_password: false, source: 'env' };
  const senderNode = document.getElementById('access_mail_sender_email');
  const fromNode = document.getElementById('access_mail_from_email');
  const passwordNode = document.getElementById('access_mail_app_password');
  if (senderNode) senderNode.value = data.sender_email || '';
  if (fromNode) fromNode.value = data.from_email || data.sender_email || '';
  if (passwordNode) passwordNode.value = '';
  const currentPill = document.getElementById('accessMailCurrentPill');
  if (currentPill) currentPill.textContent = t('accessMailCurrentFmt')(data.sender_email || '');
  const passwordPill = document.getElementById('accessMailPasswordPill');
  if (passwordPill) {
    passwordPill.textContent = data.has_password ? t('accessMailPasswordSaved') : t('accessMailPasswordMissing');
    passwordPill.className = `access-mail-pill ${data.has_password ? 'ok' : 'warn'}`;
  }
  setAccessMailEditorOpen(accessMailEditorOpen, false);
  renderAccessDirectory(currentAccessPolicy);
}

function renderAccessEntryEditor() {
  const emailNode = document.getElementById('access_entry_email');
  const roleNode = document.getElementById('access_entry_role');
  const typeNode = document.getElementById('access_entry_type');
  if (emailNode) emailNode.value = accessEntryEditorState.email || '';
  if (roleNode) roleNode.value = accessEntryEditorState.role || 'user';
  if (typeNode) typeNode.value = accessEntryEditorState.type || 'internal';
  const pill = document.getElementById('accessEntryCurrentPill');
  if (pill) pill.textContent = t('accessEntryCurrentFmt')(accessEntryEditorState.originalEmail || accessEntryEditorState.email || '');
  setAccessEntryEditorOpen(accessEntryEditorState.open, false);
}

function setAccessPolicyNote(text, isError = false) {
  const node = document.getElementById('access_policy_note');
  if (!node) return;
  node.textContent = text || '';
  node.style.color = isError ? '#be123c' : '#98a2b3';
}

function parseAccessEmailLines(text) {
  return Array.from(new Set(String(text || '')
    .split(/[\\n,;]+/)
    .map(item => String(item || '').trim().toLowerCase())
    .filter(Boolean)));
}

function getAccessPolicyLists(policy = currentAccessPolicy) {
  const data = policy || { allowed_emails: [], admin_emails: [] };
  return {
    allowed: Array.isArray(data.allowed_emails) ? data.allowed_emails.map(item => String(item || '').trim().toLowerCase()).filter(Boolean) : [],
    admins: Array.isArray(data.admin_emails) ? data.admin_emails.map(item => String(item || '').trim().toLowerCase()).filter(Boolean) : [],
    managed: Array.isArray(data.managed_emails) ? data.managed_emails.map(item => String(item || '').trim().toLowerCase()).filter(Boolean) : [],
  };
}

function syncAccessPolicyEditors(policy = currentAccessPolicy) {
  const { allowed, admins } = getAccessPolicyLists(policy);
  const allowedNode = document.getElementById('access_allowed_emails');
  const adminNode = document.getElementById('access_admin_emails');
  if (allowedNode) allowedNode.value = allowed.join('\\n');
  if (adminNode) adminNode.value = admins.join('\\n');
}

function isValidAccessEmail(email) {
  return /^[^\\s@]+@[^\\s@]+\\.[^\\s@]+$/.test(String(email || '').trim());
}

function buildAccessDirectoryRows(policy = currentAccessPolicy) {
  const data = policy || { allowed_emails: [], admin_emails: [], updated_at: null };
  const { allowed, admins, managed } = getAccessPolicyLists(data);
  const emailTypes = getAccessEmailTypes(data);
  const currentEmail = String(authState.email || '').trim().toLowerCase();
  const union = Array.from(new Set([...managed, ...admins, ...allowed])).sort((a, b) => {
    const aSelf = !!currentEmail && String(a || '').trim().toLowerCase() === currentEmail;
    const bSelf = !!currentEmail && String(b || '').trim().toLowerCase() === currentEmail;
    if (aSelf && !bSelf) return -1;
    if (!aSelf && bSelf) return 1;
    return a.localeCompare(b);
  });
  const updated = data.updated_at ? toLocalStamp(data.updated_at) : '-';
  const rows = union.map(email => {
    const isAdmin = admins.includes(email);
    const canLogin = isAdmin || allowed.includes(email) || managed.includes(email);
    const type = normalizeAccessType(emailTypes[email], email);
    const isCurrentUser = !!currentEmail && String(email || '').trim().toLowerCase() === currentEmail;
    return {
      key: email,
      email,
      title: email,
      subtitle: isAdmin ? t('accessAdminEntrySub') : t('accessAllowedEntrySub'),
      access: isAdmin ? 'admin' : 'allowed',
      role: isAdmin ? 'admin' : 'user',
      type,
      status: isAdmin ? 'admin' : (canLogin ? 'active' : 'open'),
      updated,
      initial: email.charAt(0).toUpperCase() || 'G',
      isSystem: false,
      isCurrentUser,
    };
  });
  rows.unshift({
    key: '__open__',
    email: '',
    title: t('accessOpenEntryTitle'),
    subtitle: `${t('accessOpenEntrySub')} · ${t('accessOpenEntryMailFmt')(currentMailConfig.sender_email || '')}`,
    access: 'open',
    role: 'otp',
    type: 'internal',
    status: 'open',
    updated,
    initial: 'OTP',
    isSystem: true,
  });
  return rows.filter(row => {
    const query = String(accessDirectoryQuery || '').trim().toLowerCase();
    const roleOk = accessDirectoryRole === 'all' || row.role === accessDirectoryRole;
    const scopeOk = accessDirectoryScope === 'all' || row.access === accessDirectoryScope;
    const typeOk = accessDirectoryType === 'all' || row.type === accessDirectoryType;
    const queryOk = !query || [row.title, row.subtitle, row.access, row.role, row.type, row.status]
      .join(' ')
      .toLowerCase()
      .includes(query);
    return roleOk && scopeOk && typeOk && queryOk;
  });
}

function updateAccessDirectoryFilters() {
  const roleSelect = document.getElementById('accessRoleFilterSelect');
  const scopeSelect = document.getElementById('accessScopeFilterSelect');
  const typeSelect = document.getElementById('accessTypeFilterSelect');
  if (roleSelect) roleSelect.value = accessDirectoryRole;
  if (scopeSelect) scopeSelect.value = accessDirectoryScope;
  if (typeSelect) typeSelect.value = accessDirectoryType;
}

function renderAccessDirectory(policy = currentAccessPolicy) {
  updateAccessDirectoryFilters();
  const rows = buildAccessDirectoryRows(policy);
  const countNode = document.getElementById('accessDirectoryCount');
  if (countNode) countNode.textContent = String(rows.length);
  const body = document.getElementById('accessDirectoryBody');
  if (!body) return;
  if (!rows.length) {
    body.innerHTML = `<tr><td colspan="6"><div class="access-empty">${esc(t('accessDirectoryNoMatch'))}</div></td></tr>`;
    return;
  }
  const typeLabel = type => type === 'internal' ? t('accessTypeInternal') : t('accessTypeExternal');
  const roleLabel = role => role === 'admin' ? t('roleAdmin') : (role === 'otp' ? t('accessScopeOpen') : t('roleUser'));
  const statusLabel = status => status === 'admin' ? t('accessStatusAdmin') : (status === 'open' ? t('accessStatusOpen') : t('accessStatusActive'));
  const rowActions = row => {
    if (row.isSystem) {
      return `<div class="access-row-actions"><button class="access-row-btn edit" type="button" onclick="setAccessMailEditorOpen(true, true)">${esc(t('accessMailEdit'))}</button></div>`;
    }
    const token = encodeURIComponent(row.email);
    const edit = `<button class="access-row-btn edit" type="button" onclick="openAccessEntryEditor('${token}')">${esc(t('accessMailEdit'))}</button>`;
    const remove = `<button class="access-row-btn remove" type="button" onclick="removeAccessEmail('${token}')">${esc(t('accessRemove'))}</button>`;
    return `<div class="access-row-actions">${edit}${remove}</div>`;
  };
  body.innerHTML = rows.map(row => `
    <tr>
      <td>
        <div class="access-person">
          <div class="access-avatar ${esc(row.access)}">${esc(row.initial)}</div>
          <div class="access-person-meta">
            <div class="access-person-name">${esc(row.title)}${row.isCurrentUser ? ` <span class="access-you-tag">(${esc(t('accessYouTag'))})</span>` : ''}</div>
            <div class="access-person-sub">${esc(row.subtitle)}</div>
          </div>
        </div>
      </td>
      <td><span class="access-role-pill ${esc(row.role)}">${esc(roleLabel(row.role))}</span></td>
      <td><span class="access-type-pill ${esc(row.type)}">${esc(typeLabel(row.type))}</span></td>
      <td><span class="access-status ${esc(row.status)}">${esc(statusLabel(row.status))}</span></td>
      <td>${esc(row.updated)}</td>
      <td>${rowActions(row)}</td>
    </tr>`).join('');
}

function setAccessDirectoryQuery(value) {
  accessDirectoryQuery = String(value || '').trim();
  renderAccessDirectory(currentAccessPolicy);
}

function setAccessDirectoryRole(role) {
  accessDirectoryRole = ['all', 'admin', 'user'].includes(String(role || '').toLowerCase()) ? String(role).toLowerCase() : 'all';
  renderAccessDirectory(currentAccessPolicy);
}

function setAccessDirectoryScope(scope) {
  accessDirectoryScope = ['all', 'allowed', 'admin', 'open'].includes(String(scope || '').toLowerCase()) ? String(scope).toLowerCase() : 'all';
  renderAccessDirectory(currentAccessPolicy);
}

function setAccessDirectoryType(type) {
  accessDirectoryType = ['all', 'internal', 'external'].includes(String(type || '').toLowerCase()) ? String(type).toLowerCase() : 'all';
  renderAccessDirectory(currentAccessPolicy);
}

function openAccessEntryEditor(email) {
  const target = decodeURIComponent(String(email || '')).trim().toLowerCase();
  if (!target) return;
  setAccessMailEditorOpen(false, false);
  const lists = getAccessPolicyLists(currentAccessPolicy);
  const emailTypes = getAccessEmailTypes(currentAccessPolicy);
  accessEntryEditorState = {
    open: true,
    originalEmail: target,
    email: target,
    role: lists.admins.includes(target) ? 'admin' : 'user',
    type: normalizeAccessType(emailTypes[target], target),
  };
  renderAccessEntryEditor();
  setAccessEntryNote('');
}

async function loadMailConfig() {
  if (!isAdminUser()) return;
  try {
    const out = await req('/api/admin/mail-config');
    currentMailConfig = out.config || { sender_email: '', from_email: '', has_password: false, updated_at: null, source: 'env' };
    renderMailConfig(currentMailConfig);
    setMailConfigNote('');
  } catch (e) {
    setMailConfigNote(e.message, true);
  }
}

async function reloadAccessAdminPanel() {
  await Promise.all([loadAccessPolicy(), loadMailConfig()]);
  setMailConfigNote(t('accessMailReloaded'));
}

async function saveMailConfig() {
  if (!isAdminUser()) {
    setMailConfigNote(t('adminOnly'), true);
    return;
  }
  try {
    const payload = {
      sender_email: String(document.getElementById('access_mail_sender_email')?.value || '').trim(),
      from_email: String(document.getElementById('access_mail_from_email')?.value || '').trim(),
      app_password: String(document.getElementById('access_mail_app_password')?.value || '').trim(),
    };
    const out = await req('/api/admin/mail-config', { method: 'POST', body: JSON.stringify(payload) });
    currentMailConfig = out.config || currentMailConfig;
    renderMailConfig(currentMailConfig);
    setMailConfigNote(t('accessMailSaved'));
  } catch (e) {
    setMailConfigNote(e.message, true);
  }
}

async function saveAccessEntryEditor() {
  const originalEmail = String(accessEntryEditorState.originalEmail || '').trim().toLowerCase();
  const nextEmail = String(document.getElementById('access_entry_email')?.value || '').trim().toLowerCase();
  const nextRole = String(document.getElementById('access_entry_role')?.value || 'user').trim().toLowerCase();
  const nextType = normalizeAccessType(String(document.getElementById('access_entry_type')?.value || 'internal').trim().toLowerCase(), nextEmail);
  if (!isValidAccessEmail(nextEmail)) {
    setAccessEntryNote(t('accessEntryInvalid'), true);
    return;
  }
  const lists = getAccessPolicyLists(currentAccessPolicy);
  const emailTypes = { ...getAccessEmailTypes(currentAccessPolicy) };
  const allowedSet = new Set(lists.allowed);
  const adminSet = new Set(lists.admins);
  const managedSet = new Set(lists.managed);
  allowedSet.delete(originalEmail);
  adminSet.delete(originalEmail);
  managedSet.delete(originalEmail);
  delete emailTypes[originalEmail];
  if (nextRole === 'admin') {
    adminSet.add(nextEmail);
    if (allowedSet.size) allowedSet.add(nextEmail);
    managedSet.add(nextEmail);
  } else if (allowedSet.size) {
    allowedSet.add(nextEmail);
    managedSet.add(nextEmail);
  } else {
    managedSet.add(nextEmail);
  }
  emailTypes[nextEmail] = nextType;
  currentAccessPolicy = { ...(currentAccessPolicy || {}), managed_emails: Array.from(managedSet), email_types: emailTypes };
  setAccessPolicyListsInEditor(Array.from(allowedSet), Array.from(adminSet));
  try {
    await saveAccessPolicy();
    accessEntryEditorState = { open: false, originalEmail: nextEmail, email: nextEmail, role: nextRole === 'admin' ? 'admin' : 'user', type: nextType };
    renderAccessEntryEditor();
    setAccessPolicyNote(t('accessEntrySaved'));
  } catch (e) {
    await loadAccessPolicy();
    setAccessEntryNote(e.message, true);
  }
}

function setAccessPolicyListsInEditor(allowed, admins) {
  const normalizedAllowed = Array.from(new Set((allowed || []).map(item => String(item || '').trim().toLowerCase()).filter(Boolean)));
  const normalizedAdmins = Array.from(new Set((admins || []).map(item => String(item || '').trim().toLowerCase()).filter(Boolean)));
  const currentManaged = Array.isArray(currentAccessPolicy?.managed_emails) ? currentAccessPolicy.managed_emails : [];
  const normalizedManaged = Array.from(new Set(currentManaged.map(item => String(item || '').trim().toLowerCase()).filter(Boolean)));
  const normalizedTypes = getAccessEmailTypes({ ...(currentAccessPolicy || {}), managed_emails: normalizedManaged, allowed_emails: normalizedAllowed, admin_emails: normalizedAdmins });
  currentAccessPolicy = {
    ...(currentAccessPolicy || {}),
    allowed_emails: normalizedAllowed,
    admin_emails: normalizedAdmins,
    managed_emails: normalizedManaged,
    email_types: normalizedTypes,
  };
  const allowedNode = document.getElementById('access_allowed_emails');
  const adminNode = document.getElementById('access_admin_emails');
  if (allowedNode) allowedNode.value = normalizedAllowed.join('\\n');
  if (adminNode) adminNode.value = normalizedAdmins.join('\\n');
}

async function addAccessEmailFromSearch() {
  const input = document.getElementById('accessDirectorySearch');
  const email = String(input?.value || '').trim().toLowerCase();
  if (!isValidAccessEmail(email)) {
    setAccessPolicyNote(t('accessQuickAddInvalid'), true);
    if (input) input.focus();
    return;
  }
  const lists = getAccessPolicyLists(currentAccessPolicy);
  const managedSet = new Set(lists.managed);
  managedSet.add(email);
  const emailTypes = { ...getAccessEmailTypes(currentAccessPolicy), [email]: normalizeAccessType('', email) };
  if (lists.allowed.length) {
    lists.allowed = Array.from(new Set([...lists.allowed, email]));
  }
  currentAccessPolicy = { ...(currentAccessPolicy || {}), managed_emails: Array.from(managedSet), email_types: emailTypes };
  setAccessPolicyListsInEditor(lists.allowed, lists.admins);
  try {
    await saveAccessPolicy();
    setAccessPolicyNote(t('accessQuickAddDoneFmt')(email));
  } catch (e) {
    await loadAccessPolicy();
    setAccessPolicyNote(e.message, true);
  }
}

async function changeAccessRole(email, nextRole) {
  const target = decodeURIComponent(String(email || '')).trim().toLowerCase();
  if (!target) return;
  const lists = getAccessPolicyLists(currentAccessPolicy);
  const allowedSet = new Set(lists.allowed);
  const adminSet = new Set(lists.admins);
  const managedSet = new Set(lists.managed);
  const emailTypes = { ...getAccessEmailTypes(currentAccessPolicy) };
  if (String(nextRole || '').toLowerCase() === 'admin') {
    adminSet.add(target);
    if (allowedSet.size) allowedSet.add(target);
    managedSet.add(target);
  } else {
    adminSet.delete(target);
    managedSet.add(target);
  }
  emailTypes[target] = normalizeAccessType(emailTypes[target], target);
  currentAccessPolicy = { ...(currentAccessPolicy || {}), managed_emails: Array.from(managedSet), email_types: emailTypes };
  setAccessPolicyListsInEditor(Array.from(allowedSet), Array.from(adminSet));
  try {
    await saveAccessPolicy();
  } catch (e) {
    await loadAccessPolicy();
    setAccessPolicyNote(e.message, true);
  }
}

async function removeAccessEmail(email) {
  const target = decodeURIComponent(String(email || '')).trim().toLowerCase();
  if (!target) return;
  const lists = getAccessPolicyLists(currentAccessPolicy);
  const emailTypes = { ...getAccessEmailTypes(currentAccessPolicy) };
  delete emailTypes[target];
  currentAccessPolicy = {
    ...(currentAccessPolicy || {}),
    managed_emails: lists.managed.filter(item => item !== target),
    email_types: emailTypes,
  };
  setAccessPolicyListsInEditor(
    lists.allowed.filter(item => item !== target),
    lists.admins.filter(item => item !== target),
  );
  try {
    await saveAccessPolicy();
  } catch (e) {
    await loadAccessPolicy();
    setAccessPolicyNote(e.message, true);
  }
}

function renderAccessPolicySummary(policy = currentAccessPolicy) {
  const data = policy || { allowed_emails: [], admin_emails: [], updated_at: null };
  const { allowed, admins, managed } = getAccessPolicyLists(data);
  const allowedUnion = Array.from(new Set([...managed, ...admins, ...allowed]));
  const updated = data.updated_at ? toLocalStamp(data.updated_at) : '-';
  const host = document.getElementById('accessSummaryTimeline');
  if (!host) return;
  const chips = (items, emptyText) => {
    if (!items.length) return `<span class="access-chip empty">${esc(emptyText)}</span>`;
    return items.map(item => `<span class="access-chip">${esc(item)}</span>`).join('');
  };
  host.innerHTML = [
    `<div class="access-summary-block"><div class="access-summary-label">${esc(t('accessSummaryCurrentMail'))}</div><div class="access-summary-main">${esc(authState.email || '-')}</div></div>`,
    `<div class="access-summary-block"><div class="access-summary-label">${esc(t('accessSummaryCurrentRole'))}</div><div class="access-summary-main"><span class="access-role-pill ${(authState.role || 'user').toLowerCase() === 'admin' ? 'admin' : 'user'}">${esc(getRoleLabel())}</span></div></div>`,
    `<div class="access-summary-block"><div class="access-summary-label">${esc(t('accessSummaryAllowed'))}</div><div class="access-chip-list">${chips(allowed, t('accessSummaryOpen'))}</div></div>`,
    `<div class="access-summary-block"><div class="access-summary-label">${esc(t('accessSummaryAdmins'))}</div><div class="access-chip-list">${chips(admins, t('accessSummaryEmptyAdmins'))}</div></div>`,
    `<div class="access-summary-block"><div class="access-summary-label">${esc(t('accessSummaryUpdated'))}</div><div class="access-summary-main dim">${esc(updated)}</div></div>`,
  ].join('');
}

function syncAuthUI() {
  const roleBadge = document.getElementById('authRoleBadge');
  if (roleBadge) {
    roleBadge.textContent = getRoleLabel();
    roleBadge.className = `auth-role auth-role-${authState.role || 'user'}`;
  }
  renderOverviewGreeting();
  const accessButton = document.getElementById('access_nav_button');
  if (accessButton) accessButton.style.display = isAdminUser() ? 'flex' : 'none';
  const settingsButton = document.getElementById('settings_nav_button');
  if (settingsButton) settingsButton.style.display = 'flex';
  const stateNode = document.querySelector('#view-settings .state');
  if (stateNode) stateNode.textContent = t('settingsState');
  const accessStateNode = document.querySelector('#view-access .state');
  if (accessStateNode) accessStateNode.textContent = isAdminUser() ? t('accessState') : t('adminOnly');
  if (!isAdminUser() && document.getElementById('view-access')?.classList.contains('active')) {
    switchView('overview');
  }
}

function setSheetNameHint(text, isError = false) {
  const node = document.getElementById('sheet_name_hint');
  if (!node) return;
  node.textContent = text || '';
  node.style.color = isError ? '#be123c' : '#98a2b3';
}

function renderSheetNameSuggestions(titles) {
  const list = document.getElementById('sheet_name_suggestions');
  if (!list) return;
  list.innerHTML = (titles || []).map(title => `<option value="${esc(title)}"></option>`).join('');
}

async function fetchSheetNameSuggestions(force = false) {
  const rawUrl = String(document.getElementById('sheet_url')?.value || '').trim();
  if (!rawUrl) {
    sheetNameSuggestKey = '';
    renderSheetNameSuggestions([]);
    setSheetNameHint('');
    return;
  }
  if (!force && sheetNameSuggestKey === rawUrl && Array.isArray(sheetNameSuggestCache[rawUrl])) {
    const cached = sheetNameSuggestCache[rawUrl];
    renderSheetNameSuggestions(cached);
    setSheetNameHint(cached.length ? t('sheetNameHintCountFmt')(cached.length) : t('sheetNameHintEmpty'));
    return;
  }
  setSheetNameHint(t('sheetNameHintLoading'));
  try {
    const qs = new URLSearchParams({ sheet_url: rawUrl });
    if (currentSettingsCache.credentials_path) qs.set('credentials_path', currentSettingsCache.credentials_path);
    const out = await req('/api/sheets/names?' + qs.toString());
    const titles = Array.isArray(out.titles) ? out.titles : [];
    sheetNameSuggestKey = rawUrl;
    sheetNameSuggestCache[rawUrl] = titles;
    renderSheetNameSuggestions(titles);
    if (!String(document.getElementById('sheet_name')?.value || '').trim() && titles.length === 1) {
      document.getElementById('sheet_name').value = titles[0];
    }
    setSheetNameHint(titles.length ? t('sheetNameHintCountFmt')(titles.length) : t('sheetNameHintEmpty'));
  } catch (e) {
    renderSheetNameSuggestions([]);
    setSheetNameHint(e.message, true);
  }
}

function scheduleSheetNameSuggestions(force = false) {
  if (sheetNameSuggestTimer) clearTimeout(sheetNameSuggestTimer);
  sheetNameSuggestTimer = setTimeout(() => {
    fetchSheetNameSuggestions(force);
  }, force ? 0 : 450);
}

function bindSheetNameAutocomplete() {
  const urlInput = document.getElementById('sheet_url');
  const nameInput = document.getElementById('sheet_name');
  if (!urlInput || urlInput.dataset.sheetSuggestBound === '1') return;
  urlInput.dataset.sheetSuggestBound = '1';
  ['input', 'change', 'paste'].forEach(evt => {
    urlInput.addEventListener(evt, () => scheduleSheetNameSuggestions(false));
  });
  urlInput.addEventListener('blur', () => scheduleSheetNameSuggestions(true));
  if (nameInput) {
    nameInput.addEventListener('focus', () => {
      if (String(urlInput.value || '').trim()) scheduleSheetNameSuggestions(true);
    });
  }
}

function renderRunShareInfo(settings) {
  const s = settings || {};
  const emailNode = document.getElementById('runShareEmail');
  if (!emailNode) return;
  emailNode.textContent = s.service_account_email || t('noServiceEmail');
}

function renderServiceAccountCard(settings) {
  const s = settings || {};
  const card = document.getElementById('settings_service_card');
  if (!card) return;
  card.style.display = s.service_account_fixed ? 'none' : '';
}

function resetServiceAccountFileInput() {
  const fileInput = document.getElementById('settings_service_account_file');
  const hiddenInput = document.getElementById('settings_service_account_json');
  const hint = document.getElementById('settings_service_account_file_hint');
  if (fileInput) fileInput.value = '';
  if (hiddenInput) hiddenInput.value = '';
  if (hint) {
    delete hint.dataset.fileName;
    hint.textContent = t('serviceJsonNoFile');
  }
}

function handleServiceAccountFileChange(event) {
  const input = event?.target || document.getElementById('settings_service_account_file');
  const file = input?.files?.[0];
  const hiddenInput = document.getElementById('settings_service_account_json');
  const hint = document.getElementById('settings_service_account_file_hint');
  if (!file) {
    if (hiddenInput) hiddenInput.value = '';
    if (hint) {
      delete hint.dataset.fileName;
      hint.textContent = t('serviceJsonNoFile');
    }
    return;
  }
  const reader = new FileReader();
  reader.onload = () => {
    if (hiddenInput) hiddenInput.value = String(reader.result || '');
    if (hint) {
      hint.dataset.fileName = file.name;
      hint.textContent = t('serviceJsonSelectedFmt')(file.name);
    }
  };
  reader.onerror = () => {
    if (hiddenInput) hiddenInput.value = '';
    if (hint) {
      delete hint.dataset.fileName;
      hint.textContent = t('serviceJsonNoFile');
    }
    setSettingsNote(t('serviceJsonReadError'), true);
  };
  reader.readAsText(file, 'utf-8');
}

function renderSettingsSummary(settings) {
  const s = settings || {};
  document.getElementById('settings_summary_viewport').textContent = `${s.viewport_width || '-'} x ${s.viewport_height || '-'}`;
  document.getElementById('settings_summary_timeout').textContent = `${s.page_timeout_ms || '-'} ms`;
  document.getElementById('settings_summary_full_page').textContent = s.full_page_capture ? t('fullPage') : t('viewportOnly');
  const serviceState = s.service_account_fixed ? t('fixedCredentials') : (s.service_account_saved ? t('saved') : t('notSaved'));
  document.getElementById('settings_summary_service_account').textContent = serviceState;
  document.getElementById('settings_summary_service_email').textContent = s.service_account_email || t('noServiceEmail');
  renderRunShareInfo(s);
  renderServiceAccountCard(s);
  const status = document.getElementById('settings_service_status');
  status.className = 'badge ' + (s.service_account_saved ? 'ok' : 'info');
  status.textContent = serviceState;
}

async function loadDefaults() {
  const [d, s] = await Promise.all([req('/api/default-config'), req('/api/settings')]);
  currentSettingsCache = s || {};
  sheet_url.value = d.sheet_url || s.sheet_url || '';
  sheet_name.value = d.sheet_name || s.sheet_name || '';
  drive_id.value = d.drive_id || s.drive_id || '';
  const overwriteNode = document.getElementById('force_run_all');
  if (overwriteNode) overwriteNode.checked = true;
  document.getElementById('settings_viewport_width').value = s.viewport_width || 1920;
  document.getElementById('settings_viewport_height').value = s.viewport_height || 1400;
  document.getElementById('settings_page_timeout_ms').value = s.page_timeout_ms || 3000;
  document.getElementById('settings_full_page_capture').checked = !!s.full_page_capture;
  renderSettingsSummary(s);
  if (isAdminUser()) await Promise.all([loadAccessPolicy(), loadMailConfig()]);
  if (String(sheet_url.value || '').trim()) scheduleSheetNameSuggestions(true);
  else setSheetNameHint('');
}

async function saveSidebarSettings() {
  try {
    const payload = {
      credentials_path: currentSettingsCache.credentials_path || '',
      service_account_json: document.getElementById('settings_service_account_json').value,
      sheet_url: sheet_url.value,
      sheet_name: sheet_name.value,
      drive_id: drive_id.value,
      viewport_width: Number(document.getElementById('settings_viewport_width').value || 1920),
      viewport_height: Number(document.getElementById('settings_viewport_height').value || 1400),
      page_timeout_ms: Number(document.getElementById('settings_page_timeout_ms').value || 3000),
      ready_state: currentSettingsCache.ready_state || 'interactive',
      full_page_capture: document.getElementById('settings_full_page_capture').checked,
    };
    const out = await req('/api/settings', { method: 'POST', body: JSON.stringify(payload) });
    const saved = out.settings || payload;
    currentSettingsCache = saved;
    resetServiceAccountFileInput();
    renderSettingsSummary(saved);
    if (String(sheet_url.value || '').trim()) scheduleSheetNameSuggestions(true);
    setSettingsNote(t('saved'));
  } catch (e) {
    setSettingsNote(e.message, true);
  }
}

async function loadAccessPolicy() {
  if (!isAdminUser()) return;
  try {
    const out = await req('/api/admin/access-policy');
    currentAccessPolicy = out.policy || { allowed_emails: [], admin_emails: [] };
    syncAccessPolicyEditors(currentAccessPolicy);
    renderAccessDirectory(currentAccessPolicy);
    renderAccessEntryEditor();
    renderAccessPolicySummary(currentAccessPolicy);
    setAccessPolicyNote('');
  } catch (e) {
    setAccessPolicyNote(e.message, true);
  }
}

async function saveAccessPolicy() {
  if (!isAdminUser()) {
    setAccessPolicyNote(t('adminOnly'), true);
    return;
  }
  try {
    const allowedNode = document.getElementById('access_allowed_emails');
    const adminNode = document.getElementById('access_admin_emails');
    const payload = {
      allowed_emails: allowedNode ? allowedNode.value : (currentAccessPolicy.allowed_emails || []).join('\\n'),
      admin_emails: adminNode ? adminNode.value : (currentAccessPolicy.admin_emails || []).join('\\n'),
      managed_emails: Array.isArray(currentAccessPolicy.managed_emails) ? currentAccessPolicy.managed_emails : [],
      email_types: currentAccessPolicy.email_types || {},
    };
    const out = await req('/api/admin/access-policy', { method: 'POST', body: JSON.stringify(payload) });
    currentAccessPolicy = out.policy || {};
    syncAccessPolicyEditors(currentAccessPolicy);
    renderAccessDirectory(currentAccessPolicy);
    renderAccessPolicySummary(currentAccessPolicy);
    const sentCount = Array.isArray(out.notifications?.sent) ? out.notifications.sent.length : 0;
    const failedCount = Array.isArray(out.notifications?.failed) ? out.notifications.failed.length : 0;
    if (sentCount && failedCount) setAccessPolicyNote(t('accessNotifyPartialFmt')(sentCount, failedCount));
    else if (sentCount) setAccessPolicyNote(`${t('accessPolicySaved')} · ${t('accessNotifySentFmt')(sentCount)}`);
    else if (failedCount) setAccessPolicyNote(t('accessNotifyPartialFmt')(0, failedCount), true);
    else setAccessPolicyNote(t('accessPolicySaved'));
  } catch (e) {
    setAccessPolicyNote(e.message, true);
    throw e;
  }
}

async function launchChrome() {
  try {
    const out = await req('/api/chrome/launch', {
      method: 'POST',
      body: JSON.stringify({ run_mode: currentRunMode, browser_port: getModeBasePort(currentRunMode) })
    });
    setStatus(out.message || 'Chrome launch requested', 'running');
  } catch (e) { alert(e.message); }
}

function buildMappingsForCurrentMode() {
  return ensureMappingBlocks(currentRunMode).map((block, index) => sanitizeMappingBlockForMode(currentRunMode, block, index + 1));
}

async function startJob() {
  try {
    const mappings = buildMappingsForCurrentMode();
    const firstStartLine = mappings.length ? Number(mappings[0].start_line || 4) : 4;
    const forceRunAll = !!document.getElementById('force_run_all')?.checked;
    const browserPort = getModeBasePort(currentRunMode);
    const out = await req('/api/jobs/start', {
      method: 'POST',
      body: JSON.stringify({
        run_mode: currentRunMode,
        sheet_url: sheet_url.value,
        sheet_name: sheet_name.value,
        drive_id: drive_id.value,
        browser_port: browserPort,
        start_line: firstStartLine,
        mappings,
        force_run_all: !!forceRunAll,
        credentials_input: currentSettingsCache.credentials_path || '',
        capture_five_per_link: currentRunMode === 'booking' && captureFivePerLink,
        auto_launch_chrome: DEFAULT_AUTO_LAUNCH_CHROME
      })
    });
    currentJobId = out.job_id;
    setSelectedJobIdForMode(currentRunMode, out.job_id);
    await refreshJobs();
    await pollCurrent();
    ensureTimers();
  } catch (e) { alert(e.message); }
}

async function stopJob() {
  if (!currentJobId) { alert('Choose a job first'); return; }
  try {
    const st = currentJobSnapshot || await req('/api/jobs/' + currentJobId);
    const status = String(st?.status || '').toLowerCase();
    if (!['running', 'paused'].includes(status)) {
      throw new Error('Job này không ở trạng thái dừng / tiếp tục được');
    }
    await req('/api/jobs/' + currentJobId + '/pause-toggle', { method: 'POST' });
    await pollCurrent();
    await refreshJobs();
  } catch (e) { alert(e.message); }
}

async function refreshJobs() {
  try {
    const out = await req('/api/jobs');
    const jobs = out.jobs || [];
    processJobLifecycleNotifications(jobs);
    jobsCache = jobs;
    syncModeSelections();
    if (currentJobId && !jobs.some(job => job.id === currentJobId)) currentJobId = null;
    if (!currentJobId && jobs.length) currentJobId = jobs[0].id;
    if (document.getElementById('view-runs')?.classList.contains('active')) {
      currentJobId = resolveModeJobId(currentRunMode);
      if (!currentJobId) {
        currentJobSnapshot = null;
        currentLogsCache = [];
        renderRunMonitor(null, []);
      }
    }
    document.getElementById('jobCountText').textContent = t('jobsLoadedFmt')(jobs.length);
    document.getElementById('jobCountText').dataset.jobs = jobs.length;
    const rows = jobs.map(j => {
      const s = j.summary || { done: 0, total: 0 };
      const active = currentJobId === j.id ? 'active' : '';
      const modeLabel = getJobMode(j).slice(0, 3).toUpperCase();
      return `<tr class="${active}" onclick="selectJob('${j.id}')"><td>${statusBadge(j.status)}</td><td title="${esc(getJobMode(j))} · ${esc(j.id)}">${esc(modeLabel)} · ${esc(j.id.slice(0,8))}</td><td>${s.done}/${s.total}</td></tr>`;
    }).join('');
    document.getElementById('jobsBody').innerHTML = rows;
    renderOverview();
    renderProjects();
    renderActivities(getActivityLogsFromJobs());
    return true;
  } catch (e) {
    setStatus('Load jobs error: ' + e.message, 'failed');
    return false;
  }
}

function resetSyncFeedback(btn) {
  if (!btn) return;
  btn.classList.remove('is-loading', 'is-done', 'is-error');
  btn.disabled = false;
  const label = btn.querySelector('span');
  if (label) label.textContent = t('sync');
}

async function refreshJobsWithFeedback(btn) {
  if (!btn || btn.classList.contains('is-loading')) return;
  if (syncFeedbackTimer) {
    clearTimeout(syncFeedbackTimer);
    syncFeedbackTimer = null;
  }
  const label = btn.querySelector('span');
  btn.classList.remove('is-done', 'is-error');
  btn.classList.add('is-loading');
  btn.disabled = true;
  if (label) label.textContent = t('syncing');
  const ok = await refreshJobs();
  btn.classList.remove('is-loading');
  btn.classList.add(ok ? 'is-done' : 'is-error');
  if (label) label.textContent = ok ? t('synced') : t('syncFailed');
  syncFeedbackTimer = setTimeout(() => resetSyncFeedback(btn), 1400);
}

function selectJob(jobId) {
  currentJobId = jobId;
  const matched = (jobsCache || []).find(job => job.id === jobId);
  if (matched) {
    setSelectedJobIdForMode(getJobMode(matched), jobId);
  }
  pollCurrent();
  refreshJobs();
}

async function pollCurrent() {
  if (!currentJobId) return;
  try {
    const st = await req('/api/jobs/' + currentJobId);
    currentJobSnapshot = st;
    const s = st.summary || { done: 0, total: 0, success: 0, failed: 0, eta: '---' };
    setKPI(s, currentJobId);
    setStatus('Status: ' + st.status + ' | Detail: ' + (st.detail || '-'), st.status);
    const lg = await req('/api/jobs/' + currentJobId + '/logs?limit=200');
    const logs = lg.logs || [];
    currentLogsCache = logs;
    const targetJob = (jobsCache || []).find(job => job.id === currentJobId);
    if (targetJob) targetJob.recent_logs = logs.slice(-20);
    renderRunMonitor(st, logs);
    updateRunActionButtons(st);
    renderOverview();
    renderProjects();
    renderActivities(getActivityLogsFromJobs());
  } catch (e) {
    setStatus('Poll error: ' + e.message, 'failed');
  }
}

function ensureTimers() {
  if (!pollTimer) pollTimer = setInterval(pollCurrent, 800);
  if (!jobsTimer) jobsTimer = setInterval(refreshJobs, 3000);
}

async function init() {
  syncAuthUI();
  bindSheetNameAutocomplete();
  await loadDefaults();
  await refreshJobs();
  await pollCurrent();
  renderOverview();
  renderActivities([]);
  renderRunMonitor(null, []);
  renderAccessPolicySummary(currentAccessPolicy);
  ensureTimers();
  applyTheme();
  applyLanguage();
  setStatus('ready', 'idle');
}

init().catch(e => setStatus('Init error: ' + e.message, 'failed'));
</script>
</body>
</html>"""
        .replace("__AUTH_EMAIL_TITLE__", auth_email or "unknown@example.com")
        .replace("__AUTH_EMAIL__", auth_email or "unknown@example.com")
        .replace("__AUTH_EMAIL_DISPLAY__", auth_email or "unknown@example.com")
        .replace("__AUTH_ROLE_CLASS__", auth_role or "user")
        .replace("__AUTH_ROLE__", auth_role or "user")
        .replace("__AUTH_ROLE_DISPLAY__", auth_role_display)
        .replace("__ADMIN_NAV_STYLE__", "" if auth_role_raw == "admin" else "display:none")
        .replace("__ADMIN_SECTION_STYLE__", "" if auth_role_raw == "admin" else "display:none")
        .replace("__SETTINGS_NAV_STYLE__", "")
        .replace("__SETTINGS_SECTION_STYLE__", "")
        .replace("__AUTH_IS_ADMIN__", "true" if auth_role_raw == "admin" else "false")
    )


@app.api_route("/health", methods=["GET", "HEAD"])
def health():
    return {"ok": True, "time": _utc_now_iso()}


@app.get("/api/default-config")
def default_config(request: Request):
    user_email = _require_api_auth(request)
    saved_settings = _read_saved_settings(user_email)
    payload = {
        "sheet_url": evidence.DEFAULT_SHEET_URL,
        "sheet_name": evidence.DEFAULT_SHEET_NAME_TARGET,
        "drive_id": evidence.DEFAULT_DRIVE_FOLDER_ID,
        "credentials_path": "",
    }
    payload["sheet_url"] = str(saved_settings.get("sheet_url", payload["sheet_url"]))
    payload["sheet_name"] = str(saved_settings.get("sheet_name", payload["sheet_name"]))
    payload["drive_id"] = str(saved_settings.get("drive_id", payload["drive_id"]))
    payload["credentials_path"] = str(saved_settings.get("credentials_path", payload["credentials_path"]))
    return payload


@app.get("/api/settings")
def get_settings(request: Request):
    user_email = _require_api_auth(request)
    data = _build_settings_payload(_read_saved_settings(user_email))
    return data


@app.get("/api/admin/access-policy")
def get_access_policy(request: Request):
    _require_admin(request)
    return {"ok": True, "policy": _read_auth_policy()}


@app.get("/api/admin/mail-config")
def get_mail_config(request: Request):
    _require_admin(request)
    return {"ok": True, "config": _read_mail_config(secret=False)}


@app.get("/api/sheets/names")
def list_sheet_names(request: Request, sheet_url: str, credentials_path: str = ""):
    user_email = _require_api_auth(request)
    saved = _read_saved_settings(user_email)
    cred_path = str(credentials_path or "").strip() or str(saved.get("credentials_path", "")).strip()
    spreadsheet = _open_spreadsheet(sheet_url, cred_path)
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


@app.post("/api/settings")
def save_settings(request: Request, payload: SettingsUpdateRequest):
    user_email = _require_api_auth(request)
    credentials_path = str(payload.credentials_path or "").strip()
    inline_json = str(payload.service_account_json or "").strip()
    if inline_json:
        try:
            parsed = json.loads(inline_json)
        except Exception as exc:
            raise HTTPException(status_code=400, detail=f"Service account JSON không hợp lệ: {exc}") from exc
        out_path = _user_service_account_path(user_email)
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(parsed, f, ensure_ascii=False, indent=2)
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
    data = _build_settings_payload(_write_saved_settings(user_email, patch))
    return {"ok": True, "settings": data}


@app.post("/api/admin/access-policy")
def save_access_policy(request: Request, payload: AccessPolicyUpdateRequest):
    admin_email = _require_admin(request)
    previous_policy = _read_auth_policy()
    allowed_emails = _parse_email_list(payload.allowed_emails)
    admin_emails = _parse_email_list(payload.admin_emails)
    managed_emails = _parse_email_list(payload.managed_emails)
    email_types = payload.email_types if isinstance(payload.email_types, dict) else {}
    if not admin_emails:
        admin_emails = [admin_email]
    if admin_email not in admin_emails:
        raise HTTPException(status_code=400, detail="Không thể tự gỡ quyền admin của chính bạn trong phiên này")
    policy = _write_auth_policy({"allowed_emails": allowed_emails, "admin_emails": admin_emails, "managed_emails": managed_emails, "email_types": email_types})
    notifications = _notify_access_policy_changes(previous_policy, policy)
    request.session["auth_role"] = _get_user_role(admin_email)
    return {"ok": True, "policy": policy, "notifications": notifications}


@app.post("/api/admin/mail-config")
def save_mail_config(request: Request, payload: MailConfigUpdateRequest):
    _require_admin(request)
    config = _write_mail_config(
        {
            "sender_email": payload.sender_email,
            "from_email": payload.from_email,
            "app_password": payload.app_password,
        }
    )
    return {"ok": True, "config": config}


@app.post("/api/chrome/launch")
def launch_chrome(request: Request, payload: LaunchChromeRequest):
    owner_email = _require_api_auth(request)
    run_mode = _normalize_run_mode(payload.run_mode)
    with JOBS_LOCK:
        running_id = _any_running_job_for_mode(run_mode, owner_email=owner_email)
    if running_id:
        raise HTTPException(status_code=409, detail=f"Mode {run_mode} đang có job chạy: {running_id}. Không thể mở lại Chrome lúc này.")
    browser_port = int(payload.browser_port or _get_mode_base_port(run_mode))
    profile_path = (payload.profile_path or "").strip() or _get_mode_profile(run_mode, 0)
    ok, info = evidence.launch_chrome_for_login(
        browser_port=browser_port,
        profile_path=profile_path,
    )
    if not ok:
        raise HTTPException(status_code=500, detail=info)
    return {"ok": True, "message": info}


@app.post("/api/chrome/launch-block/{block_index}")
def launch_chrome_block(block_index: int, request: Request, run_mode: str = "seeding"):
    owner_email = _require_api_auth(request)
    run_mode = _normalize_run_mode(run_mode)
    with JOBS_LOCK:
        running_id = _any_running_job_for_mode(run_mode, owner_email=owner_email)
    if running_id:
        raise HTTPException(status_code=409, detail=f"Mode {run_mode} đang có job chạy: {running_id}. Không thể mở lại Chrome block lúc này.")
    idx = int(block_index)
    base_port = _get_mode_base_port(run_mode)
    port = evidence.get_post_port(idx, base_port)
    profile = _get_mode_profile(run_mode, idx)
    ok, info = evidence.launch_chrome_for_login(browser_port=port, profile_path=profile)
    if not ok:
        raise HTTPException(status_code=500, detail=info)
    return {"ok": True, "message": info, "browser_port": port, "profile_path": profile}


@app.post("/api/jobs/start")
def start_job(request: Request, payload: JobStartRequest):
    owner_email = _require_api_auth(request)
    run_mode = _normalize_run_mode(payload.run_mode)
    saved_settings = _read_saved_settings(owner_email)
    with JOBS_LOCK:
        running_id = _any_running_job_for_mode(run_mode, owner_email=owner_email)
        if running_id:
            raise HTTPException(status_code=409, detail=f"Mode {run_mode} đang có job chạy: {running_id}")

    credentials_input = str(payload.credentials_input or "").strip() or str(saved_settings.get("credentials_path", "")).strip()
    credentials_path = _resolve_credentials_input(credentials_input, owner_email)

    sheet_url = evidence.normalize_sheet_input(payload.sheet_url)
    drive_id = evidence.normalize_drive_folder_input(payload.drive_id)
    merged_settings = _build_settings_payload(saved_settings)
    runtime_settings = {
        "credentials_path": credentials_path,
        "viewport_width": int(merged_settings.get("viewport_width", 1920) or 1920),
        "viewport_height": int(merged_settings.get("viewport_height", 1400) or 1400),
        "page_timeout_ms": int(merged_settings.get("page_timeout_ms", 3000) or 3000),
        "ready_state": str(merged_settings.get("ready_state", "interactive") or "interactive"),
        "full_page_capture": bool(merged_settings.get("full_page_capture", False)),
    }
    _write_saved_settings(
        owner_email,
        {
            "credentials_path": credentials_path,
            "sheet_url": sheet_url,
            "sheet_name": payload.sheet_name.strip(),
            "drive_id": drive_id,
        }
    )

    mapping_payload = [m.model_dump() for m in payload.mappings] or [_default_mapping(payload.start_line, payload.run_mode)]
    run_mode = _infer_job_mode(mapping_payload, fallback=run_mode)
    browser_port = _get_mode_base_port(run_mode)
    profile_path = _get_mode_profile(run_mode, 0)

    if payload.auto_launch_chrome:
        has_non_scan = any(str((m or {}).get("mode", "seeding")).lower() != "scan" for m in mapping_payload)
        if has_non_scan:
            ok, info = evidence.launch_chrome_for_login(
                browser_port=browser_port,
                profile_path=profile_path,
            )
            if not ok:
                raise HTTPException(status_code=500, detail=f"Launch Chrome thất bại: {info}")

    request_snapshot = {
        "owner_email": owner_email,
        "mode": run_mode,
        "drive_id": drive_id,
        "sheet_url": sheet_url,
        "sheet_name": payload.sheet_name.strip(),
        "browser_port": browser_port,
        "profile_path": profile_path,
        "credentials_path": credentials_path,
        "runtime_settings": runtime_settings,
        "start_line": int(payload.start_line),
        "force_run_all": bool(payload.force_run_all),
        "only_run_error_rows": bool(payload.only_run_error_rows),
        "capture_five_per_link": bool(payload.capture_five_per_link),
        "target_rows": [],
        "target_block_name": "",
        "mappings": mapping_payload,
    }
    return _enqueue_job(
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
def replay_job_row(job_id: str, request: Request, payload: ReplayRowRequest):
    owner_email = _require_api_auth(request)
    row = int(payload.row)
    if row < 1:
        raise HTTPException(status_code=400, detail="Row không hợp lệ")

    with JOBS_LOCK:
        source_job = JOBS.get(job_id)
        if not source_job or _job_owner_email(source_job) != owner_email:
            raise HTTPException(status_code=404, detail="Không tìm thấy job nguồn")
        run_mode = _get_job_mode(source_job)
        running_id = _any_running_job_for_mode(run_mode, owner_email=owner_email)
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

    return _enqueue_job(
        owner_email=owner_email,
        request_snapshot=source_request,
        run_mode=run_mode,
        start_line=int(replay_start_line),
        force_run_all=True,
        only_run_error_rows=False,
        capture_five_per_link=bool(source_request.get("capture_five_per_link")),
        detail=detail,
    )


@app.post("/api/jobs/{job_id}/stop")
def stop_job(job_id: str, request: Request):
    owner_email = _require_api_auth(request)
    with JOBS_LOCK:
        job = JOBS.get(job_id)
        if not job or _job_owner_email(job) != owner_email:
            raise HTTPException(status_code=404, detail="Không tìm thấy job")
        adapter: WebAppAdapter = job["adapter"]
        if adapter:
            adapter.is_running = False
        job["status"] = "stopped"
        job["finished_at"] = _utc_now_iso()
    _persist_jobs(force=True)
    return {"ok": True, "job_id": job_id, "status": "stopped"}


@app.delete("/api/jobs/{job_id}")
def delete_job(job_id: str, request: Request):
    owner_email = _require_api_auth(request)
    with JOBS_LOCK:
        job = JOBS.get(job_id)
        if not job or _job_owner_email(job) != owner_email:
            raise HTTPException(status_code=404, detail="Không tìm thấy job")
        if str(job.get("status") or "").strip().lower() in {"running", "paused"}:
            raise HTTPException(status_code=409, detail="Không thể xóa job đang chạy hoặc đang tạm dừng")
        JOBS.pop(job_id, None)
    _persist_jobs(force=True)
    return {"ok": True, "job_id": job_id}


@app.get("/api/jobs/{job_id}/export-log")
def export_job_log(job_id: str, request: Request):
    owner_email = _require_api_auth(request)
    with JOBS_LOCK:
        job = JOBS.get(job_id)
        if not job or _job_owner_email(job) != owner_email:
            raise HTTPException(status_code=404, detail="Không tìm thấy job")
        job_snapshot = _serialize_job(job)
    rows = _build_export_log_rows(job_snapshot)
    if not rows:
        raise HTTPException(status_code=400, detail="Chưa có log để xuất")
    export_dir = os.path.join(evidence.TEMP_DIR, "web_exports")
    os.makedirs(export_dir, exist_ok=True)
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    mode = _safe_filename_part(_get_job_mode(job_snapshot))
    sheet = _safe_filename_part((job_snapshot.get("request") or {}).get("sheet_name", ""))
    job_short = _safe_filename_part(str(job_snapshot.get("id", ""))[:8])
    filename = f"evidence_log_{mode}_{sheet or 'sheet'}_{job_short}_{stamp}.xlsx"
    out_path = os.path.join(export_dir, filename)
    headers = ["Time", "Post", "#", "Result", "Message"]
    evidence.write_colored_xlsx_builtin(out_path, headers, rows)
    return FileResponse(
        out_path,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        filename=filename,
    )


@app.post("/api/jobs/{job_id}/pause-toggle")
def pause_toggle_job(job_id: str, request: Request):
    owner_email = _require_api_auth(request)
    with JOBS_LOCK:
        job = JOBS.get(job_id)
        if not job or _job_owner_email(job) != owner_email:
            raise HTTPException(status_code=404, detail="Không tìm thấy job")
        adapter: WebAppAdapter = job.get("adapter")
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
    _persist_jobs(force=True)
    return {"ok": True, "job_id": job_id, "status": status}


@app.get("/api/jobs")
def list_jobs(request: Request):
    owner_email = _require_api_auth(request)
    out = []
    with JOBS_LOCK:
        for job in JOBS.values():
            if _job_owner_email(job) != owner_email:
                continue
            out.append(
                {
                    "id": job["id"],
                    "mode": _get_job_mode(job),
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
    return {"jobs": out}


@app.get("/api/jobs/{job_id}")
def get_job(job_id: str, request: Request):
    owner_email = _require_api_auth(request)
    with JOBS_LOCK:
        job = JOBS.get(job_id)
        if not job or _job_owner_email(job) != owner_email:
            raise HTTPException(status_code=404, detail="Không tìm thấy job")
        return {
            "id": job["id"],
            "mode": _get_job_mode(job),
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
    owner_email = _require_api_auth(request)
    lim = max(1, min(int(limit), 1000))
    with JOBS_LOCK:
        job = JOBS.get(job_id)
        if not job or _job_owner_email(job) != owner_email:
            raise HTTPException(status_code=404, detail="Không tìm thấy job")
        logs = list(job.get("logs", []))
    return {"job_id": job_id, "logs": logs[-lim:]}


if __name__ == "__main__":
    import uvicorn

    host = os.environ.get("HOST", "0.0.0.0")
    port = int(os.environ.get("PORT", "8000"))
    uvicorn.run("web_ui:app", host=host, port=port, reload=False)

