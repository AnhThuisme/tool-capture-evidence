from __future__ import annotations

import base64
from concurrent.futures import ThreadPoolExecutor, as_completed
import html
import json
import os
import re
import secrets
import smtplib
import ssl
import subprocess
import tempfile
import threading
import time
import traceback
import unicodedata
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

# Web mode must not open Tk native dialogs from evidence.py worker threads.
# Those dialogs can initialize Tcl/Tk off the main UI loop and crash Python on macOS.
evidence.messagebox = None
evidence.filedialog = None


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


def _today_local_date_string() -> str:
    return datetime.now().strftime("%Y-%m-%d")


def _normalize_hostname(value: Any) -> str:
    raw = str(value or "").strip().lower()
    if not raw:
        return ""
    raw = re.sub(r"^[a-z][a-z0-9+.-]*://", "", raw)
    raw = raw.split("/", 1)[0].split("?", 1)[0].split("#", 1)[0]
    raw = raw.rsplit("@", 1)[-1]
    if raw.startswith("[") and "]" in raw:
        raw = raw[1:raw.index("]")]
    elif raw.count(":") == 1:
        raw = raw.rsplit(":", 1)[0]
    return raw.strip().strip(".")


def _parse_hostname_list(value: Any) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for part in re.split(r"[\s,;]+", str(value or "")):
        host = _normalize_hostname(part)
        if not host or host in seen:
            continue
        seen.add(host)
        out.append(host)
    return out


def _local_browser_hostnames() -> list[str]:
    defaults = ["127.0.0.1", "localhost"]
    configured = [
        *_parse_hostname_list(os.getenv("WEB_LOCAL_HOSTNAMES", "")),
        *_parse_hostname_list(os.getenv("CLOUDFLARE_PUBLIC_HOSTNAME", "")),
    ]
    seen: set[str] = set()
    out: list[str] = []
    for host in [*defaults, *configured]:
        normalized = _normalize_hostname(host)
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        out.append(normalized)
    return out


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
        highlight_sheet_errors: bool,
        scan_negative_filter: bool,
        scan_keyword_filter: bool,
        job_store: dict[str, Any],
        persist_callback=None,
        log_limit: int = 0,
        attach_only_existing_browser: bool = False,
        shared_run_state: dict[str, Any] | None = None,
    ):
        self._shared_run_state = shared_run_state if isinstance(shared_run_state, dict) else None
        self._is_running = True
        self._is_paused = False
        if self._shared_run_state is not None:
            self._shared_run_state.setdefault("is_running", True)
            self._shared_run_state.setdefault("is_paused", False)
            self._is_running = bool(self._shared_run_state.get("is_running", True))
            self._is_paused = bool(self._shared_run_state.get("is_paused", False))
        self.driver = None
        self.start_line = int(start_line)
        self.force_run_all = _Flag(force_run_all)
        self.only_run_error_rows = _Flag(only_run_error_rows)
        self.capture_five_per_link = _Flag(capture_five_per_link)
        self.highlight_sheet_errors = _Flag(highlight_sheet_errors)
        self.scan_negative_filter = _Flag(scan_negative_filter)
        self.scan_keyword_filter = _Flag(scan_keyword_filter)

        self.progress = {"value": 0}
        self._job_store = job_store
        self._log_limit = max(int(log_limit or 0), 0)
        self._persist_callback = persist_callback or (lambda force=False: None)
        self.attach_only_existing_browser = bool(attach_only_existing_browser)

        self.label_detail = _LabelProxy(self._on_detail)
        self.label_status = _LabelProxy(self._on_status)

    @property
    def is_running(self) -> bool:
        if self._shared_run_state is not None:
            return bool(self._shared_run_state.get("is_running", self._is_running))
        return bool(self._is_running)

    @is_running.setter
    def is_running(self, value: bool):
        next_value = bool(value)
        self._is_running = next_value
        if self._shared_run_state is not None:
            self._shared_run_state["is_running"] = next_value

    @property
    def is_paused(self) -> bool:
        if self._shared_run_state is not None:
            return bool(self._shared_run_state.get("is_paused", self._is_paused))
        return bool(self._is_paused)

    @is_paused.setter
    def is_paused(self, value: bool):
        next_value = bool(value)
        self._is_paused = next_value
        if self._shared_run_state is not None:
            self._shared_run_state["is_paused"] = next_value

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

    def update_progress_summary(
        self,
        done: int,
        total: int,
        ok_count: int,
        fail_count: int,
        eta_text: str = "---",
        unavailable_count: int = 0,
    ):
        self._job_store["summary"] = {
            "done": int(done),
            "total": int(total),
            "success": int(ok_count),
            "failed": int(fail_count),
            "unavailable": int(unavailable_count),
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
        if self._log_limit > 0:
            overflow = len(logs) - self._log_limit
            if overflow > 0:
                del logs[:overflow]
        self._persist()

    def update_issue_cells_live(
        self,
        row: int,
        post_name: str,
        columns: list[str] | tuple[str, ...] | set[str] | None,
        message: str,
        kind: str = "",
        clear: bool = False,
    ):
        row_num = int(row)
        post = str(post_name or "").strip()
        issue_kind = str(kind or "").strip().lower()
        cells = list(self._job_store.setdefault("issue_cells", []))

        def _same_row(item: dict[str, Any]) -> bool:
            try:
                return int(item.get("row") or 0) == row_num and str(item.get("post") or "").strip() == post
            except Exception:
                return False

        if clear:
            cells = [item for item in cells if not _same_row(item)]
            self._job_store["issue_cells"] = cells
            self._persist()
            return

        normalized_columns: list[str] = []
        for value in list(columns or []):
            col = str(value or "").strip().upper()
            if col and col not in normalized_columns:
                normalized_columns.append(col)
        if not normalized_columns:
            normalized_columns.append("-")

        message_text = str(message or "").strip()
        cells = [item for item in cells if not (_same_row(item) and str(item.get("kind") or "").strip().lower() == issue_kind)]
        for col in normalized_columns:
            cells.append(
                {
                    "row": row_num,
                    "post": post,
                    "column": col,
                    "kind": issue_kind,
                    "message": message_text,
                }
            )
        self._job_store["issue_cells"] = cells
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
    sheet_url: str = ""
    sheet_name: str = ""
    drive_id: str = ""
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
    highlight_sheet_errors: bool = False
    scan_negative_filter: bool = False
    scan_keyword_filter: bool = False
    scan_negative_terms: str = ""
    scan_keyword_terms: str = ""
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
    scan_negative_terms: str = ""
    scan_keyword_terms: str = ""
    viewport_width: int = 1920
    viewport_height: int = 1400
    page_timeout_ms: int = 200
    tiktok_captcha_wait_sec: int = 15
    please_wait_delay_sec: float = 2.0
    tiktok_force_focus: bool = True
    ready_state: str = "interactive"
    full_page_capture: bool = False
    mappings_by_mode: dict[str, list[MappingBlock]] = Field(default_factory=dict)
    run_flags_by_mode: dict[str, dict[str, Any]] = Field(default_factory=dict)


class QuickScanColumnsRequest(BaseModel):
    sheet_url: str
    sheet_name: str
    mode: str = "seeding"
    columns: list[str] = Field(default_factory=list)


class AccessPolicyUpdateRequest(BaseModel):
    allowed_emails: str = ""
    admin_emails: str = ""
    managed_emails: list[str] = Field(default_factory=list)
    email_types: dict[str, str] = Field(default_factory=dict)


class MailConfigUpdateRequest(BaseModel):
    sender_email: str = ""
    from_email: str = ""
    app_password: str = ""


class ActivityEventRequest(BaseModel):
    kind: str = ""
    message: str = ""
    level: str = "info"
    run_mode: str = ""
    block_name: str = ""
    browser_port: int | None = None
    job_id: str = ""
    row: int | None = None


app = FastAPI(title="Tool Evidence", version="1.0.0")


@app.exception_handler(Exception)
async def unhandled_exception_handler(request: Request, exc: Exception):
    try:
        tb = "".join(traceback.format_exception(type(exc), exc, exc.__traceback__))
        evidence.write_log(f"[UNHANDLED] {request.method} {request.url.path}: {exc}\n{tb}")
    except Exception:
        pass
    return JSONResponse(
        status_code=500,
        content={"ok": False, "detail": f"Server error: {exc}"},
    )


def _session_secret_key() -> str:
    configured = str(os.getenv("WEB_SESSION_SECRET", "")).strip()
    if configured:
        return configured
    # Keep a stable fallback so sessions are not invalidated on each cold start.
    # For production, always set WEB_SESSION_SECRET explicitly.
    return "tool-evidence-dev-session-secret-change-me"


app.add_middleware(
    SessionMiddleware,
    secret_key=_session_secret_key(),
    same_site="lax",
    https_only=False,
    max_age=int(os.getenv("WEB_SESSION_MAX_AGE_SEC", "43200") or 43200),
)

JOBS_LOCK = threading.Lock()
JOBS: dict[str, dict[str, Any]] = {}
JOB_HISTORY_PATH = os.path.join(evidence.BASE_DIR, "web_job_history.json")
ACTIVITY_HISTORY_PATH = os.path.join(evidence.BASE_DIR, "web_activity_history.json")
AUTH_POLICY_PATH = os.path.join(evidence.BASE_DIR, "web_auth_policy.json")
MAIL_CONFIG_PATH = os.path.join(evidence.BASE_DIR, "web_mail_config.json")
# Avoid persisting a multi-MB job history file too frequently while workers
# are appending logs in parallel.
JOB_PERSIST_MIN_INTERVAL_SEC = 2.0
JOB_LIST_RECENT_LOG_LIMIT_DEFAULT = 40
JOB_LIST_RECENT_LOG_LIMIT_MAX = 200
_LAST_JOB_PERSIST_TS = 0.0
JOB_PERSIST_LOCK = threading.Lock()
RUN_MODES = ("seeding", "booking", "scan")
OTP_STORE_LOCK = threading.Lock()
OTP_STORE: dict[str, dict[str, Any]] = {}
OTP_TTL_SEC = max(60, int(os.getenv("WEB_AUTH_OTP_TTL_SEC", "600") or 600))
OTP_RESEND_COOLDOWN_SEC = max(10, int(os.getenv("WEB_AUTH_RESEND_COOLDOWN_SEC", "45") or 45))
OTP_MAX_ATTEMPTS = max(1, int(os.getenv("WEB_AUTH_MAX_ATTEMPTS", "6") or 6))
SHEET_NAMES_CACHE_LOCK = threading.Lock()
SHEET_NAMES_CACHE: dict[str, dict[str, Any]] = {}
SHEET_NAMES_CACHE_TTL_SEC = max(30, int(os.getenv("WEB_SHEET_NAMES_CACHE_TTL_SEC", "300") or 300))
SHEET_LINK_COLUMNS_CACHE_LOCK = threading.Lock()
SHEET_LINK_COLUMNS_CACHE: dict[str, dict[str, Any]] = {}
SHEET_LINK_COLUMNS_CACHE_TTL_SEC = max(30, int(os.getenv("WEB_SHEET_LINK_COLUMNS_CACHE_TTL_SEC", "300") or 300))
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
DEFAULT_SHARED_BROWSER_PORT = 9223
SHARED_DEBUG_PROFILE_PATH = os.path.expanduser("~/.chrome-debug-evidence")

_SCREENSHOT_HOOK_LOCK = threading.Lock()
_SCREENSHOT_HOOK_REFCOUNT = 0
_SCREENSHOT_HOOK_ORIGINAL = None
_FOCUS_HOOK_LOCK = threading.Lock()
_FOCUS_HOOK_REFCOUNT = 0
_FOCUS_HOOK_ORIGINAL = None

print(
    f"[startup-config] base_dir={evidence.BASE_DIR} settings={evidence.SETTINGS_PATH} "
    f"port_env={os.getenv('PORT', '') or 'unset'}"
)


SETTINGS_USER_KEYS = {
    "credentials_path",
    "sheet_url",
    "sheet_name",
    "drive_id",
    "scan_negative_terms",
    "scan_keyword_terms",
    "viewport_width",
    "viewport_height",
    "page_timeout_ms",
    "tiktok_captcha_wait_sec",
    "please_wait_delay_sec",
    "tiktok_force_focus",
    "ready_state",
    "full_page_capture",
    "mappings_by_mode",
    "run_flags_by_mode",
}


def _read_saved_settings_root() -> dict[str, Any]:
    settings_path = _settings_storage_path()
    if not os.path.exists(settings_path):
        return {}
    try:
        with open(settings_path, "r", encoding="utf-8") as f:
            data = json.load(f) or {}
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def _normalize_match_text(value: str) -> str:
    s = str(value or "").lower()
    s = re.sub(r"\s+", " ", s).strip()
    return s


def _is_tiktok_url(value: str) -> bool:
    u = str(value or "").lower()
    return ("tiktok.com" in u) or ("vt.tiktok.com" in u)


def _install_screenshot_wait_hook(delay_sec: float):
    global _SCREENSHOT_HOOK_REFCOUNT, _SCREENSHOT_HOOK_ORIGINAL
    wait_sec = max(0.0, float(delay_sec or 0.0))
    if wait_sec <= 0:
        return None
    try:
        from selenium.webdriver.remote.webdriver import WebDriver as _RemoteWebDriver
    except Exception:
        return None
    markers = (
        "please wait",
        "please wait...",
        "vui long cho",
        "vui long cho...",
        "vui lòng chờ",
        "vui lòng chờ...",
    )

    def _hook(driver_self, *args, **kwargs):
        try:
            current_url = str(getattr(driver_self, "current_url", "") or "")
            if _is_tiktok_url(current_url):
                txt_raw = (
                    driver_self.execute_script(
                        "return (document.body && document.body.innerText) ? document.body.innerText : ''"
                    )
                    or ""
                )
                txt_norm = _normalize_match_text(txt_raw)
                if any(_normalize_match_text(marker) in txt_norm for marker in markers):
                    time.sleep(wait_sec)
        except Exception:
            pass
        return _SCREENSHOT_HOOK_ORIGINAL(driver_self, *args, **kwargs)

    with _SCREENSHOT_HOOK_LOCK:
        if _SCREENSHOT_HOOK_REFCOUNT == 0:
            _SCREENSHOT_HOOK_ORIGINAL = _RemoteWebDriver.get_screenshot_as_png
            _RemoteWebDriver.get_screenshot_as_png = _hook
        _SCREENSHOT_HOOK_REFCOUNT += 1

    def _restore():
        global _SCREENSHOT_HOOK_REFCOUNT, _SCREENSHOT_HOOK_ORIGINAL
        try:
            from selenium.webdriver.remote.webdriver import WebDriver as _RemoteWebDriver
        except Exception:
            return
        with _SCREENSHOT_HOOK_LOCK:
            if _SCREENSHOT_HOOK_REFCOUNT > 0:
                _SCREENSHOT_HOOK_REFCOUNT -= 1
            if _SCREENSHOT_HOOK_REFCOUNT == 0 and _SCREENSHOT_HOOK_ORIGINAL is not None:
                _RemoteWebDriver.get_screenshot_as_png = _SCREENSHOT_HOOK_ORIGINAL
                _SCREENSHOT_HOOK_ORIGINAL = None

    return _restore


def _force_browser_window_foreground() -> bool:
    if os.name != "nt":
        return False
    script = r"""
Add-Type @"
using System;
using System.Runtime.InteropServices;
public static class WinApi {
  [DllImport("user32.dll")] public static extern bool SetForegroundWindow(IntPtr hWnd);
  [DllImport("user32.dll")] public static extern bool ShowWindowAsync(IntPtr hWnd, int nCmdShow);
  [DllImport("user32.dll")] public static extern bool SetWindowPos(IntPtr hWnd, IntPtr hWndInsertAfter, int X, int Y, int cx, int cy, uint uFlags);
}
"@
$HWND_TOPMOST = [intptr](-1)
$HWND_NOTOPMOST = [intptr](-2)
$SW_RESTORE = 9
$SWP_NOMOVE = 0x0002
$SWP_NOSIZE = 0x0001
$SWP_NOACTIVATE = 0x0010
$targets = @('chrome','msedge')
$ok = $false
foreach ($name in $targets) {
  $p = Get-Process -Name $name -ErrorAction SilentlyContinue | Where-Object { $_.MainWindowHandle -ne 0 } | Sort-Object StartTime -Descending | Select-Object -First 1
  if ($null -ne $p) {
    $h = [intptr]$p.MainWindowHandle
    [WinApi]::ShowWindowAsync($h, $SW_RESTORE) | Out-Null
    [WinApi]::SetWindowPos($h, $HWND_TOPMOST, 0,0,0,0, ($SWP_NOMOVE -bor $SWP_NOSIZE -bor $SWP_NOACTIVATE)) | Out-Null
    Start-Sleep -Milliseconds 80
    [WinApi]::SetWindowPos($h, $HWND_NOTOPMOST, 0,0,0,0, ($SWP_NOMOVE -bor $SWP_NOSIZE -bor $SWP_NOACTIVATE)) | Out-Null
    [WinApi]::SetForegroundWindow($h) | Out-Null
    $ok = $true
    break
  }
}
if ($ok) { exit 0 } else { exit 1 }
"""
    try:
        result = subprocess.run(
            ["powershell", "-NoProfile", "-Command", script],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            timeout=5,
            check=False,
        )
        return result.returncode == 0
    except Exception:
        return False


def _install_tiktok_focus_boost_hook(enabled: bool):
    global _FOCUS_HOOK_REFCOUNT, _FOCUS_HOOK_ORIGINAL
    if not enabled:
        return None
    if not hasattr(evidence, "focus_chrome_for_tiktok_challenge"):
        return None

    def _hook(driver_self, *args, **kwargs):
        base_ok = False
        try:
            base_ok = bool(_FOCUS_HOOK_ORIGINAL(driver_self, *args, **kwargs))
        except Exception:
            base_ok = False
        boosted_ok = _force_browser_window_foreground()
        return bool(base_ok or boosted_ok)

    with _FOCUS_HOOK_LOCK:
        if _FOCUS_HOOK_REFCOUNT == 0:
            _FOCUS_HOOK_ORIGINAL = evidence.focus_chrome_for_tiktok_challenge
            evidence.focus_chrome_for_tiktok_challenge = _hook
        _FOCUS_HOOK_REFCOUNT += 1

    def _restore():
        global _FOCUS_HOOK_REFCOUNT, _FOCUS_HOOK_ORIGINAL
        with _FOCUS_HOOK_LOCK:
            if _FOCUS_HOOK_REFCOUNT > 0:
                _FOCUS_HOOK_REFCOUNT -= 1
            if _FOCUS_HOOK_REFCOUNT == 0 and _FOCUS_HOOK_ORIGINAL is not None:
                evidence.focus_chrome_for_tiktok_challenge = _FOCUS_HOOK_ORIGINAL
                _FOCUS_HOOK_ORIGINAL = None

    return _restore


def _filter_settings_payload(data: Any) -> dict[str, Any]:
    if not isinstance(data, dict):
        return {}
    filtered = {key: data.get(key) for key in SETTINGS_USER_KEYS if key in data}
    if "scan_negative_terms" in filtered:
        filtered["scan_negative_terms"] = str(filtered.get("scan_negative_terms") or "")
    if "scan_keyword_terms" in filtered:
        filtered["scan_keyword_terms"] = str(filtered.get("scan_keyword_terms") or "")
    if "tiktok_captcha_wait_sec" in filtered:
        try:
            filtered["tiktok_captcha_wait_sec"] = max(5, int(filtered.get("tiktok_captcha_wait_sec") or 15))
        except Exception:
            filtered["tiktok_captcha_wait_sec"] = 15
    if "please_wait_delay_sec" in filtered:
        try:
            filtered["please_wait_delay_sec"] = max(0.0, float(filtered.get("please_wait_delay_sec") or 2.0))
        except Exception:
            filtered["please_wait_delay_sec"] = 2.0
    if "tiktok_force_focus" in filtered:
        filtered["tiktok_force_focus"] = bool(filtered.get("tiktok_force_focus"))
    if "mappings_by_mode" in filtered:
        filtered["mappings_by_mode"] = _normalize_mappings_by_mode(filtered.get("mappings_by_mode"))
    if "run_flags_by_mode" in filtered:
        filtered["run_flags_by_mode"] = _normalize_run_flags_by_mode(filtered.get("run_flags_by_mode"))
    return filtered


def _normalize_saved_mapping_block(raw: Any, mode: str, index: int) -> dict[str, Any]:
    mode_key = _normalize_run_mode(mode)
    default_start_line = 4
    if isinstance(raw, dict):
        try:
            default_start_line = max(1, int(str(raw.get("start_line", 4)).strip() or 4))
        except Exception:
            default_start_line = 4
    base = dict(_default_mapping(default_start_line, mode_key))
    if isinstance(raw, dict):
        base.update(raw)
    base["mode"] = mode_key
    try:
        base["start_line"] = max(1, int(str(base.get("start_line", 4)).strip() or 4))
    except Exception:
        base["start_line"] = 4
    text_keys = (
        "name",
        "sheet_url",
        "sheet_name",
        "drive_id",
        "col_url",
        "col_profile",
        "col_content",
        "col_screenshot",
        "col_drive",
        "col_air_date",
        "fixed_air_date",
    )
    for key in text_keys:
        base[key] = str(base.get(key, "") or "").strip()
    for key in ("col_url", "col_profile", "col_content", "col_screenshot", "col_drive", "col_air_date"):
        base[key] = base[key].upper()
    if mode_key == "scan":
        base["col_air_date"] = ""
    else:
        air_date = str(base.get("col_air_date", "") or "").strip()
        if re.fullmatch(r"[A-Z]{1,3}", air_date):
            base["col_air_date"] = air_date
        else:
            base["col_air_date"] = _today_local_date_string()
    base["name"] = base["name"] or f"{'Scan' if mode_key == 'scan' else 'Post'} {index}"
    return base


def _normalize_mappings_by_mode(data: Any) -> dict[str, list[dict[str, Any]]]:
    if not isinstance(data, dict):
        return {}
    normalized: dict[str, list[dict[str, Any]]] = {}
    for raw_mode, items in data.items():
        mode_key = _normalize_run_mode(raw_mode)
        if mode_key not in {"seeding", "booking", "scan"}:
            continue
        if not isinstance(items, list) or not items:
            continue
        normalized[mode_key] = [
            _normalize_saved_mapping_block(item, mode_key, idx + 1)
            for idx, item in enumerate(items)
        ]
    return normalized


def _default_run_flags(mode: str) -> dict[str, Any]:
    mode_key = _normalize_run_mode(mode)
    return {
        "force_run_all": True,
        "highlight_sheet_errors": True,
        "capture_five_per_link": bool(mode_key == "booking" and False),
        "scan_negative_filter": False,
        "scan_keyword_filter": False,
    }


def _normalize_run_flags_for_mode(mode: str, raw: Any) -> dict[str, Any]:
    base = _default_run_flags(mode)
    if isinstance(raw, dict):
        base["force_run_all"] = bool(raw.get("force_run_all", base["force_run_all"]))
        base["highlight_sheet_errors"] = bool(raw.get("highlight_sheet_errors", base["highlight_sheet_errors"]))
        base["capture_five_per_link"] = bool(raw.get("capture_five_per_link", base["capture_five_per_link"]))
        base["scan_negative_filter"] = bool(raw.get("scan_negative_filter", base["scan_negative_filter"]))
        base["scan_keyword_filter"] = bool(raw.get("scan_keyword_filter", base["scan_keyword_filter"]))
    if _normalize_run_mode(mode) != "booking":
        base["capture_five_per_link"] = False
    if _normalize_run_mode(mode) != "scan":
        base["scan_negative_filter"] = False
        base["scan_keyword_filter"] = False
    return base


def _normalize_run_flags_by_mode(data: Any) -> dict[str, dict[str, Any]]:
    normalized = {
        mode: _default_run_flags(mode)
        for mode in RUN_MODES
    }
    if not isinstance(data, dict):
        return normalized
    for raw_mode, value in data.items():
        mode_key = _normalize_run_mode(raw_mode)
        if mode_key not in RUN_MODES:
            continue
        normalized[mode_key] = _normalize_run_flags_for_mode(mode_key, value)
    return normalized


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


def _running_on_vercel() -> bool:
    return bool(str(os.getenv("VERCEL", "")).strip())


def _assert_job_runtime_supported() -> None:
    if _running_on_vercel():
        raise HTTPException(
            status_code=400,
            detail=(
                "Vercel không hỗ trợ chạy job Selenium/Chrome nền cho Tool Evidence. "
                "Hãy chạy job trên máy local, VPS, hoặc Render/Railway có process nền."
            ),
        )


def _settings_storage_path() -> str:
    if _running_on_vercel():
        return os.path.join("/tmp", "tool-evidence", "app_settings.json")
    return evidence.SETTINGS_PATH


def _user_service_account_path(email: str) -> str:
    cred_dir = (
        os.path.join("/tmp", "tool-evidence", "service_accounts")
        if _running_on_vercel()
        else os.path.join(evidence.APP_DIR, "service_accounts")
    )
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


def _running_on_render() -> bool:
    return bool(str(os.getenv("RENDER", "")).strip())


def _allow_smtp_fallback_on_render() -> bool:
    return str(os.getenv("WEB_AUTH_ALLOW_SMTP_FALLBACK_ON_RENDER", "0")).strip().lower() in {"1", "true", "yes", "on"}


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
        try:
            _send_email_via_gmail_api(to_email, subject, plain_body, html_body, from_name)
            return
        except Exception as gmail_exc:
            # OAuth refresh token can break on cloud deploys; fallback to SMTP/Outlook instead of blocking login.
            gmail_detail = getattr(gmail_exc, "detail", None) if isinstance(gmail_exc, HTTPException) else str(gmail_exc)
            # Render free instances often block SMTP ports, so avoid misleading SMTP network errors by default.
            if _running_on_render() and not _allow_smtp_fallback_on_render():
                raise HTTPException(
                    status_code=500,
                    detail=(
                        f"Gửi mail thất bại. Google OAuth: {gmail_detail}. "
                        "Render đang tắt SMTP fallback mặc định; hãy kiểm tra lại GMAIL_API_CLIENT_ID, "
                        "GMAIL_API_CLIENT_SECRET, GMAIL_API_REFRESH_TOKEN, GMAIL_API_FROM_EMAIL."
                    ),
                ) from gmail_exc
            try:
                _send_email_via_smtp(to_email, subject, plain_body, html_body, from_name)
                return
            except Exception as smtp_exc:
                if not _outlook_auth_enabled():
                    smtp_detail = getattr(smtp_exc, "detail", None) if isinstance(smtp_exc, HTTPException) else str(smtp_exc)
                    raise HTTPException(
                        status_code=500,
                        detail=f"Gửi mail thất bại. Google OAuth: {gmail_detail}. SMTP: {smtp_detail}",
                    ) from smtp_exc
                try:
                    _send_email_via_outlook(to_email, subject, plain_body)
                    return
                except HTTPException as outlook_exc:
                    smtp_detail = getattr(smtp_exc, "detail", None) if isinstance(smtp_exc, HTTPException) else str(smtp_exc)
                    outlook_detail = outlook_exc.detail if isinstance(outlook_exc, HTTPException) else str(outlook_exc)
                    raise HTTPException(
                        status_code=500,
                        detail=f"Gửi mail thất bại. Google OAuth: {gmail_detail}. SMTP: {smtp_detail}. Outlook: {outlook_detail}",
                    ) from smtp_exc
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
        raise HTTPException(status_code=500, detail=f"Không lấy được access token Gmail API: {exc}") from exc
    try:
        data = resp.json()
    except Exception:
        data = {}
    if not (200 <= resp.status_code < 300):
        raw_err = data.get("error")
        err_code = raw_err if isinstance(raw_err, str) else ""
        detail = data.get("error_description") or err_code or resp.text or f"HTTP {resp.status_code}"
        if err_code == "invalid_grant":
            detail = (
                f"{detail}. Refresh token không hợp lệ/hết hạn/không cùng OAuth client. "
                "Hãy tạo lại refresh token (scope gmail.send) đúng với CLIENT_ID/CLIENT_SECRET hiện tại."
            )
        raise HTTPException(status_code=500, detail=f"Google OAuth thất bại: {detail}")
    token = str(data.get("access_token") or "").strip()
    if not token:
        raise HTTPException(status_code=500, detail="Google OAuth không trả access token")
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


def _is_loopback_host(host: str) -> bool:
    raw = str(host or "").strip().lower()
    if not raw:
        return False
    host_only = raw.split(":", 1)[0].strip("[]")
    return host_only in {"127.0.0.1", "localhost", "::1"}


def _auth_email_from_request(request: Request) -> str:
    try:
        session_data = request.session or {}
    except AssertionError:
        session_data = request.scope.get("session") or {}
    session_email = _normalize_email(session_data.get("auth_email", ""))
    if session_email:
        return session_email
    # Local-agent fallback: allow explicit user header only on loopback requests.
    try:
        headers = request.headers or {}
        host = str(headers.get("host") or "").strip().lower()
        forwarded_for = str(headers.get("x-forwarded-for") or "").strip()
        candidate = _normalize_email(headers.get("x-tool-evidence-user", ""))
        if candidate and _is_loopback_host(host) and not forwarded_for:
            return candidate
    except Exception:
        pass
    return ""


def _get_user_role(email: str) -> str:
    normalized = _normalize_email(email)
    if not normalized:
        return ""
    admins = set(_read_auth_policy().get("admin_emails") or [])
    return "admin" if normalized in admins else "user"


def _is_admin_email(email: str) -> bool:
    return _get_user_role(email) == "admin"


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
    settings_path = _settings_storage_path()
    settings_dir = os.path.dirname(settings_path)
    if settings_dir:
        os.makedirs(settings_dir, exist_ok=True)
    with open(settings_path, "w", encoding="utf-8") as f:
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
    resolved_credentials = _resolve_existing_credentials_path(str(getattr(evidence, "JSON_PATH", "")))
    return {
        "credentials_path": resolved_credentials,
        "sheet_url": str(getattr(evidence, "DEFAULT_SHEET_URL", "")),
        "sheet_name": str(getattr(evidence, "DEFAULT_SHEET_NAME_TARGET", "")),
        "drive_id": str(getattr(evidence, "DEFAULT_DRIVE_FOLDER_ID", "")),
        "scan_negative_terms": "",
        "scan_keyword_terms": "",
        "viewport_width": width,
        "viewport_height": height,
        "page_timeout_ms": int(float(getattr(evidence, "PAGE_READY_TIMEOUT", 3)) * 1000),
        "tiktok_captcha_wait_sec": int(float(getattr(evidence, "TIKTOK_CAPTCHA_MAX_WAIT_SEC", 15)) or 15),
        "please_wait_delay_sec": float(getattr(evidence, "PLEASE_WAIT_EXTRA_CAPTURE_DELAY_SEC", 2.0) or 2.0),
        "tiktok_force_focus": bool(getattr(evidence, "TIKTOK_CAPTCHA_FORCE_FOCUS", True)),
        "ready_state": "interactive",
        "full_page_capture": False,
        "mappings_by_mode": {},
        "run_flags_by_mode": _normalize_run_flags_by_mode({}),
    }


def _apply_runtime_settings(data: dict[str, Any]) -> None:
    width = max(320, int(data.get("viewport_width", 1920) or 1920))
    height = max(320, int(data.get("viewport_height", 1400) or 1400))
    timeout_ms = max(200, int(data.get("page_timeout_ms", 200) or 200))
    tiktok_captcha_wait_sec = max(5, int(data.get("tiktok_captcha_wait_sec", 15) or 15))
    please_wait_delay_sec = max(0.0, float(data.get("please_wait_delay_sec", 2.0) or 2.0))
    tiktok_force_focus = bool(data.get("tiktok_force_focus", True))
    evidence.CAPTURE_WINDOW_SIZE = f"{width},{height}"
    evidence.PAGE_READY_TIMEOUT = max(1, int(round(timeout_ms / 1000)))
    evidence.TIKTOK_CAPTCHA_MAX_WAIT_SEC = float(tiktok_captcha_wait_sec)
    evidence.PLEASE_WAIT_EXTRA_CAPTURE_DELAY_SEC = float(please_wait_delay_sec)
    evidence.TIKTOK_CAPTCHA_FORCE_FOCUS = tiktok_force_focus
    cred_path = str(data.get("credentials_path", "")).strip()
    evidence.JSON_PATH = cred_path


def _capture_runtime_settings() -> dict[str, Any]:
    width, height = _window_size_parts(getattr(evidence, "CAPTURE_WINDOW_SIZE", "1920,1400"))
    return {
        "credentials_path": str(getattr(evidence, "JSON_PATH", "")),
        "scan_negative_terms": "",
        "scan_keyword_terms": "",
        "viewport_width": width,
        "viewport_height": height,
        "page_timeout_ms": int(float(getattr(evidence, "PAGE_READY_TIMEOUT", 3)) * 1000),
        "tiktok_captcha_wait_sec": int(float(getattr(evidence, "TIKTOK_CAPTCHA_MAX_WAIT_SEC", 15)) or 15),
        "please_wait_delay_sec": float(getattr(evidence, "PLEASE_WAIT_EXTRA_CAPTURE_DELAY_SEC", 2.0) or 2.0),
        "tiktok_force_focus": bool(getattr(evidence, "TIKTOK_CAPTCHA_FORCE_FOCUS", True)),
    }


def _build_settings_payload(data: dict[str, Any] | None = None) -> dict[str, Any]:
    merged = dict(_settings_defaults())
    merged.update(data or {})
    cred_path = _resolve_existing_credentials_path(str(merged.get("credentials_path", "")).strip())
    merged["credentials_path"] = cred_path
    merged["scan_negative_terms"] = str(merged.get("scan_negative_terms", "") or "")
    merged["scan_keyword_terms"] = str(merged.get("scan_keyword_terms", "") or "")
    try:
        merged["tiktok_captcha_wait_sec"] = max(5, int(merged.get("tiktok_captcha_wait_sec", 15) or 15))
    except Exception:
        merged["tiktok_captcha_wait_sec"] = 15
    try:
        merged["please_wait_delay_sec"] = max(0.0, float(merged.get("please_wait_delay_sec", 2.0) or 2.0))
    except Exception:
        merged["please_wait_delay_sec"] = 2.0
    merged["tiktok_force_focus"] = bool(merged.get("tiktok_force_focus", True))
    merged["mappings_by_mode"] = _normalize_mappings_by_mode(merged.get("mappings_by_mode"))
    merged["run_flags_by_mode"] = _normalize_run_flags_by_mode(merged.get("run_flags_by_mode"))
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


def _resolve_existing_credentials_path(preferred_path: str = "") -> str:
    preferred = str(preferred_path or "").strip()
    if preferred and os.path.exists(preferred):
        return os.path.normpath(preferred)
    fallback = str(getattr(evidence, "JSON_PATH", "") or "").strip()
    if fallback and os.path.exists(fallback):
        return os.path.normpath(fallback)
    try:
        dynamic = str(evidence.resolve_credentials_path() or "").strip()
    except Exception:
        dynamic = ""
    if dynamic and os.path.exists(dynamic):
        return os.path.normpath(dynamic)
    return ""


def _resolve_credentials_input(credentials_input: str, user_email: str | None = None) -> str:
    raw = str(credentials_input or "").strip()
    if not raw:
        return _resolve_existing_credentials_path("")

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
        fallback = _resolve_existing_credentials_path("")
        if fallback:
            return fallback
        raise HTTPException(status_code=400, detail=f"Không tìm thấy credentials file: {path}")
    return path


def _open_spreadsheet(sheet_url: str, credentials_path: str):
    norm_url = evidence.normalize_sheet_input(sheet_url)
    if not norm_url:
        raise HTTPException(status_code=400, detail="Thiếu Sheet URL")
    cred_path = _resolve_existing_credentials_path(str(credentials_path or "").strip())
    if not cred_path or not os.path.exists(cred_path):
        has_b64 = bool(str(os.getenv("GOOGLE_CREDENTIALS_JSON_B64", "")).strip())
        has_path = bool(str(os.getenv("GOOGLE_CREDENTIALS_PATH", "")).strip())
        raise HTTPException(
            status_code=400,
            detail=(
                "Chưa có credentials để đọc Google Sheets. "
                f"Env GOOGLE_CREDENTIALS_JSON_B64={'ON' if has_b64 else 'OFF'}, "
                f"GOOGLE_CREDENTIALS_PATH={'ON' if has_path else 'OFF'}."
            ),
        )
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


def _resolve_worksheet(spreadsheet: Any, sheet_url: str, sheet_name: str):
    requested = str(sheet_name or "").strip()
    try:
        return evidence.resolve_worksheet(spreadsheet, sheet_name=requested, sheet_url=sheet_url)
    except Exception:
        pass

    def _normalize_title(value: str) -> str:
        text = str(value or "").strip().lower()
        text = "".join(
            ch for ch in unicodedata.normalize("NFD", text)
            if unicodedata.category(ch) != "Mn"
        )
        text = re.sub(r"\s+", " ", text)
        return text

    try:
        worksheets = list(spreadsheet.worksheets() or [])
    except Exception as exc:
        raise HTTPException(status_code=400, detail=f"Không đọc được danh sách sheet: {exc}") from exc

    # Fallback 1: case-insensitive + compact spaces.
    target_norm = _normalize_title(requested)
    if target_norm:
        for ws in worksheets:
            title = str(getattr(ws, "title", "") or "").strip()
            if title and _normalize_title(title) == target_norm:
                return ws

    available = [str(getattr(ws, "title", "") or "").strip() for ws in worksheets]
    available = [name for name in available if name]
    preview = ", ".join(available[:12])
    suffix = "..." if len(available) > 12 else ""
    if requested:
        raise HTTPException(
            status_code=400,
            detail=f"Không tìm thấy sheet: {requested}. Sheet hiện có: {preview}{suffix}",
        )
    raise HTTPException(status_code=400, detail="Thiếu Sheet Name")


def _worksheet_sheet_id(worksheet: Any) -> int:
    for value in (
        getattr(worksheet, "id", None),
        getattr(worksheet, "sheet_id", None),
        getattr(worksheet, "_properties", {}).get("sheetId")
        if isinstance(getattr(worksheet, "_properties", None), dict)
        else None,
        getattr(worksheet, "_properties", {}).get("id")
        if isinstance(getattr(worksheet, "_properties", None), dict)
        else None,
    ):
        try:
            sheet_id = int(value)
        except (TypeError, ValueError):
            continue
        if sheet_id >= 0:
            return sheet_id
    return -1


def _sheet_names_cache_key(user_email: str, sheet_url: str, credentials_path: str) -> str:
    return "|".join(
        [
            str(user_email or "").strip().lower(),
            evidence.normalize_sheet_input(sheet_url),
            os.path.normpath(str(credentials_path or "").strip()).lower(),
        ]
    )


def _get_cached_sheet_titles(cache_key: str) -> list[str] | None:
    now = time.time()
    with SHEET_NAMES_CACHE_LOCK:
        cached = SHEET_NAMES_CACHE.get(cache_key)
        if not cached:
            return None
        if (now - float(cached.get("ts", 0.0) or 0.0)) > SHEET_NAMES_CACHE_TTL_SEC:
            SHEET_NAMES_CACHE.pop(cache_key, None)
            return None
        titles = cached.get("titles")
        return list(titles) if isinstance(titles, list) else None


def _store_cached_sheet_titles(cache_key: str, titles: list[str]) -> None:
    with SHEET_NAMES_CACHE_LOCK:
        SHEET_NAMES_CACHE[cache_key] = {"ts": time.time(), "titles": list(titles or [])}


def _sheet_link_columns_cache_key(user_email: str, sheet_url: str, sheet_name: str, credentials_path: str, start_row: int) -> str:
    return "|".join(
        [
            str(user_email or "").strip().lower(),
            evidence.normalize_sheet_input(sheet_url),
            str(sheet_name or "").strip(),
            os.path.normpath(str(credentials_path or "").strip()).lower(),
            str(max(1, int(start_row or 1))),
        ]
    )


def _get_cached_sheet_link_columns(cache_key: str) -> dict[str, Any] | None:
    now = time.time()
    with SHEET_LINK_COLUMNS_CACHE_LOCK:
        cached = SHEET_LINK_COLUMNS_CACHE.get(cache_key)
        if not cached:
            return None
        if (now - float(cached.get("ts", 0.0) or 0.0)) > SHEET_LINK_COLUMNS_CACHE_TTL_SEC:
            SHEET_LINK_COLUMNS_CACHE.pop(cache_key, None)
            return None
        payload = cached.get("payload")
        return dict(payload) if isinstance(payload, dict) else None


def _store_cached_sheet_link_columns(cache_key: str, payload: dict[str, Any]) -> None:
    with SHEET_LINK_COLUMNS_CACHE_LOCK:
        SHEET_LINK_COLUMNS_CACHE[cache_key] = {"ts": time.time(), "payload": dict(payload or {})}


def _clear_sheet_link_columns_cache(user_email: str, sheet_url: str, sheet_name: str, credentials_path: str) -> None:
    prefix = "|".join(
        [
            str(user_email or "").strip().lower(),
            evidence.normalize_sheet_input(sheet_url),
            str(sheet_name or "").strip(),
            os.path.normpath(str(credentials_path or "").strip()).lower(),
        ]
    )
    with SHEET_LINK_COLUMNS_CACHE_LOCK:
        stale_keys = [key for key in SHEET_LINK_COLUMNS_CACHE.keys() if str(key).startswith(prefix + "|")]
        for key in stale_keys:
            SHEET_LINK_COLUMNS_CACHE.pop(key, None)


def _extract_sheet_link_columns(worksheet: Any, start_row: int = 4, sample_rows: int = 120, max_columns: int = 100) -> dict[str, Any]:
    first_row = max(1, int(start_row or 1))
    # Booking sheets may place URLs far below headers/summary blocks,
    # so keep a deeper default scan window than before.
    rows_to_scan = max(60, min(int(sample_rows or 120), 700))
    cols_to_scan = max(8, min(int(max_columns or 100), 182))
    last_col_letter = evidence.col_index_to_letter(cols_to_scan)

    counts: dict[str, int] = {}
    drive_counts: dict[str, int] = {}
    samples: dict[str, str] = {}
    scanned_ranges: list[str] = []

    def _scan_range(scan_start_row: int, scan_rows: int) -> None:
        scan_first = max(1, int(scan_start_row or 1))
        scan_len = max(10, int(scan_rows or 10))
        scan_last = scan_first + scan_len - 1
        cell_range = f"A{scan_first}:{last_col_letter}{scan_last}"
        scanned_ranges.append(cell_range)

        try:
            display_rows = worksheet.get(cell_range, value_render_option="UNFORMATTED_VALUE") or []
        except Exception:
            try:
                display_rows = worksheet.get(cell_range) or []
            except Exception as exc:
                raise HTTPException(
                    status_code=400,
                    detail=f"Không đọc được dữ liệu sheet ở vùng {cell_range}: {exc}",
                ) from exc
        try:
            formula_rows = worksheet.get(cell_range, value_render_option="FORMULA") or []
        except Exception:
            formula_rows = []

        total_rows = max(len(display_rows), len(formula_rows))
        for row_idx in range(total_rows):
            display_row = display_rows[row_idx] if row_idx < len(display_rows) and isinstance(display_rows[row_idx], list) else []
            formula_row = formula_rows[row_idx] if row_idx < len(formula_rows) and isinstance(formula_rows[row_idx], list) else []
            total_cols = min(cols_to_scan, max(len(display_row), len(formula_row)))
            for col_idx in range(total_cols):
                display_cell = str(display_row[col_idx] if col_idx < len(display_row) else "").strip()
                formula_cell = str(formula_row[col_idx] if col_idx < len(formula_row) else "").strip()
                url = ""
                if formula_cell:
                    url = evidence.extract_url_from_hyperlink_formula(formula_cell) or ""
                if not url and display_cell:
                    url = (
                        evidence.normalize_web_source_url(display_cell)
                        or evidence.normalize_scan_source_url(display_cell)
                        or ""
                    )
                if not url and display_cell:
                    m = re.search(r"https?://[^\s)\"'>]+", display_cell, flags=re.IGNORECASE)
                    if m:
                        raw_candidate = str(m.group(0) or "").strip().rstrip(".,;")
                        url = (
                            evidence.normalize_web_source_url(raw_candidate)
                            or evidence.normalize_scan_source_url(raw_candidate)
                            or raw_candidate
                        )
                if not url:
                    continue
                col_letter = evidence.col_index_to_letter(col_idx + 1)
                counts[col_letter] = counts.get(col_letter, 0) + 1
                if "drive.google.com" in url.lower() or "docs.google.com" in url.lower():
                    drive_counts[col_letter] = drive_counts.get(col_letter, 0) + 1
                if col_letter not in samples:
                    samples[col_letter] = url

    # Pass 1: scan from selected start row.
    _scan_range(first_row, rows_to_scan)
    # Pass 2 fallback: still empty -> scan deeper windows (common in booking sheets).
    if not counts:
        _scan_range(first_row + rows_to_scan, 500)
    if not counts:
        _scan_range(first_row + rows_to_scan + 500, 500)

    ordered_columns = sorted(counts.keys(), key=lambda col: (-counts[col], evidence.col_letter_to_index(col) or 9999))
    drive_columns = sorted(drive_counts.keys(), key=lambda col: (-drive_counts[col], evidence.col_letter_to_index(col) or 9999))
    return {
        "columns": ordered_columns,
        "drive_columns": drive_columns,
        "counts": counts,
        "samples": samples,
        "start_row": first_row,
        "range": ", ".join(scanned_ranges),
    }


def _normalize_selected_columns(columns: Any) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    raw_values = list(columns or []) if isinstance(columns, (list, tuple, set)) else []
    indexed: list[tuple[int, str]] = []
    for value in raw_values:
        column = str(value or "").strip().upper()
        if not column:
            continue
        idx = evidence.col_letter_to_index(column) or 0
        if idx <= 0 or column in seen:
            continue
        seen.add(column)
        indexed.append((idx, column))
    indexed.sort(key=lambda item: item[0])
    return [column for _, column in indexed]


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


def _compact_request_for_client(raw_request: Any) -> dict[str, Any]:
    request = dict(raw_request or {})
    try:
        browser_port = int(request.get("browser_port") or 0)
    except Exception:
        browser_port = 0
    try:
        start_line = max(1, int(request.get("start_line") or 1))
    except Exception:
        start_line = 1
    return {
        "owner_email": _normalize_email(request.get("owner_email")) or "",
        "mode": _infer_job_mode(request.get("mappings"), fallback=request.get("mode") or "seeding"),
        "drive_id": str(request.get("drive_id") or ""),
        "sheet_url": str(request.get("sheet_url") or ""),
        "sheet_name": str(request.get("sheet_name") or ""),
        "browser_port": browser_port,
        "start_line": start_line,
        "force_run_all": bool(request.get("force_run_all")),
        "only_run_error_rows": bool(request.get("only_run_error_rows")),
        "capture_five_per_link": bool(request.get("capture_five_per_link")),
        "highlight_sheet_errors": bool(request.get("highlight_sheet_errors")),
        "scan_negative_filter": bool(request.get("scan_negative_filter")),
        "scan_keyword_filter": bool(request.get("scan_keyword_filter")),
        "root_job_id": str(request.get("root_job_id") or ""),
        "target_rows": list(request.get("target_rows") or []),
        "target_block_name": str(request.get("target_block_name") or ""),
        "mappings": list(request.get("mappings") or []),
    }


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
    # Keep one base port per mode; worker blocks derive from get_post_port.
    return int(DEFAULT_SHARED_BROWSER_PORT)


def _get_mode_profile(run_mode: str | None, block_index: int = 0, browser_port: int | None = None) -> str:
    # Use isolated browser profile per block/port so each block can run
    # its own Chrome debug session (e.g. 9223, 9324, 9325) in parallel.
    mode_name = _normalize_run_mode(run_mode)
    raw = evidence.get_block_profile(block_index, mode_name, browser_port=browser_port)
    try:
        return evidence._resolve_writable_profile_dir(raw, browser_port=int(browser_port or _get_mode_base_port(mode_name)), log_prefix="web_ui: ")
    except Exception:
        return raw


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


def _can_view_job(job: dict[str, Any] | None, viewer_email: str) -> bool:
    normalized_viewer = _normalize_email(viewer_email)
    if not normalized_viewer or not job:
        return False
    return _job_owner_email(job) == normalized_viewer or _is_admin_email(normalized_viewer)


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


def _derive_continue_request_snapshot(source_job: dict[str, Any]) -> tuple[dict[str, Any], dict[str, int]]:
    source_request = json.loads(json.dumps(source_job.get("request") or {}))
    mappings = list(source_request.get("mappings") or [])
    if not mappings:
        raise HTTPException(status_code=400, detail="Không tìm thấy mapping để chạy tiếp")

    block_last_rows: dict[str, int] = {}
    unnamed_last_row = 0
    for log in list(source_job.get("logs") or []):
        try:
            row = int(log.get("row") or 0)
        except Exception:
            row = 0
        if row <= 0:
            continue
        block_name = _extract_log_block_name_py(log)
        if block_name:
            block_last_rows[block_name] = max(block_last_rows.get(block_name, 0), row)
        else:
            unnamed_last_row = max(unnamed_last_row, row)

    next_lines: dict[str, int] = {}
    next_starts: list[int] = []
    request_start_line = max(1, int(source_request.get("start_line") or 4))
    for item in mappings:
        block_name = str((item or {}).get("name", "") or "").strip()
        try:
            item_start_line = max(1, int(str(item.get("start_line", request_start_line)).strip() or request_start_line))
        except Exception:
            item_start_line = request_start_line
        last_row = block_last_rows.get(block_name, 0)
        if last_row <= 0 and len(mappings) == 1:
            last_row = unnamed_last_row
        next_start_line = max(item_start_line, last_row + 1) if last_row > 0 else item_start_line
        item["start_line"] = next_start_line
        if block_name:
            next_lines[block_name] = next_start_line
        next_starts.append(next_start_line)

    source_request["mappings"] = mappings
    source_request["start_line"] = min(next_starts) if next_starts else request_start_line
    source_request["target_rows"] = []
    source_request["target_block_name"] = ""
    return source_request, next_lines


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


def _read_activity_events() -> list[dict[str, Any]]:
    if not os.path.exists(ACTIVITY_HISTORY_PATH):
        return []
    try:
        with open(ACTIVITY_HISTORY_PATH, "r", encoding="utf-8") as f:
            data = json.load(f) or []
        return data if isinstance(data, list) else []
    except Exception:
        return []


def _write_activity_events(events: list[dict[str, Any]]) -> None:
    temp_path = ACTIVITY_HISTORY_PATH + ".tmp"
    with open(temp_path, "w", encoding="utf-8") as f:
        json.dump(events, f, ensure_ascii=False, indent=2)
    os.replace(temp_path, ACTIVITY_HISTORY_PATH)


def _append_activity_event(
    owner_email: str,
    *,
    kind: str,
    message: str,
    level: str = "info",
    run_mode: str = "",
    block_name: str = "",
    browser_port: int | None = None,
    job_id: str = "",
    row: int | None = None,
) -> dict[str, Any]:
    entry = {
        "id": str(uuid.uuid4()),
        "owner_email": _normalize_email(owner_email),
        "ts": _utc_now_iso(),
        "kind": str(kind or "").strip() or "event",
        "message": str(message or "").strip() or "Activity",
        "level": str(level or "info").strip().lower() or "info",
        "run_mode": _normalize_run_mode(run_mode),
        "block_name": str(block_name or "").strip(),
        "browser_port": int(browser_port) if browser_port is not None else None,
        "job_id": str(job_id or "").strip(),
        "row": int(row) if row is not None else None,
    }
    events = _read_activity_events()
    events.append(entry)
    _write_activity_events(events)
    return entry


def _list_activity_events(owner_email: str, limit: int = 0, include_all: bool = False) -> list[dict[str, Any]]:
    normalized = _normalize_email(owner_email)
    if include_all and _is_admin_email(normalized):
        rows = list(_read_activity_events())
    else:
        rows = [item for item in _read_activity_events() if _normalize_email(item.get("owner_email")) == normalized]
    rows.sort(key=lambda item: str(item.get("ts") or ""), reverse=True)
    limit_value = int(limit or 0)
    return rows if limit_value <= 0 else rows[: max(1, min(limit_value, 5000))]


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
        "issue_cells": list(job.get("issue_cells") or []),
        "completion": dict(job.get("completion") or {}) if job.get("completion") else None,
        "error": job.get("error"),
    }


def _resolved_job_history_path() -> str:
    return os.path.abspath(str(JOB_HISTORY_PATH or "web_job_history.json"))


def _persist_jobs(force: bool = False) -> None:
    global _LAST_JOB_PERSIST_TS
    now = time.time()
    with JOB_PERSIST_LOCK:
        if not force and (now - _LAST_JOB_PERSIST_TS) < JOB_PERSIST_MIN_INTERVAL_SEC:
            return
        with JOBS_LOCK:
            payload = [_serialize_job(job) for job in JOBS.values()]
        payload.sort(key=lambda x: str(x.get("created_at") or ""), reverse=True)

        history_path = _resolved_job_history_path()
        history_dir = os.path.dirname(history_path)
        os.makedirs(history_dir, exist_ok=True)
        tmp_fd, temp_path = tempfile.mkstemp(
            prefix=os.path.basename(history_path) + ".",
            suffix=".tmp",
            dir=history_dir,
        )
        try:
            with os.fdopen(tmp_fd, "w", encoding="utf-8") as f:
                json.dump(payload, f, ensure_ascii=False, indent=2)
            os.replace(temp_path, history_path)
            _LAST_JOB_PERSIST_TS = now
        finally:
            try:
                if os.path.exists(temp_path):
                    os.remove(temp_path)
            except Exception:
                pass


def _load_persisted_jobs() -> None:
    history_path = _resolved_job_history_path()
    if not os.path.exists(history_path):
        return
    try:
        with open(history_path, "r", encoding="utf-8") as f:
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
            "issue_cells": list(item.get("issue_cells") or []),
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
            "sheet_url": "",
            "sheet_name": "",
            "drive_id": "",
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
            "sheet_url": "",
            "sheet_name": "",
            "drive_id": "",
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
        "sheet_url": "",
        "sheet_name": "",
        "drive_id": "",
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
    highlight_sheet_errors: bool,
    scan_negative_filter: bool,
    scan_keyword_filter: bool,
    detail: str = "Chờ chạy",
) -> dict[str, Any]:
    job_id = str(uuid.uuid4())
    shared_run_state = {"is_running": True, "is_paused": False}
    adapter = WebAppAdapter(
        start_line=int(start_line),
        force_run_all=force_run_all,
        only_run_error_rows=only_run_error_rows,
        capture_five_per_link=capture_five_per_link,
        highlight_sheet_errors=highlight_sheet_errors,
        scan_negative_filter=scan_negative_filter,
        scan_keyword_filter=scan_keyword_filter,
        job_store={},
        persist_callback=_persist_jobs,
        attach_only_existing_browser=False,
        shared_run_state=shared_run_state,
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
        "summary": {"done": 0, "total": 0, "success": 0, "failed": 0, "unavailable": 0, "eta": "---"},
        "detail": str(detail or "Chờ chạy"),
        "ui_status": "READY",
        "ui_color": "",
        "inputs_enabled": True,
        "logs": [],
        "error_rows": {},
        "issue_cells": [],
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
        app_adapter.attach_only_existing_browser = False
        job["status"] = "running"
        job["started_at"] = _utc_now_iso()
    _persist_jobs(force=True)

    previous_runtime = _capture_runtime_settings()
    restore_screenshot_hook = None
    restore_focus_hook = None
    try:
        runtime_settings = dict(req.get("runtime_settings") or {})
        if runtime_settings:
            _apply_runtime_settings(runtime_settings)
            restore_screenshot_hook = _install_screenshot_wait_hook(
                float(runtime_settings.get("please_wait_delay_sec", 2.0) or 2.0)
            )
            restore_focus_hook = _install_tiktok_focus_boost_hook(
                bool(runtime_settings.get("tiktok_force_focus", True))
            )
        multi_seeding_blocks = list(req.get("multi_seeding_blocks") or [])
        if multi_seeding_blocks and _normalize_run_mode(req.get("mode")) == "seeding":
            normalized_triplets = {
                (
                    str(block.get("sheet_url", req.get("sheet_url", ""))).strip(),
                    str(block.get("sheet_name", req.get("sheet_name", ""))).strip(),
                    str(block.get("drive_id", req.get("drive_id", ""))).strip(),
                )
                for block in multi_seeding_blocks
            }
            if len(normalized_triplets) == 1:
                # Same sheet + same drive across blocks: run once so evidence.main_logic
                # can execute block workers in parallel.
                merged = next(iter(normalized_triplets))
                merged_mappings = []
                for block in multi_seeding_blocks:
                    block_mapping = dict(block.get("mapping") or {})
                    if block_mapping:
                        merged_mappings.append(block_mapping)
                with JOBS_LOCK:
                    live_job = JOBS.get(job_id)
                    if live_job:
                        live_job["detail"] = f"Song song {len(merged_mappings)} block trên 1 sheet"
                _persist_jobs(force=False)
                evidence.main_logic(
                    app_adapter,
                    merged[2] or req["drive_id"],
                    merged[0] or req["sheet_url"],
                    merged[1] or req["sheet_name"],
                    start_line=req["start_line"],
                    browser_port=req["browser_port"],
                    mappings=merged_mappings or req["mappings"],
                    primary_profile_path=req.get("profile_path"),
                    target_rows=req.get("target_rows"),
                    target_block_name=req.get("target_block_name"),
                )
            else:
                total_multi = len(multi_seeding_blocks)
                with JOBS_LOCK:
                    live_job = JOBS.get(job_id)
                    if live_job:
                        live_job["detail"] = f"Song song {total_multi} block nhiều sheet"
                _persist_jobs(force=False)
                evidence.write_log(f"[DEBUG] Multi-sheet parallel dispatch: {total_multi} block(s)")

                def _run_multi_sheet_block(block_idx: int, block_payload: dict[str, Any]):
                    with JOBS_LOCK:
                        live_job = JOBS.get(job_id)
                        if live_job:
                            live_job["detail"] = (
                                f"Sheet {block_idx}/{total_multi}: "
                                f"{str(block_payload.get('sheet_name') or '').strip() or '...'}"
                            )
                    _persist_jobs(force=False)
                    block_mapping = dict(block_payload.get("mapping") or {})
                    per_block_port = evidence.get_post_port(
                        max(0, int(block_idx) - 1),
                        int(req.get("browser_port") or _get_mode_base_port("seeding")),
                    )
                    # Use an isolated adapter per block so Chrome driver/session state
                    # is never shared across concurrent multi-sheet workers.
                    block_adapter = WebAppAdapter(
                        start_line=int(block_mapping.get("start_line") or req.get("start_line") or 4),
                        force_run_all=bool(req.get("force_run_all", False)),
                        only_run_error_rows=bool(req.get("only_run_error_rows", False)),
                        capture_five_per_link=bool(req.get("capture_five_per_link", False)),
                        highlight_sheet_errors=bool(req.get("highlight_sheet_errors", True)),
                        scan_negative_filter=bool(req.get("scan_negative_filter", False)),
                        scan_keyword_filter=bool(req.get("scan_keyword_filter", False)),
                        job_store=app_adapter._job_store,
                        persist_callback=_persist_jobs,
                        log_limit=int(getattr(app_adapter, "_log_limit", 0) or 0),
                        attach_only_existing_browser=False,
                        shared_run_state=getattr(app_adapter, "_shared_run_state", None),
                    )
                    evidence.main_logic(
                        block_adapter,
                        str(block_payload.get("drive_id", req.get("drive_id", ""))),
                        str(block_payload.get("sheet_url", req.get("sheet_url", ""))),
                        str(block_payload.get("sheet_name", req.get("sheet_name", ""))),
                        start_line=int(block_mapping.get("start_line") or req.get("start_line") or 4),
                        browser_port=per_block_port,
                        mappings=[block_mapping] if block_mapping else req["mappings"],
                        primary_profile_path=req.get("profile_path"),
                        target_rows=req.get("target_rows"),
                        target_block_name=req.get("target_block_name"),
                    )

                worker_count = max(1, total_multi)
                with ThreadPoolExecutor(max_workers=worker_count) as ex:
                    futures = [
                        ex.submit(_run_multi_sheet_block, idx, block)
                        for idx, block in enumerate(multi_seeding_blocks, start=1)
                    ]
                    for future in as_completed(futures):
                        future.result()
        else:
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
                success_count = 0
                failed_count = 0
                try:
                    success_count = int(summary.get("success") or 0)
                except Exception:
                    success_count = 0
                try:
                    failed_count = int(summary.get("failed") or 0)
                except Exception:
                    failed_count = 0
                unavailable_count = 0
                try:
                    unavailable_count = int(summary.get("unavailable") or 0)
                except Exception:
                    unavailable_count = 0
                has_runtime_activity = (
                    bool(job.get("logs"))
                    or done > 0
                    or success_count > 0
                    or failed_count > 0
                    or unavailable_count > 0
                )
                if current_status not in {"stopped", "failed"}:
                    if total > 0 and done >= total:
                        job["status"] = "completed"
                    else:
                        job["status"] = "stopped"
                        detail_text = str(job.get("detail") or "").strip()
                        detail_lower = detail_text.lower()
                        if total <= 0 and not done:
                            if (not detail_text) or detail_lower in {"chờ chạy", "cho chay", "running", "queued"}:
                                job["detail"] = "Không có dòng hợp lệ để xử lý. Kiểm tra Link URL, Start Line hoặc chế độ retry."
                        elif not detail_text:
                            job["detail"] = (
                                "Chưa có tác vụ nào được xử lý."
                                if not has_runtime_activity
                                else "Tiến trình kết thúc trước khi xử lý hết dữ liệu."
                            )
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
                error_text = str(exc).strip() or "Unknown error"
                job["error"] = error_text
                if not str(job.get("detail") or "").strip():
                    job["detail"] = error_text
                summary = dict(job.get("summary") or {})
                summary["eta"] = "---"
                job["summary"] = summary
                job["ui_status"] = "FAILED"
                job["ui_color"] = "#ef4444"
                job["finished_at"] = _utc_now_iso()
        _persist_jobs(force=True)
    finally:
        if restore_focus_hook:
            try:
                restore_focus_hook()
            except Exception:
                pass
        if restore_screenshot_hook:
            try:
                restore_screenshot_hook()
            except Exception:
                pass
        with JOBS_LOCK:
            job = JOBS.get(job_id)
            if job:
                job.pop("thread", None)
        _persist_jobs(force=True)
        _apply_runtime_settings(previous_runtime)


@app.middleware("http")
async def _auth_guard(request: Request, call_next):
    return await call_next(request)


def _html_no_cache_response(content: str, status_code: int = 200) -> HTMLResponse:
    response = HTMLResponse(content, status_code=status_code)
    response.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
    response.headers["Pragma"] = "no-cache"
    response.headers["Expires"] = "0"
    return response


@app.get("/login", response_class=HTMLResponse)
def login_page(request: Request):
    if _auth_email_from_request(request):
        return RedirectResponse(url="/", status_code=302)
    return _html_no_cache_response(LOGIN_PAGE_HTML)


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
        return _html_no_cache_response("ok", status_code=200)
    auth_email_raw = _auth_email_from_request(request)
    if not auth_email_raw:
        return RedirectResponse(url="/login", status_code=302)
    _ensure_bootstrap_admin(auth_email_raw)
    auth_role_raw = _get_user_role(auth_email_raw) or "user"
    request.session["auth_role"] = auth_role_raw
    auth_email = html.escape(auth_email_raw, quote=True)
    auth_role = html.escape(auth_role_raw, quote=True)
    auth_role_display = "Admin" if auth_role_raw == "admin" else "User"
    return _html_no_cache_response(
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
body{margin:0;min-height:100vh;background:linear-gradient(180deg,var(--bg-grad-1),var(--bg-grad-2));font-family:Segoe UI,Arial,sans-serif;color:var(--text);overflow-x:hidden}
.shell{width:100%;max-width:100vw;min-height:100vh;padding:10px;overflow-x:hidden}
.board{width:100%;max-width:calc(100vw - 20px);min-height:calc(100vh - 20px);background:var(--panel);border:1px solid var(--line);border-radius:18px;box-shadow:var(--shadow);display:grid;grid-template-columns:216px minmax(0,1fr);overflow:hidden}
.sidebar{background:var(--panel-soft);border-right:1px solid var(--line);padding:16px 12px;display:flex;flex-direction:column;gap:12px}
.dot{position:relative;width:68px;height:68px;border-radius:20px;background:#ffffff url('/assets/brand-mascot') center/92% no-repeat;box-shadow:0 14px 30px rgba(59,130,246,.2);border:1px solid rgba(191,219,254,.34);flex:0 0 auto;overflow:hidden}
.dot::before,.dot::after{display:none}
.brand-row{position:relative;display:flex;align-items:center;gap:14px;min-height:94px;padding:16px 16px;border:1px solid rgba(123,168,255,.14);border-radius:20px;background:linear-gradient(135deg,rgba(76,110,196,.18),rgba(255,255,255,.02) 48%,rgba(37,99,235,.08));overflow:hidden;box-shadow:inset 0 1px 0 rgba(255,255,255,.05)}
.brand-row::after{content:"";position:absolute;right:-28px;top:-24px;width:108px;height:108px;border-radius:50%;background:rgba(96,139,255,.12);filter:blur(6px)}
.brand-copy{position:relative;z-index:1;display:flex;flex-direction:column;gap:4px;min-width:0}
.brand-copy strong{font-size:17px;line-height:1.08;letter-spacing:-.03em;color:#fff}
.brand-copy span{font-size:10px;color:#a9bddc;letter-spacing:.18em;text-transform:uppercase;font-weight:700}
[data-theme="light"] .brand-row{background:linear-gradient(135deg,rgba(91,147,211,.12),rgba(255,255,255,.92) 48%,rgba(239,244,255,.86));border-color:rgba(91,147,211,.18)}
[data-theme="light"] .brand-copy strong{color:#0f172a}
[data-theme="light"] .brand-copy span{color:#51627f}
.side-nav{display:flex;flex-direction:column;gap:6px;margin-top:2px}
.side-group{display:flex;flex-direction:column;gap:6px}
.side-btn{width:100%;min-height:34px;border-radius:11px;border:1px solid transparent;display:flex;align-items:center;gap:7px;color:var(--muted);font-size:11px;background:var(--panel);padding:0 10px;cursor:pointer;text-align:left}
.side-icon{display:inline-grid;place-items:center;width:18px;height:18px;border-radius:6px;background:var(--soft);color:var(--muted)}
.side-icon svg{width:12px;height:12px;stroke:currentColor;fill:none;stroke-width:1.8;stroke-linecap:round;stroke-linejoin:round}
.side-btn.active{border-color:#dbeafe;background:#eef4ff;color:#12315f}
.side-btn.active .side-icon{background:#2f80ed;color:#fff}
[data-theme="dark"] .side-btn.active{border-color:#355072;background:#1a2940;color:#dbe6f5}
[data-theme="dark"] .side-btn.active .side-icon{background:#5b93d3;color:#fff}
.side-subnav{display:none;flex-direction:column;gap:4px;margin:-2px 0 2px 28px}
.side-group.open .side-subnav{display:flex}
.side-subbtn{border:1px solid transparent;border-radius:9px;background:transparent;color:var(--muted);padding:6px 8px;font-size:10px;text-align:left;cursor:pointer}
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
.access-directory-foot{display:flex;align-items:center;justify-content:flex-start;gap:12px;flex-wrap:wrap;margin-top:14px}
.access-directory-foot .settings-note{margin:0;min-height:auto;flex:1}
.access-layout{margin-top:12px}
.main{padding:14px 18px 18px;min-width:0;overflow-x:hidden}
.jobs-wrap::-webkit-scrollbar,.monitor-table-wrap::-webkit-scrollbar,#projectsList::-webkit-scrollbar,#activitiesTimeline::-webkit-scrollbar{width:10px;height:10px}
.jobs-wrap::-webkit-scrollbar-thumb,.monitor-table-wrap::-webkit-scrollbar-thumb,#projectsList::-webkit-scrollbar-thumb,#activitiesTimeline::-webkit-scrollbar-thumb{background:#cbd5e1;border-radius:999px;border:2px solid transparent;background-clip:padding-box}
.jobs-wrap::-webkit-scrollbar-track,.monitor-table-wrap::-webkit-scrollbar-track,#projectsList::-webkit-scrollbar-track,#activitiesTimeline::-webkit-scrollbar-track{background:transparent}
[data-theme="dark"] .jobs-wrap::-webkit-scrollbar-thumb,[data-theme="dark"] .monitor-table-wrap::-webkit-scrollbar-thumb,[data-theme="dark"] #projectsList::-webkit-scrollbar-thumb,[data-theme="dark"] #activitiesTimeline::-webkit-scrollbar-thumb{background:#41516d;border-radius:999px;border:2px solid transparent;background-clip:padding-box}
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
.h1{font-size:24px;font-weight:700;letter-spacing:-.01em}
.runs-head .h1{font-size:19px;line-height:1.2}
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
.view{display:none;min-width:0}
.view.active{display:block}
.runs-head{display:flex;justify-content:space-between;align-items:flex-start;gap:16px;margin-bottom:14px}
.runs-head .headline{flex:1;justify-content:flex-start;padding:14px 0 0}
.run-config-head{font-size:18px;font-weight:700}
.run-layout{display:grid;grid-template-columns:minmax(360px,.9fr) minmax(0,1.1fr);gap:12px;align-items:stretch}
.run-form{padding:12px 14px;height:100%}
.run-grid{display:grid;grid-template-columns:1fr;gap:10px}
.run-share-note{margin-top:14px;padding:10px 12px;border:1px solid var(--line);border-radius:12px;background:var(--panel-soft);display:grid;grid-template-columns:max-content minmax(0,1fr);align-items:center;gap:10px}
.run-share-top{margin:0 0 0 auto;max-width:720px;min-width:0}
.run-share-title{font-size:11px;font-weight:700;color:#2d6df6;letter-spacing:.01em;white-space:nowrap}
.run-share-email{margin-top:0;padding:8px 10px;border:1px solid var(--line);border-radius:8px;background:var(--input-bg);font-size:12px;color:var(--input-fg);white-space:nowrap;overflow:hidden;text-overflow:ellipsis;word-break:normal}
.sheet-link-suggest{display:flex;flex-direction:column;gap:8px;margin-top:12px;padding:10px 12px;border:1px dashed rgba(91,147,211,.24);border-radius:12px;background:linear-gradient(180deg,rgba(255,255,255,.03),rgba(255,255,255,.015)),var(--panel-soft);width:min(100%,460px);align-self:flex-start}
.sheet-link-suggest.open{display:flex;width:100%;max-width:none}
.sheet-link-suggest.idle{min-height:68px;justify-content:flex-start;padding:10px 12px}
.sheet-link-suggest.mode-booking.idle{min-height:92px;padding:10px 12px}
.sheet-link-suggest.idle .sheet-link-suggest-head{display:flex;align-items:flex-start;justify-content:space-between;gap:12px}
.sheet-link-suggest.idle .sheet-link-suggest-meta{display:none}
.sheet-link-suggest.idle .sheet-link-suggest-actions{width:auto;justify-content:flex-end;margin-left:auto}
.sheet-link-suggest.idle .sheet-link-suggest-action-btn{min-width:120px;min-height:0;padding:8px 12px;border-style:solid;border-radius:8px;display:inline-flex;align-items:center;justify-content:center}
.sheet-link-suggest-head{display:flex;align-items:center;justify-content:space-between;gap:12px;flex-wrap:nowrap}
.sheet-link-suggest-title{font-size:11px;font-weight:800;letter-spacing:.12em;text-transform:uppercase;color:#8ea0bf}
.sheet-link-suggest-meta{font-size:11px;color:var(--muted);min-height:16px}
.sheet-link-suggest-actions{display:flex;align-items:center;justify-content:flex-end;gap:10px;flex-wrap:wrap;margin-left:auto}
.sheet-link-suggest-action-group{display:flex;align-items:center;gap:8px;flex-wrap:wrap}
.sheet-link-suggest-action-group.buttons{justify-content:flex-end}
.sheet-link-suggest-action-btn{min-height:30px;padding:0 12px;border-radius:999px}
.sheet-link-suggest-action-btn.icon-only{min-width:34px;width:34px;height:34px;padding:0;display:inline-flex;align-items:center;justify-content:center}
.sheet-link-suggest-action-btn.icon-only svg{width:15px;height:15px;stroke:currentColor;fill:none;stroke-width:1.9;stroke-linecap:round;stroke-linejoin:round}
.sheet-link-suggest-action-btn.active{border-color:rgba(52,195,143,.32);background:rgba(52,195,143,.14);color:var(--green)}
.sheet-link-suggest-action-meta{font-size:11px;color:var(--muted)}
.sheet-link-suggest-rows{display:flex;flex-wrap:wrap;gap:8px}
.sheet-link-suggest-chip{display:inline-flex;align-items:center;justify-content:center;gap:6px;min-height:30px;padding:0 12px;border-radius:999px;border:1px solid rgba(91,147,211,.22);background:var(--blue-soft);color:var(--blue);font-size:12px;font-weight:700;cursor:pointer}
.sheet-link-suggest-chip:hover{border-color:rgba(47,128,237,.4)}
.sheet-link-suggest-chip.active{background:rgba(52,195,143,.14);border-color:rgba(52,195,143,.26);color:var(--green)}
.sheet-link-suggest-chip.selected{background:rgba(245,158,11,.14);border-color:rgba(245,158,11,.32);color:#b45309}
.sheet-link-suggest-empty{font-size:12px;color:var(--muted)}
[data-theme="dark"] .sheet-link-suggest{border-color:rgba(91,147,211,.28)}
[data-theme="dark"] .sheet-link-suggest-chip{background:#1a2940;color:#dbe6f5;border-color:#355072}
[data-theme="dark"] .sheet-link-suggest-chip.active{background:#153527;color:#9be6be;border-color:#25573d}
[data-theme="dark"] .sheet-link-suggest-chip.selected{background:#3a2a18;color:#f3c58e;border-color:#6f502e}
.run-form .run-actions{justify-content:flex-start;align-items:center}
.mapping-panel{border:1px solid var(--line);border-radius:16px;background:var(--panel-soft);overflow:hidden;min-width:0;height:100%}
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
.mapping-add-row.booking .mapping-toggle-card{margin-left:auto}
.mapping-check{display:inline-flex;align-items:center;gap:8px;font-size:12px;color:var(--text)}
.mapping-check input{width:16px;height:16px}
.scan-filter-editor{margin-top:12px;padding:0;border:0;border-radius:0;background:transparent;display:flex;flex-direction:column;gap:12px;width:100%;max-width:none;align-self:stretch}
.scan-filter-head{display:flex;align-items:center;justify-content:space-between;gap:10px;flex-wrap:wrap}
.scan-filter-title{font-size:13px;font-weight:700;color:var(--text)}
.scan-filter-note{font-size:11px;color:var(--muted)}
.scan-filter-grid{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:12px}
.scan-filter-block{border:1px solid var(--line);border-radius:12px;background:var(--panel-soft);padding:12px;display:flex;flex-direction:column;gap:10px;min-width:0}
.scan-filter-block-head{display:flex;align-items:flex-start;justify-content:space-between;gap:10px}
.scan-filter-block-title{font-size:13px;font-weight:700;color:var(--text);line-height:1.3}
.scan-filter-switch{display:inline-flex;align-items:center;justify-content:center;padding:0;background:transparent;border:0;flex:0 0 auto}
.scan-filter-switch .mapping-toggle-switch{width:44px;height:26px}
.scan-filter-switch .mapping-toggle-slider{width:44px;height:26px}
.scan-filter-switch .mapping-toggle-slider::after{top:3px;left:3px;width:18px;height:18px}
.scan-filter-switch .mapping-toggle-switch input:checked + .mapping-toggle-slider::after{left:23px}
.scan-filter-block textarea{width:100%;min-height:108px;padding:10px 12px;border:1px solid var(--line);border-radius:10px;background:var(--input-bg);color:var(--input-fg);font-size:12px;line-height:1.45;resize:vertical}
.scan-filter-block textarea:focus{outline:none;border-color:#60a5fa;box-shadow:0 0 0 3px rgba(96,165,250,.14)}
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
.run-actions{display:flex;gap:10px 12px;flex-wrap:wrap;margin-top:12px}
.action-btn{min-height:44px;padding:0 16px;border-radius:14px;display:inline-flex;align-items:center;gap:10px;font-weight:700;letter-spacing:-.01em;box-shadow:0 10px 24px rgba(15,23,42,.08);transition:transform .15s ease,box-shadow .15s ease,filter .15s ease}
.action-btn:hover{transform:translateY(-1px);box-shadow:0 14px 30px rgba(15,23,42,.12);filter:saturate(1.03)}
.action-btn .action-icon{width:22px;height:22px;display:inline-flex;align-items:center;justify-content:center;border-radius:999px;flex:0 0 auto}
.action-btn svg{width:14px;height:14px;stroke:currentColor;fill:none;stroke-width:2;stroke-linecap:round;stroke-linejoin:round}
.action-btn .action-label{white-space:nowrap}
.action-btn.start{background:linear-gradient(135deg,#3b82f6,#2563eb);border-color:#2563eb;color:#fff}
.action-btn.start .action-icon{background:rgba(255,255,255,.18);color:#fff}
.action-btn.pause{background:linear-gradient(135deg,#ef4444,#dc2626);border-color:#b91c1c;color:#fff}
.action-btn.pause .action-icon{background:rgba(255,255,255,.18);color:#fff}
.action-btn.resume{background:linear-gradient(135deg,#22c55e,#16a34a);border-color:#1f9a4a;color:#fff}
.action-btn.resume .action-icon{background:rgba(255,255,255,.18);color:#fff}
.action-btn.red{background:linear-gradient(135deg,#fff1f2,#ffe4e6);border-color:#fecdd3;color:#be123c}
.action-btn.red .action-icon{background:rgba(190,24,93,.1);color:#be123c}
.action-btn.soft{background:var(--panel);border-color:var(--line);color:var(--text);box-shadow:none}
.action-btn.soft:hover{box-shadow:none;filter:none}
.action-btn.soft .action-icon{background:rgba(148,163,184,.14);color:var(--muted)}
.action-btn.soft.stop{background:#fff7f7;border-color:#fecdd3;color:#be123c}
.action-btn.soft.stop .action-icon{background:rgba(190,24,93,.1);color:#be123c}
[data-theme="dark"] .action-btn{box-shadow:none}
[data-theme="dark"] .action-btn.start{background:linear-gradient(135deg,#3b82f6,#1d4ed8);border-color:#2563eb}
[data-theme="dark"] .action-btn.pause{background:linear-gradient(135deg,#dc2626,#991b1b);border-color:#7f1d1d}
[data-theme="dark"] .action-btn.resume{background:linear-gradient(135deg,#22c55e,#15803d);border-color:#1d8d46}
[data-theme="dark"] .action-btn.red{background:linear-gradient(135deg,#2b1822,#351a24);border-color:#5b2435;color:#fda4af}
[data-theme="dark"] .action-btn.red .action-icon{background:rgba(253,164,175,.12);color:#fda4af}
[data-theme="dark"] .action-btn.soft{background:var(--panel-soft);border-color:var(--line);color:var(--text)}
[data-theme="dark"] .action-btn.soft .action-icon{background:rgba(148,163,184,.12);color:#cbd5e1}
[data-theme="dark"] .action-btn.soft.stop{background:#2b1822;border-color:#5b2435;color:#fda4af}
[data-theme="dark"] .action-btn.soft.stop .action-icon{background:rgba(253,164,175,.12);color:#fda4af}
.run-overwrite-card{display:inline-grid;grid-template-columns:minmax(0,1fr) auto;align-items:center;gap:12px;min-width:220px;max-width:280px;flex:0 1 auto;padding:10px 12px;border:1px solid var(--line);border-radius:14px;background:linear-gradient(180deg,rgba(255,255,255,.03),rgba(255,255,255,.01)),var(--panel-soft);cursor:pointer}
.run-overwrite-copy{display:flex;flex-direction:column;gap:0;min-width:0}
.run-overwrite-title{font-size:13px;font-weight:700;color:var(--text);line-height:1.2}
.run-overwrite-help{display:none}
.run-overwrite-switch{position:relative;display:inline-flex;align-items:center;justify-content:center;width:52px;height:30px;flex:0 0 auto}
.run-overwrite-switch input{position:absolute;inset:0;opacity:0;cursor:pointer}
.run-overwrite-slider{position:relative;display:block;width:52px;height:30px;border-radius:999px;background:#cbd5e1;border:1px solid rgba(148,163,184,.28);transition:background .2s ease,border-color .2s ease,box-shadow .2s ease}
.run-overwrite-slider::after{content:"";position:absolute;top:3px;left:3px;width:22px;height:22px;border-radius:50%;background:#fff;box-shadow:0 4px 12px rgba(15,23,42,.16);transition:left .2s ease}
.run-overwrite-switch input:checked + .run-overwrite-slider{background:linear-gradient(135deg,#3b82f6,#2563eb);border-color:#2563eb;box-shadow:0 0 0 4px rgba(59,130,246,.12)}
.run-overwrite-switch input:checked + .run-overwrite-slider::after{left:25px}
[data-theme="dark"] .run-overwrite-card{background:linear-gradient(180deg,rgba(255,255,255,.025),rgba(255,255,255,.01)),var(--panel-soft)}
[data-theme="dark"] .run-overwrite-slider{background:#334155;border-color:#475569}
[data-theme="dark"] .run-overwrite-slider::after{background:#f8fafc}
.run-actions-main{display:flex;align-items:center;gap:8px;flex-wrap:wrap;margin-left:auto}
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
.monitor-block-progress{display:flex;flex-direction:column;gap:8px;margin-top:10px}
.monitor-block-progress[hidden]{display:none}
.monitor-block-progress-row{display:grid;grid-template-columns:minmax(0,1fr) auto;gap:10px;align-items:center;padding:8px 10px;border:1px solid var(--line);border-radius:10px;background:var(--panel)}
.monitor-block-progress-name{font-size:12px;font-weight:700;color:var(--text);white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
.monitor-block-progress-meta{font-size:11px;color:var(--muted);white-space:nowrap}
.monitor-error-main{margin-top:10px}
.monitor-error-stats{display:flex;flex-wrap:wrap;gap:8px}
.monitor-error-stat{display:inline-flex;align-items:center;gap:6px;padding:7px 10px;border-radius:999px;border:1px solid rgba(91,147,211,.22);background:var(--blue-soft);color:var(--blue);font-size:12px;font-weight:700}
.monitor-error-stat strong{font-size:13px;font-weight:800}
.monitor-error-stat.success{background:rgba(52,195,143,.14);border-color:rgba(52,195,143,.28);color:var(--green)}
.monitor-error-stat.failed{background:rgba(240,138,160,.14);border-color:rgba(240,138,160,.28);color:var(--red)}
.monitor-error-stat.unavailable{background:rgba(245,158,11,.14);border-color:rgba(245,158,11,.28);color:#b45309}
.monitor-error-summary{font-size:12px;color:var(--text);margin-top:8px;line-height:1.45;min-height:0}
.monitor-issue-strip{display:flex;align-items:flex-start;gap:12px;max-width:100%;margin:0 18px 6px;padding:10px 12px;border:1px solid rgba(98,122,168,.14);border-radius:14px;background:linear-gradient(180deg,rgba(239,244,255,.95),rgba(233,240,252,.9))}
.monitor-issue-strip[hidden]{display:none !important}
.monitor-issue-strip + .monitor-issue-strip{margin-top:-2px}
.monitor-issue-strip strong{flex:0 0 148px;display:inline-flex;align-items:center;justify-content:center;text-align:center;font-size:11px;letter-spacing:.08em;text-transform:uppercase;color:#5f74a2;padding:7px 10px;border-radius:999px;background:rgba(67,97,238,.08);border:1px solid rgba(96,165,250,.14)}
.monitor-issue-rows{display:flex;flex-wrap:wrap;gap:8px;align-items:center;min-height:32px}
.monitor-issue-chip{display:inline-flex;align-items:center;justify-content:center;padding:5px 10px;border-radius:999px;background:rgba(99,102,241,.1);border:1px solid rgba(129,140,248,.18);color:#31456f;font-size:12px;font-weight:600;letter-spacing:.01em;line-height:1}
.monitor-issue-chip.more{background:rgba(148,163,184,.08);border-color:rgba(148,163,184,.16);color:#51627f}
.monitor-issue-chip.action{cursor:pointer}
.monitor-issue-chip.action:hover{border-color:rgba(96,165,250,.28);color:#183153;background:rgba(96,165,250,.12)}
.monitor-table-card{margin-top:14px;border:1px solid var(--line);border-radius:22px;overflow:hidden;background:var(--panel-soft);min-width:0}
.monitor-table-head{display:flex;justify-content:space-between;align-items:center;gap:12px;padding:16px 18px;border-bottom:1px solid var(--line)}
.monitor-table-title{font-size:16px;font-weight:700}
.monitor-table-actions{display:flex;align-items:center;justify-content:flex-end;gap:8px;flex-wrap:wrap}
.monitor-export-btn{display:inline-flex;align-items:center;gap:8px;padding:8px 12px;border:1px solid var(--line);border-radius:999px;background:var(--panel);color:var(--text);font-size:12px;font-weight:700;cursor:pointer}
.monitor-export-btn svg{width:14px;height:14px;stroke:currentColor;fill:none;stroke-width:1.9;stroke-linecap:round;stroke-linejoin:round}
.monitor-export-btn:hover{border-color:rgba(47,128,237,.35);color:var(--blue)}
.monitor-table-wrap{max-height:360px;overflow-y:auto;overflow-x:hidden;width:100%}
.monitor-table{width:100%;border-collapse:collapse;table-layout:fixed}
.monitor-table th,.monitor-table td{padding:12px 16px;font-size:13px;text-align:left;border-bottom:1px solid var(--line);vertical-align:top;word-break:break-word;overflow-wrap:anywhere}
.monitor-table th:nth-child(1),.monitor-table td:nth-child(1){width:132px}
.monitor-table th:nth-child(2),.monitor-table td:nth-child(2){width:108px}
.monitor-table th:nth-child(3),.monitor-table td:nth-child(3){width:80px}
.monitor-table th:nth-child(4),.monitor-table td:nth-child(4){width:128px}
.monitor-table th:nth-child(6),.monitor-table td:nth-child(6){width:118px}
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
[data-theme="dark"] .monitor-issue-strip{border-color:rgba(98,122,168,.22);background:linear-gradient(180deg, rgba(30,41,59,.5), rgba(15,23,42,.42))}
[data-theme="dark"] .monitor-issue-strip strong{color:#9eb0d1;background:rgba(67,97,238,.12);border-color:rgba(96,165,250,.18)}
[data-theme="dark"] .monitor-issue-chip{background:rgba(99,102,241,.14);border-color:rgba(129,140,248,.24);color:#dbe7ff}
[data-theme="dark"] .monitor-issue-chip.more{background:rgba(148,163,184,.12);border-color:rgba(148,163,184,.2);color:#b8c3d9}
[data-theme="dark"] .monitor-issue-chip.action:hover{border-color:rgba(191,219,254,.32);color:#eef4ff;background:rgba(99,102,241,.16)}
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
#projectsList{max-height:min(72vh,980px);overflow-y:auto;padding-right:4px}
.list-row{display:flex;justify-content:space-between;align-items:center;padding:10px 12px;border:1px solid var(--line);border-radius:10px;background:var(--panel-soft);font-size:12px}
.project-item{width:100%;text-align:left;cursor:pointer;gap:8px;background:var(--panel-soft);min-height:56px;align-items:center}
.project-item.list-row{padding:6px 9px;border-radius:9px}
.project-item.active{border-color:#bfdbfe;background:var(--blue-soft)}
[data-theme="dark"] .project-item.active{border-color:#355072;background:#1a2940}
.project-list-head{display:flex;justify-content:space-between;align-items:flex-end;gap:12px;flex-wrap:wrap}
.project-filter-stack{display:flex;justify-content:flex-end;align-items:center;gap:10px 14px;flex-wrap:wrap;min-width:0}
.project-mode-filters,.project-status-filters{min-width:0}
.project-filter-select{display:flex;align-items:center;gap:8px;min-width:0}
.project-filter-select span{font-size:9px;font-weight:800;letter-spacing:.1em;text-transform:uppercase;color:var(--muted);white-space:nowrap;flex:0 0 auto}
.project-filter-input{width:170px;min-width:0;height:32px;padding:0 30px 0 10px;border:1px solid var(--line);border-radius:9px;background:var(--panel-soft);color:var(--text);font-size:11px;font-weight:700;outline:none;cursor:pointer;appearance:auto;-webkit-appearance:menulist}
.project-filter-input:focus{outline:none;border-color:#60a5fa;box-shadow:0 0 0 3px rgba(96,165,250,.14)}
[data-theme="dark"] .project-filter-input{background:#162033}
[data-theme="dark"] .project-filter-input option{background:#162033;color:#dbe6f5}
.project-item-main{display:flex;flex-direction:column;gap:3px;min-width:0}
.project-item-title{font-size:11px;font-weight:700;line-height:1.2;display:block;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
.project-item-meta{font-size:9.5px;color:var(--muted);display:flex;align-items:center;gap:5px;flex-wrap:nowrap;overflow:hidden}
.project-item-meta span{min-width:0;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
.project-item .mode-pill{min-height:17px;padding:0 6px;font-size:8.5px}
.mode-pill{display:inline-flex;align-items:center;justify-content:center;min-height:20px;padding:0 8px;border-radius:999px;border:1px solid transparent;font-size:10px;font-weight:700;letter-spacing:.04em;text-transform:uppercase}
.mode-pill.mode-seeding{background:#ecfdf3;color:#166534;border-color:#bbf7d0}
.mode-pill.mode-booking{background:#fff7ed;color:#c2410c;border-color:#fed7aa}
.mode-pill.mode-scan{background:#eef4ff;color:#1d4ed8;border-color:#bfdbfe}
[data-theme="dark"] .mode-pill.mode-seeding{background:#153527;color:#9be6be;border-color:#25573d}
[data-theme="dark"] .mode-pill.mode-booking{background:#3a2a18;color:#f3c58e;border-color:#6f502e}
[data-theme="dark"] .mode-pill.mode-scan{background:#1a2940;color:#dbe6f5;border-color:#355072}
.project-item-side{display:grid;grid-template-columns:98px 48px 26px;align-items:center;justify-items:end;column-gap:7px;flex-shrink:0}
.project-item-progress{font-size:10px;font-weight:700;color:var(--text)}
.project-item-side .project-status-badge{width:98px}
.project-item .project-status-badge{min-height:18px;padding:0 8px;font-size:9.5px}
.project-status-badge{display:inline-flex;align-items:center;justify-content:center;min-height:22px;padding:0 10px;border-radius:999px;border:1px solid transparent;font-size:11px;font-weight:700;letter-spacing:.01em}
.project-status-badge.status-queued{background:#f8fafc;color:#475569;border-color:#cbd5e1}
.project-status-badge.status-running{background:#ede9fe;color:#6d28d9;border-color:#c4b5fd}
.project-status-badge.status-paused{background:#fff7ed;color:#c2410c;border-color:#fdba74}
.project-status-badge.status-stopped{background:#f8fafc;color:#64748b;border-color:#cbd5e1}
.project-status-badge.status-completed{background:#eef4ff;color:#1d4ed8;border-color:#bfdbfe}
.project-status-badge.status-failed{background:#fff1f2;color:#be123c;border-color:#fecdd3}
[data-theme="dark"] .project-status-badge.status-queued{background:#182338;color:#dbe6f5;border-color:#475467}
[data-theme="dark"] .project-status-badge.status-running{background:#2a2152;color:#ddd6fe;border-color:#6d56d6}
[data-theme="dark"] .project-status-badge.status-paused{background:#3a2a18;color:#f3c58e;border-color:#6f502e}
[data-theme="dark"] .project-status-badge.status-stopped{background:#182338;color:#cbd5e1;border-color:#475467}
[data-theme="dark"] .project-status-badge.status-completed{background:#1a2940;color:#b7d2f3;border-color:#355072}
[data-theme="dark"] .project-status-badge.status-failed{background:#2a1620;color:#fda4af;border-color:#5b2435}
.project-delete-btn{display:inline-flex;align-items:center;justify-content:center;width:26px;height:26px;border:1px solid #fecaca;border-radius:8px;background:#fff1f2;color:#be123c;cursor:pointer}
.project-delete-btn svg{width:11px;height:11px;stroke:currentColor;fill:none;stroke-width:1.8;stroke-linecap:round;stroke-linejoin:round}
.project-delete-btn:hover{background:#ffe4e6}
[data-theme="dark"] .project-delete-btn{background:#2a1620;border-color:#5b2435;color:#fda4af}
[data-theme="dark"] .project-delete-btn:hover{background:#351a24}
.project-card-head{display:flex;justify-content:flex-start;align-items:center;gap:10px}
.project-detail-actions{margin-top:10px;display:flex;justify-content:flex-start}
.project-card-head .project-detail-actions{margin-top:0}
.project-nav-btn{display:inline-flex;align-items:center;justify-content:center;width:34px;height:34px;border:1px solid #bfdbfe;border-radius:999px;background:#eef4ff;color:#1d4ed8;cursor:pointer}
.project-nav-btn svg{width:15px;height:15px;stroke:currentColor;fill:none;stroke-width:2;stroke-linecap:round;stroke-linejoin:round}
.project-nav-btn:hover{background:#dbeafe}
[data-theme="dark"] .project-nav-btn{background:#1a2940;border-color:#355072;color:#dbe6f5}
[data-theme="dark"] .project-nav-btn:hover{background:#223149}
.project-log-panel{margin-top:12px;border:1px solid var(--line);border-radius:14px;background:var(--panel-soft);overflow:hidden}
.project-log-head{display:flex;align-items:center;justify-content:space-between;gap:10px;padding:10px 12px;border-bottom:1px solid var(--line)}
.project-log-title{font-size:13px;font-weight:700}
.project-log-sub{font-size:11px;color:var(--muted)}
.project-log-list{max-height:280px;overflow:auto;display:flex;flex-direction:column}
.project-log-item{padding:10px 12px;border-bottom:1px solid var(--line);display:flex;flex-direction:column;gap:6px}
.project-log-item:last-child{border-bottom:0}
.project-log-top{display:flex;align-items:center;justify-content:space-between;gap:8px;flex-wrap:wrap}
.project-log-meta{font-size:11px;color:var(--muted);display:flex;align-items:center;gap:8px;flex-wrap:wrap}
.project-log-message{font-size:12px;line-height:1.45;color:var(--text);word-break:break-word;overflow-wrap:anywhere}
.project-log-empty{padding:14px 12px;font-size:12px;color:var(--muted)}
.project-log-list::-webkit-scrollbar{width:10px;height:10px}
.project-log-list::-webkit-scrollbar-thumb{background:#cbd5e1;border-radius:999px;border:2px solid transparent;background-clip:padding-box}
[data-theme="dark"] .project-log-list::-webkit-scrollbar-thumb{background:#41516d;border-radius:999px;border:2px solid transparent;background-clip:padding-box}
.timeline{display:flex;flex-direction:column;gap:10px}
.timeline-item{padding:10px 12px;border-left:3px solid var(--blue);background:var(--panel-soft);border-radius:0 10px 10px 0}
#projectsSnapshot{margin-top:6px !important;gap:6px}
#projectsSnapshot .timeline-item{padding:7px 10px;border-left-width:2px;border-radius:0 8px 8px 0}
#projectsSnapshot .timeline-item strong{display:block;font-size:9.5px;line-height:1.1;letter-spacing:.08em;text-transform:uppercase;color:var(--muted);margin-bottom:2px}
#projectsSnapshot .timeline-item > div{font-size:13px;line-height:1.24}
#projectsSnapshot .snapshot-pair{display:grid;grid-template-columns:minmax(0,1.25fr) minmax(0,.85fr);gap:6px}
#projectsSnapshot .snapshot-pair .timeline-item{margin:0}
#projectsSnapshot .project-log-panel{margin-top:8px;border-radius:10px}
#projectsSnapshot .project-log-head{padding:8px 10px}
#projectsSnapshot .project-log-item{padding:8px 10px;gap:4px}
#projectsSnapshot .project-log-title{font-size:12px}
#projectsSnapshot .project-log-meta{font-size:10px;gap:6px}
#projectsSnapshot .project-log-message{font-size:11px;line-height:1.35}
@media (max-width:760px){#projectsSnapshot .snapshot-pair{grid-template-columns:1fr}}
#activitiesTimeline{max-height:min(74vh,960px);overflow-y:auto;padding-right:4px}
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
.completion-alert-host{position:fixed;top:18px;left:50%;transform:translateX(-50%);display:flex;flex-direction:column;gap:10px;z-index:10020;pointer-events:none;width:min(460px,calc(100vw - 32px))}
.completion-alert{pointer-events:auto;display:flex;align-items:center;gap:12px;padding:12px 14px;border-radius:18px;border:1px solid rgba(52,195,143,.34);background:linear-gradient(180deg,rgba(16,40,28,.98),rgba(11,28,20,.98));box-shadow:0 18px 40px rgba(0,0,0,.34),0 0 0 1px rgba(52,195,143,.08);color:#ecfdf3;animation:completion-alert-in .18s ease forwards,completion-alert-pulse 1.8s ease-in-out infinite}
.completion-alert-icon{width:38px;height:38px;border-radius:12px;display:grid;place-items:center;flex:0 0 auto;background:rgba(52,195,143,.16);border:1px solid rgba(134,239,172,.28);color:#9be6be;font-size:19px;font-weight:900}
.completion-alert-copy{min-width:0;flex:1}
.completion-alert-kicker{display:none}
.completion-alert-title{font-size:15px;font-weight:900;line-height:1.2;color:#f0fdf4;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
.completion-alert-message{margin-top:2px;font-size:12px;line-height:1.4;color:#d1fae5;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
.completion-alert-meta{margin-top:6px;display:flex;flex-wrap:wrap;gap:6px}
.completion-alert-chip{display:inline-flex;align-items:center;min-height:22px;padding:0 8px;border-radius:999px;border:1px solid rgba(134,239,172,.18);background:rgba(15,23,42,.24);color:#ecfdf3;font-size:11px;font-weight:700}
.completion-alert-close{flex:0 0 auto;width:34px;height:34px;border-radius:999px;border:1px solid rgba(134,239,172,.22);background:rgba(15,23,42,.34);color:#ecfdf3;font-size:18px;font-weight:800;cursor:pointer}
.completion-alert-close:hover{background:rgba(15,23,42,.5);border-color:rgba(134,239,172,.4)}
@keyframes completion-alert-in{from{opacity:0;transform:translateY(-10px)}to{opacity:1;transform:translateY(0)}}
@keyframes completion-alert-pulse{0%,100%{box-shadow:0 18px 40px rgba(0,0,0,.34),0 0 0 1px rgba(52,195,143,.08)}50%{box-shadow:0 18px 40px rgba(0,0,0,.34),0 0 0 1px rgba(52,195,143,.16),0 0 0 6px rgba(52,195,143,.06)}}
@media (max-width:980px){.board{grid-template-columns:1fr}.layout,.bottom{grid-template-columns:1fr}.search{display:none}}
@media (max-width:980px){.run-layout,.run-grid,.cards-3,.cards-4,.settings-layout,.monitor-grid,.mapping-scan-grid,.admin-access-grid,.access-layout,.access-mail-grid,.access-entry-grid,.overview-top-grid,.scan-filter-grid,.scan-filter-block-body{grid-template-columns:1fr}.access-entry-editor.open{grid-template-columns:1fr;grid-template-areas:"head" "meta" "form" "actions"}.sidebar{border-right:0;border-bottom:1px solid var(--line)}.runs-head{flex-direction:column;align-items:stretch}.runs-head .headline{padding:14px 0 0}.run-share-top{max-width:none;min-width:0;margin:0}.run-share-note{grid-template-columns:1fr;align-items:stretch}.run-share-title{white-space:normal}.access-directory-actions,.access-filter-row,.access-filter-group,.access-mail-foot,.access-entry-foot{align-items:stretch}.access-search{min-width:0;max-width:none;width:100%}.access-row-actions{justify-content:flex-start}.access-entry-editor.open>.access-entry-foot{align-items:stretch}.access-entry-editor.open>.access-entry-foot .settings-note{text-align:left}.overview-note{flex-direction:column;align-items:stretch}.overview-cta{justify-content:center}.project-filter-stack{min-width:0;align-items:stretch}.project-filter-select{width:100%}.project-filter-input{width:100%;min-width:0}.project-item{min-height:0;align-items:flex-start}.project-item-side{grid-template-columns:1fr;justify-items:flex-start;row-gap:6px}.project-item-side .project-status-badge{width:auto}.completion-alert-host{top:12px;width:calc(100vw - 20px)}.completion-alert{padding:11px 12px;gap:10px}.completion-alert-title{font-size:14px}.completion-alert-message{font-size:11px}.completion-alert-chip{font-size:10px}.completion-alert-close{width:30px;height:30px}}
@media (max-width:980px){.run-share-note{grid-template-columns:max-content minmax(0,1fr);align-items:center}.run-share-title{white-space:nowrap}}
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
          </div>
        </div>
        <div class="side-nav">
          <div id="runs_group" class="side-group">
            <button class="side-btn active" data-view="runs" onclick="switchView('runs', this)"><span class="side-icon"><svg viewBox="0 0 24 24"><path d="M6 4h12"/><path d="M9 4v4l-3 5a4 4 0 0 0 3.4 6h5.2A4 4 0 0 0 18 13l-3-5V4"/><path d="M8 14h8"/></svg></span><span>Runs</span></button>
            <div class="side-subnav">
              <button id="run_mode_seeding" class="side-subbtn" type="button" onclick="openRunMode('seeding')">Seeding</button>
              <button id="run_mode_booking" class="side-subbtn" type="button" onclick="openRunMode('booking')">Booking</button>
              <button id="run_mode_scan" class="side-subbtn" type="button" onclick="openRunMode('scan')">Scan</button>
            </div>
          </div>
          <button class="side-btn" data-view="projects" onclick="switchView('projects', this)"><span class="side-icon"><svg viewBox="0 0 24 24"><rect x="3" y="4" width="18" height="14" rx="2"/><path d="M3 10h18"/><path d="M8 20h8"/></svg></span><span>Projects</span></button>
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

        <section id="view-overview" class="view">
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

        <section id="view-runs" class="view active">
          <div class="runs-head">
            <div class="headline">
              <div id="runTitleText" class="h1">Seeding</div>
            </div>
            <div id="runtimeBadge" class="state" style="margin-left:auto">Runtime: Host</div>
            <div class="run-share-note run-share-top">
              <div id="runShareLabel" class="run-share-title">Chia sẻ Sheet & Drive folder cho (quyền Editor):</div>
              <div id="runShareEmail" class="run-share-email">Chưa có email service account</div>
            </div>
          </div>
          <div class="run-layout">
            <section class="card run-form">
              <div class="run-grid">
                <div class="field"><label>Sheet URL</label><input id="sheet_url" /><div id="sheet_url_hint" class="settings-note"></div></div>
                <div id="sheet_name_field" class="field"><label>Sheet Name</label><input id="sheet_name" list="sheet_name_suggestions" autocomplete="off" /><datalist id="sheet_name_suggestions"></datalist><div id="sheet_name_hint" class="settings-note"></div></div>
                <div id="drive_id_field" class="field"><label for="drive_id">Drive Folder ID</label><input id="drive_id" /></div>
              </div>
              <div class="run-actions">
                <label class="run-overwrite-card">
                  <span class="run-overwrite-copy">
                    <span id="overwriteRunLabel" class="run-overwrite-title">Overwrite</span>
                    <span id="overwriteRunHelp" class="run-overwrite-help">Always rerun and replace previous results</span>
                  </span>
                  <span class="run-overwrite-switch">
                    <input id="force_run_all" type="checkbox" checked onchange="rememberCurrentRunFlags()" />
                    <span class="run-overwrite-slider"></span>
                  </span>
                </label>
                <label class="run-overwrite-card">
                  <span class="run-overwrite-copy">
                    <span id="highlightSheetErrorsLabel" class="run-overwrite-title">Highlight Sheet Errors</span>
                    <span id="highlightSheetErrorsHelp" class="run-overwrite-help">Color output cells on the sheet when a row is unavailable or fails</span>
                  </span>
                  <span class="run-overwrite-switch">
                    <input id="highlight_sheet_errors" type="checkbox" onchange="rememberCurrentRunFlags()" />
                    <span class="run-overwrite-slider"></span>
                  </span>
                </label>
                <div id="bookingRunExtraToggles"></div>
                <div class="run-actions-main">
                  <button class="btn action-btn start" onclick="startJob()">
                    <span class="action-icon" aria-hidden="true">
                      <svg viewBox="0 0 24 24"><path d="M8 6.5v11l9-5.5-9-5.5Z"></path></svg>
                    </span>
                    <span id="startJobLabel" class="action-label">Start Job</span>
                  </button>
                  <button class="btn action-btn pause" onclick="stopJob()">
                    <span id="pauseJobIconWrap" class="action-icon" aria-hidden="true">
                      <svg id="pauseJobIcon" viewBox="0 0 24 24"><rect x="7" y="7" width="10" height="10" rx="1.5"></rect></svg>
                    </span>
                    <span id="pauseJobLabel" class="action-label">Stop</span>
                  </button>
                </div>
              </div>
              <div id="sheet_link_suggest" class="sheet-link-suggest">
                <div class="sheet-link-suggest-head">
                  <div id="sheet_link_suggest_title" class="sheet-link-suggest-title">Cột có link</div>
                  <div id="sheet_link_suggest_actions" class="sheet-link-suggest-actions">
                    <div class="sheet-link-suggest-action-group buttons">
                      <button class="btn sheet-link-suggest-action-btn" type="button" onclick="handleSheetLinkQuickAction()">Quét nhanh</button>
                    </div>
                  </div>
                </div>
                <div id="sheet_link_suggest_meta" class="sheet-link-suggest-meta"></div>
                <div id="sheet_link_suggest_rows" class="sheet-link-suggest-rows"></div>
                <datalist id="sheet_link_column_datalist"></datalist>
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
                <div id="runMonitorBlockProgress" class="monitor-block-progress" hidden></div>
              </section>
              <section class="monitor-mini">
                <div id="runMonitorErrorLabel" class="monitor-mini-label">Loi theo link sheet</div>
                <div id="runMonitorErrorMain" class="monitor-error-main">Khong co loi</div>
                <div id="runMonitorErrorMeta" class="monitor-error-summary">Success 0 - Failed 0</div>
              </section>
            </div>
            <div class="monitor-table-card">
                <div id="runMonitorIssueRowsStrip" class="monitor-issue-strip" hidden>
                  <strong id="runMonitorIssueRowsLabel">Loi</strong>
                  <div id="runMonitorErrorRows" class="monitor-issue-rows"></div>
                </div>
                <div id="runMonitorUnavailableRowsStrip" class="monitor-issue-strip" hidden>
                  <strong id="runMonitorUnavailableRowsLabel">Khong kha dung</strong>
                  <div id="runMonitorUnavailableRows" class="monitor-issue-rows"></div>
                </div>
                <div class="monitor-table-head">
                  <div id="runMonitorTableTitle" class="monitor-table-title">Bang log xu ly</div>
                  <div class="monitor-table-actions">
                    <button class="btn action-btn soft" type="button" onclick="continueJob()">
                      <span id="continueJobIconWrap" class="action-icon" aria-hidden="true">
                        <svg id="continueJobIcon" viewBox="0 0 24 24"><path d="M8 6.5v11l9-5.5-9-5.5Z"></path></svg>
                      </span>
                      <span id="continueJobLabel" class="action-label">Continue</span>
                    </button>
                    <button class="btn action-btn soft" type="button" onclick="startErrorRowsJob()">
                      <span id="errorOnlyJobIconWrap" class="action-icon" aria-hidden="true">
                        <svg id="errorOnlyJobIcon" viewBox="0 0 24 24"><path d="M12 8v5"></path><circle cx="12" cy="16.5" r=".9" fill="currentColor" stroke="none"></circle><path d="M10.2 4.8 3.9 16a1.4 1.4 0 0 0 1.22 2.1h13.76A1.4 1.4 0 0 0 20.1 16L13.8 4.8a1.4 1.4 0 0 0-2.6 0Z"></path></svg>
                      </span>
                      <span id="errorOnlyJobLabel" class="action-label">Chạy lỗi</span>
                    </button>
                  <button id="exportLogBtn" class="monitor-export-btn" type="button" onclick="exportCurrentLog()">
                    <svg viewBox="0 0 24 24" aria-hidden="true"><path d="M12 3v12"></path><path d="m7 10 5 5 5-5"></path><path d="M5 19h14"></path></svg>
                    <span id="exportLogLabel">Xuất log Excel</span>
                  </button>
                </div>
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
                <div class="project-filter-stack">
                  <div id="projectsModeFilters" class="project-mode-filters"></div>
                  <div id="projectsStatusFilters" class="project-status-filters"></div>
                </div>
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
                    <th id="accessTableHeadActions">Actions</th>
                  </tr>
                </thead>
                <tbody id="accessDirectoryBody"></tbody>
              </table>
            </div>
            <div class="access-directory-foot">
              <div id="access_policy_note" class="settings-note"></div>
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
                <input id="settings_page_timeout_ms" type="number" min="200" step="50" />
              </div>
              <div class="list-row" style="margin-top:12px">
                <div>
                  <div id="settings_tiktok_force_focus_label" style="font-weight:600">Push Chrome to front on TikTok captcha</div>
                  <div id="settings_tiktok_force_focus_help" class="muted">When slider captcha appears, force the browser window to foreground so you can solve it immediately.</div>
                </div>
                <input id="settings_tiktok_force_focus" type="checkbox" style="width:18px;height:18px" />
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
<div id="completionAlertHost" class="completion-alert-host" aria-live="assertive" aria-atomic="false"></div>
<script>
let currentJobId = null;
let pollTimer = null;
let jobsTimer = null;
let syncFeedbackTimer = null;
let completionTitleFlashTimer = null;
let completionTitleFlashText = '';
const defaultDocumentTitle = document.title;
let jobsCache = [];
let currentJobSnapshot = null;
let currentLogsCache = [];
let currentLogsCursor = 0;
let currentLogsJobId = '';
let currentJobIdByMode = { seeding: null, booking: null, scan: null };
let currentProjectJobId = null;
let currentProjectModeFilter = 'all';
let currentProjectStatusFilter = 'all';
let projectLogsCacheByJobId = {};
let projectLogsInflightByJobId = {};
let currentSettingsCache = {};
let currentRunMode = 'seeding';
let currentMappingBlocksByMode = {};
let currentRunFlagsByMode = {};
let captureFivePerLink = false;
let sheetNameSuggestTimer = null;
let sheetNameSuggestKey = '';
const SHEET_NAME_CACHE_TTL_MS = 3 * 60 * 1000;
const SHEET_NAME_SUGGEST_DEBOUNCE_MS = 300;
let sheetNameSuggestCache = {};
let sheetNameSuggestInflight = {};
const SHEET_COLUMN_CACHE_TTL_MS = 3 * 60 * 1000;
const SHEET_LINK_SUMMARY_DEBOUNCE_MS = 250;
let sheetColumnSuggestTimer = null;
let sheetLinkSummaryTimer = null;
let sheetColumnSuggestKey = '';
let scanFilterSaveTimer = null;
let sheetColumnSuggestCache = {};
let sheetColumnSuggestInflight = {};
let currentSheetLinkColumns = [];
let sheetLinkSuggestPayloadByMode = {
  seeding: { columns: [], counts: {} },
  booking: { columns: [], counts: {} },
  scan: { columns: [], counts: {} },
};
let sheetLinkSuggestSourceKeyByMode = { seeding: '', booking: '', scan: '' };
let activeSheetColumnTarget = null;
let bulkSheetLinkSelectionMode = false;
let bulkSheetLinkSelectionsByMode = { seeding: [], booking: [], scan: [] };
let sheetLinkSuggestLoadedByMode = { seeding: false, booking: false, scan: false };
let sheetLinkSuggestLoadingByMode = { seeding: false, booking: false, scan: false };
let monitorIssueExpandState = { failed: false, unavailable: false };
let monitorIssueExpandJobId = '';
let pendingMappingScrollMode = '';
let pendingMappingHighlightIndex = -1;
let currentAccessPolicy = { allowed_emails: [], admin_emails: [], managed_emails: [], email_types: {}, updated_at: null };
let currentMailConfig = { sender_email: '', from_email: '', has_password: false, updated_at: null, source: 'env' };
let currentActivityEvents = [];
let accessDirectoryQuery = '';
let accessDirectoryRole = 'all';
let accessDirectoryScope = 'all';
let accessDirectoryType = 'all';
let accessMailEditorOpen = false;
let accessEntryEditorState = { open: false, originalEmail: '', email: '', role: 'user', type: 'internal' };
let jobStatusMemory = {};
let notifiedCompletedJobKeys = new Set();
let pollInFlight = false;
let jobsRefreshInFlight = false;
let startJobInFlight = false;
let launchChromeInFlightByKey = {};
const BROWSER_PORT_BY_MODE = { seeding: 9223, booking: 9223, scan: 9223 };
const DEFAULT_SHARED_BROWSER_PORT = 9223;
const DEFAULT_AUTO_LAUNCH_CHROME = true;
const MAX_MONITOR_LOG_CACHE = 1200;
const JOBS_REFRESH_ACTIVITY_LIMIT = 120;
const JOB_POLL_INTERVAL_MS = 1500;
const JOBS_LIST_REFRESH_INTERVAL_MS = 12000;
let currentLang = localStorage.getItem('ui_lang') || 'vi';
let currentTheme = localStorage.getItem('ui_theme') || 'light';
const authState = {
  email: '__AUTH_EMAIL__',
  role: '__AUTH_ROLE__',
  isAdmin: __AUTH_IS_ADMIN__,
};
const LOCAL_BROWSER_HOSTS = new Set(__LOCAL_BROWSER_HOSTS__);
const localAgentState = {
  origin: localStorage.getItem('toolEvidence.localAgentOrigin') || 'http://127.0.0.1:8765',
  enabled: false,
  checked: false,
  lastError: '',
};
const LOCAL_AGENT_RUNTIME_PREFIXES = [
  '/api/settings',
  '/api/sheets/names',
  '/api/sheets/column-suggestions',
  '/api/activity',
  '/api/chrome/',
  '/api/jobs',
];

function isConfiguredLocalBrowserHost(host) {
  return LOCAL_BROWSER_HOSTS.has(String(host || '').trim().toLowerCase());
}

function isLocalBrowserOrigin() {
  return isConfiguredLocalBrowserHost(window.location.hostname);
}

function isLocalAgentRuntimePath(url) {
  const path = String(url || '').trim();
  if (!path) return false;
  const lowered = path.toLowerCase();
  if (lowered.startsWith('http://') || lowered.startsWith('https://')) return false;
  return LOCAL_AGENT_RUNTIME_PREFIXES.some(prefix => path === prefix || path.startsWith(prefix));
}

function shouldUseLocalAgent(url) {
  return !isLocalBrowserOrigin() && !!localAgentState.enabled && isLocalAgentRuntimePath(url);
}

function runtimeHref(url) {
  return shouldUseLocalAgent(url) ? `${localAgentState.origin}${url}` : url;
}

function updateRuntimeBadge() {
  const node = document.getElementById('runtimeBadge');
  if (!node) return;
  const useLocal = !isLocalBrowserOrigin() && !!localAgentState.enabled;
  node.textContent = useLocal ? t('runtimeLocal') : t('runtimeHost');
  node.style.background = useLocal ? '#dcfce7' : '#eef2f6';
  node.style.color = useLocal ? '#166534' : '#334155';
}

function timeoutError(url, timeoutMs) {
  const seconds = Math.max(1, Math.round((Number(timeoutMs) || 0) / 1000));
  return new Error(`Yêu cầu bị quá thời gian ${seconds}s: ${url}`);
}

async function fetchWithTimeout(url, options = {}, timeoutMs = 0) {
  const resolvedTimeout = Math.max(0, Number(timeoutMs) || 0);
  if (!resolvedTimeout || typeof AbortController === 'undefined' || options.signal) {
    return fetch(url, options);
  }
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), resolvedTimeout);
  try {
    return await fetch(url, { ...options, signal: controller.signal });
  } catch (e) {
    if (e?.name === 'AbortError') throw timeoutError(url, resolvedTimeout);
    throw e;
  } finally {
    clearTimeout(timer);
  }
}

function resolveRequestTimeoutMs(url, opts = {}) {
  const requested = Number(opts.timeout_ms || 0);
  if (Number.isFinite(requested) && requested > 0) return requested;
  const raw = String(url || '').toLowerCase();
  if (raw.includes('/api/chrome/')) return 20000;
  if (raw.includes('/api/jobs/start')) return 35000;
  return 45000;
}

async function detectLocalAgent() {
  if (isLocalBrowserOrigin()) {
    localAgentState.enabled = false;
    localAgentState.checked = true;
    localAgentState.lastError = '';
    updateRuntimeBadge();
    return false;
  }
  try {
    const res = await fetchWithTimeout(`${localAgentState.origin}/health`, {
      method: 'GET',
      cache: 'no-store',
    }, 2500);
    const data = await res.json().catch(() => ({}));
    if (!res.ok || !data?.ok) throw new Error(data.detail || ('HTTP ' + res.status));
    localAgentState.enabled = true;
    localAgentState.checked = true;
    localAgentState.lastError = '';
    updateRuntimeBadge();
    return true;
  } catch (e) {
    localAgentState.enabled = false;
    localAgentState.checked = true;
    localAgentState.lastError = String(e?.message || e || 'Local agent unavailable');
    updateRuntimeBadge();
    return false;
  }
}

async function agentReq(url, opts = {}) {
  if (!authState.email) throw new Error('Thiếu email đăng nhập để gọi local agent');
  const timeoutMs = resolveRequestTimeoutMs(url, opts);
  const fetchOptions = { ...opts };
  delete fetchOptions.timeout_ms;
  const headers = {
    'Content-Type': 'application/json',
    'X-Tool-Evidence-User': authState.email,
    ...(fetchOptions.headers || {}),
  };
  let res = null;
  try {
    res = await fetchWithTimeout(`${localAgentState.origin}${url}`, { ...fetchOptions, headers }, timeoutMs);
  } catch (e) {
    localAgentState.enabled = false;
    localAgentState.lastError = String(e?.message || e || 'Local agent unavailable');
    if (String(e?.message || '').includes('Yêu cầu bị quá thời gian')) {
      throw e;
    }
    throw new Error('Không kết nối được local agent trên máy này');
  }
  const data = await res.json().catch(() => ({}));
  if (!res.ok) throw new Error(data.detail || ('HTTP ' + res.status));
  return data;
}

window.toolEvidenceSetLocalAgentOrigin = function(origin) {
  let raw = String(origin || '').trim();
  while (raw.endsWith('/')) raw = raw.slice(0, -1);
  if (!raw) {
    localStorage.removeItem('toolEvidence.localAgentOrigin');
    localAgentState.origin = 'http://127.0.0.1:8765';
    return localAgentState.origin;
  }
  localStorage.setItem('toolEvidence.localAgentOrigin', raw);
  localAgentState.origin = raw;
  return raw;
};

const I18N = {
  vi: {
    searchPlaceholder: 'Tìm job hoặc trạng thái...',
    launchChrome: 'Mở Chrome',
    loginFacebookBeforeRun: 'Lauch Chrome',
    runtimeHost: 'Runtime: Host',
    runtimeLocal: 'Runtime: Local Agent',
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
    highlightSheetErrors: 'Tô màu lỗi trên sheet',
    highlightSheetErrorsHelp: 'Khi row chạy xong, tô màu ngay các ô kết quả trên Sheet: trắng cho Thành công, vàng cho Không khả dụng, đỏ cho Lỗi.',
    scanNegativeFilter: 'Lọc từ ngữ tiêu cực',
    scanNegativeFilterHelp: 'Khi bật, Scan sẽ đánh dấu lỗi nếu text hoặc OCR chứa các từ tiêu cực đã nhập bên dưới.',
    scanKeywordFilter: 'Quét theo từ khóa',
    scanKeywordFilterHelp: 'Khi bật, Scan sẽ đánh dấu lỗi nếu text hoặc OCR chứa một trong các từ khóa đã nhập bên dưới.',
    scanFilterPanelTitle: 'Bộ lọc Scan',
    runMode: 'Chế độ chạy',
    columnMapping: 'Column Mapping',
    seeding: 'Seeding',
    booking: 'Booking',
    scan: 'Scan',
    runModeSeedingHelp: 'Seeding chạy nền ổn định. Khi cần xác thực, bấm Chrome theo từng block để đăng nhập trước, đóng cửa sổ rồi chạy job.',
    runModeBookingHelp: 'Booking chạy nền ổn định. Khi cần xác thực, bấm Chrome theo từng block để đăng nhập trước, đóng cửa sổ rồi chạy job.',
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
    airDate: 'Date',
    sheetUrl: 'Sheet URL',
    sheetName: 'Tên Sheet',
    driveFolder: 'Drive Folder ID',
    sheetUrlHintLoading: 'Đang tải danh sách sheet...',
    sheetUrlHintEmpty: 'Không tìm thấy sheet nào trong file này',
    sheetUrlHintCountFmt: count => `Tìm thấy ${count} sheet`,
    sheetNameInvalidFmt: name => `Không tìm thấy sheet: ${name}`,
    sheetLinkCellHintLoading: 'Đang quét ô có URL...',
    sheetLinkCellHintCountFmt: count => `Phát hiện ${count} cột có URL`,
    sheetLinkSuggestTitle: 'Cột có link',
    sheetLinkSuggestSheetFmt: sheet => `Sheet: ${sheet}`,
    sheetLinkSuggestFind: 'Tìm cột',
    sheetLinkSuggestLoading: 'Đang quét cột có link...',
    sheetLinkSuggestHelp: 'Chọn cột chứa Link URL rồi bấm Tạo block, hệ thống sẽ thêm Drive URL ở cột bên phải (1 ô) và Screenshot ở cột bên phải tiếp theo (2 ô).',
    sheetLinkSuggestScanHelp: 'Chọn cột chứa Link hình ảnh rồi chọn Tạo block, hệ thống sẽ tự nhận Text Column là ô bên trái và Result Column là thêm 1 ô mới bên phải.',
    sheetLinkSuggestReady: 'Bấm Quét nhanh để quét các cột đang chứa link trong sheet này.',
    sheetLinkSuggestNeedSheet: 'Chọn đúng Sheet Name để hệ thống có thể quét cột link.',
    sheetLinkSuggestActiveFmt: (field, block) => `Đang chọn cho ${field}${block ? ` · ${block}` : ''}`,
    sheetLinkSuggestEmpty: 'Chưa phát hiện cột nào có link trong sheet này.',
    sheetLinkSuggestCountFmt: count => `Phát hiện ${count} cột có link`,
    sheetLinkBulkToggle: 'Chọn nhiều cột',
    sheetLinkBulkAdd: 'Tạo block',
    sheetLinkFillBlocks: 'Tạo block',
    sheetLinkQuickScan: 'Quét nhanh',
    sheetLinkQuickCreate: 'Tạo cột nhanh',
    sheetLinkBulkClear: 'Bỏ chọn',
    sheetLinkReload: 'Load lại',
    sheetLinkBulkSelectedFmt: count => `Đã chọn ${count} cột`,
    sheetLinkBulkHelp: 'Bật chọn nhiều để bấm nhiều cột link và tự thêm block.',
    sheetLinkBulkUnsupported: 'Chọn nhiều cột hiện hỗ trợ cho Seeding, Booking và Scan.',
    sheetLinkBulkNone: 'Chưa chọn cột nào',
    sheetLinkBulkAddedFmt: count => `Đã tạo ${count} block từ các cột đã chọn`,
    sheetLinkBulkNoNew: 'Các cột đã chọn đã có block rồi',
    sheetLinkQuickScanNoSelection: 'Chưa chọn cột nào để quét nhanh',
    sheetLinkQuickScanDoneFmt: count => `Đã tạo ${count} block quét nhanh và thêm 2 cột mới cho mỗi cột đã chọn`,
    browserPort: 'Browser Port',
    startLine: 'Dòng bắt đầu',
    autoLaunchChrome: 'Tự mở Chrome',
    startJob: 'Chạy job',
    overwriteRun: 'Chạy đè',
    pauseJob: 'Tạm dừng',
    stopJob: 'Dừng',
    resumeJob: 'Tiếp tục',
    continueJob: 'Chạy tiếp',
    errorOnlyJob: 'Chạy lỗi',
    refreshJobs: 'Làm mới job',
    runQueue: 'Hàng đợi job',
    runQueueHelp: 'Chọn job để theo dõi. Mỗi mode được chạy 1 job cùng lúc.',
    liveLogs: 'Live log',
    errorRows: 'Lỗi',
    selectedJobMeta: 'Job đang chọn',
    monitorKicker: '4. Kết quả & Theo dõi',
    monitorTitle: 'Theo dõi tiến độ và lỗi',
    monitorJob: 'Job',
    monitorProgress: 'Tiến độ',
    monitorErrors: 'Thống kê',
    monitorTable: 'Bảng log xử lý',
    monitorNoJob: 'Chưa chọn job',
    monitorNoErrors: 'Không có lỗi',
    monitorIssueSummaryLabel: 'Tóm tắt',
    monitorIssueRowsLabel: 'Lỗi',
    monitorIssueUnavailableRowsLabel: 'Không khả dụng',
    monitorIssueExpandFmt: count => `+${count}`,
    monitorIssueCollapse: 'Thu gọn',
    monitorIssueStatsLabel: 'Thống kê',
    monitorIssueSummaryNone: 'Không có lỗi cần tổng hợp',
    monitorIssueSummaryTopFmt: (label, count) => `Chủ yếu: ${label} (${count})`,
    monitorIssueSummaryTopMoreFmt: (label, count, more) => `Chủ yếu: ${label} (${count}) · +${more} loại lỗi khác`,
    jobFinishedTitle: 'Hoàn tất',
    jobFinishedToastFmt: (name, done, total) => `${name} đã chạy xong ${done}/${total} dòng.`,
    jobFinishedBannerTitle: 'Dự án đã hoàn tất',
    jobFinishedBannerDismiss: 'Đã thấy',
    monitorNoLogs: 'Chưa có dữ liệu',
    monitorSuccessFailedFmt: (ok, fail, unavailable = 0) => `Thành công ${ok} · Lỗi ${fail} · Không khả dụng ${unavailable}`,
    monitorIssueCellCountFmt: count => `${count} ô`,
    unavailableLabel: 'Không khả dụng',
    time: 'Time',
    post: 'Post',
    result: 'Kết quả',
    message: 'Thông điệp',
    replay: 'Replay',
    exportLog: 'Xuất log Excel',
    noLogsToExport: 'Chưa có log để xuất',
    replayStartedFmt: row => `Đã tạo replay cho dòng ${row}`,
    continueStarted: 'Đã tạo job chạy tiếp',
    errorOnlyStarted: 'Đã tạo job chạy lại các dòng lỗi',
    noData: 'Chưa có dữ liệu',
    projectsState: 'Lưu các run hoàn tất và xem lại chi tiết',
    groupedProjects: 'Dự án đã lưu',
    completedGroups: 'Sheet đã lưu',
    largestGroup: 'Dự án đang chọn',
    groupedRegistry: 'Thư viện dự án',
    groupSnapshot: 'Chi tiết dự án',
    projectLogs: 'Log dự án',
    projectLogsSub: 'Lưu theo dự án đang chọn',
    projectModeLabel: 'Mode',
    projectStatusLabel: 'Trạng thái',
    allProjects: 'Tất cả',
    projectStatusAll: 'Tất cả',
    projectStatusRunning: 'Đang chạy',
    projectStatusCompleted: 'Hoàn tất',
    projectStatusStopped: 'Đã dừng',
    projectStatusFailed: 'Lỗi',
    projectOwner: 'Người chạy',
    noProjectsInFilter: 'Chưa có dự án trong nhóm này',
    projectLogsLoading: 'Đang tải log dự án...',
    projectNoLogs: 'Chưa có log cho dự án này',
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
    recentTimeline: 'Lịch sử hoạt động',
    activityLevel: 'Hoạt động',
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
    tiktokCaptchaWait: 'Thời gian chờ captcha TikTok (giây)',
    pleaseWaitDelay: 'Delay thêm khi có "Please wait" (giây)',
    tiktokForceFocus: 'Đẩy Chrome lên trước khi gặp captcha TikTok',
    tiktokForceFocusHelp: 'Khi slider xuất hiện, ép cửa sổ trình duyệt nổi lên để bạn thao tác ngay.',
    scanNegativeTermsLabel: 'Từ ngữ tiêu cực cho Scan',
    scanNegativeTermsHelp: 'Mỗi dòng một từ hoặc cụm từ. Khi Scan bật lọc từ tiêu cực, row chứa từ này sẽ bị đánh dấu lỗi.',
    scanNegativeTermsPlaceholder: 'spam\\nlừa đảo\\nchửi bới',
    scanKeywordTermsLabel: 'Từ khóa cho Scan',
    scanKeywordTermsHelp: 'Mỗi dòng một từ hoặc cụm từ. Khi bật quét theo từ khóa, row chứa từ này sẽ bị đánh dấu lỗi.',
    scanKeywordTermsPlaceholder: 'khuyến mãi\\ngiảm giá\\nđặt hàng',
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
    loginFacebookBeforeRun: 'Lauch Chrome',
    runtimeHost: 'Runtime: Host',
    runtimeLocal: 'Runtime: Local Agent',
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
    highlightSheetErrors: 'Highlight errors on sheet',
    highlightSheetErrorsHelp: 'Color the sheet output cells after each row finishes: white for success, yellow for unavailable, and red for failed.',
    scanNegativeFilter: 'Negative word filter',
    scanNegativeFilterHelp: 'When enabled, Scan flags a row if the OCR or text contains negative terms entered below.',
    scanKeywordFilter: 'Keyword scan',
    scanKeywordFilterHelp: 'When enabled, Scan flags rows when OCR or text contains any keyword entered below.',
    scanFilterPanelTitle: 'Scan filters',
    runMode: 'Run mode',
    columnMapping: 'Column Mapping',
    seeding: 'Seeding',
    booking: 'Booking',
    scan: 'Scan',
    runModeSeedingHelp: 'Seeding runs best in background mode. When auth is needed, open each block Chrome first, sign in, close it, then run the job.',
    runModeBookingHelp: 'Booking runs best in background mode. When auth is needed, open each block Chrome first, sign in, close it, then run the job.',
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
    airDate: 'Date',
    sheetUrl: 'Sheet URL',
    sheetName: 'Sheet Name',
    driveFolder: 'Drive Folder ID',
    sheetUrlHintLoading: 'Loading sheet names...',
    sheetUrlHintEmpty: 'No sheets found in this spreadsheet',
    sheetUrlHintCountFmt: count => `${count} sheets found`,
    sheetNameInvalidFmt: name => `Sheet not found: ${name}`,
    sheetLinkCellHintLoading: 'Scanning URL cells...',
    sheetLinkCellHintCountFmt: count => `${count} URL columns found`,
    sheetLinkSuggestTitle: 'Detected link columns',
    sheetLinkSuggestSheetFmt: sheet => `Sheet: ${sheet}`,
    sheetLinkSuggestFind: 'Find columns',
    sheetLinkSuggestLoading: 'Scanning link columns...',
    sheetLinkSuggestHelp: 'Choose the Link URL column, then click Create blocks. The app auto-adds Drive URL on the right and Screenshot next to Drive URL.',
    sheetLinkSuggestScanHelp: 'Image link: pick a link column and click Create blocks. The app will set Text Column to the left cell and Result Column to a new cell on the right.',
    sheetLinkSuggestReady: 'Click Quick scan to detect link-bearing columns in this sheet.',
    sheetLinkSuggestNeedSheet: 'Choose a valid sheet name so the app can scan link columns.',
    sheetLinkSuggestActiveFmt: (field, block) => `Selecting for ${field}${block ? ` · ${block}` : ''}`,
    sheetLinkSuggestEmpty: 'No link columns detected in this sheet yet.',
    sheetLinkSuggestCountFmt: count => `${count} link columns found`,
    sheetLinkBulkToggle: 'Select multiple columns',
    sheetLinkBulkAdd: 'Create blocks',
    sheetLinkFillBlocks: 'Create blocks',
    sheetLinkQuickScan: 'Quick scan',
    sheetLinkQuickCreate: 'Quick create',
    sheetLinkBulkClear: 'Clear',
    sheetLinkReload: 'Reload',
    sheetLinkBulkSelectedFmt: count => `${count} columns selected`,
    sheetLinkBulkHelp: 'Enable multi-select to pick several link columns and create blocks automatically.',
    sheetLinkBulkUnsupported: 'Multi-select is currently available for Seeding, Booking, and Scan.',
    sheetLinkBulkNone: 'No columns selected yet',
    sheetLinkBulkAddedFmt: count => `Created ${count} blocks from the selected columns`,
    sheetLinkBulkNoNew: 'All selected columns already have blocks',
    sheetLinkQuickScanNoSelection: 'No columns selected for quick scan',
    sheetLinkQuickScanDoneFmt: count => `Created ${count} quick-scan blocks and added 2 new columns for each selection`,
    browserPort: 'Browser Port',
    startLine: 'Start Line',
    autoLaunchChrome: 'Auto Launch Chrome',
    startJob: 'Start Job',
    overwriteRun: 'Overwrite',
    pauseJob: 'Pause',
    stopJob: 'Stop',
    resumeJob: 'Resume',
    continueJob: 'Continue',
    errorOnlyJob: 'Run errors only',
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
    monitorErrors: 'Summary',
    monitorTable: 'Processing log table',
    monitorNoJob: 'No job selected',
    monitorNoErrors: 'No errors',
    monitorIssueSummaryLabel: 'Summary',
    monitorIssueRowsLabel: 'Failed',
    monitorIssueUnavailableRowsLabel: 'Unavailable',
    monitorIssueExpandFmt: count => `+${count}`,
    monitorIssueCollapse: 'Collapse',
    monitorIssueStatsLabel: 'Stats',
    monitorIssueSummaryNone: 'No issue summary',
    monitorIssueSummaryTopFmt: (label, count) => `Top issue: ${label} (${count})`,
    monitorIssueSummaryTopMoreFmt: (label, count, more) => `Top issue: ${label} (${count}) · +${more} more issue types`,
    jobFinishedTitle: 'Completed',
    jobFinishedToastFmt: (name, done, total) => `${name} finished ${done}/${total} rows.`,
    jobFinishedBannerTitle: 'Project completed',
    jobFinishedBannerDismiss: 'Dismiss',
    monitorNoLogs: 'No data yet',
    monitorSuccessFailedFmt: (ok, fail, unavailable = 0) => `Success ${ok} · Failed ${fail} · Unavailable ${unavailable}`,
    monitorIssueCellCountFmt: count => `${count} cells`,
    unavailableLabel: 'Unavailable',
    time: 'Time',
    post: 'Post',
    result: 'Result',
    message: 'Message',
    replay: 'Replay',
    exportLog: 'Export Excel Log',
    noLogsToExport: 'No logs to export',
    replayStartedFmt: row => `Replay job queued for row ${row}`,
    continueStarted: 'Continue job queued',
    errorOnlyStarted: 'Error-only job queued',
    noData: 'No data',
    projectsState: 'Store completed runs and reopen their details',
    groupedProjects: 'Saved Projects',
    completedGroups: 'Saved Sheets',
    largestGroup: 'Selected Project',
    groupedRegistry: 'Project Library',
    groupSnapshot: 'Project Detail',
    projectLogs: 'Project logs',
    projectLogsSub: 'Saved with the selected project',
    projectModeLabel: 'Mode',
    projectStatusLabel: 'Status',
    allProjects: 'All',
    projectStatusAll: 'All',
    projectStatusRunning: 'Running',
    projectStatusCompleted: 'Completed',
    projectStatusStopped: 'Stopped',
    projectStatusFailed: 'Failed',
    projectOwner: 'Owner',
    noProjectsInFilter: 'No projects in this category',
    projectLogsLoading: 'Loading project logs...',
    projectNoLogs: 'No logs for this project yet',
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
    recentTimeline: 'Activity History',
    activityLevel: 'Activity',
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
    tiktokCaptchaWait: 'TikTok captcha wait (sec)',
    pleaseWaitDelay: 'Please wait extra delay (sec)',
    tiktokForceFocus: 'Push Chrome to front on TikTok captcha',
    tiktokForceFocusHelp: 'When slider captcha appears, force the browser window to foreground so you can solve it immediately.',
    scanNegativeTermsLabel: 'Negative words for Scan',
    scanNegativeTermsHelp: 'Use one word or phrase per line. When Scan negative filtering is enabled, rows containing these terms are flagged as failed.',
    scanNegativeTermsPlaceholder: 'spam\\nscam\\nabuse',
    scanKeywordTermsLabel: 'Scan keywords',
    scanKeywordTermsHelp: 'Use one word or phrase per line. When keyword scan is enabled, rows containing these terms are flagged as failed.',
    scanKeywordTermsPlaceholder: 'promotion\\ndiscount\\norder now',
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

function getTodayLocalDateString() {
  const now = new Date();
  const y = now.getFullYear();
  const m = String(now.getMonth() + 1).padStart(2, '0');
  const d = String(now.getDate()).padStart(2, '0');
  return `${y}-${m}-${d}`;
}

function isSheetColumnRef(value) {
  const raw = String(value || '').trim();
  return /^[A-Za-z]{1,3}$/.test(raw);
}

function resolveAirDateForMode(mode, rawValue) {
  const key = String(mode || '').toLowerCase();
  if (key === 'scan') return '';
  const raw = String(rawValue || '').trim();
  if (isSheetColumnRef(raw)) return raw.toUpperCase();
  return getTodayLocalDateString();
}

function sanitizeMappingBlockForMode(mode, block, index = 1) {
  const key = String(mode || 'seeding').toLowerCase();
  const next = {
    ...defaultMappingBlock(key, index),
    ...(block || {}),
    start_line: Number(block?.start_line || 4),
    mode: key,
  };
  console.log(`[DEBUG] sanitizeMappingBlockForMode(${key}, index=${index}): input start_line=${block?.start_line}, output start_line=${next.start_line}`);
  if (key === 'seeding') {
    next.col_profile = '';
    next.col_content = '';
    next.sheet_url = String(next.sheet_url || '').trim();
    next.sheet_name = String(next.sheet_name || '').trim();
    next.drive_id = String(next.drive_id || '').trim();
    next.col_air_date = resolveAirDateForMode(key, next.col_air_date);
  } else if (key === 'booking') {
    next.sheet_url = '';
    next.sheet_name = '';
    next.drive_id = '';
    next.col_air_date = resolveAirDateForMode(key, next.col_air_date);
  } else if (key === 'scan') {
    next.col_profile = '';
    next.col_screenshot = '';
    next.col_air_date = resolveAirDateForMode(key, next.col_air_date);
    next.sheet_url = '';
    next.sheet_name = '';
    next.drive_id = '';
  }
  return next;
}

function normalizeMappingsByModeForClient(raw = {}) {
  const next = {};
  ['seeding', 'booking', 'scan'].forEach(mode => {
    const items = Array.isArray(raw?.[mode]) ? raw[mode] : [];
    if (!items.length) return;
    next[mode] = items.map((block, index) => sanitizeMappingBlockForMode(mode, block, index + 1));
  });
  return next;
}

function defaultRunFlagsForMode(mode) {
  const key = String(mode || 'seeding').toLowerCase();
  return {
    force_run_all: true,
    highlight_sheet_errors: true,
    capture_five_per_link: key === 'booking' ? false : false,
    scan_negative_filter: false,
    scan_keyword_filter: false,
  };
}

function normalizeRunFlagsByModeForClient(raw = {}) {
  const next = {};
  ['seeding', 'booking', 'scan'].forEach(mode => {
    next[mode] = {
      ...defaultRunFlagsForMode(mode),
      ...((raw && typeof raw === 'object' && raw[mode] && typeof raw[mode] === 'object') ? raw[mode] : {}),
    };
    if (mode !== 'booking') next[mode].capture_five_per_link = false;
    if (mode !== 'scan') next[mode].scan_negative_filter = false;
    if (mode !== 'scan') next[mode].scan_keyword_filter = false;
    next[mode].force_run_all = next[mode].force_run_all !== false;
    next[mode].highlight_sheet_errors = !!next[mode].highlight_sheet_errors;
    next[mode].capture_five_per_link = !!next[mode].capture_five_per_link;
    next[mode].scan_negative_filter = !!next[mode].scan_negative_filter;
    next[mode].scan_keyword_filter = !!next[mode].scan_keyword_filter;
  });
  return next;
}

function ensureRunFlagsForMode(mode = currentRunMode) {
  const key = String(mode || 'seeding').toLowerCase();
  if (!currentRunFlagsByMode || typeof currentRunFlagsByMode !== 'object') currentRunFlagsByMode = {};
  const normalized = normalizeRunFlagsByModeForClient(currentRunFlagsByMode);
  currentRunFlagsByMode = normalized;
  return currentRunFlagsByMode[key] || defaultRunFlagsForMode(key);
}

function rememberCurrentRunFlags(mode = currentRunMode) {
  const key = String(mode || currentRunMode || 'seeding').toLowerCase();
  const flags = ensureRunFlagsForMode(key);
  const overwriteNode = document.getElementById('force_run_all');
  const highlightNode = document.getElementById('highlight_sheet_errors');
  const negativeNode = document.getElementById('scan_negative_filter');
  const keywordNode = document.getElementById('scan_keyword_filter');
  flags.force_run_all = overwriteNode ? !!overwriteNode.checked : flags.force_run_all !== false;
  flags.highlight_sheet_errors = highlightNode ? !!highlightNode.checked : !!flags.highlight_sheet_errors;
  flags.capture_five_per_link = key === 'booking' ? !!captureFivePerLink : false;
  flags.scan_negative_filter = key === 'scan' ? !!negativeNode?.checked : false;
  flags.scan_keyword_filter = key === 'scan' ? !!keywordNode?.checked : false;
  currentRunFlagsByMode[key] = flags;
  return flags;
}

function applyRunFlagsForMode(mode = currentRunMode) {
  const key = String(mode || currentRunMode || 'seeding').toLowerCase();
  const flags = ensureRunFlagsForMode(key);
  const overwriteNode = document.getElementById('force_run_all');
  const highlightNode = document.getElementById('highlight_sheet_errors');
  const negativeNode = document.getElementById('scan_negative_filter');
  const keywordNode = document.getElementById('scan_keyword_filter');
  if (overwriteNode) overwriteNode.checked = flags.force_run_all !== false;
  if (highlightNode) highlightNode.checked = !!flags.highlight_sheet_errors;
  if (negativeNode) negativeNode.checked = key === 'scan' ? !!flags.scan_negative_filter : false;
  if (keywordNode) keywordNode.checked = key === 'scan' ? !!flags.scan_keyword_filter : false;
  captureFivePerLink = key === 'booking' ? !!flags.capture_five_per_link : false;
}

function serializeMappingsByModeForSave() {
  const payload = {};
  Object.entries(currentMappingBlocksByMode || {}).forEach(([mode, items]) => {
    const key = String(mode || '').toLowerCase();
    if (!['seeding', 'booking', 'scan'].includes(key)) return;
    const blocks = Array.isArray(items) ? items : [];
    if (!blocks.length) return;
    payload[key] = blocks.map((block, index) => sanitizeMappingBlockForMode(key, block, index + 1));
  });
  console.log('[DEBUG] serializeMappingsByModeForSave result:', payload);
  return payload;
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
    sheet_url: '',
    sheet_name: '',
    drive_id: '',
    col_profile: isBooking ? 'B' : '',
    col_content: isBooking ? 'I' : '',
    col_url: 'K',
    col_drive: 'L',
    col_screenshot: 'J',
    col_air_date: getTodayLocalDateString(),
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
  console.log(`[DEBUG] ensureMappingBlocks(${key}):`, currentMappingBlocksByMode[key]);
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
      { key: 'sheet_name', label: t('sheetName') },
      { key: 'drive_id', label: t('driveFolder') },
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

function getMappingFieldInputId(mode, index, key) {
  return `mapping_${String(mode || 'seeding')}_${Number(index) || 0}_${String(key || '')}`;
}

function isLinkSuggestionField(mode, key) {
  const normalizedMode = String(mode || '').toLowerCase();
  const normalizedKey = String(key || '').toLowerCase();
  if (normalizedMode === 'scan') return normalizedKey === 'col_url';
  return ['col_url', 'col_drive', 'col_screenshot'].includes(normalizedKey);
}

function supportsBulkSheetLinkMode(mode = currentRunMode) {
  const normalizedMode = String(mode || currentRunMode || '').toLowerCase();
  return ['seeding', 'booking', 'scan'].includes(normalizedMode);
}

function getBulkSheetLinkSelections(mode = currentRunMode) {
  const normalizedMode = String(mode || currentRunMode || 'seeding').toLowerCase();
  if (!Array.isArray(bulkSheetLinkSelectionsByMode[normalizedMode])) {
    bulkSheetLinkSelectionsByMode[normalizedMode] = [];
  }
  return bulkSheetLinkSelectionsByMode[normalizedMode];
}

function clearBulkSheetLinkSelections(mode = currentRunMode) {
  const normalizedMode = String(mode || currentRunMode || 'seeding').toLowerCase();
  bulkSheetLinkSelectionsByMode[normalizedMode] = [];
}

function toggleBulkSheetLinkMode(nextEnabled = null) {
  if (!supportsBulkSheetLinkMode(currentRunMode)) {
    alert(t('sheetLinkBulkUnsupported'));
    return;
  }
  bulkSheetLinkSelectionMode = nextEnabled == null ? !bulkSheetLinkSelectionMode : !!nextEnabled;
  if (!bulkSheetLinkSelectionMode) {
    clearBulkSheetLinkSelections(currentRunMode);
  }
  renderSheetLinkSuggestions();
}

function toggleBulkSheetLinkSelection(column) {
  if (!supportsBulkSheetLinkMode(currentRunMode)) {
    alert(t('sheetLinkBulkUnsupported'));
    return;
  }
  const normalized = String(column || '').trim().toUpperCase();
  if (!normalized) return;
  const selected = getBulkSheetLinkSelections(currentRunMode);
  const index = selected.indexOf(normalized);
  if (index >= 0) selected.splice(index, 1);
  else selected.push(normalized);
  renderSheetLinkSuggestions();
}

function sheetColumnLetterToIndex(column) {
  const normalized = String(column || '').trim().toUpperCase();
  if (!normalized) return 0;
  let value = 0;
  for (const ch of normalized) {
    const code = ch.charCodeAt(0);
    if (code < 65 || code > 90) return 0;
    value = (value * 26) + (code - 64);
  }
  return value;
}

function sheetColumnIndexToLetter(index) {
  let n = Number(index || 0);
  if (!Number.isFinite(n) || n <= 0) return '';
  let out = '';
  while (n > 0) {
    const remainder = (n - 1) % 26;
    out = String.fromCharCode(65 + remainder) + out;
    n = Math.floor((n - 1) / 26);
  }
  return out;
}

function shiftSheetColumn(column, delta = 0) {
  const base = sheetColumnLetterToIndex(column);
  if (!base) return '';
  return sheetColumnIndexToLetter(base + Number(delta || 0));
}

function getScanContentColumnFromImageColumn(column) {
  return shiftSheetColumn(column, -1);
}

function buildBulkSuggestedBlock(mode, column, index, template = null) {
  const normalizedMode = String(mode || currentRunMode || 'seeding').toLowerCase();
  const base = sanitizeMappingBlockForMode(normalizedMode, template || defaultMappingBlock(normalizedMode, index), index);
  const normalizedColumn = String(column || '').trim().toUpperCase();
  const next1 = shiftSheetColumn(normalizedColumn, 1);
  const next2 = shiftSheetColumn(normalizedColumn, 2);
  base.name = normalizedMode === 'scan' ? `Scan ${index}` : `Post ${index}`;
  if (normalizedMode === 'scan') {
    const templateImageIndex = sheetColumnLetterToIndex(base.col_url);
    const templateResultIndex = sheetColumnLetterToIndex(base.col_drive);
    base.col_url = normalizedColumn;
    const inferredContent = getScanContentColumnFromImageColumn(normalizedColumn);
    if (inferredContent) base.col_content = inferredContent;
    else base.col_content = '';
    if (templateImageIndex && templateResultIndex) {
      const shiftedResult = shiftSheetColumn(normalizedColumn, templateResultIndex - templateImageIndex);
      if (shiftedResult) base.col_drive = shiftedResult;
    }
    return base;
  }
  if (normalizedMode === 'booking') {
    base.col_url = normalizedColumn;
    if (next1) base.col_screenshot = next1;
    if (next2) base.col_drive = next2;
  } else if (normalizedMode === 'seeding') {
    base.col_url = normalizedColumn;
    if (next1) base.col_screenshot = next1;
    if (next2) base.col_drive = next2;
  }
  return base;
}

function buildSuggestedBlockFromColumns(mode, linkColumn, driveColumn, screenshotColumn, index, template = null) {
  const normalizedMode = String(mode || currentRunMode || 'seeding').toLowerCase();
  const base = sanitizeMappingBlockForMode(normalizedMode, template || defaultMappingBlock(normalizedMode, index), index);
  const normalizedLink = String(linkColumn || '').trim().toUpperCase();
  const normalizedDrive = String(driveColumn || '').trim().toUpperCase();
  const normalizedScreenshot = String(screenshotColumn || '').trim().toUpperCase();
  base.name = normalizedMode === 'scan' ? `Scan ${index}` : `Post ${index}`;
  base.col_url = normalizedLink;
  if (normalizedMode === 'scan') {
    const inferredContent = getScanContentColumnFromImageColumn(normalizedLink);
    if (inferredContent) base.col_content = inferredContent;
    else base.col_content = '';
    if (normalizedDrive) base.col_drive = normalizedDrive;
    return base;
  }
  if (normalizedMode === 'booking' || normalizedMode === 'seeding') {
    if (normalizedDrive) base.col_drive = normalizedDrive;
    if (normalizedScreenshot) base.col_screenshot = normalizedScreenshot;
  }
  return base;
}

async function addBlocksFromSelectedLinkColumns() {
  if (!supportsBulkSheetLinkMode(currentRunMode)) {
    alert(t('sheetLinkBulkUnsupported'));
    return;
  }
  const selected = getBulkSheetLinkSelections(currentRunMode).slice();
  if (!selected.length) {
    alert(t('sheetLinkBulkNone'));
    return;
  }
  const rawUrl = String(document.getElementById('sheet_url')?.value || '').trim();
  const rawName = String(document.getElementById('sheet_name')?.value || '').trim();
  if (!rawUrl || !rawName) {
    alert(t('sheetLinkQuickScanNoSelection'));
    return;
  }
  try {
    const out = await req('/api/sheets/quick-block-columns', {
      method: 'POST',
      body: JSON.stringify({
        sheet_url: rawUrl,
        sheet_name: rawName,
        mode: currentRunMode,
        columns: selected,
      }),
    });
    const items = Array.isArray(out?.items) ? out.items : [];
    if (!items.length) {
      alert(t('sheetLinkBulkNoNew'));
      return;
    }
    const blocks = ensureMappingBlocks(currentRunMode);
    const template = blocks.length ? blocks[blocks.length - 1] : defaultMappingBlock(currentRunMode, 1);
    items.forEach(item => {
      const nextIndex = blocks.length + 1;
      blocks.push(
        buildSuggestedBlockFromColumns(
          currentRunMode,
          item.link_column || item.source_column || '',
          item.drive_column || '',
          item.screenshot_column || '',
          nextIndex,
          template,
        )
      );
    });
    pendingMappingScrollMode = currentRunMode;
    pendingMappingHighlightIndex = Math.max(0, blocks.length - items.length);
    activeSheetColumnTarget = null;
    clearBulkSheetLinkSelections(currentRunMode);
    bulkSheetLinkSelectionMode = false;
    renderMappingEditor();
    await fetchSheetLinkSuggestions(true);
    setStatus(t('sheetLinkBulkAddedFmt')(items.length), 'done');
  } catch (e) {
    alert(e.message);
  }
}

async function quickScanSelectedLinkColumns() {
  await addBlocksFromSelectedLinkColumns();
}

function setSheetColumnTarget(mode, index, key) {
  if (!isLinkSuggestionField(mode, key)) return;
  activeSheetColumnTarget = {
    mode: String(mode || currentRunMode || 'seeding').toLowerCase(),
    index: Number(index) || 0,
    key: String(key || '').trim(),
  };
  renderSheetLinkSuggestions();
}

function getActiveSheetColumnTargetValue() {
  if (!activeSheetColumnTarget) return '';
  const blocks = ensureMappingBlocks(activeSheetColumnTarget.mode);
  const block = blocks[activeSheetColumnTarget.index];
  if (!block) return '';
  return String(block[activeSheetColumnTarget.key] || '').trim().toUpperCase();
}

function currentSheetColumnStartRow() {
  const blocks = ensureMappingBlocks(currentRunMode);
  const rows = blocks
    .map(block => Number(block?.start_line || 4))
    .filter(value => Number.isFinite(value) && value > 0);
  return rows.length ? Math.max(1, Math.min(...rows)) : 4;
}

function getCachedSheetLinkColumns(rawUrl, rawName, startRow, allowStale = false) {
  const key = `${String(rawUrl || '').trim()}|${String(rawName || '').trim()}|${Math.max(1, Number(startRow || 4) || 4)}`;
  const entry = sheetColumnSuggestCache[key];
  if (!entry || !Array.isArray(entry.columns)) return null;
  if (allowStale) return entry;
  if ((Date.now() - Number(entry.ts || 0)) > SHEET_COLUMN_CACHE_TTL_MS) return null;
  return entry;
}

function getSheetLinkSuggestPayload(mode = currentRunMode) {
  const normalizedMode = String(mode || currentRunMode || 'seeding').toLowerCase();
  if (!sheetLinkSuggestPayloadByMode[normalizedMode]) {
    sheetLinkSuggestPayloadByMode[normalizedMode] = { columns: [], counts: {} };
  }
  return sheetLinkSuggestPayloadByMode[normalizedMode];
}

function resetSheetLinkSuggestions(mode = currentRunMode) {
  const normalizedMode = String(mode || currentRunMode || 'seeding').toLowerCase();
  sheetLinkSuggestLoadedByMode[normalizedMode] = false;
  sheetLinkSuggestLoadingByMode[normalizedMode] = false;
  sheetLinkSuggestPayloadByMode[normalizedMode] = { columns: [], counts: {} };
  sheetLinkSuggestSourceKeyByMode[normalizedMode] = '';
  if (String(normalizedMode) === String(currentRunMode || '').toLowerCase()) {
    currentSheetLinkColumns = [];
    setSheetNameHint('');
    renderSheetLinkSuggestions();
  }
}

async function handleSheetLinkQuickAction() {
  const modeKey = String(currentRunMode || 'seeding').toLowerCase();
  const rawUrl = String(document.getElementById('sheet_url')?.value || '').trim();
  const rawName = String(document.getElementById('sheet_name')?.value || '').trim();
  const startRow = currentSheetColumnStartRow();
  const currentKey = `${rawUrl}|${rawName}|${startRow}`;
  const sourceKey = String(sheetLinkSuggestSourceKeyByMode[modeKey] || '');
  const loadedForCurrentSheet = !!sheetLinkSuggestLoadedByMode[modeKey] && sourceKey === currentKey;
  if (modeKey === 'scan') {
    activeSheetColumnTarget = null;
  }
  if (!loadedForCurrentSheet) {
    const cached = getCachedSheetLinkColumns(rawUrl, rawName, startRow, false);
    if (cached) {
      sheetLinkSuggestLoadedByMode[modeKey] = true;
      sheetLinkSuggestPayloadByMode[modeKey] = cached;
      sheetLinkSuggestSourceKeyByMode[modeKey] = currentKey;
      renderSheetLinkSuggestions(cached);
      return;
    }
    await findSheetLinkSuggestions(true);
    return;
  }
  await quickScanSelectedLinkColumns();
}

async function reloadSheetLinkSuggestions() {
  const modeKey = String(currentRunMode || 'seeding').toLowerCase();
  const rawUrl = String(document.getElementById('sheet_url')?.value || '').trim();
  const rawName = String(document.getElementById('sheet_name')?.value || '').trim();
  const startRow = currentSheetColumnStartRow();
  const cacheKey = `${rawUrl}|${rawName}|${startRow}`;
  delete sheetColumnSuggestCache[cacheKey];
  delete sheetColumnSuggestInflight[cacheKey];
  sheetLinkSuggestLoadedByMode[modeKey] = false;
  sheetLinkSuggestPayloadByMode[modeKey] = { columns: [], counts: {} };
  sheetLinkSuggestSourceKeyByMode[modeKey] = '';
  clearBulkSheetLinkSelections(modeKey);
  activeSheetColumnTarget = null;
  await findSheetLinkSuggestions(true);
}

async function findSheetLinkSuggestions(force = true) {
  const normalizedMode = String(currentRunMode || 'seeding').toLowerCase();
  sheetLinkSuggestLoadingByMode[normalizedMode] = true;
  renderSheetLinkSuggestions();
  try {
    await fetchSheetLinkSuggestions(force);
  } finally {
    sheetLinkSuggestLoadingByMode[normalizedMode] = false;
    renderSheetLinkSuggestions();
    requestAnimationFrame(() => {
      document.getElementById('sheet_link_suggest')?.scrollIntoView({ behavior: 'smooth', block: 'nearest' });
    });
  }
}

function renderSheetLinkSuggestions(payload = null) {
  const host = document.getElementById('sheet_link_suggest');
  const titleNode = document.getElementById('sheet_link_suggest_title');
  const metaNode = document.getElementById('sheet_link_suggest_meta');
  const actionsNode = document.getElementById('sheet_link_suggest_actions');
  const rowsNode = document.getElementById('sheet_link_suggest_rows');
  const datalist = document.getElementById('sheet_link_column_datalist');
  if (!host || !titleNode || !metaNode || !actionsNode || !rowsNode || !datalist) return;

  const modeKey = String(currentRunMode || 'seeding').toLowerCase();
  host.style.display = '';
  const data = payload || getSheetLinkSuggestPayload(modeKey) || { columns: currentSheetLinkColumns || [] };
  const columns = Array.isArray(data.columns) ? data.columns.map(value => String(value || '').trim().toUpperCase()).filter(Boolean) : [];
  const counts = data && typeof data.counts === 'object' && data.counts ? data.counts : {};
  const totalUrlColumns = columns.length || Object.keys(counts).length;
  const rawSheetName = String(document.getElementById('sheet_name')?.value || '').trim();
  const rawSheetUrl = String(document.getElementById('sheet_url')?.value || '').trim();
  const startRow = currentSheetColumnStartRow();
  const currentKey = `${rawSheetUrl}|${rawSheetName}|${startRow}`;
  const loaded = !!sheetLinkSuggestLoadedByMode[modeKey] && String(sheetLinkSuggestSourceKeyByMode[modeKey] || '') === currentKey;
  const loading = !!sheetLinkSuggestLoadingByMode[modeKey];
  const idle = !loaded && !loading;
  currentSheetLinkColumns = columns;
  titleNode.textContent = t('sheetLinkSuggestTitle');
  host.classList.toggle('idle', idle);
  host.classList.toggle('mode-seeding', modeKey === 'seeding');
  host.classList.toggle('mode-booking', modeKey === 'booking');
  host.classList.toggle('mode-scan', modeKey === 'scan');
  const bulkSupported = supportsBulkSheetLinkMode(currentRunMode);
  const bulkSelections = getBulkSheetLinkSelections(currentRunMode);

  const active = activeSheetColumnTarget;
  const scanSingleTarget = modeKey === 'scan' && !!active;
  let helperText = '';
  if (loading) {
    helperText = t('sheetLinkSuggestLoading');
  } else if (loaded && active && ensureMappingBlocks(active.mode)[active.index]) {
    const fields = mappingFieldsForMode(active.mode);
    const field = fields.find(item => item.key === active.key);
    const block = ensureMappingBlocks(active.mode)[active.index] || {};
    helperText = t('sheetLinkSuggestActiveFmt')(field?.label || active.key, String(block?.name || '').trim());
  } else if (loaded) {
    helperText = columns.length ? (modeKey === 'scan' ? t('sheetLinkSuggestScanHelp') : t('sheetLinkSuggestHelp')) : '';
  }
  const sheetLabel = loaded && rawSheetName ? t('sheetLinkSuggestSheetFmt')(rawSheetName) : '';
  const metaText = [sheetLabel, helperText].filter(Boolean).join(' · ');
  metaNode.textContent = '';
  if (loading) {
    setSheetNameHint(t('sheetLinkCellHintLoading'));
  } else if (loaded && rawSheetName) {
    setSheetNameHint(t('sheetLinkCellHintCountFmt')(totalUrlColumns));
  } else {
    setSheetNameHint('');
  }

  const quickScanDisabled = loading;
  if (bulkSupported) {
    const bulkAddLabel = modeKey === 'seeding' ? t('sheetLinkFillBlocks') : t('sheetLinkBulkAdd');
    actionsNode.innerHTML = loaded ? `
      <div class="sheet-link-suggest-action-group buttons">
        <button class="btn sheet-link-suggest-action-btn icon-only" type="button" title="${esc(t('sheetLinkReload'))}" aria-label="${esc(t('sheetLinkReload'))}" onclick="reloadSheetLinkSuggestions()" ${quickScanDisabled ? 'disabled' : ''}>
          <svg viewBox="0 0 24 24" aria-hidden="true"><path d="M21 12a9 9 0 1 1-2.64-6.36"></path><path d="M21 3v6h-6"></path></svg>
        </button>
        <button class="btn blue sheet-link-suggest-action-btn" type="button" onclick="addBlocksFromSelectedLinkColumns()" ${quickScanDisabled ? 'disabled' : ''}>${esc(bulkAddLabel)}</button>
      </div>
    ` : `
      <div class="sheet-link-suggest-action-group buttons" style="width:100%;justify-content:center">
        <button class="btn sheet-link-suggest-action-btn" type="button" onclick="handleSheetLinkQuickAction()" ${quickScanDisabled ? 'disabled' : ''}>${esc(t('sheetLinkQuickScan'))}</button>
      </div>
    `;
  } else {
    actionsNode.innerHTML = `
      <div class="sheet-link-suggest-action-group buttons">
        <button class="btn sheet-link-suggest-action-btn" type="button" onclick="handleSheetLinkQuickAction()" ${quickScanDisabled ? 'disabled' : ''}>${esc(t('sheetLinkQuickScan'))}</button>
      </div>
    `;
  }
  metaNode.textContent = loading ? t('sheetLinkSuggestLoading') : metaText;

  datalist.innerHTML = columns.map(col => `<option value="${esc(col)}"></option>`).join('');
  host.classList.add('open');
  if (!loaded) {
    rowsNode.innerHTML = '';
    return;
  }
  if (!columns.length) {
    rowsNode.innerHTML = `<div class="sheet-link-suggest-empty">${esc(String(document.getElementById('sheet_name')?.value || '').trim() ? t('sheetLinkSuggestEmpty') : '')}</div>`;
    return;
  }
  const activeValue = getActiveSheetColumnTargetValue();
  rowsNode.innerHTML = columns.map(col => {
    const count = Number(counts?.[col] || 0);
    const activeClass = scanSingleTarget && activeValue === col ? ' active' : '';
    const selectedClass = (!scanSingleTarget && bulkSupported && bulkSelections.includes(col)) ? ' selected' : '';
    const title = count > 0 ? `${col} · ${count}` : col;
    const clickHandler = (!scanSingleTarget && bulkSupported)
      ? `toggleBulkSheetLinkSelection('${esc(col)}')`
      : `applySuggestedSheetColumn('${esc(col)}')`;
    return `<button class="sheet-link-suggest-chip${activeClass}${selectedClass}" type="button" title="${esc(title)}" onclick="${clickHandler}">${esc(col)}</button>`;
  }).join('');
}

async function fetchSheetLinkSuggestions(force = false) {
  const modeKey = String(currentRunMode || 'seeding').toLowerCase();
  const rawUrl = String(document.getElementById('sheet_url')?.value || '').trim();
  const rawName = String(document.getElementById('sheet_name')?.value || '').trim();
  const host = document.getElementById('sheet_link_suggest');
  const rowsNode = document.getElementById('sheet_link_suggest_rows');
  const metaNode = document.getElementById('sheet_link_suggest_meta');
  if (!rawUrl || !rawName) {
    currentSheetLinkColumns = [];
    sheetLinkSuggestLoadedByMode[modeKey] = false;
    sheetLinkSuggestPayloadByMode[modeKey] = { columns: [], counts: {} };
    if (rowsNode) rowsNode.innerHTML = '';
    if (metaNode) metaNode.textContent = '';
    return;
  }
  if (!force && !isKnownSheetName(rawUrl, rawName)) {
    currentSheetLinkColumns = [];
    sheetLinkSuggestLoadedByMode[modeKey] = false;
    sheetLinkSuggestPayloadByMode[modeKey] = { columns: [], counts: {} };
    if (rowsNode) rowsNode.innerHTML = '';
    if (metaNode) metaNode.textContent = '';
    return;
  }
  const startRow = currentSheetColumnStartRow();
  const cacheKey = `${rawUrl}|${rawName}|${startRow}`;
  const cached = getCachedSheetLinkColumns(rawUrl, rawName, startRow, false);
  if (cached && !force) {
    sheetColumnSuggestKey = cacheKey;
    sheetLinkSuggestLoadedByMode[modeKey] = true;
    sheetLinkSuggestPayloadByMode[modeKey] = cached;
    sheetLinkSuggestSourceKeyByMode[modeKey] = cacheKey;
    renderSheetLinkSuggestions(cached);
    return;
  }
  if (sheetColumnSuggestInflight[cacheKey]) {
    const pending = await sheetColumnSuggestInflight[cacheKey];
    sheetColumnSuggestKey = cacheKey;
    sheetLinkSuggestLoadedByMode[modeKey] = true;
    sheetLinkSuggestPayloadByMode[modeKey] = pending;
    sheetLinkSuggestSourceKeyByMode[modeKey] = cacheKey;
    renderSheetLinkSuggestions(pending);
    return;
  }
  if (host) host.classList.add('open');
  if (rowsNode) rowsNode.innerHTML = `<div class="sheet-link-suggest-empty">${esc(t('sheetLinkSuggestLoading'))}</div>`;
  if (metaNode) metaNode.textContent = '';

  function deriveFallbackLinkColumns() {
    const out = [];
    const seen = new Set();
    const add = (value) => {
      const col = String(value || '').trim().toUpperCase();
      if (!col || seen.has(col)) return;
      seen.add(col);
      out.push(col);
    };
    // Keep currently configured mapping column(s) first.
    const modeBlocks = ensureMappingBlocks(modeKey);
    modeBlocks.forEach(block => add(block?.col_url));
    // If Booking detects empty, reuse Seeding detections for the same sheet.
    if (modeKey === 'booking') {
      const seedingKey = String(sheetLinkSuggestSourceKeyByMode?.seeding || '');
      const sameSheet = seedingKey.startsWith(`${rawUrl}|${rawName}|`);
      const seedingPayload = sameSheet ? (sheetLinkSuggestPayloadByMode?.seeding || {}) : {};
      const seedingColumns = Array.isArray(seedingPayload.columns) ? seedingPayload.columns : [];
      seedingColumns.forEach(add);
    }
    return out;
  }

  try {
    sheetColumnSuggestInflight[cacheKey] = (async () => {
      const qs = new URLSearchParams({
        sheet_url: rawUrl,
        sheet_name: rawName,
        start_row: String(startRow),
      });
      if (force) qs.set('force', '1');
      if (currentSettingsCache.credentials_path) qs.set('credentials_path', currentSettingsCache.credentials_path);
      const out = await req('/api/sheets/column-suggestions?' + qs.toString());
      if (String(out.sheet_name || '').trim() && String(out.sheet_name || '').trim() !== rawName) {
        sheet_name.value = String(out.sheet_name || '').trim();
        rememberResolvedSheetName(rawUrl, out.sheet_name);
      }
      return {
        columns: Array.isArray(out.columns) ? out.columns : [],
        counts: out.counts && typeof out.counts === 'object' ? out.counts : {},
        drive_columns: Array.isArray(out.drive_columns) ? out.drive_columns : [],
        samples: out.samples && typeof out.samples === 'object' ? out.samples : {},
      };
    })();
    const payload = await sheetColumnSuggestInflight[cacheKey];
    const finalPayload = { ...payload };
    if ((!Array.isArray(finalPayload.columns) || !finalPayload.columns.length) && modeKey === 'booking') {
      const fallbackColumns = deriveFallbackLinkColumns();
      if (fallbackColumns.length) {
        finalPayload.columns = fallbackColumns;
        finalPayload.counts = finalPayload.counts && typeof finalPayload.counts === 'object' ? finalPayload.counts : {};
        fallbackColumns.forEach(col => {
          if (!Object.prototype.hasOwnProperty.call(finalPayload.counts, col)) finalPayload.counts[col] = 0;
        });
      }
    }
    sheetColumnSuggestCache[cacheKey] = { ...finalPayload, ts: Date.now() };
    sheetColumnSuggestKey = cacheKey;
    sheetLinkSuggestLoadedByMode[modeKey] = true;
    sheetLinkSuggestPayloadByMode[modeKey] = finalPayload;
    sheetLinkSuggestSourceKeyByMode[modeKey] = cacheKey;
    renderSheetLinkSuggestions(finalPayload);
  } catch (e) {
    const stale = getCachedSheetLinkColumns(rawUrl, rawName, startRow, true);
    if (stale) {
      sheetLinkSuggestLoadedByMode[modeKey] = true;
      sheetLinkSuggestPayloadByMode[modeKey] = stale;
      sheetLinkSuggestSourceKeyByMode[modeKey] = cacheKey;
      renderSheetLinkSuggestions(stale);
    } else if (rowsNode) {
      sheetLinkSuggestLoadedByMode[modeKey] = true;
      sheetLinkSuggestPayloadByMode[modeKey] = { columns: [], counts: {}, error: e.message };
      sheetLinkSuggestSourceKeyByMode[modeKey] = cacheKey;
      if (host) host.classList.add('open');
      rowsNode.innerHTML = `<div class="sheet-link-suggest-empty">${esc(e.message)}</div>`;
    }
  } finally {
    delete sheetColumnSuggestInflight[cacheKey];
  }
}

async function refreshSheetLinkCountSummary(force = false) {
  const modeKey = String(currentRunMode || 'seeding').toLowerCase();
  if (modeKey === 'scan') {
    setSheetNameHint('');
    return;
  }
  const rawUrl = String(document.getElementById('sheet_url')?.value || '').trim();
  const rawName = String(document.getElementById('sheet_name')?.value || '').trim();
  if (!rawUrl || !rawName) {
    setSheetNameHint('');
    return;
  }
  let knownSheet = isKnownSheetName(rawUrl, rawName);
  if (!knownSheet && force) {
    try {
      await fetchSheetNameSuggestions(true);
    } catch (_) {}
    knownSheet = isKnownSheetName(rawUrl, rawName);
  }
  if (!knownSheet) {
    setSheetNameHint(rawName ? t('sheetNameInvalidFmt')(rawName) : '', !!rawName);
    return;
  }
  const startRow = currentSheetColumnStartRow();
  const cached = getCachedSheetLinkColumns(rawUrl, rawName, startRow, false);
  if (cached && !force) {
    const totalCached = Array.isArray(cached.columns) ? cached.columns.length : Object.keys(cached.counts || {}).length;
    setSheetNameHint(t('sheetLinkCellHintCountFmt')(totalCached));
    return;
  }
  setSheetNameHint(t('sheetLinkCellHintLoading'));
  const cacheKey = `${rawUrl}|${rawName}|${startRow}`;
  try {
    if (!sheetColumnSuggestInflight[cacheKey]) {
      sheetColumnSuggestInflight[cacheKey] = (async () => {
        const qs = new URLSearchParams({
          sheet_url: rawUrl,
          sheet_name: rawName,
          start_row: String(startRow),
        });
        if (force) qs.set('force', '1');
        if (currentSettingsCache.credentials_path) qs.set('credentials_path', currentSettingsCache.credentials_path);
        const out = await req('/api/sheets/column-suggestions?' + qs.toString());
        if (String(out.sheet_name || '').trim() && String(out.sheet_name || '').trim() !== rawName) {
          sheet_name.value = String(out.sheet_name || '').trim();
          rememberResolvedSheetName(rawUrl, out.sheet_name);
        }
        return {
          columns: Array.isArray(out.columns) ? out.columns : [],
          counts: out.counts && typeof out.counts === 'object' ? out.counts : {},
          drive_columns: Array.isArray(out.drive_columns) ? out.drive_columns : [],
          samples: out.samples && typeof out.samples === 'object' ? out.samples : {},
        };
      })();
    }
    const payload = await sheetColumnSuggestInflight[cacheKey];
    sheetColumnSuggestCache[cacheKey] = { ...payload, ts: Date.now() };
    const total = Array.isArray(payload.columns) ? payload.columns.length : Object.keys(payload.counts || {}).length;
    setSheetNameHint(t('sheetLinkCellHintCountFmt')(total));
  } catch (e) {
    const stale = getCachedSheetLinkColumns(rawUrl, rawName, startRow, true);
    if (stale) {
      const totalStale = Array.isArray(stale.columns) ? stale.columns.length : Object.keys(stale.counts || {}).length;
      setSheetNameHint(t('sheetLinkCellHintCountFmt')(totalStale));
    } else {
      setSheetNameHint(e.message, true);
    }
  } finally {
    delete sheetColumnSuggestInflight[cacheKey];
  }
}

function scheduleSheetLinkSuggestions(force = false) {
  if (String(currentRunMode || '').toLowerCase() === 'scan') {
    renderSheetLinkSuggestions();
    return;
  }
  if (!sheetLinkSuggestLoadedByMode[String(currentRunMode || 'seeding').toLowerCase()]) {
    renderSheetLinkSuggestions();
    return;
  }
  if (sheetColumnSuggestTimer) clearTimeout(sheetColumnSuggestTimer);
  sheetColumnSuggestTimer = setTimeout(() => {
    fetchSheetLinkSuggestions(force);
  }, force ? 0 : 800);
}

function scheduleSheetLinkCountSummary(force = false) {
  if (sheetLinkSummaryTimer) clearTimeout(sheetLinkSummaryTimer);
  sheetLinkSummaryTimer = setTimeout(() => {
    refreshSheetLinkCountSummary(force);
  }, force ? 0 : SHEET_LINK_SUMMARY_DEBOUNCE_MS);
}

async function applySuggestedSheetColumn(column) {
  const normalizedColumn = String(column || '').trim().toUpperCase();
  let target = activeSheetColumnTarget;
  const isScanMode = String(currentRunMode || '').toLowerCase() === 'scan';
  const hadExplicitTarget = !!target;
  if (!target && isScanMode) {
    const blocks = ensureMappingBlocks('scan');
    const existingIndex = blocks.findIndex(
      block => String(block?.col_url || '').trim().toUpperCase() === normalizedColumn
    );
    if (existingIndex >= 0) {
      pendingMappingScrollMode = 'scan';
      pendingMappingHighlightIndex = existingIndex;
      activeSheetColumnTarget = null;
      renderMappingEditor();
      return;
    }
    let preferredIndex = blocks.findIndex(block => !String(block?.col_url || '').trim());
    if (preferredIndex < 0) {
      blocks.push(defaultMappingBlock('scan', blocks.length + 1));
      preferredIndex = blocks.length - 1;
      pendingMappingScrollMode = 'scan';
      pendingMappingHighlightIndex = preferredIndex;
    }
    target = {
      mode: 'scan',
      index: preferredIndex >= 0 ? preferredIndex : 0,
      key: 'col_url',
    };
    activeSheetColumnTarget = target;
  }
  if (!target) {
    alert(t('sheetLinkSuggestHelp'));
    return;
  }
  const blocks = ensureMappingBlocks(target.mode);
  if (!blocks[target.index]) {
    activeSheetColumnTarget = null;
    renderSheetLinkSuggestions();
    return;
  }
  const modeKey = String(target.mode || '').toLowerCase();
  const keyKey = String(target.key || '').toLowerCase();
  let resolvedColumn = normalizedColumn;
  if (modeKey === 'seeding' && keyKey === 'col_url') {
    const rawUrl = String(document.getElementById('sheet_url')?.value || '').trim();
    const sheetNameInputId = getMappingFieldInputId('seeding', target.index, 'sheet_name');
    const liveSheetName = String(document.getElementById(sheetNameInputId)?.value || '').trim();
    const blockSheetName = liveSheetName || String(blocks[target.index]?.sheet_name || '').trim();
    if (rawUrl && blockSheetName) {
      try {
        const out = await req('/api/sheets/quick-block-columns', {
          method: 'POST',
          body: JSON.stringify({
            sheet_url: rawUrl,
            sheet_name: blockSheetName,
            mode: 'seeding',
            columns: [normalizedColumn],
          }),
        });
        const first = Array.isArray(out?.items) ? out.items[0] : null;
        resolvedColumn = String(first?.link_column || normalizedColumn).trim().toUpperCase() || normalizedColumn;
        const resolvedDrive = String(first?.drive_column || '').trim().toUpperCase();
        const resolvedScreenshot = String(first?.screenshot_column || '').trim().toUpperCase();
        if (resolvedDrive) blocks[target.index].col_drive = resolvedDrive;
        if (resolvedScreenshot) blocks[target.index].col_screenshot = resolvedScreenshot;
      } catch (e) {
        alert(e.message || 'Không cộng được cột trên Sheet.');
        return;
      }
    }
  }
  blocks[target.index][target.key] = resolvedColumn;
  if (modeKey === 'seeding' && keyKey === 'col_url') {
    const next1 = shiftSheetColumn(resolvedColumn, 1);
    const next2 = shiftSheetColumn(resolvedColumn, 2);
    if (next1) blocks[target.index].col_drive = next1;
    if (next2) blocks[target.index].col_screenshot = next2;
  }
  if (modeKey === 'booking' && keyKey === 'col_url') {
    const next1 = shiftSheetColumn(resolvedColumn, 1);
    const next2 = shiftSheetColumn(resolvedColumn, 2);
    if (next1) blocks[target.index].col_screenshot = next1;
    if (next2) blocks[target.index].col_drive = next2;
  }
  if (modeKey === 'scan' && keyKey === 'col_url') {
    const inferredContent = getScanContentColumnFromImageColumn(resolvedColumn);
    if (inferredContent) {
      blocks[target.index].col_content = inferredContent;
    }
  }
  renderMappingEditor();
  if (isScanMode && !hadExplicitTarget) {
    activeSheetColumnTarget = null;
    renderSheetLinkSuggestions();
    return;
  }
  renderSheetLinkSuggestions();
  requestAnimationFrame(() => {
    const input = document.getElementById(getMappingFieldInputId(target.mode, target.index, target.key));
    if (input) input.focus();
  });
}

function updateMappingBlock(mode, index, key, value) {
  const blocks = ensureMappingBlocks(mode);
  if (!blocks[index]) return;
  blocks[index][key] = key === 'start_line' ? Number(value || 4) : String(value || '');
  if (String(mode || '').toLowerCase() === 'seeding' && String(key || '').toLowerCase() === 'sheet_name') {
    const preferredName = String(blocks[index][key] || '').trim();
    if (preferredName) blocks[index].name = preferredName;
    if (Number(index) === 0) {
      const sheetNameInput = document.getElementById('sheet_name');
      if (sheetNameInput) sheetNameInput.value = preferredName;
    }
  }
  console.log(`[DEBUG] updateMappingBlock(${mode}, ${index}, ${key}): ${value} -> ${blocks[index][key]}`);
  if (String(mode || '').toLowerCase() === currentRunMode && String(key || '').toLowerCase() === 'start_line') {
    resetSheetLinkSuggestions();
  }
}

function removeMappingBlock(index) {
  const blocks = ensureMappingBlocks(currentRunMode);
  if (blocks.length <= 1) return;
  blocks.splice(index, 1);
  renderMappingEditor();
}

function addMappingBlock() {
  const blocks = ensureMappingBlocks(currentRunMode);
  if (currentRunMode === 'seeding' && blocks.length >= 3) {
    alert('Seeding hiện hỗ trợ tối đa 3 block.');
    return;
  }
  blocks.push(defaultMappingBlock(currentRunMode, blocks.length + 1));
  pendingMappingScrollMode = currentRunMode;
  pendingMappingHighlightIndex = blocks.length - 1;
  renderMappingEditor();
}

function toggleCaptureFivePerLink(checked) {
  captureFivePerLink = !!checked;
  rememberCurrentRunFlags(currentRunMode);
}

function getModeBasePort(mode = currentRunMode) {
  return Number(DEFAULT_SHARED_BROWSER_PORT);
}

function getChromePortForBlock(index, mode = currentRunMode) {
  const basePort = Number(getModeBasePort(mode)) || Number(DEFAULT_SHARED_BROWSER_PORT) || 9223;
  const blockIndex = Math.max(0, Number(index) || 0);
  if (blockIndex <= 0) return basePort;
  return basePort + 100 + blockIndex;
}

function openAirDatePicker(mode, index) {
  const blocks = ensureMappingBlocks(mode);
  const currentValue = String(blocks?.[index]?.col_air_date || '').trim();
  const picker = document.getElementById(`air_date_picker_${mode}_${index}`);
  if (!picker) return;
  picker.value = currentValue || getTodayLocalDateString();
  if (typeof picker.showPicker === 'function') picker.showPicker();
  else picker.click();
}

function applyAirDate(mode, index, value) {
  updateMappingBlock(mode, index, 'col_air_date', value || '');
  renderMappingEditor();
}

async function scanSeedingBlockSheet(index) {
  if (currentRunMode !== 'seeding') return;
  const blocks = ensureMappingBlocks('seeding');
  const block = blocks[index];
  if (!block) return;
  const rawSheetUrl = String(document.getElementById('sheet_url')?.value || '').trim();
  if (!rawSheetUrl) {
    alert('Nhập link Sheet chính bên trái trước.');
    return;
  }
  try {
    const qsNames = new URLSearchParams({ sheet_url: rawSheetUrl });
    if (currentSettingsCache.credentials_path) qsNames.set('credentials_path', currentSettingsCache.credentials_path);
    const namesOut = await req('/api/sheets/names?' + qsNames.toString());
    const titles = Array.isArray(namesOut?.titles) ? namesOut.titles.filter(Boolean) : [];
    if (!titles.length) throw new Error('Sheet này không có tab nào khả dụng.');
    const sheetNameInputId = getMappingFieldInputId('seeding', index, 'sheet_name');
    const liveSheetName = String(document.getElementById(sheetNameInputId)?.value || '').trim();
    let chosen = liveSheetName || String(block.sheet_name || '').trim();
    if (!chosen || !titles.includes(chosen)) chosen = String(titles[0] || '').trim();
    block.sheet_name = chosen;
    block.name = chosen;
    const sheetNameInput = document.getElementById('sheet_name');
    if (sheetNameInput) sheetNameInput.value = chosen;
    rememberResolvedSheetName(rawSheetUrl, chosen);
    block.drive_id = String(document.getElementById('drive_id')?.value || '').trim();

    const qsCols = new URLSearchParams({
      sheet_url: rawSheetUrl,
      sheet_name: chosen,
      start_row: String(Number(block.start_line || 4) || 4),
      force: '1',
    });
    if (currentSettingsCache.credentials_path) qsCols.set('credentials_path', currentSettingsCache.credentials_path);
    const colsOut = await req('/api/sheets/column-suggestions?' + qsCols.toString());
    const allCols = Array.isArray(colsOut?.columns) ? colsOut.columns : [];
    // Keep the exact same ranking as "quét cột có link": top by API suggestion.
    const pickedUrl = String(allCols[0] || '').trim().toUpperCase();
    if (pickedUrl) {
      block.col_url = pickedUrl;
      const next1 = shiftSheetColumn(pickedUrl, 1);
      const next2 = shiftSheetColumn(pickedUrl, 2);
      if (next1) block.col_drive = next1;
      if (next2) block.col_screenshot = next2;
      activeSheetColumnTarget = { mode: 'seeding', index: Number(index) || 0, key: 'col_url' };
    } else {
      block.col_url = '';
    }
    const payload = {
      columns: allCols.map(col => String(col || '').trim().toUpperCase()).filter(Boolean),
      counts: colsOut && typeof colsOut.counts === 'object' ? colsOut.counts : {},
      drive_columns: Array.isArray(colsOut?.drive_columns) ? colsOut.drive_columns : [],
      samples: colsOut && typeof colsOut.samples === 'object' ? colsOut.samples : {},
    };
    const startRow = Math.max(1, Number(block.start_line || 4) || 4);
    const cacheKey = `${rawSheetUrl}|${chosen}|${startRow}`;
    sheetColumnSuggestCache[cacheKey] = { ...payload, ts: Date.now() };
    const modeKey = String(currentRunMode || 'seeding').toLowerCase();
    sheetLinkSuggestLoadedByMode[modeKey] = true;
    sheetLinkSuggestPayloadByMode[modeKey] = payload;
    sheetLinkSuggestSourceKeyByMode[modeKey] = cacheKey;
    setStatus(
      pickedUrl
        ? `Đã quét block ${index + 1}: ${chosen} · Link ${pickedUrl}`
        : `Đã quét block ${index + 1}: ${chosen} · không tìm thấy cột link`,
      pickedUrl ? 'done' : 'failed'
    );
    renderMappingEditor();
    renderSheetLinkSuggestions(payload);
  } catch (e) {
    alert(e.message);
  }
}

function isLocalWebHost() {
  return isConfiguredLocalBrowserHost(window.location.hostname);
}

async function launchChromeBlock(index, mode = currentRunMode, explicitPort = null) {
  const runModeKey = String(mode || currentRunMode || 'seeding').toLowerCase();
  const actionKey = `${runModeKey}:${Number(index) || 0}`;
  if (launchChromeInFlightByKey[actionKey]) return;
  launchChromeInFlightByKey[actionKey] = true;
  try {
    const runMode = runModeKey;
    const blockIndex = Number(index) || 0;
    const port = Number(explicitPort) || getChromePortForBlock(blockIndex, runMode);
    const previousMode = currentRunMode;
    currentRunMode = runMode;
    const blockName = getBlockActivityName(blockIndex);
    currentRunMode = previousMode;
    if (!isLocalWebHost()) {
      const isMac = /mac/i.test(String(navigator.platform || ''));
      if (!localAgentState.checked) {
        await detectLocalAgent();
      }
      if (localAgentState.enabled) {
        const out = await req(`/api/chrome/launch-block/${blockIndex}?run_mode=${encodeURIComponent(runMode)}&browser_port=${port}`, { method: 'POST' });
        try {
          await logActivityEvent({
            kind: 'login',
            level: 'info',
            run_mode: runMode,
            block_name: blockName,
            browser_port: Number(out?.browser_port || port),
            message: `${blockName}: đã mở Chrome ${Number(out?.browser_port || port)} qua local agent`,
          });
        } catch (_) {}
        setStatus(out.message || `Đã mở Chrome ${Number(out?.browser_port || port)} trên máy local`, 'running');
        return;
      }
      if (isMac) {
        setStatus(
          `Mac chưa kết nối local agent. Hãy chạy file run_local_agent.command, giữ cửa sổ đó mở, rồi tải lại trang và bấm lại Chrome ${port}.`,
          'failed'
        );
        return;
      }
      setStatus(
        `Chưa kết nối local agent. Hãy chạy local agent trên máy này rồi bấm lại Chrome ${port}.`,
        'failed'
      );
      return;
    }
    const out = await req(`/api/chrome/launch-block/${blockIndex}?run_mode=${encodeURIComponent(runMode)}&browser_port=${port}`, { method: 'POST' });
    try {
      await logActivityEvent({
        kind: 'login',
        level: 'info',
        run_mode: runMode,
        block_name: blockName,
        browser_port: Number(out?.browser_port || port),
        message: `${blockName}: đã mở Chrome ${Number(out?.browser_port || port)} để đăng nhập`,
      });
    } catch (_) {}
    setStatus(out.message || 'Chrome launch requested', 'running');
  } catch (e) {
    try {
      const maybePort = Number(explicitPort) || getChromePortForBlock(Number(index) || 0, String(mode || currentRunMode || 'seeding').toLowerCase());
      setStatus(`Không mở được Chrome ${maybePort}: ${String(e?.message || e || 'Unknown error')}`, 'failed');
    } catch (_) {}
    alert(e.message);
  } finally {
    launchChromeInFlightByKey[actionKey] = false;
  }
}

function handleScanTermsInput(kind, value) {
  const key = kind === 'keyword' ? 'scan_keyword_terms' : 'scan_negative_terms';
  currentSettingsCache[key] = String(value || '');
  scheduleScanFilterSettingsSave();
}

function renderScanFilterEditor() {
  const host = document.getElementById('scanFilterEditor');
  if (!host) return;
  host.hidden = true;
  host.innerHTML = '';
}

function renderMappingEditor() {
  const blocks = ensureMappingBlocks(currentRunMode);
  const fields = mappingFieldsForMode(currentRunMode);
  const host = document.getElementById('mappingBlocks');
  const addButton = document.getElementById('mappingAddButton');
  if (addButton) addButton.textContent = t('addBlock');
  if (!host) return;
  if (currentRunMode === 'scan') {
    host.innerHTML = `<div class="mapping-seeding-row">${blocks.map((block, index) => {
      const blockClass = pendingMappingScrollMode === currentRunMode && pendingMappingHighlightIndex === index
        ? 'mapping-block mapping-block-new'
        : 'mapping-block';
      const title = block.name || `Scan ${index + 1}`;
      const rows = fields.map(field => {
        const value = block[field.key] ?? '';
        const inputType = field.type === 'number' ? 'number' : 'text';
        const inputId = getMappingFieldInputId(currentRunMode, index, field.key);
        const listAttr = isLinkSuggestionField(currentRunMode, field.key) ? ' list="sheet_link_column_datalist"' : '';
        const focusAttr = isLinkSuggestionField(currentRunMode, field.key) ? ` onfocus="setSheetColumnTarget('${currentRunMode}', ${index}, '${field.key}')"` : '';
        return `<div class="mapping-label">${esc(field.label)}</div><div><input id="${esc(inputId)}" class="mapping-input" type="${inputType}" value="${esc(value)}"${listAttr}${focusAttr} oninput="updateMappingBlock('${currentRunMode}', ${index}, '${field.key}', this.value)" /></div>`;
      }).join('');
      return `<section class="${blockClass}">
        <div class="mapping-block-head">
          <div class="mapping-block-title">${esc(title)}</div>
          ${blocks.length > 1 ? `<button class="btn red mapping-remove" type="button" onclick="removeMappingBlock(${index})">x</button>` : ''}
        </div>
        <div class="mapping-block-grid">${rows}</div>
      </section>`;
    }).join('')}</div>`;
  } else if (currentRunMode === 'seeding' || currentRunMode === 'booking') {
    host.innerHTML = `<div class="mapping-seeding-row">${blocks.map((block, index) => {
      const blockClass = pendingMappingScrollMode === currentRunMode && pendingMappingHighlightIndex === index
        ? 'mapping-block mapping-block-new'
        : 'mapping-block';
      if (currentRunMode === 'seeding') {
        const preferredName = String(block.sheet_name || block.name || '').trim();
        if (preferredName) block.name = preferredName;
        const sharedDriveId = String(document.getElementById('drive_id')?.value || '').trim();
        if (!String(block.drive_id || '').trim() && sharedDriveId) block.drive_id = sharedDriveId;
      }
      const rows = fields.map(field => {
        const value = block[field.key] ?? '';
        const inputId = getMappingFieldInputId(currentRunMode, index, field.key);
        const listAttr = isLinkSuggestionField(currentRunMode, field.key) ? ' list="sheet_link_column_datalist"' : '';
        const focusAttr = isLinkSuggestionField(currentRunMode, field.key) ? ` onfocus="setSheetColumnTarget('${currentRunMode}', ${index}, '${field.key}')"` : '';
        if (field.key === 'col_air_date') {
          return `<div class="mapping-label">${esc(field.label)}</div><div class="mapping-field-combo"><input id="${esc(inputId)}" class="mapping-input" type="text" value="${esc(value)}" placeholder="${esc(getTodayLocalDateString())}" oninput="updateMappingBlock('${currentRunMode}', ${index}, '${field.key}', this.value)" /><button class="btn mapping-icon-btn" type="button" onclick="openAirDatePicker('${currentRunMode}', ${index})">...</button><input id="air_date_picker_${currentRunMode}_${index}" type="date" style="position:absolute;opacity:0;pointer-events:none;width:1px;height:1px" onchange="applyAirDate('${currentRunMode}', ${index}, this.value)" /></div>`;
        }
        if (currentRunMode === 'seeding' && field.key === 'sheet_name') {
          return `<div class="mapping-label">${esc(field.label)}</div><div class="mapping-field-combo"><input id="${esc(inputId)}" class="mapping-input" type="text" value="${esc(value)}" list="sheet_name_suggestions" placeholder="Tên tab (sheet)" oninput="updateMappingBlock('${currentRunMode}', ${index}, '${field.key}', this.value)" /><button class="btn mapping-icon-btn" type="button" title="Quét tab này & cột link" onclick="scanSeedingBlockSheet(${index})">🔎</button></div>`;
        }
        const inputType = field.type === 'number' ? 'number' : 'text';
        if (field.key === 'name') {
          return `<div class="mapping-label">${esc(field.label)}</div><div class="mapping-field-combo"><input id="${esc(inputId)}" class="mapping-input" type="${inputType}" value="${esc(value)}" oninput="updateMappingBlock('${currentRunMode}', ${index}, '${field.key}', this.value)" />${blocks.length > 1 ? `<button class="btn red mapping-remove" type="button" onclick="removeMappingBlock(${index})">x</button>` : ''}</div>`;
        }
        return `<div class="mapping-label">${esc(field.label)}</div><div><input id="${esc(inputId)}" class="mapping-input" type="${inputType}" value="${esc(value)}"${listAttr}${focusAttr} oninput="updateMappingBlock('${currentRunMode}', ${index}, '${field.key}', this.value)" /></div>`;
      }).join('');
      const chromePort = getChromePortForBlock(index, currentRunMode);
      const chromeRow = `<div class="mapping-label">${esc(t('chrome'))}</div><div><button class="btn mapping-chrome-btn" type="button" onclick="launchChromeBlock(${index}, '${currentRunMode}', ${chromePort})">${esc(`${t('chrome')} ${chromePort}`)}</button></div>`;
      const blockTitle = currentRunMode === 'seeding'
        ? String(block?.sheet_name || block?.name || `Post ${index + 1}`)
        : String(block?.name || `Post ${index + 1}`);
      const blockHead = `<div class="mapping-block-head">
        <div class="mapping-block-title">${esc(blockTitle)}</div>
        ${blocks.length > 1 ? `<button class="btn red mapping-remove" type="button" onclick="removeMappingBlock(${index})">x</button>` : ''}
      </div>`;
      return `<section class="${blockClass}">${blockHead}<div class="mapping-block-grid">${rows}${chromeRow}</div></section>`;
    }).join('')}</div>`;
  }
  const addRow = document.querySelector('.mapping-add-row');
  if (addRow) {
    addRow.classList.toggle('booking', currentRunMode === 'booking');
    addRow.innerHTML = `<button id="mappingAddButton" class="btn" type="button" onclick="addMappingBlock()">${esc(t('addBlock'))}</button>`;
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
  renderScanFilterEditor();
  renderSheetLinkSuggestions();
}

function renderBookingRunExtraToggles() {
  const host = document.getElementById('bookingRunExtraToggles');
  if (!host) return;
  if (currentRunMode !== 'booking') {
    host.innerHTML = '';
    return;
  }
  host.innerHTML = `
    <label id="captureFiveCard" class="run-overwrite-card">
      <span class="run-overwrite-copy">
        <span id="captureFiveLabel" class="run-overwrite-title">${esc(t('captureFive'))}</span>
        <span id="captureFiveHelpInline" class="run-overwrite-help">${esc(t('captureFiveHelp'))}</span>
      </span>
      <span class="run-overwrite-switch">
        <input id="capture_five_per_link_toggle" type="checkbox" ${captureFivePerLink ? 'checked' : ''} onchange="toggleCaptureFivePerLink(this.checked)" />
        <span class="run-overwrite-slider"></span>
      </span>
    </label>`;
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
  const sheetNameField = document.getElementById('sheet_name_field');
  if (sheetNameField) {
    sheetNameField.style.display = currentRunMode === 'booking' ? '' : 'none';
  }
  const driveIdField = document.getElementById('drive_id_field');
  if (driveIdField) {
    driveIdField.style.display = currentRunMode === 'booking' ? '' : 'none';
  }
  if (currentRunMode === 'seeding') {
    const blocks = ensureMappingBlocks('seeding');
    const firstBlockName = String(blocks?.[0]?.sheet_name || blocks?.[0]?.name || '').trim();
    const sheetNameInput = document.getElementById('sheet_name');
    if (sheetNameInput && firstBlockName) sheetNameInput.value = firstBlockName;
  }
  const scanNegativeFilterCard = document.getElementById('scanNegativeFilterCard');
  if (scanNegativeFilterCard) scanNegativeFilterCard.style.display = 'none';
  renderBookingRunExtraToggles();
  const runsGroup = document.getElementById('runs_group');
  if (runsGroup) runsGroup.classList.toggle('open', document.getElementById('view-runs')?.classList.contains('active'));
  renderMappingEditor();
  renderSheetLinkSuggestions();
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

  const menuMap = { runs: 'runs', projects: 'projects', tasks: 'tasks', activities: 'activities', access: 'access', settings: 'settings' };
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
  setText('#runtimeBadge', (!isLocalBrowserOrigin() && localAgentState.enabled) ? t('runtimeLocal') : t('runtimeHost'));
  setText('#runShareLabel', t('runShareLabel'));
  applyRunModeUI();
  setText('label[for="sheet_url"]', t('sheetUrl'));
  setText('label[for="sheet_name"]', t('sheetName'));
  setText('#startJobLabel', t('startJob'));
  setText('#pauseJobLabel', t('stopJob'));
  setText('#continueJobLabel', t('continueJob'));
  setText('#errorOnlyJobLabel', t('errorOnlyJob'));
  setText('#overwriteRunLabel', t('overwriteRun'));
  setText('#overwriteRunHelp', t('overwriteRunHelp'));
  setText('#highlightSheetErrorsLabel', t('highlightSheetErrors'));
  setText('#highlightSheetErrorsHelp', t('highlightSheetErrorsHelp'));
  setText('#captureFiveLabel', t('captureFive'));
  setText('#captureFiveHelpInline', t('captureFiveHelp'));
  setText('#scanNegativeFilterLabel', t('scanNegativeFilter'));
  setText('#scanNegativeFilterHelp', t('scanNegativeFilterHelp'));
  setText('#sheet_link_suggest_title', t('sheetLinkSuggestTitle'));
  setText('#runMonitorKicker', t('monitorKicker'));
  setText('#runMonitorJobLabel', t('monitorJob'));
  setText('#runMonitorProgressLabel', t('monitorProgress'));
  setText('#runMonitorErrorLabel', t('monitorErrors'));
  setText('#runMonitorIssueRowsLabel', t('monitorIssueRowsLabel'));
  setText('#runMonitorUnavailableRowsLabel', t('monitorIssueUnavailableRowsLabel'));
  setText('#runMonitorErrorRows', '-');
  setText('#runMonitorUnavailableRows', '-');
  setText('#runMonitorErrorMeta', t('monitorSuccessFailedFmt')(0, 0, 0));
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
  renderSheetLinkSuggestions();
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
  setText('label[for="settings_tiktok_captcha_wait_sec"]', t('tiktokCaptchaWait'));
  setText('label[for="settings_please_wait_delay_sec"]', t('pleaseWaitDelay'));
  setText('#settings_tiktok_force_focus_label', t('tiktokForceFocus'));
  setText('#settings_tiktok_force_focus_help', t('tiktokForceFocusHelp'));
  const settingsNegativeTerms = document.getElementById('settings_scan_negative_terms');
  if (settingsNegativeTerms) settingsNegativeTerms.placeholder = t('scanNegativeTermsPlaceholder');
  const settingsKeywordTerms = document.getElementById('settings_scan_keyword_terms');
  if (settingsKeywordTerms) settingsKeywordTerms.placeholder = t('scanKeywordTermsPlaceholder');
  const scanNegativeEditor = document.getElementById('scan_negative_terms_editor');
  if (scanNegativeEditor) scanNegativeEditor.placeholder = t('scanNegativeTermsPlaceholder');
  const scanKeywordEditor = document.getElementById('scan_keyword_terms_editor');
  if (scanKeywordEditor) scanKeywordEditor.placeholder = t('scanKeywordTermsPlaceholder');
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
  updateRuntimeBadge();
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

function resetCurrentLogsState(jobId = '') {
  currentLogsJobId = String(jobId || '').trim();
  currentLogsCursor = 0;
  currentLogsCache = [];
}

function setRunMode(mode) {
  rememberCurrentRunFlags(currentRunMode);
  const nextMode = String(mode || 'seeding').toLowerCase();
  currentRunMode = ['seeding', 'booking', 'scan'].includes(nextMode) ? nextMode : 'seeding';
  applyRunFlagsForMode(currentRunMode);
  resetSheetLinkSuggestions(currentRunMode);
  const previousJobId = currentJobId;
  currentJobId = resolveModeJobId(currentRunMode);
  if (currentJobId !== previousJobId) {
    resetCurrentLogsState(currentJobId || '');
  }
  applyRunModeUI();
}

function openRunMode(mode) {
  switchView('runs');
  setRunMode(mode);
  if (currentJobId) {
    pollCurrent();
  } else {
    currentJobSnapshot = null;
    resetCurrentLogsState('');
    renderRunMonitor(null, []);
  }
}

function setLanguage(lang) {
  currentLang = lang === 'en' ? 'en' : 'vi';
  localStorage.setItem('ui_lang', currentLang);
  applyLanguage();
  renderOverview();
  renderProjects();
  renderActivities(getCombinedActivities());
  renderRunMonitor(currentJobSnapshot, currentLogsCache);
  if (String(document.getElementById('sheet_url')?.value || '').trim()) scheduleSheetNameSuggestions(false);
}

function toggleLanguage() {
  setLanguage(currentLang === 'vi' ? 'en' : 'vi');
}

function isViewActive(name) {
  return !!document.getElementById('view-' + String(name || '').trim())?.classList.contains('active');
}

async function req(url, opts = {}) {
  const useLocalAgent = shouldUseLocalAgent(url);
  if (useLocalAgent) return agentReq(url, opts);
  const timeoutMs = resolveRequestTimeoutMs(url, opts);
  const fetchOptions = { ...opts };
  delete fetchOptions.timeout_ms;
  const headers = { 'Content-Type': 'application/json', ...(fetchOptions.headers || {}) };
  const res = await fetchWithTimeout(url, { ...fetchOptions, headers }, timeoutMs);
  const data = await res.json().catch(() => ({}));
  if (res.status === 401) {
    window.location.href = '/login';
    throw new Error(data.detail || 'Authentication required');
  }
  if (!res.ok) throw new Error(data.detail || ('HTTP ' + res.status));
  return data;
}

async function loadAuthState() {
  const data = await req('/api/auth/me');
  authState.email = String(data.email || '').trim();
  authState.role = String(data.role || 'user').trim().toLowerCase() === 'admin' ? 'admin' : 'user';
  authState.isAdmin = !!data.is_admin || authState.role === 'admin';
  return data;
}

async function logActivityEvent(payload = {}) {
  try {
    const out = await req('/api/activity', {
      method: 'POST',
      body: JSON.stringify(payload),
    });
    if (out?.item) {
      currentActivityEvents = [out.item, ...(currentActivityEvents || [])];
      renderActivities(getCombinedActivities());
    }
    return out?.item || null;
  } catch (_e) {
    return null;
  }
}

function getBlockActivityName(index) {
  const block = ensureMappingBlocks(currentRunMode)[Number(index) || 0] || {};
  return String(block?.name || '').trim() || `Post ${Number(index) + 1}`;
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
      unavailable: Number(summary.unavailable || 0),
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
    unavailable = Number(summary.unavailable || 0);
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
  const base = job?.summary || {};
  const logs = Array.isArray(job?.logs) ? job.logs : (Array.isArray(job?.recent_logs) ? job.recent_logs : []);
  const touchedRows = new Set();
  const successRows = new Set();
  const failedRows = new Set();
  const unavailableRows = new Set();
  logs.forEach(item => {
    const row = Number(item?.row || 0);
    if (!Number.isFinite(row) || row <= 0) return;
    touchedRows.add(row);
    if (isUnavailableLog(item)) unavailableRows.add(row);
    else if (isFailedLog(item)) failedRows.add(row);
    else if (isSuccessLog(item)) successRows.add(row);
  });
  const done = Math.max(Number(base.done || 0), touchedRows.size);
  const success = Math.max(Number(base.success || 0), successRows.size);
  const failed = Math.max(
    Number(base.failed || 0),
    failedRows.size,
    Number(job?.error_row_count || 0),
    Object.keys(job?.error_rows || {}).length
  );
  const unavailable = Math.max(Number(base.unavailable || 0), unavailableRows.size);
  const total = Math.max(
    Number(base.total || 0),
    done,
    success + failed + unavailable
  );
  return {
    done,
    total,
    success,
    failed,
    unavailable,
    eta: String(base.eta || '---'),
  };
}

function getJobSheetLabel(job) {
  const req = job?.request || {};
  return req.sheet_name || req.sheet_url || 'Unknown sheet';
}

function getJobMode(job) {
  return String(job?.mode || job?.request?.mode || job?.request?.mappings?.[0]?.mode || 'seeding').toLowerCase();
}

function getJobOwnerEmail(job) {
  return String(job?.owner_email || job?.request?.owner_email || '').trim().toLowerCase();
}

function isJobOwnedByCurrentUser(job) {
  const viewer = String(authState.email || '').trim().toLowerCase();
  const owner = getJobOwnerEmail(job);
  return !!viewer && !!owner && viewer === owner;
}

function getJobOwnerBadge(job) {
  const owner = getJobOwnerEmail(job);
  if (!owner) return '';
  if (!isAdminUser()) return '';
  if (isJobOwnedByCurrentUser(job)) return '';
  return owner;
}

function getJobRootId(job) {
  const req = job?.request || {};
  return String(req.root_job_id || job?.id || '').trim();
}

function getJobLineageJobs(job) {
  const rootId = getJobRootId(job);
  if (!rootId) return job ? [job] : [];
  return (jobsCache || [])
    .filter(item => getJobRootId(item) === rootId)
    .sort((a, b) => {
      const at = Date.parse(String(a?.created_at || '')) || 0;
      const bt = Date.parse(String(b?.created_at || '')) || 0;
      return at - bt;
    });
}

function hasProjectLogsCacheEntry(jobId) {
  const key = String(jobId || '').trim();
  if (!key) return false;
  return Object.prototype.hasOwnProperty.call(projectLogsCacheByJobId, key);
}

function getCachedProjectLogs(jobId) {
  const key = String(jobId || '').trim();
  if (!key) return [];
  return Array.isArray(projectLogsCacheByJobId[key]) ? projectLogsCacheByJobId[key] : [];
}

async function ensureProjectLineageLogs(snapshot) {
  if (!snapshot) return;
  const lineageJobs = getJobLineageJobs(snapshot);
  if (!lineageJobs.length) return;
  const waits = [];
  lineageJobs.forEach(job => {
    const jobId = String(job?.id || '').trim();
    if (!jobId) return;
    if (projectLogsInflightByJobId[jobId]) {
      waits.push(projectLogsInflightByJobId[jobId]);
      return;
    }
    const inlineLogs = Array.isArray(job?.logs) ? job.logs : (Array.isArray(job?.recent_logs) ? job.recent_logs : []);
    if (inlineLogs.length > 0 && !hasProjectLogsCacheEntry(jobId)) {
      projectLogsCacheByJobId[jobId] = inlineLogs.slice(-1000);
      return;
    }
    if (hasProjectLogsCacheEntry(jobId)) return;
    const promise = req('/api/jobs/' + jobId + '/logs?limit=1000&since=0')
      .then(out => {
        const fetched = Array.isArray(out?.logs) ? out.logs : [];
        projectLogsCacheByJobId[jobId] = fetched;
      })
      .catch(() => {
        projectLogsCacheByJobId[jobId] = [];
      })
      .finally(() => {
        delete projectLogsInflightByJobId[jobId];
      });
    projectLogsInflightByJobId[jobId] = promise;
    waits.push(promise);
  });
  if (!waits.length) return;
  await Promise.all(waits);
  if (!isViewActive('projects')) return;
  if (String(currentProjectJobId || '').trim() !== String(snapshot?.id || '').trim()) return;
  renderProjects();
}

function getLineageDisplayLogs(snapshot, logs) {
  const currentLogs = Array.isArray(logs) ? logs : [];
  if (!snapshot) return currentLogs;
  const lineageJobs = getJobLineageJobs(snapshot);
  if (!lineageJobs.length) return currentLogs;
  const out = [];
  const seen = new Set();
  lineageJobs.forEach(job => {
    const cachedLogs = getCachedProjectLogs(job?.id);
    const sourceLogs = job?.id === snapshot?.id
      ? currentLogs.concat(cachedLogs)
      : (Array.isArray(job?.logs) ? job.logs : (Array.isArray(job?.recent_logs) ? job.recent_logs : [])).concat(cachedLogs);
    sourceLogs.forEach(item => {
      const key = [
        String(job?.id || ''),
        String(item?.ts || ''),
        String(item?.row || ''),
        String(item?.state || ''),
        String(item?.result || ''),
        String(item?.tag || ''),
        String(item?.message || ''),
      ].join('|');
      if (seen.has(key)) return;
      seen.add(key);
      out.push({ ...item, __job_id: job?.id || '' });
    });
  });
  out.sort((a, b) => {
    const at = Date.parse(String(a?.ts || '')) || 0;
    const bt = Date.parse(String(b?.ts || '')) || 0;
    if (at !== bt) return at - bt;
    return Number(a?.row || 0) - Number(b?.row || 0);
  });
  return out;
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

function isActiveJobStatus(status) {
  const value = String(status || '').toLowerCase();
  return ['queued', 'running', 'paused'].includes(value);
}

function sortJobsByRecency(jobs) {
  return [...(jobs || [])].sort((a, b) => {
    const at = Date.parse(String(a?.created_at || a?.finished_at || '')) || 0;
    const bt = Date.parse(String(b?.created_at || b?.finished_at || '')) || 0;
    return bt - at;
  });
}

function resolveModeJobId(mode) {
  const jobs = sortJobsByRecency(getJobsByMode(mode));
  if (!jobs.length) return null;
  const ownActive = jobs.find(job => isJobOwnedByCurrentUser(job) && isActiveJobStatus(job?.status));
  if (ownActive) return ownActive.id;
  const selected = getSelectedJobIdForMode(mode);
  const matched = selected ? jobs.find(job => job.id === selected) : null;
  if (matched) return matched.id;
  const ownJob = jobs.find(isJobOwnedByCurrentUser);
  if (ownJob) return ownJob.id;
  const activeJob = jobs.find(job => isActiveJobStatus(job?.status));
  return activeJob ? activeJob.id : jobs[0].id;
}

function extractBlockingJobId(message) {
  const text = String(message || '');
  const match = text.match(/\b[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\b/i);
  return match ? match[0] : '';
}

async function focusBlockingModeJob(err, requestedMode = currentRunMode) {
  const blockingJobId = extractBlockingJobId(err?.message);
  if (!blockingJobId) return false;
  await refreshJobs();
  const blockingJob = (jobsCache || []).find(job => job.id === blockingJobId);
  if (!blockingJob) return false;
  const blockingMode = getJobMode(blockingJob) || requestedMode;
  setSelectedJobIdForMode(blockingMode, blockingJobId);
  currentJobId = blockingJobId;
  if (currentRunMode !== blockingMode) {
    setRunMode(blockingMode);
  }
  await pollCurrent();
  setStatus(`${prettyWord(blockingMode)}: đang có job chạy · ${blockingJobId.slice(0, 8)}`, 'running');
  return true;
}

function syncModeSelections() {
  ['seeding', 'booking', 'scan'].forEach(mode => {
    setSelectedJobIdForMode(mode, resolveModeJobId(mode));
  });
}

function getSavedProjectJobs() {
  return (jobsCache || []).filter(job => {
    const status = String(job?.status || '').toLowerCase();
    if (['queued', 'running', 'paused', 'completed'].includes(status)) return true;
    if (!['stopped', 'failed'].includes(status)) return false;
    const summary = getJobSummary(job);
    return (
      summary.done > 0 ||
      summary.total > 0 ||
      summary.success > 0 ||
      summary.failed > 0 ||
      summary.unavailable > 0 ||
      !!String(job?.detail || '').trim() ||
      (Array.isArray(job?.recent_logs) && job.recent_logs.length > 0) ||
      (Array.isArray(job?.logs) && job.logs.length > 0)
    );
  });
}

function getProjectJobsForModeFilter() {
  const saved = getSavedProjectJobs();
  if (currentProjectModeFilter === 'all') return saved;
  return saved.filter(job => getJobMode(job) === currentProjectModeFilter);
}

function matchesProjectStatusFilter(job, statusFilter = currentProjectStatusFilter) {
  const normalized = String(statusFilter || 'all').toLowerCase();
  if (normalized === 'all') return true;
  const status = String(job?.status || '').toLowerCase();
  if (normalized === 'running') return ['queued', 'running', 'paused'].includes(status);
  return status === normalized;
}

function getFilteredProjectJobs() {
  const saved = getProjectJobsForModeFilter();
  if (currentProjectStatusFilter === 'all') return saved;
  return saved.filter(job => matchesProjectStatusFilter(job, currentProjectStatusFilter));
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
  const selected = getSelectedProjectJob();
  if (selected) ensureProjectLineageLogs(selected);
  renderProjects();
}

function setProjectModeFilter(mode) {
  currentProjectModeFilter = String(mode || 'all').toLowerCase();
  currentProjectJobId = null;
  renderProjects();
}

function setProjectStatusFilter(status) {
  currentProjectStatusFilter = String(status || 'all').toLowerCase();
  currentProjectJobId = null;
  renderProjects();
}

function expandProjectFilter(select) {
  if (!select) return;
  const optionCount = Math.max(Number(select.options?.length || 0), 0);
  const expandedSize = Math.max(2, Math.min(optionCount || 2, 6));
  select.size = expandedSize;
  select.classList.add('project-filter-input-open');
}

function collapseProjectFilter(select, delay = 120) {
  if (!select) return;
  window.setTimeout(() => {
    if (document.activeElement === select) return;
    select.size = 1;
    select.classList.remove('project-filter-input-open');
  }, delay);
}

function handleProjectFilterKeydown(event) {
  if (!event || event.key !== 'Escape') return;
  const select = event.currentTarget;
  collapseProjectFilter(select, 0);
  if (select && typeof select.blur === 'function') select.blur();
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
        owner_email: getJobOwnerEmail(job),
      });
    });
  });
  rows.sort((a, b) => {
    const left = new Date(a?.ts || 0).getTime();
    const right = new Date(b?.ts || 0).getTime();
    return right - left;
  });
  return rows;
}

function getCombinedActivities() {
  const jobLogs = getActivityLogsFromJobs().map(item => ({ ...item, __source: 'job' }));
  const manualEvents = (currentActivityEvents || []).map(item => ({
    ...item,
    row: item?.row ?? '-',
    state: String(item?.kind || 'action').toUpperCase(),
    result: String(item?.run_mode || 'manual').toUpperCase(),
    __source: 'activity',
    __job_id: String(item?.job_id || ''),
    __job_mode: String(item?.run_mode || ''),
    owner_email: String(item?.owner_email || '').trim().toLowerCase(),
  }));
  return [...jobLogs, ...manualEvents]
    .sort((a, b) => new Date(b?.ts || 0).getTime() - new Date(a?.ts || 0).getTime())
    .slice(0, 20);
}

function openProjectInRuns(jobId) {
  const job = (jobsCache || []).find(item => item.id === jobId);
  if (!job) return;
  const request = job.request || {};
  const mode = getJobMode(job);
  sheet_url.value = request.sheet_url || '';
  sheet_name.value = request.sheet_name || '';
  drive_id.value = request.drive_id || '';
  currentRunFlagsByMode[mode] = {
    ...ensureRunFlagsForMode(mode),
    force_run_all: request.force_run_all !== false,
    highlight_sheet_errors: !!request.highlight_sheet_errors,
    capture_five_per_link: !!request.capture_five_per_link,
  };
  currentMappingBlocksByMode[mode] = (request.mappings || []).length
    ? request.mappings.map((block, index) => sanitizeMappingBlockForMode(mode, block, index + 1))
    : [defaultMappingBlock(mode, 1)];
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
    delete projectLogsCacheByJobId[String(jobId || '').trim()];
    delete projectLogsInflightByJobId[String(jobId || '').trim()];
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
  const level = String(log?.level || '').toLowerCase();
  if (level === 'error' || level === 'failed') return 'error';
  if (level === 'warning' || level === 'warn') return 'warning';
  const raw = `${log?.tag || ''} ${log?.state || ''} ${log?.result || ''} ${log?.message || ''}`.toLowerCase();
  if (raw.includes('tiktok_url_mismatch') || raw.includes('url mismatch')) return 'error';
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

function primeCompletionNotifications() {
  try {
    if (!('Notification' in window)) return;
    if (Notification.permission === 'default') Notification.requestPermission().catch(() => {});
  } catch (_) {}
}

function stopCompletionTitleFlash() {
  if (completionTitleFlashTimer) {
    clearInterval(completionTitleFlashTimer);
    completionTitleFlashTimer = null;
  }
  document.title = defaultDocumentTitle;
}

function startCompletionTitleFlash(message) {
  const text = String(message || '').trim();
  if (!text) return;
  completionTitleFlashText = text;
  stopCompletionTitleFlash();
  let toggle = false;
  document.title = `${text} • ${defaultDocumentTitle}`;
  completionTitleFlashTimer = setInterval(() => {
    toggle = !toggle;
    document.title = toggle ? `${completionTitleFlashText} • ${defaultDocumentTitle}` : defaultDocumentTitle;
  }, 1200);
}

function playCompletionAlertTone() {
  try {
    const AudioCtx = window.AudioContext || window.webkitAudioContext;
    if (!AudioCtx) return;
    window.__toolEvidenceAudioCtx = window.__toolEvidenceAudioCtx || new AudioCtx();
    const ctx = window.__toolEvidenceAudioCtx;
    if (ctx.state === 'suspended') ctx.resume().catch(() => {});
    const pattern = [0, 0.22, 0.44];
    pattern.forEach((offset, index) => {
      const osc = ctx.createOscillator();
      const gain = ctx.createGain();
      osc.type = 'sine';
      osc.frequency.value = index === 1 ? 880 : 740;
      gain.gain.value = 0.0001;
      osc.connect(gain);
      gain.connect(ctx.destination);
      const startAt = ctx.currentTime + offset;
      gain.gain.setValueAtTime(0.0001, startAt);
      gain.gain.exponentialRampToValueAtTime(0.16, startAt + 0.02);
      gain.gain.exponentialRampToValueAtTime(0.0001, startAt + 0.18);
      osc.start(startAt);
      osc.stop(startAt + 0.2);
    });
  } catch (_) {}
}

function pushBrowserCompletionNotification(title, message, tag = '') {
  try {
    if (!('Notification' in window) || Notification.permission !== 'granted') return;
    const note = new Notification(title, {
      body: message,
      tag: tag || 'tool-evidence-job-finished',
      renotify: true,
    });
    setTimeout(() => note.close(), 12000);
  } catch (_) {}
}

function showCompletionAlert(job, done, total) {
  const host = document.getElementById('completionAlertHost');
  if (!host) return;
  const title = t('jobFinishedBannerTitle');
  const message = t('jobFinishedToastFmt')(getJobSheetLabel(job), done, total);
  while (host.children.length >= 2) {
    host.lastElementChild?.remove();
  }
  const alertNode = document.createElement('div');
  alertNode.className = 'completion-alert';
  alertNode.innerHTML = `
    <div class="completion-alert-icon">✓</div>
    <div class="completion-alert-copy">
      <div class="completion-alert-kicker">${esc(t('jobFinishedTitle'))}</div>
      <div class="completion-alert-title">${esc(getJobSheetLabel(job))}</div>
      <div class="completion-alert-message">${esc(message)}</div>
      <div class="completion-alert-meta">
        <span class="completion-alert-chip">${esc(prettyWord(getJobMode(job)))}</span>
        <span class="completion-alert-chip">${esc(String(job?.id || '').slice(0, 8))}</span>
        <span class="completion-alert-chip">${esc(`${done}/${total}`)}</span>
      </div>
    </div>
    <button type="button" class="completion-alert-close" title="${esc(t('jobFinishedBannerDismiss'))}" aria-label="${esc(t('jobFinishedBannerDismiss'))}">×</button>
  `;
  const closeAlert = () => {
    alertNode.remove();
    if (!host.children.length) stopCompletionTitleFlash();
  };
  alertNode.querySelector('.completion-alert-close')?.addEventListener('click', closeAlert);
  host.prepend(alertNode);
  setTimeout(closeAlert, 12000);
  playCompletionAlertTone();
  startCompletionTitleFlash(title);
  pushBrowserCompletionNotification(title, message, `job-finished-${String(job?.id || '')}`);
}

window.addEventListener('focus', () => {
  stopCompletionTitleFlash();
});
document.addEventListener('visibilitychange', () => {
  if (!document.hidden) {
    stopCompletionTitleFlash();
    refreshJobs();
    pollCurrent();
  }
});

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
    const isReallyCompleted = status === 'completed' && total > 0 && done >= total;
    if (isReallyCompleted && previousStatus && previousStatus !== 'completed' && !notifiedCompletedJobKeys.has(completionKey)) {
      notifiedCompletedJobKeys.add(completionKey);
      showToast(t('jobFinishedToastFmt')(getJobSheetLabel(job), done, total), 'success', t('jobFinishedTitle'));
      showCompletionAlert(job, done, total);
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

function compactIssuePostLabel(postName) {
  const raw = String(postName || '').trim();
  if (!raw) return '';
  let match = raw.match(/^post[ ]+([0-9]+)$/i);
  if (match) return `P${match[1]}`;
  match = raw.match(/^scan[ ]+([0-9]+)$/i);
  if (match) return `S${match[1]}`;
  match = raw.match(/^booking[ ]+([0-9]+)$/i);
  if (match) return `B${match[1]}`;
  return raw.length > 12 ? `${raw.slice(0, 12)}…` : raw;
}

function formatIssueCellChip(item) {
  const rowPart = `#${item?.row || '?'}`;
  const colPart = item?.column && item.column !== '-' ? `:${String(item.column).trim().toUpperCase()}` : '';
  return `${rowPart}${colPart}`.trim();
}

function getIssueColumnsForRequestPost(requestMeta, postLabel) {
  const mappings = Array.isArray(requestMeta?.mappings) ? requestMeta.mappings : [];
  const normalizedPost = String(postLabel || '').trim().toLowerCase();
  const normalizeCol = value => String(value || '').trim().toUpperCase();
  const addUnique = (bucket, value) => {
    const col = normalizeCol(value);
    if (col && !bucket.includes(col)) bucket.push(col);
  };
  let match = mappings.find(item => String(item?.name || '').trim().toLowerCase() === normalizedPost);
  if (!match && mappings.length === 1) match = mappings[0];
  if (!match) {
    return [];
  }
  const mode = String(match?.mode || requestMeta?.mode || currentRunMode || '').trim().toLowerCase();
  const columns = [];
  if (mode === 'scan') {
    addUnique(columns, match?.col_drive);
  } else {
    addUnique(columns, match?.col_profile);
    addUnique(columns, match?.col_content);
    addUnique(columns, match?.col_drive);
    addUnique(columns, match?.col_screenshot);
  }
  return columns;
}

function buildIssueCellEntries(errorRows, logs, issueCells = [], requestMeta = null) {
  const entries = new Map();
  const detailedRowPosts = new Set();
  const upsert = (rowValue, postLabel, columnValue, message, kind = '') => {
    const row = Number(rowValue || 0);
    if (!Number.isFinite(row) || row <= 0) return;
    const post = String(postLabel || '').trim();
    const column = String(columnValue || '').trim().toUpperCase();
    const rowPostKey = `${post}|${row}`;
    if (!column && detailedRowPosts.has(rowPostKey)) return;
    const key = `${post}|${row}|${column}`;
    if (!entries.has(key)) {
      entries.set(key, { key, row, post, column, message: String(message || '').trim(), kind: String(kind || '').trim() });
      if (column) {
        detailedRowPosts.add(rowPostKey);
      }
      return;
    }
    const existing = entries.get(key);
    if (!existing.message && message) existing.message = String(message || '').trim();
    if (!existing.kind && kind) existing.kind = String(kind || '').trim();
  };
  const upsertWithInferredColumns = (rowValue, postLabel, message, kind = '') => {
    const inferredColumns = getIssueColumnsForRequestPost(requestMeta, postLabel);
    if (inferredColumns.length) {
      inferredColumns.forEach(col => upsert(rowValue, postLabel, col, message, kind));
      return;
    }
    upsert(rowValue, postLabel, '', message, kind);
  };

  (Array.isArray(issueCells) ? issueCells : []).forEach(item => {
    upsert(
      item?.row,
      item?.post,
      item?.column,
      item?.message || '',
      item?.kind || ''
    );
  });

  if (detailedRowPosts.size) {
    for (const [key, value] of Array.from(entries.entries())) {
      if (!String(value?.column || '').trim()) {
        const rowPostKey = `${String(value?.post || '').trim()}|${Number(value?.row || 0)}`;
        if (detailedRowPosts.has(rowPostKey)) {
          entries.delete(key);
        }
      }
    }
  }

  Object.entries(errorRows || {}).forEach(([rowKey, rawMessage]) => {
    const message = String(rawMessage || '').trim();
    const post = extractLogBlockName({ message }) || '';
    upsertWithInferredColumns(rowKey, post, message, 'stored');
  });

  (Array.isArray(logs) ? logs : []).forEach(item => {
    if (!isUnavailableLog(item) && !isFailedLog(item)) return;
    upsertWithInferredColumns(
      item?.row,
      getLogPostLabel(item),
      item?.message || item?.result || item?.state || '',
      isUnavailableLog(item) ? 'unavailable' : 'failed'
    );
  });

  return Array.from(entries.values()).sort((a, b) => {
    if (a.row !== b.row) return a.row - b.row;
    if (String(a.column || '') !== String(b.column || '')) {
      return String(a.column || '').localeCompare(String(b.column || ''));
    }
    return String(a.post || '').localeCompare(String(b.post || ''));
  });
}

function isUnavailableLog(log) {
  const raw = `${log?.tag || ''} ${log?.state || ''} ${log?.result || ''} ${log?.message || ''}`.toLowerCase();
  return raw.includes('unavailable') || raw.includes('không khả dụng') || raw.includes('khong kha dung');
}

function isFailedLog(log) {
  const raw = `${log?.tag || ''} ${log?.state || ''} ${log?.result || ''} ${log?.message || ''}`.toLowerCase();
  if (raw.includes('unavailable') || raw.includes('không khả dụng') || raw.includes('khong kha dung')) return false;
  if (raw.includes('tiktok_url_mismatch') || raw.includes('url mismatch')) return true;
  return raw.includes('fail') || raw.includes('error');
}

function isSuccessLog(log) {
  const raw = `${log?.tag || ''} ${log?.state || ''} ${log?.result || ''} ${log?.message || ''}`.toLowerCase();
  if (raw.includes('unavailable') || raw.includes('không khả dụng') || raw.includes('khong kha dung')) return false;
  if (raw.includes('fail') || raw.includes('error')) return false;
  return raw.includes('ok') || raw.includes('success') || raw.includes('done');
}

function normalizeIssueSummaryLabel(rawMessage) {
  let text = String(rawMessage || '').trim();
  if (!text) return '';
  text = text.replace(/^[^:]{1,80}: */, '').trim();
  text = text.replace(/^row *#?[0-9]+ *[-:] */i, '').trim();
  if (!text) return '';
  const lowered = text.toLowerCase();
  if (lowered.includes('tiktok_url_mismatch') || lowered.includes('url mismatch')) {
    return 'TikTok mở sai bài so với link dòng';
  }
  if (lowered.includes('không khả dụng') || lowered.includes('khong kha dung') || lowered.includes('unavailable')) {
    return t('unavailableLabel');
  }
  return text.length > 88 ? `${text.slice(0, 85).trim()}...` : text;
}

function buildIssueSummaryText(errorRows, logs, fallbackError) {
  const issueCounts = new Map();
  const addIssue = message => {
    const normalized = normalizeIssueSummaryLabel(message);
    if (!normalized) return;
    issueCounts.set(normalized, (issueCounts.get(normalized) || 0) + 1);
  };

  Object.values(errorRows || {}).forEach(addIssue);
  (Array.isArray(logs) ? logs : []).forEach(item => {
    if (!isUnavailableLog(item) && !isFailedLog(item)) return;
    addIssue(item?.message || item?.result || item?.state || '');
  });
  addIssue(fallbackError);

  if (!issueCounts.size) return t('monitorIssueSummaryNone');
  const ranked = Array.from(issueCounts.entries()).sort((a, b) => {
    if (b[1] !== a[1]) return b[1] - a[1];
    return a[0].localeCompare(b[0]);
  });
  const [topLabel, topCount] = ranked[0];
  const moreKinds = Math.max(ranked.length - 1, 0);
  return moreKinds > 0
    ? t('monitorIssueSummaryTopMoreFmt')(topLabel, topCount, moreKinds)
    : t('monitorIssueSummaryTopFmt')(topLabel, topCount);
}

function canReplayLog(log) {
  const row = Number(log?.row || 0);
  if (!Number.isFinite(row) || row < 1) return false;
  const raw = `${log?.tag || ''} ${log?.state || ''} ${log?.result || ''}`.toLowerCase();
  return raw.includes('ok') || raw.includes('fail') || raw.includes('unavailable');
}

function statusBadge(status) {
  const key = String(status || '').toLowerCase();
  const normalized = key || 'queued';
  return `<span class="project-status-badge status-${esc(normalized)}">${esc(prettyWord(normalized))}</span>`;
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
  if (name === 'access' && !isAdminUser()) {
    setStatus(t('adminOnly'), 'failed');
    name = 'runs';
    tabEl = document.querySelector('.side-btn[data-view="runs"]');
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
  if (name === 'projects') renderProjects();
  if (name === 'activities') renderActivities(getCombinedActivities());
  if (name === 'runs' || name === 'overview') renderOverview();
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
  const modeFiltered = getProjectJobsForModeFilter();
  const saved = getFilteredProjectJobs();
  const selected = getSelectedProjectJob();
  if (selected) ensureProjectLineageLogs(selected);
  const selectedLineageJobs = selected ? getJobLineageJobs(selected) : [];
  const projectLogsLoading = selectedLineageJobs.some(job => !!projectLogsInflightByJobId[String(job?.id || '').trim()]);
  const uniqueSheets = new Set(saved.map(job => getJobSheetLabel(job))).size;
  const summary = getJobSummary(selected);
  const request = selected?.request || {};
  const projectLogs = selected
    ? getLineageDisplayLogs(
        selected,
        Array.isArray(selected?.logs)
          ? selected.logs
          : (Array.isArray(selected?.recent_logs) ? selected.recent_logs : [])
      ).slice().reverse()
    : [];
  const projectLogsHtml = selected
    ? `
      <div class="project-log-panel">
        <div class="project-log-head">
          <div class="project-log-title">${esc(t('projectLogs'))}</div>
          <div class="project-log-sub">${esc(t('projectLogsSub'))}</div>
        </div>
        <div class="project-log-list">
          ${projectLogs.length
            ? projectLogs.slice(0, 120).map(item => {
                const postName = getLogPostLabel(item);
                const lineageMeta = item.__job_id ? String(item.__job_id).slice(0, 8) : String(selected?.id || '').slice(0, 8);
                return `
                  <div class="project-log-item">
                    <div class="project-log-top">
                      <div class="project-log-meta">
                        <span>${esc(toLocalStamp(item.ts))}</span>
                        <span>${esc(postName)}</span>
                        <span>#${esc(item.row)}</span>
                        <span>${esc(lineageMeta)}</span>
                      </div>
                      ${resultPill(item.result, item.state, item.tag, item.message)}
                    </div>
                    <div class="project-log-message">${esc(item.message || `${item.state}/${item.result}`)}</div>
                  </div>`;
              }).join('')
            : `<div class="project-log-empty">${esc(projectLogsLoading ? t('projectLogsLoading') : t('projectNoLogs'))}</div>`}
        </div>
      </div>`
    : '';
  const filterOptions = [
    { key: 'all', label: t('allProjects'), count: allSaved.length },
    { key: 'seeding', label: getRunModeLabel('seeding'), count: allSaved.filter(job => getJobMode(job) === 'seeding').length },
    { key: 'booking', label: getRunModeLabel('booking'), count: allSaved.filter(job => getJobMode(job) === 'booking').length },
    { key: 'scan', label: getRunModeLabel('scan'), count: allSaved.filter(job => getJobMode(job) === 'scan').length },
  ];
  const statusFilterOptions = [
    { key: 'all', label: t('projectStatusAll'), count: modeFiltered.length },
    { key: 'running', label: t('projectStatusRunning'), count: modeFiltered.filter(job => matchesProjectStatusFilter(job, 'running')).length },
    { key: 'completed', label: t('projectStatusCompleted'), count: modeFiltered.filter(job => matchesProjectStatusFilter(job, 'completed')).length },
    { key: 'stopped', label: t('projectStatusStopped'), count: modeFiltered.filter(job => matchesProjectStatusFilter(job, 'stopped')).length },
    { key: 'failed', label: t('projectStatusFailed'), count: modeFiltered.filter(job => matchesProjectStatusFilter(job, 'failed')).length },
  ];
  const totalNode = document.getElementById('projectsTotalJobs');
  const sheetsNode = document.getElementById('projectsCompletedJobs');
  const selectedNode = document.getElementById('projectsSelectedJob');
  if (totalNode) totalNode.textContent = saved.length;
  if (sheetsNode) sheetsNode.textContent = uniqueSheets;
  if (selectedNode) selectedNode.textContent = selected ? `${summary.done || 0}/${summary.total || 0}` : '-';
  const focusedFilterId = document.activeElement?.id || '';
  if (focusedFilterId !== 'projectsModeFilterInput') {
    document.getElementById('projectsModeFilters').innerHTML = `
      <label class="project-filter-select">
        <span>${esc(t('projectModeLabel'))}</span>
        <select id="projectsModeFilterInput" class="project-filter-input" aria-label="${esc(t('projectModeLabel'))}" onchange="setProjectModeFilter(this.value)">
          ${filterOptions.map(opt => `<option value="${esc(opt.key)}"${currentProjectModeFilter === opt.key ? ' selected' : ''}>${esc(opt.label)} (${opt.count})</option>`).join('')}
        </select>
      </label>`;
  }
  if (focusedFilterId !== 'projectsStatusFilterInput') {
    document.getElementById('projectsStatusFilters').innerHTML = `
      <label class="project-filter-select">
        <span>${esc(t('projectStatusLabel'))}</span>
        <select id="projectsStatusFilterInput" class="project-filter-input" aria-label="${esc(t('projectStatusLabel'))}" onchange="setProjectStatusFilter(this.value)">
          ${statusFilterOptions.map(opt => `<option value="${esc(opt.key)}"${currentProjectStatusFilter === opt.key ? ' selected' : ''}>${esc(opt.label)} (${opt.count})</option>`).join('')}
        </select>
      </label>`;
  }
  document.getElementById('projectsSnapshotAction').innerHTML = selected
    ? `<div class="project-detail-actions"><button type="button" class="project-nav-btn" title="${esc(t('openProjectRun'))}" aria-label="${esc(t('openProjectRun'))}" onclick="openProjectInRuns('${selected.id}')"><svg viewBox="0 0 24 24" aria-hidden="true"><path d="M5 12h12"></path><path d="m13 6 6 6-6 6"></path></svg></button></div>`
    : '';
  document.getElementById('projectsList').innerHTML = saved.length
    ? saved.map(job => {
        const jobSummary = getJobSummary(job);
        const active = currentProjectJobId === job.id ? ' active' : '';
        const mode = getJobMode(job);
        const ownerLabel = getJobOwnerBadge(job);
        return `<div class="list-row project-item${active}" onclick="selectProject('${job.id}')">
          <div class="project-item-main">
            <div class="project-item-title">${esc(getJobSheetLabel(job))}</div>
            <div class="project-item-meta"><span class="mode-pill mode-${mode}">${esc(prettyWord(mode))}</span><span>${esc(toLocalStamp(job.finished_at || job.created_at))}</span><span>${esc(job.id.slice(0, 8))}</span>${ownerLabel ? `<span>${esc(ownerLabel)}</span>` : ''}</div>
          </div>
          <div class="project-item-side">
            ${statusBadge(job.status)}
            <span class="project-item-progress">${jobSummary.done || 0}/${jobSummary.total || 0}</span>
            ${isAdminUser() ? `<button type="button" class="project-delete-btn" title="${esc(t('deleteLabel'))}" onclick="deleteProject('${job.id}', event)">
              <svg viewBox="0 0 24 24" aria-hidden="true"><path d="M4 7h16"></path><path d="M10 11v6"></path><path d="M14 11v6"></path><path d="M6 7l1 12h10l1-12"></path><path d="M9 7V4h6v3"></path></svg>
            </button>` : ''}
          </div>
        </div>`;
      }).join('')
    : `<div class="list-row"><span>${allSaved.length ? t('noProjectsInFilter') : t('noGroupsYet')}</span><span>-</span></div>`;
  document.getElementById('projectsSnapshot').innerHTML = selected
    ? [
        `<div class="snapshot-pair">
          <div class="timeline-item"><strong>${t('group')}</strong><div>${esc(getJobSheetLabel(selected))}</div></div>
          <div class="timeline-item"><strong>${t('state')}</strong><div style="display:flex;align-items:center;gap:8px;flex-wrap:wrap">${statusBadge(selected.status)}<span class="mode-pill mode-${getJobMode(selected)}">${esc(prettyWord(getJobMode(selected)))}</span></div></div>
        </div>`,
        ...(getJobOwnerBadge(selected) ? [`<div class="timeline-item"><strong>${t('projectOwner')}</strong><div>${esc(getJobOwnerBadge(selected))}</div></div>`] : []),
        `<div class="snapshot-pair">
          <div class="timeline-item"><strong>${t('latestUpdate')}</strong><div>${esc(toLocalStamp(selected.finished_at || selected.created_at))}</div></div>
          <div class="timeline-item"><strong>${t('jobs')}</strong><div>${summary.done || 0}/${summary.total || 0} · ${summary.success || 0} ${t('success').toLowerCase()} · ${summary.failed || 0} ${t('failedLabel').toLowerCase()}</div></div>
        </div>`,
        `<div class="timeline-item"><strong>${t('driveFolder')}</strong><div>${esc(request.drive_id || '-')}</div></div>`,
        `<div class="timeline-item"><strong>${t('detailLabel')}</strong><div>${esc(selected.detail || '-')}</div></div>`,
        projectLogsHtml,
      ].join('')
    : `<div class="timeline-item"><strong>${t('noProjectGroup')}</strong><div>${t('startOrSelect')}</div></div>`;
}

function renderActivities(logs) {
  const items = (logs || []).slice();
  document.getElementById('activitiesTimeline').innerHTML = items.length
    ? items.map(x => {
        const level = classifyLog(x);
        const levelLabel = level === 'info' ? t('activityLevel') : prettyWord(level);
        const jobMeta = x.__job_id ? `${String(x.__job_mode || '').trim() ? `${prettyWord(x.__job_mode)} · ` : ''}${String(x.__job_id || '').slice(0, 8)}` : '';
        const ownerMeta = isAdminUser() && x?.owner_email ? String(x.owner_email).trim().toLowerCase() : '';
        const rowLabel = x.__source === 'activity' ? esc(String(x.state || 'ACTION')) : `#${esc(x.row)} ${esc(x.state)}/${esc(x.result)}`;
        return `<div class="timeline-item"><div style="display:flex;justify-content:space-between;gap:8px;align-items:center"><strong>${rowLabel}</strong><span class="badge ${level}">${esc(levelLabel)}</span></div><div>${esc(x.message)}</div><div class="s">${ownerMeta ? `${esc(ownerMeta)} · ` : ''}${jobMeta ? `${esc(jobMeta)} · ` : ''}${toLocalStamp(x.ts)}</div></div>`;
      }).join('')
    : `<div class="timeline-item"><strong>${t('noActivity')}</strong><div>${t('startOrSelect')}</div></div>`;
}

function toggleIssueStrip(kind) {
  const key = String(kind || '').toLowerCase();
  if (!['failed', 'unavailable'].includes(key)) return;
  monitorIssueExpandState[key] = !monitorIssueExpandState[key];
  renderRunMonitor(currentJobSnapshot, currentLogsCache);
}

function renderRunMonitor(snapshot, logs) {
  const st = snapshot || {};
  const renderJobId = String(st.id || currentJobId || '').trim();
  if (renderJobId !== monitorIssueExpandJobId) {
    monitorIssueExpandJobId = renderJobId;
    monitorIssueExpandState = { failed: false, unavailable: false };
  }
  const s = getJobSummary(st);
  let displayStatus = String(st.status || 'idle').toLowerCase();
  if (displayStatus === 'completed' && Number(s.total || 0) <= 0 && !(Array.isArray(logs) && logs.length)) {
    displayStatus = 'stopped';
  }
  const pct = s.total ? Math.round((s.done / s.total) * 100) : 0;
  const errorRows = st.error_rows || {};
  const issueCells = Array.isArray(st.issue_cells) ? st.issue_cells : [];
  const logItems = Array.isArray(logs) ? logs : [];
  const displayLogs = getLineageDisplayLogs(st, logItems);
  const successCount = Number(s.success || 0);
  let failedCount = Number(s.failed || 0);
  let unavailableCount = Number(s.unavailable || 0);
  const issueEntries = buildIssueCellEntries(errorRows, logItems, issueCells, st.request || null);
  if (issueEntries.length) {
    const failedRowsFromIssues = new Set(
      issueEntries
        .filter(item => !isUnavailableLog({ message: item.message, result: item.kind, state: item.kind, tag: item.kind }))
        .map(item => Number(item?.row || 0))
        .filter(row => Number.isFinite(row) && row > 0)
    );
    const unavailableRowsFromIssues = new Set(
      issueEntries
        .filter(item => isUnavailableLog({ message: item.message, result: item.kind, state: item.kind, tag: item.kind }))
        .map(item => Number(item?.row || 0))
        .filter(row => Number.isFinite(row) && row > 0)
    );
    failedCount = Math.max(failedCount, failedRowsFromIssues.size);
    unavailableCount = Math.max(unavailableCount, unavailableRowsFromIssues.size);
  }
  const hasIssueState = (failedCount + unavailableCount) > 0 || issueEntries.length > 0 || String(st.status || '').toLowerCase() === 'failed' || !!String(st.error || '').trim();
  const statusLabel = prettyWord(displayStatus || 'idle');
  const latestLog = logItems.length ? logItems[logItems.length - 1] : null;
  const detailText = String(st.detail || latestLog?.message || '').trim();
  const etaText = s.eta && s.eta !== '---' ? `${t('eta')}: ${s.eta}` : '';
  const title = st.request ? getJobSheetLabel(st) : t('monitorNoJob');
  const metaParts = [];
  const ownerLabel = getJobOwnerBadge(st);
  const ownJob = isJobOwnedByCurrentUser(st);
  if (st.mode || st.request?.mode) metaParts.push(prettyWord(getJobMode(st)));
  if (ownerLabel) metaParts.push(ownerLabel);
  if (currentJobId) metaParts.push(currentJobId.slice(0, 8));
  if (st.created_at) metaParts.push(toLocalStamp(st.created_at));
  const statusNode = document.getElementById('runMonitorStatus');
  statusNode.textContent = statusLabel;
  statusNode.style.background = 'var(--blue-soft)';
  statusNode.style.color = 'var(--blue)';
  statusNode.style.borderColor = 'rgba(91,147,211,.25)';
  if (displayStatus === 'completed') {
    statusNode.style.background = 'rgba(52,195,143,.16)';
    statusNode.style.color = 'var(--green)';
    statusNode.style.borderColor = 'rgba(52,195,143,.35)';
  } else if (displayStatus === 'paused') {
    statusNode.style.background = 'rgba(245,158,11,.16)';
    statusNode.style.color = '#b45309';
    statusNode.style.borderColor = 'rgba(245,158,11,.35)';
  } else if (displayStatus === 'failed') {
    statusNode.style.background = 'rgba(240,138,160,.16)';
    statusNode.style.color = 'var(--red)';
    statusNode.style.borderColor = 'rgba(240,138,160,.35)';
  } else if (displayStatus === 'stopped') {
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
  renderRunMonitorBlockProgress(st, logItems);
  document.getElementById('runMonitorErrorMain').innerHTML = `
    <div class="monitor-error-stats">
      <span class="monitor-error-stat success">${esc(t('success'))} <strong>${esc(successCount)}</strong></span>
      <span class="monitor-error-stat failed">${esc(t('errorRows'))} <strong>${esc(failedCount)}</strong></span>
      <span class="monitor-error-stat unavailable">${esc(t('unavailableLabel'))} <strong>${esc(unavailableCount)}</strong></span>
    </div>
  `;
  const issueRowsStrip = document.getElementById('runMonitorIssueRowsStrip');
  const unavailableRowsStrip = document.getElementById('runMonitorUnavailableRowsStrip');
  const issueRowsNode = document.getElementById('runMonitorErrorRows');
  const unavailableRowsNode = document.getElementById('runMonitorUnavailableRows');
  const failedIssueItems = issueEntries.filter(item =>
    !isUnavailableLog({ message: item.message, result: item.kind, state: item.kind, tag: item.kind })
  );
  const unavailableIssueItems = issueEntries.filter(item =>
    isUnavailableLog({ message: item.message, result: item.kind, state: item.kind, tag: item.kind })
  );
  const failedExpanded = !!monitorIssueExpandState.failed;
  const unavailableExpanded = !!monitorIssueExpandState.unavailable;
  if (issueRowsNode) {
    if (failedIssueItems.length) {
      const visibleRows = failedExpanded ? failedIssueItems : failedIssueItems.slice(0, 8);
      const chips = visibleRows.map(item => {
        return `<span class="monitor-issue-chip">${esc(formatIssueCellChip(item))}</span>`;
      });
      if (failedIssueItems.length > 8) {
        chips.push(
          failedExpanded
            ? `<button class="monitor-issue-chip more action" type="button" onclick="toggleIssueStrip('failed')">${esc(t('monitorIssueCollapse'))}</button>`
            : `<button class="monitor-issue-chip more action" type="button" onclick="toggleIssueStrip('failed')">${esc(t('monitorIssueExpandFmt')(failedIssueItems.length - 8))}</button>`
        );
      }
      issueRowsNode.innerHTML = chips.join('');
    } else {
      issueRowsNode.innerHTML = '';
    }
  }
  if (unavailableRowsNode) {
    if (unavailableIssueItems.length) {
      const visibleRows = unavailableExpanded ? unavailableIssueItems : unavailableIssueItems.slice(0, 8);
      const chips = visibleRows.map(item => {
        return `<span class="monitor-issue-chip unavailable">${esc(formatIssueCellChip(item))}</span>`;
      });
      if (unavailableIssueItems.length > 8) {
        chips.push(
          unavailableExpanded
            ? `<button class="monitor-issue-chip more action" type="button" onclick="toggleIssueStrip('unavailable')">${esc(t('monitorIssueCollapse'))}</button>`
            : `<button class="monitor-issue-chip more action" type="button" onclick="toggleIssueStrip('unavailable')">${esc(t('monitorIssueExpandFmt')(unavailableIssueItems.length - 8))}</button>`
        );
      }
      unavailableRowsNode.innerHTML = chips.join('');
    } else {
      unavailableRowsNode.innerHTML = '';
    }
  }
  document.getElementById('runMonitorErrorMeta').textContent = '';
  if (issueRowsStrip) issueRowsStrip.hidden = failedIssueItems.length <= 0;
  if (unavailableRowsStrip) unavailableRowsStrip.hidden = unavailableIssueItems.length <= 0;

  const rows = displayLogs.slice().reverse();
  const replayLocked = ['running', 'paused'].includes(displayStatus) || !ownJob;
  document.getElementById('runMonitorRows').innerHTML = rows.length
    ? rows.map(x => {
        const postName = getLogPostLabel(x);
        const message = x.message || `${x.state}/${x.result}`;
        const replayBlockName = extractLogBlockName(x);
        const replayButton = canReplayLog(x)
          ? `<button class="monitor-replay-btn" type="button" ${replayLocked ? `disabled title="${!ownJob ? 'Chỉ replay được job của chính bạn' : 'Job đang chạy, chưa thể replay'}"` : `onclick="replayLogRow('${esc(st.id || currentJobId || '')}', ${Number(x.row || 0)}, '${esc(replayBlockName)}')"`}>
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
  const pauseLabel = document.getElementById('pauseJobLabel');
  const pauseIcon = document.getElementById('pauseJobIcon');
  const pauseButton = pauseLabel ? pauseLabel.closest('button') : null;
  const continueLabel = document.getElementById('continueJobLabel');
  const continueIcon = document.getElementById('continueJobIcon');
  const continueButton = continueLabel ? continueLabel.closest('button') : null;
  const errorOnlyLabel = document.getElementById('errorOnlyJobLabel');
  const errorOnlyIcon = document.getElementById('errorOnlyJobIcon');
  const errorOnlyButton = errorOnlyLabel ? errorOnlyLabel.closest('button') : null;
  if (!pauseLabel || !pauseIcon || !pauseButton || !continueLabel || !continueIcon || !continueButton || !errorOnlyLabel || !errorOnlyIcon || !errorOnlyButton) return;
  const status = String(snapshot?.status || '').toLowerCase();
  const ownJob = isJobOwnedByCurrentUser(snapshot);
  const canStop = ownJob && ['running', 'paused'].includes(status);
  const canContinue = ownJob && ['stopped', 'failed', 'completed'].includes(status) && !!String(snapshot?.id || currentJobId || '').trim();
  const issueCells = Array.isArray(snapshot?.issue_cells) ? snapshot.issue_cells : [];
  const issueRetryRows = new Set(
    issueCells
      .filter(item => {
        const kind = String(item?.kind || '').toLowerCase();
        return kind === 'failed' || kind === 'error';
      })
      .map(item => Number(item?.row || 0))
      .filter(row => Number.isFinite(row) && row > 0)
  );
  const retryErrorCount = issueRetryRows.size;
  const canRetryErrors = ownJob && ['stopped', 'failed', 'completed'].includes(status) && retryErrorCount > 0;
  pauseButton.classList.remove('resume', 'pause', 'red', 'soft', 'stop');
  if (canContinue) {
    pauseLabel.textContent = t('continueJob');
    pauseIcon.innerHTML = '<path d="M8 6.5v11l9-5.5-9-5.5Z"></path>';
    pauseButton.classList.add('resume');
    pauseButton.disabled = false;
    pauseButton.title = ownJob ? '' : 'Chỉ chạy tiếp được job của chính bạn';
    pauseButton.onclick = continueJob;
  } else {
    pauseLabel.textContent = t('stopJob');
    pauseIcon.innerHTML = '<rect x="7" y="7" width="10" height="10" rx="1.5"></rect>';
    pauseButton.classList.add('pause');
    pauseButton.disabled = !canStop;
    pauseButton.title = ownJob ? '' : 'Chỉ dừng được job của chính bạn';
    pauseButton.onclick = stopJob;
  }

  continueLabel.textContent = t('continueJob');
  continueIcon.innerHTML = '<path d="M8 6.5v11l9-5.5-9-5.5Z"></path>';
  continueButton.classList.remove('pause', 'resume', 'red', 'stop');
  continueButton.classList.add('soft');
  continueButton.disabled = !canContinue;
  continueButton.title = ownJob ? '' : 'Chỉ chạy tiếp được job của chính bạn';

  errorOnlyLabel.textContent = t('errorOnlyJob');
  errorOnlyIcon.innerHTML = '<path d="M12 8v5"></path><circle cx="12" cy="16.5" r=".9" fill="currentColor" stroke="none"></circle><path d="M10.2 4.8 3.9 16a1.4 1.4 0 0 0 1.22 2.1h13.76A1.4 1.4 0 0 0 20.1 16L13.8 4.8a1.4 1.4 0 0 0-2.6 0Z"></path>';
  errorOnlyButton.classList.remove('pause', 'resume', 'red', 'stop');
  errorOnlyButton.classList.add('soft');
  errorOnlyButton.disabled = !canRetryErrors;
  errorOnlyButton.title = !ownJob ? 'Chỉ chạy lại lỗi được với job của chính bạn' : (retryErrorCount > 0 ? '' : 'Job này chưa có dòng lỗi');
}

async function replayLogRow(jobId, row, blockName = '') {
  try {
    primeCompletionNotifications();
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
    const sourceJob = (jobsCache || []).find(job => job.id === jobId);
    if (await focusBlockingModeJob(e, getJobMode(sourceJob || {}))) return;
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
  const exportQuery = new URLSearchParams({ ts: String(Date.now()) });
  if (shouldUseLocalAgent(`/api/jobs/${encodeURIComponent(jobId)}/export-log`) && authState.email) {
    exportQuery.set('user_email', authState.email);
  }
  link.href = runtimeHref(`/api/jobs/${encodeURIComponent(jobId)}/export-log?${exportQuery.toString()}`);
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
    body.innerHTML = `<tr><td colspan="5"><div class="access-empty">${esc(t('accessDirectoryNoMatch'))}</div></td></tr>`;
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
  const authEmailNode = document.querySelector('.auth-email');
  if (authEmailNode) {
    const emailText = String(authState.email || '').trim() || '-';
    authEmailNode.textContent = emailText;
    authEmailNode.title = emailText === '-' ? '' : emailText;
  }
  renderOverviewGreeting();
  const accessButton = document.getElementById('access_nav_button');
  if (accessButton) accessButton.style.display = isAdminUser() ? 'flex' : 'none';
  const settingsButton = document.getElementById('settings_nav_button');
  if (settingsButton) settingsButton.style.display = 'flex';
  const accessView = document.getElementById('view-access');
  if (accessView) accessView.style.display = isAdminUser() ? '' : 'none';
  const settingsView = document.getElementById('view-settings');
  if (settingsView) settingsView.style.display = '';
  const stateNode = document.querySelector('#view-settings .state');
  if (stateNode) stateNode.textContent = t('settingsState');
  const accessStateNode = document.querySelector('#view-access .state');
  if (accessStateNode) accessStateNode.textContent = isAdminUser() ? t('accessState') : t('adminOnly');
  if (!isAdminUser() && document.getElementById('view-access')?.classList.contains('active')) {
    switchView('runs');
  }
}

function setSheetUrlHint(text, isError = false) {
  const node = document.getElementById('sheet_url_hint');
  if (!node) return;
  node.textContent = text || '';
  node.style.color = isError ? '#be123c' : '#98a2b3';
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

function getCachedSheetNameTitles(rawUrl, allowStale = false) {
  const entry = sheetNameSuggestCache[String(rawUrl || '').trim()];
  if (!entry || !Array.isArray(entry.titles)) return null;
  if (allowStale) return entry.titles;
  if ((Date.now() - Number(entry.ts || 0)) > SHEET_NAME_CACHE_TTL_MS) return null;
  return entry.titles;
}

function isKnownSheetName(rawUrl, rawName) {
  const titles = getCachedSheetNameTitles(rawUrl, true) || [];
  const target = String(rawName || '').trim().toLowerCase();
  if (!target) return false;
  return titles.some(title => String(title || '').trim().toLowerCase() === target);
}

function rememberResolvedSheetName(rawUrl, sheetTitle) {
  const normalizedUrl = String(rawUrl || '').trim();
  const normalizedTitle = String(sheetTitle || '').trim();
  if (!normalizedUrl || !normalizedTitle) return;
  const entry = sheetNameSuggestCache[normalizedUrl];
  const existing = Array.isArray(entry?.titles)
    ? entry.titles.map(value => String(value || '').trim()).filter(Boolean)
    : [];
  if (!existing.some(value => value.toLowerCase() === normalizedTitle.toLowerCase())) {
    existing.push(normalizedTitle);
  }
  sheetNameSuggestCache[normalizedUrl] = { titles: existing, ts: Date.now() };
}

async function fetchSheetNameSuggestions(force = false) {
  const rawUrl = String(document.getElementById('sheet_url')?.value || '').trim();
  if (!rawUrl) {
    sheetNameSuggestKey = '';
    renderSheetNameSuggestions([]);
    setSheetUrlHint('');
    setSheetNameHint('');
    return;
  }
  const cached = getCachedSheetNameTitles(rawUrl, false);
  if (cached && (!force || sheetNameSuggestKey === rawUrl)) {
    renderSheetNameSuggestions(cached);
    setSheetUrlHint(cached.length ? t('sheetUrlHintCountFmt')(cached.length) : t('sheetUrlHintEmpty'));
    return;
  }
  if (sheetNameSuggestInflight[rawUrl]) {
    const pendingTitles = await sheetNameSuggestInflight[rawUrl];
    sheetNameSuggestKey = rawUrl;
    renderSheetNameSuggestions(pendingTitles);
    setSheetUrlHint(pendingTitles.length ? t('sheetUrlHintCountFmt')(pendingTitles.length) : t('sheetUrlHintEmpty'));
    return;
  }
  setSheetUrlHint(t('sheetUrlHintLoading'));
  try {
    sheetNameSuggestInflight[rawUrl] = (async () => {
      const qs = new URLSearchParams({ sheet_url: rawUrl });
      if (currentSettingsCache.credentials_path) qs.set('credentials_path', currentSettingsCache.credentials_path);
      const out = await req('/api/sheets/names?' + qs.toString());
      return Array.isArray(out.titles) ? out.titles : [];
    })();
    const titles = await sheetNameSuggestInflight[rawUrl];
    sheetNameSuggestKey = rawUrl;
    sheetNameSuggestCache[rawUrl] = { titles, ts: Date.now() };
    renderSheetNameSuggestions(titles);
    if (!String(document.getElementById('sheet_name')?.value || '').trim() && titles.length === 1) {
      document.getElementById('sheet_name').value = titles[0];
    }
    setSheetUrlHint(titles.length ? t('sheetUrlHintCountFmt')(titles.length) : t('sheetUrlHintEmpty'));
  } catch (e) {
    const staleTitles = getCachedSheetNameTitles(rawUrl, true);
    if (staleTitles) {
      renderSheetNameSuggestions(staleTitles);
      setSheetUrlHint(staleTitles.length ? t('sheetUrlHintCountFmt')(staleTitles.length) : t('sheetUrlHintEmpty'));
    } else {
      renderSheetNameSuggestions([]);
      setSheetUrlHint(e.message, true);
    }
  } finally {
    delete sheetNameSuggestInflight[rawUrl];
  }
}

function scheduleSheetNameSuggestions(force = false) {
  if (sheetNameSuggestTimer) clearTimeout(sheetNameSuggestTimer);
  sheetNameSuggestTimer = setTimeout(() => {
    fetchSheetNameSuggestions(force);
  }, force ? 0 : SHEET_NAME_SUGGEST_DEBOUNCE_MS);
}

function getMonitorRequestBlocks(snapshot) {
  const request = snapshot?.request || {};
  const multi = Array.isArray(request?.multi_seeding_blocks) ? request.multi_seeding_blocks : [];
  if (multi.length) {
    return multi.map((block, index) => ({
      name: String(block?.name || '').trim() || `Post ${index + 1}`,
    }));
  }
  const mapping = Array.isArray(request?.mapping) ? request.mapping : [];
  if (mapping.length) {
    return mapping.map((block, index) => ({
      name: String(block?.name || '').trim() || `Post ${index + 1}`,
    }));
  }
  return [];
}

function isDoneLikeLog(log) {
  const state = String(log?.state || '').trim().toUpperCase();
  const result = String(log?.result || '').trim().toUpperCase();
  const tag = String(log?.tag || '').trim().toUpperCase();
  if (['OK', 'ERROR', 'UNAVAILABLE'].includes(result)) return true;
  if (['OK', 'ERROR', 'UNAVAILABLE'].includes(state)) return true;
  if (['OK', 'ERROR', 'UNAVAILABLE'].includes(tag)) return true;
  return false;
}

function renderRunMonitorBlockProgress(snapshot, logs) {
  const host = document.getElementById('runMonitorBlockProgress');
  if (!host) return;
  const blocks = getMonitorRequestBlocks(snapshot);
  if (!blocks.length) {
    host.hidden = true;
    host.innerHTML = '';
    return;
  }
  const rowsByBlock = new Map();
  blocks.forEach(block => {
    const key = String(block.name || '').trim().toLowerCase();
    rowsByBlock.set(key, {
      name: block.name,
      doneRows: new Set(),
      okRows: new Set(),
      failedRows: new Set(),
      unavailableRows: new Set(),
    });
  });
  (Array.isArray(logs) ? logs : []).forEach(item => {
    const postName = getLogPostLabel(item);
    const key = String(postName || '').trim().toLowerCase();
    const bucket = rowsByBlock.get(key);
    if (!bucket) return;
    const row = Number(item?.row || 0);
    const result = String(item?.result || item?.state || item?.tag || '').trim().toUpperCase();
    if (isDoneLikeLog(item) && row > 0) bucket.doneRows.add(row);
    if (result === 'OK' && row > 0) bucket.okRows.add(row);
    if (result === 'ERROR' && row > 0) bucket.failedRows.add(row);
    if (result === 'UNAVAILABLE' && row > 0) bucket.unavailableRows.add(row);
  });
  const rows = Array.from(rowsByBlock.values());
  host.innerHTML = rows.map(item => {
    const done = item.doneRows.size;
    const ok = item.okRows.size;
    const failed = item.failedRows.size;
    const unavailable = item.unavailableRows.size;
    return `<div class="monitor-block-progress-row">
      <div class="monitor-block-progress-name">${esc(item.name)}</div>
      <div class="monitor-block-progress-meta">${esc(`${done} done · OK ${ok} · Lỗi ${failed} · KKG ${unavailable}`)}</div>
    </div>`;
  }).join('');
  host.hidden = false;
}

function bindSheetNameAutocomplete() {
  const urlInput = document.getElementById('sheet_url');
  const nameInput = document.getElementById('sheet_name');
  if (!urlInput || urlInput.dataset.sheetSuggestBound === '1') return;
  urlInput.dataset.sheetSuggestBound = '1';
  ['input', 'change', 'paste'].forEach(evt => {
    urlInput.addEventListener(evt, () => {
      scheduleSheetNameSuggestions(false);
      resetSheetLinkSuggestions();
      setSheetNameHint('');
    });
  });
  urlInput.addEventListener('blur', () => {
    scheduleSheetNameSuggestions(true);
  });
  if (nameInput) {
    ['input', 'change', 'paste'].forEach(evt => {
      nameInput.addEventListener(evt, () => {
        resetSheetLinkSuggestions();
        setSheetNameHint('');
        scheduleSheetLinkCountSummary(false);
      });
    });
    nameInput.addEventListener('blur', () => {
      scheduleSheetLinkCountSummary(false);
    });
    nameInput.addEventListener('focus', () => {
      const rawUrl = String(urlInput.value || '').trim();
      if (rawUrl && !getCachedSheetNameTitles(rawUrl, false)) scheduleSheetNameSuggestions(false);
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
  document.getElementById('settings_summary_timeout').textContent = `${s.page_timeout_ms || '-'} ms | ${t('tiktokCaptchaWait')}: ${s.tiktok_captcha_wait_sec || '-'} s | ${t('pleaseWaitDelay')}: ${s.please_wait_delay_sec || '-'} s`;
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

function getScanNegativeTermsValue() {
  const runNode = document.getElementById('scan_negative_terms_editor');
  if (runNode) return String(runNode.value || '');
  const settingsNode = document.getElementById('settings_scan_negative_terms');
  return settingsNode ? String(settingsNode.value || '') : '';
}

function getScanKeywordTermsValue() {
  const runNode = document.getElementById('scan_keyword_terms_editor');
  if (runNode) return String(runNode.value || '');
  const settingsNode = document.getElementById('settings_scan_keyword_terms');
  return settingsNode ? String(settingsNode.value || '') : '';
}

function setScanFilterNote(message = '', isError = false) {
  const note = document.getElementById('scan_filter_note');
  if (!note) return;
  note.textContent = String(message || '');
  note.style.color = isError ? '#fca5a5' : '';
}

function buildSettingsSavePayload() {
  const payload = {
    credentials_path: currentSettingsCache.credentials_path || '',
    service_account_json: document.getElementById('settings_service_account_json')?.value || '',
    sheet_url: sheet_url.value,
    sheet_name: sheet_name.value,
    drive_id: drive_id.value,
    scan_negative_terms: getScanNegativeTermsValue(),
    scan_keyword_terms: getScanKeywordTermsValue(),
    viewport_width: Number(document.getElementById('settings_viewport_width')?.value || 1920),
    viewport_height: Number(document.getElementById('settings_viewport_height')?.value || 1400),
    page_timeout_ms: Number(document.getElementById('settings_page_timeout_ms')?.value || 200),
    // Code-managed runtime fields: persist from current settings/runtime only.
    tiktok_captcha_wait_sec: Number(currentSettingsCache.tiktok_captcha_wait_sec || 15),
    please_wait_delay_sec: Number(currentSettingsCache.please_wait_delay_sec ?? 2),
    tiktok_force_focus: !!document.getElementById('settings_tiktok_force_focus')?.checked,
    ready_state: currentSettingsCache.ready_state || 'interactive',
    full_page_capture: !!document.getElementById('settings_full_page_capture')?.checked,
    mappings_by_mode: serializeMappingsByModeForSave(),
    run_flags_by_mode: currentRunFlagsByMode,
  };
  console.log('[DEBUG] buildSettingsSavePayload result:', payload);
  return payload;
}

async function persistScanFilterSettings(showNote = false) {
  rememberCurrentRunFlags('scan');
  const payload = buildSettingsSavePayload();
  const out = await req('/api/settings', { method: 'POST', body: JSON.stringify(payload) });
  const saved = out.settings || payload;
  currentSettingsCache = saved;
  currentMappingBlocksByMode = normalizeMappingsByModeForClient(saved.mappings_by_mode || serializeMappingsByModeForSave());
  currentRunFlagsByMode = normalizeRunFlagsByModeForClient(saved.run_flags_by_mode || currentRunFlagsByMode);
  if (document.getElementById('settings_scan_negative_terms')) {
    document.getElementById('settings_scan_negative_terms').value = saved.scan_negative_terms || '';
  }
  if (document.getElementById('settings_scan_keyword_terms')) {
    document.getElementById('settings_scan_keyword_terms').value = saved.scan_keyword_terms || '';
  }
  renderSettingsSummary(saved);
  renderScanFilterEditor();
  if (showNote) setScanFilterNote(t('saved'));
  return saved;
}

function scheduleScanFilterSettingsSave() {
  if (currentRunMode !== 'scan') return;
  window.clearTimeout(scanFilterSaveTimer);
  setScanFilterNote(t('saving'));
  scanFilterSaveTimer = window.setTimeout(async () => {
    try {
      await persistScanFilterSettings(true);
    } catch (error) {
      setScanFilterNote(error?.message || 'Save failed', true);
    }
  }, 420);
}

async function loadDefaults() {
  let d = {};
  let s = {};
  try {
    const [defaultsOut, settingsOut] = await Promise.all([req('/api/default-config'), req('/api/settings')]);
    d = defaultsOut || {};
    s = settingsOut || {};
  } catch (e) {
    setStatus('Load defaults warning: ' + String(e?.message || e || 'unknown error'), 'failed');
  }

  currentSettingsCache = s || {};
  currentMappingBlocksByMode = normalizeMappingsByModeForClient(currentSettingsCache.mappings_by_mode || {});
  currentRunFlagsByMode = normalizeRunFlagsByModeForClient(currentSettingsCache.run_flags_by_mode || {});
  if (sheet_url) sheet_url.value = s.sheet_url || d.sheet_url || '';
  if (sheet_name) sheet_name.value = s.sheet_name || d.sheet_name || '';
  if (drive_id) drive_id.value = s.drive_id || d.drive_id || '';
  applyRunFlagsForMode(currentRunMode);

  const viewportWidthNode = document.getElementById('settings_viewport_width');
  if (viewportWidthNode) viewportWidthNode.value = s.viewport_width || 1920;
  const viewportHeightNode = document.getElementById('settings_viewport_height');
  if (viewportHeightNode) viewportHeightNode.value = s.viewport_height || 1400;
  const pageTimeoutNode = document.getElementById('settings_page_timeout_ms');
  if (pageTimeoutNode) pageTimeoutNode.value = s.page_timeout_ms || 200;
  const tiktokWaitNode = document.getElementById('settings_tiktok_captcha_wait_sec');
  if (tiktokWaitNode) tiktokWaitNode.value = s.tiktok_captcha_wait_sec || 15;
  const pleaseWaitNode = document.getElementById('settings_please_wait_delay_sec');
  if (pleaseWaitNode) pleaseWaitNode.value = s.please_wait_delay_sec ?? 2;
  const focusNode = document.getElementById('settings_tiktok_force_focus');
  if (focusNode) focusNode.checked = !!s.tiktok_force_focus;
  const settingsNegativeTerms = document.getElementById('settings_scan_negative_terms');
  if (settingsNegativeTerms) settingsNegativeTerms.value = s.scan_negative_terms || '';
  const settingsKeywordTerms = document.getElementById('settings_scan_keyword_terms');
  if (settingsKeywordTerms) settingsKeywordTerms.value = s.scan_keyword_terms || '';
  const fullPageNode = document.getElementById('settings_full_page_capture');
  if (fullPageNode) fullPageNode.checked = !!s.full_page_capture;
  renderSettingsSummary(s);
  if (isAdminUser()) await Promise.all([loadAccessPolicy(), loadMailConfig()]);
  renderMappingEditor();
  if (String(sheet_url?.value || '').trim()) {
    scheduleSheetNameSuggestions(false);
    if (String(sheet_name?.value || '').trim()) scheduleSheetLinkCountSummary(false);
  } else {
    setSheetUrlHint('');
    setSheetNameHint('');
  }
  resetSheetLinkSuggestions();
}

async function saveSidebarSettings() {
  try {
    rememberCurrentRunFlags(currentRunMode);
    const payload = buildSettingsSavePayload();
    console.log('[DEBUG] saveSidebarSettings payload:', payload);
    const out = await req('/api/settings', { method: 'POST', body: JSON.stringify(payload) });
    console.log('[DEBUG] saveSidebarSettings response:', out);
    const saved = out.settings || payload;
    currentSettingsCache = saved;
    currentMappingBlocksByMode = normalizeMappingsByModeForClient(saved.mappings_by_mode || serializeMappingsByModeForSave());
    currentRunFlagsByMode = normalizeRunFlagsByModeForClient(saved.run_flags_by_mode || currentRunFlagsByMode);
    applyRunFlagsForMode(currentRunMode);
    resetServiceAccountFileInput();
    renderSettingsSummary(saved);
    if (String(sheet_url.value || '').trim()) {
      scheduleSheetNameSuggestions(false);
    }
    resetSheetLinkSuggestions();
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
    const browserPort = getModeBasePort(currentRunMode);
    const out = await req('/api/chrome/launch', {
      method: 'POST',
      body: JSON.stringify({ run_mode: currentRunMode, browser_port: browserPort })
    });
    await logActivityEvent({
      kind: 'chrome',
      level: 'info',
      run_mode: currentRunMode,
      browser_port: browserPort,
      message: `${prettyWord(currentRunMode)}: đã mở Chrome ${browserPort}`,
    });
    setStatus(out.message || 'Chrome launch requested', 'running');
  } catch (e) { alert(e.message); }
}

async function loginFacebookBeforeRun() {
  await launchChrome();
  alert(currentLang === 'vi'
    ? 'Chrome đã mở. Bạn đăng nhập Facebook xong rồi quay lại bấm Chạy job.'
    : 'Chrome is open. Please log in to Facebook, then come back and start the job.');
}

function buildMappingsForCurrentMode() {
  return ensureMappingBlocks(currentRunMode).map((block, index) => sanitizeMappingBlockForMode(currentRunMode, block, index + 1));
}

async function startJob() {
  if (startJobInFlight) return;
  startJobInFlight = true;
  try {
    primeCompletionNotifications();
    console.log('[DEBUG] currentMappingBlocksByMode:', currentMappingBlocksByMode);
    console.log('[DEBUG] currentRunMode:', currentRunMode);
    const mappings = buildMappingsForCurrentMode();
    console.log('[DEBUG] buildMappingsForCurrentMode:', mappings);
    const firstStartLine = mappings.length && Number.isFinite(Number(mappings[0].start_line)) ? Number(mappings[0].start_line) : 4;
    console.log('[DEBUG] firstStartLine:', firstStartLine, 'mappings[0]:', mappings[0]);
    const modeFlags = rememberCurrentRunFlags(currentRunMode);
    const forceRunAll = !!modeFlags.force_run_all;
    const highlightSheetErrors = !!modeFlags.highlight_sheet_errors;
    const scanNegativeFilter = currentRunMode === 'scan' && !!modeFlags.scan_negative_filter;
    const scanKeywordFilter = currentRunMode === 'scan' && !!modeFlags.scan_keyword_filter;
    const scanNegativeTerms = currentRunMode === 'scan' ? getScanNegativeTermsValue() : '';
    const scanKeywordTerms = currentRunMode === 'scan' ? getScanKeywordTermsValue() : '';
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
        highlight_sheet_errors: !!highlightSheetErrors,
        scan_negative_filter: !!scanNegativeFilter,
        scan_keyword_filter: !!scanKeywordFilter,
        scan_negative_terms: scanNegativeTerms,
        scan_keyword_terms: scanKeywordTerms,
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
  } catch (e) {
    if (await focusBlockingModeJob(e, currentRunMode)) return;
    alert(e.message);
  } finally {
    startJobInFlight = false;
  }
}

async function startErrorRowsJob() {
  if (!currentJobId) { alert(t('monitorNoJob')); return; }
  try {
    primeCompletionNotifications();
    const st = currentJobSnapshot || await req('/api/jobs/' + currentJobId);
    if (!isJobOwnedByCurrentUser(st)) {
      throw new Error('Chỉ chạy lại lỗi được với job của chính bạn');
    }
    const issueCells = Array.isArray(st?.issue_cells) ? st.issue_cells : [];
    const errorRowCount = new Set(
      issueCells
        .filter(item => {
          const kind = String(item?.kind || '').toLowerCase();
          return kind === 'failed' || kind === 'error';
        })
        .map(item => Number(item?.row || 0))
        .filter(row => Number.isFinite(row) && row > 0)
    ).size;
    if (!errorRowCount) {
      throw new Error('Job này chưa có dòng lỗi để chạy lại');
    }
    const out = await req('/api/jobs/' + currentJobId + '/retry-errors', { method: 'POST' });
    const runMode = getJobMode(st);
    currentJobId = out.job_id;
    setSelectedJobIdForMode(runMode, out.job_id);
    await refreshJobs();
    await pollCurrent();
    ensureTimers();
    setStatus(`${t('errorOnlyStarted')} · ${String(out.job_id || '').slice(0, 8)}`, 'running');
  } catch (e) {
    const jobMode = getJobMode(currentJobSnapshot || {});
    if (await focusBlockingModeJob(e, jobMode || currentRunMode)) return;
    alert(e.message);
  }
}

async function stopJob() {
  if (!currentJobId) { alert('Choose a job first'); return; }
  try {
    const st = currentJobSnapshot || await req('/api/jobs/' + currentJobId);
    const status = String(st?.status || '').toLowerCase();
    if (!['running', 'paused'].includes(status)) {
      throw new Error('Ch? c? th? d?ng job ?ang ch?y');
    }
    await req('/api/jobs/' + currentJobId + '/stop', { method: 'POST' });
    await pollCurrent();
    await refreshJobs();
  } catch (e) { alert(e.message); }
}

async function continueJob() {
  if (!currentJobId) { alert(t('monitorNoJob')); return; }
  try {
    primeCompletionNotifications();
    const st = currentJobSnapshot || await req('/api/jobs/' + currentJobId);
    const status = String(st?.status || '').toLowerCase();
    const jobMode = getJobMode(st);
    if (!['stopped', 'failed', 'completed'].includes(status)) {
      throw new Error('Chỉ có thể chạy tiếp từ job đã dừng, lỗi hoặc hoàn tất');
    }
    const out = await req('/api/jobs/' + currentJobId + '/continue', { method: 'POST' });
    currentJobId = out.job_id;
    setSelectedJobIdForMode(jobMode, out.job_id);
    await refreshJobs();
    await pollCurrent();
    ensureTimers();
    setStatus(`${t('continueStarted')} · ${String(out.job_id || '').slice(0, 8)}`, 'running');
  } catch (e) {
    const jobMode = getJobMode(currentJobSnapshot || {});
    if (await focusBlockingModeJob(e, jobMode || currentRunMode)) return;
    alert(e.message);
  }
}

async function pauseJob() {
  return stopJob();
}

async function refreshJobs() {
  if (jobsRefreshInFlight) return true;
  jobsRefreshInFlight = true;
  try {
    const previousJobId = currentJobId;
    const [out, activityOut] = await Promise.all([
      req('/api/jobs?jobs_limit=120&recent_log_limit=8&include_recent_logs=0&include_issue_details=0'),
      req('/api/activity?limit=' + JOBS_REFRESH_ACTIVITY_LIMIT),
    ]);
    const jobs = out.jobs || [];
    currentActivityEvents = activityOut.items || [];
    processJobLifecycleNotifications(jobs);
    jobsCache = jobs;
    syncModeSelections();
    if (currentJobId && !jobs.some(job => job.id === currentJobId)) currentJobId = null;
    if (!currentJobId && jobs.length) currentJobId = jobs[0].id;
    if (document.getElementById('view-runs')?.classList.contains('active')) {
      currentJobId = resolveModeJobId(currentRunMode);
      if (!currentJobId) {
        currentJobSnapshot = null;
        resetCurrentLogsState('');
        renderRunMonitor(null, []);
      }
    }
    if (currentJobId !== previousJobId) {
      resetCurrentLogsState(currentJobId || '');
    }
    document.getElementById('jobCountText').textContent = t('jobsLoadedFmt')(jobs.length);
    document.getElementById('jobCountText').dataset.jobs = jobs.length;
    const rows = jobs.map(j => {
      const s = j.summary || { done: 0, total: 0 };
      const active = currentJobId === j.id ? 'active' : '';
      const modeLabel = getJobMode(j).slice(0, 3).toUpperCase();
      const ownerLabel = getJobOwnerBadge(j);
      return `<tr class="${active}" onclick="selectJob('${j.id}')"><td>${statusBadge(j.status)}</td><td title="${esc(getJobMode(j))} · ${esc(j.id)}">${esc(modeLabel)} · ${esc(j.id.slice(0,8))}${ownerLabel ? `<div class="muted" style="font-size:11px;margin-top:2px">${esc(ownerLabel)}</div>` : ''}</td><td>${s.done}/${s.total}</td></tr>`;
    }).join('');
    document.getElementById('jobsBody').innerHTML = rows;
    if (isViewActive('runs') || isViewActive('overview')) renderOverview();
    if (isViewActive('projects')) renderProjects();
    if (isViewActive('activities')) renderActivities(getCombinedActivities());
    return true;
  } catch (e) {
    setStatus('Load jobs error: ' + e.message, 'failed');
    return false;
  } finally {
    jobsRefreshInFlight = false;
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
  if (currentJobId !== jobId) {
    resetCurrentLogsState(jobId || '');
  }
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
  if (pollInFlight) return;
  pollInFlight = true;
  try {
    const activeJobId = String(currentJobId || '').trim();
    const st = await req('/api/jobs/' + activeJobId);
    if (activeJobId !== String(currentJobId || '').trim()) return;
    currentJobSnapshot = st;
    const s = st.summary || { done: 0, total: 0, success: 0, failed: 0, eta: '---' };
    setKPI(s, activeJobId);
    setStatus('Status: ' + st.status + ' | Detail: ' + (st.detail || '-'), st.status);
    if (currentLogsJobId !== activeJobId) {
      resetCurrentLogsState(activeJobId);
    }
    const lg = await req('/api/jobs/' + activeJobId + '/logs?limit=200&since=' + currentLogsCursor);
    const logs = Array.isArray(lg?.logs) ? lg.logs : [];
    if (lg?.reset || currentLogsCursor <= 0) {
      currentLogsCache = logs.slice();
    } else if (logs.length) {
      currentLogsCache = currentLogsCache.concat(logs);
    }
    const nextCursor = Number(lg?.next_cursor ?? currentLogsCursor + logs.length);
    currentLogsCursor = Number.isFinite(nextCursor) && nextCursor >= 0 ? nextCursor : 0;
    if (currentLogsCache.length > MAX_MONITOR_LOG_CACHE) {
      currentLogsCache = currentLogsCache.slice(-MAX_MONITOR_LOG_CACHE);
    }
    const targetJob = (jobsCache || []).find(job => job.id === activeJobId);
    if (targetJob) targetJob.recent_logs = currentLogsCache.slice(-40);
    renderRunMonitor(st, currentLogsCache);
    updateRunActionButtons(st);
  } catch (e) {
    setStatus('Poll error: ' + e.message, 'failed');
  } finally {
    pollInFlight = false;
  }
}

function ensureTimers() {
  if (!pollTimer) {
    pollTimer = setInterval(() => {
      const status = String(currentJobSnapshot?.status || '').toLowerCase();
      const isActive = ['queued', 'running', 'paused'].includes(status);
      if (document.hidden && !isActive) return;
      pollCurrent();
    }, JOB_POLL_INTERVAL_MS);
  }
  if (!jobsTimer) {
    jobsTimer = setInterval(() => {
      const status = String(currentJobSnapshot?.status || '').toLowerCase();
      const isActive = ['queued', 'running', 'paused'].includes(status);
      if (document.hidden && !isActive) return;
      refreshJobs();
    }, JOBS_LIST_REFRESH_INTERVAL_MS);
  }
}

async function init() {
  try {
    await loadAuthState();
    await detectLocalAgent();
    syncAuthUI();
    switchView('runs', document.querySelector('.side-btn[data-view="runs"]'));
    bindSheetNameAutocomplete();
    await loadDefaults();
    await refreshJobs();
    if (currentJobId) {
      await pollCurrent();
    } else {
      renderRunMonitor(null, []);
    }
    renderOverview();
    renderActivities(getCombinedActivities());
    renderAccessPolicySummary(currentAccessPolicy);
    ensureTimers();
    applyTheme();
    applyLanguage();
    setStatus('ready', 'idle');
  } catch (e) {
    setStatus('Init error: ' + String(e?.message || e || 'unknown error'), 'failed');
    try { renderMappingEditor(); } catch (_) {}
    try { renderRunMonitor(null, []); } catch (_) {}
  }
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
        .replace("__LOCAL_BROWSER_HOSTS__", json.dumps(_local_browser_hostnames()))
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
    payload["credentials_path"] = _resolve_existing_credentials_path(
        str(saved_settings.get("credentials_path", payload["credentials_path"]))
    )
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
    cache_key = _sheet_names_cache_key(user_email, sheet_url, cred_path)
    cached_titles = _get_cached_sheet_titles(cache_key)
    if cached_titles is not None:
        return {
            "ok": True,
            "sheet_url": evidence.normalize_sheet_input(sheet_url),
            "titles": cached_titles,
            "cached": True,
        }
    spreadsheet = _open_spreadsheet(sheet_url, cred_path)
    titles = []
    for ws in spreadsheet.worksheets():
        title = str(getattr(ws, "title", "")).strip()
        if title:
            titles.append(title)
    _store_cached_sheet_titles(cache_key, titles)
    return {
        "ok": True,
        "sheet_url": evidence.normalize_sheet_input(sheet_url),
        "titles": titles,
        "cached": False,
    }


@app.get("/api/sheets/column-suggestions")
def list_sheet_link_columns(
    request: Request,
    sheet_url: str,
    sheet_name: str,
    credentials_path: str = "",
    start_row: int = 4,
    force: bool = False,
):
    try:
        user_email = _require_api_auth(request)
        saved = _read_saved_settings(user_email)
        cred_path = str(credentials_path or "").strip() or str(saved.get("credentials_path", "")).strip()
        name = str(sheet_name or "").strip()
        if not name:
            raise HTTPException(status_code=400, detail="Thiếu Sheet Name")
        cache_key = _sheet_link_columns_cache_key(user_email, sheet_url, name, cred_path, start_row)
        if force:
            _clear_sheet_link_columns_cache(user_email, sheet_url, name, cred_path)
            cached_payload = None
        else:
            cached_payload = _get_cached_sheet_link_columns(cache_key)
        if cached_payload is not None:
            return {
                "ok": True,
                "sheet_url": evidence.normalize_sheet_input(sheet_url),
                "sheet_name": name,
                "cached": True,
                **cached_payload,
            }
        spreadsheet = _open_spreadsheet(sheet_url, cred_path)
        worksheet = _resolve_worksheet(spreadsheet, sheet_url, name)
        payload = _extract_sheet_link_columns(worksheet, start_row=start_row)
        _store_cached_sheet_link_columns(cache_key, payload)
        return {
            "ok": True,
            "sheet_url": evidence.normalize_sheet_input(sheet_url),
            "sheet_name": str(getattr(worksheet, "title", name) or name),
            "cached": False,
            **payload,
        }
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=400, detail=f"Không quét được cột link: {exc}") from exc


@app.post("/api/sheets/quick-block-columns")
def create_quick_block_columns(request: Request, payload: QuickScanColumnsRequest):
    user_email = _require_api_auth(request)
    mode = _normalize_run_mode(payload.mode)
    if mode not in {"seeding", "booking", "scan"}:
        raise HTTPException(status_code=400, detail="Quét nhanh hiện chỉ hỗ trợ cho Seeding, Booking và Scan")
    normalized_columns = _normalize_selected_columns(payload.columns)
    if not normalized_columns:
        raise HTTPException(status_code=400, detail="Chưa chọn cột nào để quét nhanh")
    saved = _read_saved_settings(user_email)
    cred_path = str(saved.get("credentials_path", "")).strip()
    spreadsheet = _open_spreadsheet(payload.sheet_url, cred_path)
    worksheet = _resolve_worksheet(spreadsheet, payload.sheet_url, str(payload.sheet_name or "").strip())

    sheet_id = _worksheet_sheet_id(worksheet)
    if sheet_id < 0:
        raise HTTPException(status_code=400, detail="Không lấy được sheet id để tạo cột nhanh")

    selected_indexes = [evidence.col_letter_to_index(column) or 0 for column in normalized_columns]
    requests: list[dict[str, Any]] = []
    for column_index in sorted(selected_indexes, reverse=True):
        width = 1 if mode == "scan" else 2
        requests.append(
            {
                "insertDimension": {
                    "range": {
                        "sheetId": sheet_id,
                        "dimension": "COLUMNS",
                        "startIndex": int(column_index),
                        "endIndex": int(column_index + width),
                    },
                    "inheritFromBefore": True,
                }
            }
        )

    try:
        spreadsheet.batch_update({"requests": requests})
    except Exception as exc:
        raise HTTPException(status_code=400, detail=f"Không tạo được cột mới trên Sheet: {exc}") from exc

    items: list[dict[str, str]] = []
    left_insertions = 0
    for source_column, source_index in zip(normalized_columns, selected_indexes):
        if mode == "scan":
            final_link_index = source_index + left_insertions
            result_index = final_link_index + 1
            items.append(
                {
                    "source_column": source_column,
                    "link_column": evidence.col_index_to_letter(final_link_index),
                    "drive_column": evidence.col_index_to_letter(result_index),
                    "screenshot_column": "",
                }
            )
            left_insertions += 1
        else:
            final_link_index = source_index + (left_insertions * 2)
            drive_index = final_link_index + 1
            screenshot_index = final_link_index + 2
            items.append(
                {
                    "source_column": source_column,
                    "link_column": evidence.col_index_to_letter(final_link_index),
                    "drive_column": evidence.col_index_to_letter(drive_index),
                    "screenshot_column": evidence.col_index_to_letter(screenshot_index),
                }
            )
            left_insertions += 1

    _clear_sheet_link_columns_cache(
        user_email=user_email,
        sheet_url=payload.sheet_url,
        sheet_name=payload.sheet_name,
        credentials_path=cred_path,
    )

    return {
        "ok": True,
        "mode": mode,
        "sheet_url": evidence.normalize_sheet_input(payload.sheet_url),
        "sheet_name": str(getattr(worksheet, "title", str(payload.sheet_name or "").strip()) or str(payload.sheet_name or "").strip()),
        "items": items,
    }


@app.post("/api/settings")
def save_settings(request: Request, payload: SettingsUpdateRequest):
    user_email = _require_api_auth(request)
    evidence.write_log(f"[DEBUG] save_settings received payload.mappings_by_mode: {payload.mappings_by_mode}")
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

    fixed_tiktok_wait = max(5, int(float(getattr(evidence, "TIKTOK_CAPTCHA_MAX_WAIT_SEC", 15)) or 15))
    fixed_please_wait = max(0.0, float(getattr(evidence, "PLEASE_WAIT_EXTRA_CAPTURE_DELAY_SEC", 2.0) or 2.0))
    patch = {
        "credentials_path": credentials_path,
        "sheet_url": str(payload.sheet_url or "").strip(),
        "sheet_name": str(payload.sheet_name or "").strip(),
        "drive_id": str(payload.drive_id or "").strip(),
        "scan_negative_terms": str(payload.scan_negative_terms or ""),
        "scan_keyword_terms": str(payload.scan_keyword_terms or ""),
        "viewport_width": max(320, int(payload.viewport_width or 1920)),
        "viewport_height": max(320, int(payload.viewport_height or 1400)),
        "page_timeout_ms": max(200, int(payload.page_timeout_ms or 200)),
        # Force these values from code-level runtime constants (ignore UI payload).
        "tiktok_captcha_wait_sec": fixed_tiktok_wait,
        "please_wait_delay_sec": fixed_please_wait,
        "tiktok_force_focus": bool(payload.tiktok_force_focus),
        "ready_state": str(payload.ready_state or "interactive").strip() or "interactive",
        "full_page_capture": bool(payload.full_page_capture),
        "mappings_by_mode": _normalize_mappings_by_mode(
            {
                mode: [item.model_dump() for item in items]
                for mode, items in dict(payload.mappings_by_mode or {}).items()
            }
        ),
        "run_flags_by_mode": _normalize_run_flags_by_mode(payload.run_flags_by_mode),
    }
    evidence.write_log(f"[DEBUG] save_settings normalized patch['mappings_by_mode']: {patch['mappings_by_mode']}")
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


@app.get("/api/activity")
def list_activity(request: Request, limit: int = 0):
    owner_email = _require_api_auth(request)
    return {"ok": True, "items": _list_activity_events(owner_email, limit=limit, include_all=True)}


@app.post("/api/activity")
def save_activity(request: Request, payload: ActivityEventRequest):
    owner_email = _require_api_auth(request)
    event = _append_activity_event(
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


@app.post("/api/chrome/launch")
def launch_chrome(request: Request, payload: LaunchChromeRequest):
    _assert_job_runtime_supported()
    _require_api_auth(request)
    run_mode = _normalize_run_mode(payload.run_mode)
    browser_port = int(payload.browser_port or _get_mode_base_port(run_mode))
    profile_path = (payload.profile_path or "").strip() or _get_mode_profile(run_mode, 0, browser_port=browser_port)
    ok, info = evidence.launch_chrome_for_login(
        browser_port=browser_port,
        profile_path=profile_path,
    )
    if not ok:
        raise HTTPException(status_code=500, detail=info)
    return {"ok": True, "message": info}


@app.post("/api/chrome/launch-block/{block_index}")
def launch_chrome_block(block_index: int, request: Request, run_mode: str = "seeding", browser_port: int | None = None):
    _assert_job_runtime_supported()
    _require_api_auth(request)
    run_mode = _normalize_run_mode(run_mode)
    idx = int(block_index)
    base_port = _get_mode_base_port(run_mode)
    port = int(browser_port or evidence.get_post_port(idx, base_port))
    profile = _get_mode_profile(run_mode, idx, browser_port=port)
    ok, info = evidence.launch_chrome_for_login(browser_port=port, profile_path=profile)
    if not ok:
        raise HTTPException(status_code=500, detail=info)
    return {"ok": True, "message": info, "browser_port": port, "profile_path": profile}


@app.post("/api/jobs/start")
def start_job(request: Request, payload: JobStartRequest):
    _assert_job_runtime_supported()
    owner_email = _require_api_auth(request)
    evidence.write_log(f"[DEBUG] start_job received: start_line={payload.start_line}, mappings={payload.mappings}")
    run_mode = _normalize_run_mode(payload.run_mode)
    saved_settings = _read_saved_settings(owner_email)
    with JOBS_LOCK:
        running_id = _any_running_job_for_mode(run_mode, owner_email=owner_email)
        if running_id:
            raise HTTPException(status_code=409, detail=f"Mode {run_mode} đang có job chạy: {running_id}")

    credentials_input = str(payload.credentials_input or "").strip() or str(saved_settings.get("credentials_path", "")).strip()
    credentials_path = _resolve_credentials_input(credentials_input, owner_email)

    mapping_payload = [m.model_dump() for m in payload.mappings] or [_default_mapping(payload.start_line, payload.run_mode)]
    run_mode = _infer_job_mode(mapping_payload, fallback=run_mode)
    sheet_url = evidence.normalize_sheet_input(payload.sheet_url)
    raw_sheet_name = str(payload.sheet_name or "").strip()
    drive_id = evidence.normalize_drive_folder_input(payload.drive_id)

    multi_seeding_blocks: list[dict[str, Any]] = []
    if run_mode == "seeding":
        candidate_blocks = [m for m in mapping_payload if str(m.get("sheet_name", "")).strip() or str(m.get("name", "")).strip()]
        if candidate_blocks:
            if len(candidate_blocks) > 3:
                raise HTTPException(status_code=400, detail="Seeding hiện hỗ trợ tối đa 3 sheet / 1 lần chạy")
            for idx, block in enumerate(candidate_blocks, start=1):
                block_sheet_url = sheet_url
                block_sheet_name = str(block.get("sheet_name", "")).strip() or str(block.get("name", "")).strip()
                block_drive_id = evidence.normalize_drive_folder_input(str(block.get("drive_id", "")).strip() or drive_id)
                if not block_sheet_url:
                    raise HTTPException(status_code=400, detail="Thiếu Sheet URL chính")
                if not block_sheet_name:
                    raise HTTPException(status_code=400, detail=f"Block {idx}: thiếu Sheet Name")
                try:
                    block_spreadsheet = _open_spreadsheet(block_sheet_url, credentials_path)
                    block_worksheet = _resolve_worksheet(block_spreadsheet, block_sheet_url, block_sheet_name)
                    resolved_block_sheet_name = str(getattr(block_worksheet, "title", block_sheet_name) or block_sheet_name).strip() or block_sheet_name
                except HTTPException:
                    raise
                except Exception as exc:
                    raise HTTPException(status_code=400, detail=f"Block {idx}: không mở được sheet: {exc}") from exc
                block_mapping = dict(block)
                block_mapping["name"] = resolved_block_sheet_name
                block_mapping["sheet_name"] = resolved_block_sheet_name
                block_mapping["sheet_url"] = block_sheet_url
                block_mapping["drive_id"] = block_drive_id
                multi_seeding_blocks.append(
                    {
                        "sheet_url": block_sheet_url,
                        "sheet_name": resolved_block_sheet_name,
                        "drive_id": block_drive_id,
                        "mapping": block_mapping,
                    }
                )
            sheet_url = multi_seeding_blocks[0]["sheet_url"]
            raw_sheet_name = multi_seeding_blocks[0]["sheet_name"]
            drive_id = multi_seeding_blocks[0]["drive_id"]

    if not raw_sheet_name:
        raise HTTPException(status_code=400, detail="Thiếu Sheet Name")
    # Validate target worksheet before starting runtime to avoid "running but no write"
    # when the Sheet Name is mistyped or no longer exists.
    try:
        spreadsheet = _open_spreadsheet(sheet_url, credentials_path)
        worksheet = _resolve_worksheet(spreadsheet, sheet_url, raw_sheet_name)
        resolved_sheet_name = str(getattr(worksheet, "title", raw_sheet_name) or raw_sheet_name).strip() or raw_sheet_name
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=400, detail=f"Không mở được sheet trước khi chạy: {exc}") from exc
    merged_settings = _build_settings_payload(saved_settings)
    resolved_negative_terms = str(payload.scan_negative_terms or merged_settings.get("scan_negative_terms", "") or "")
    resolved_keyword_terms = str(payload.scan_keyword_terms or merged_settings.get("scan_keyword_terms", "") or "")
    runtime_settings = {
        "credentials_path": credentials_path,
        "scan_negative_terms": resolved_negative_terms,
        "scan_keyword_terms": resolved_keyword_terms,
        "viewport_width": int(merged_settings.get("viewport_width", 1920) or 1920),
        "viewport_height": int(merged_settings.get("viewport_height", 1400) or 1400),
        "page_timeout_ms": int(merged_settings.get("page_timeout_ms", 200) or 200),
        "tiktok_captcha_wait_sec": int(merged_settings.get("tiktok_captcha_wait_sec", 15) or 15),
        "please_wait_delay_sec": float(merged_settings.get("please_wait_delay_sec", 2.0) or 2.0),
        "tiktok_force_focus": bool(merged_settings.get("tiktok_force_focus", True)),
        "ready_state": str(merged_settings.get("ready_state", "interactive") or "interactive"),
        "full_page_capture": bool(merged_settings.get("full_page_capture", False)),
    }
    saved_mappings_by_mode = _normalize_mappings_by_mode(saved_settings.get("mappings_by_mode"))
    saved_mappings_by_mode[run_mode] = _normalize_mappings_by_mode({run_mode: mapping_payload}).get(run_mode, [])
    saved_run_flags_by_mode = _normalize_run_flags_by_mode(saved_settings.get("run_flags_by_mode"))
    saved_run_flags_by_mode[run_mode] = _normalize_run_flags_for_mode(
        run_mode,
        {
            "force_run_all": bool(payload.force_run_all),
            "highlight_sheet_errors": bool(payload.highlight_sheet_errors),
            "capture_five_per_link": bool(payload.capture_five_per_link),
            "scan_negative_filter": bool(payload.scan_negative_filter),
            "scan_keyword_filter": bool(payload.scan_keyword_filter),
        },
    )
    _write_saved_settings(
        owner_email,
        {
            "credentials_path": credentials_path,
            "sheet_url": sheet_url,
            "sheet_name": resolved_sheet_name,
            "drive_id": drive_id,
            "scan_negative_terms": resolved_negative_terms,
            "scan_keyword_terms": resolved_keyword_terms,
            "mappings_by_mode": saved_mappings_by_mode,
            "run_flags_by_mode": saved_run_flags_by_mode,
        }
    )
    browser_port = _get_mode_base_port(run_mode)
    profile_path = _get_mode_profile(run_mode, 0, browser_port=browser_port)

    if payload.auto_launch_chrome and run_mode != "scan":
        mapped_blocks = list(mapping_payload)
        run_mode_hint = run_mode

        def _auto_launch_chrome_background():
            for idx, mapping in enumerate(mapped_blocks):
                block_mode = _normalize_run_mode(str((mapping or {}).get("mode", run_mode_hint)))
                if block_mode == "scan":
                    continue
                block_port = evidence.get_post_port(idx, _get_mode_base_port(block_mode))
                block_profile = _get_mode_profile(block_mode, idx, browser_port=block_port)
                ok, info = evidence.launch_chrome_for_login(
                    browser_port=block_port,
                    profile_path=block_profile,
                )
                if not ok:
                    evidence.write_log(
                        f"[WARN] Auto launch Chrome failed ({block_mode} block {idx + 1}, port {block_port}): {info}"
                    )

        threading.Thread(target=_auto_launch_chrome_background, daemon=True).start()

    request_snapshot = {
        "owner_email": owner_email,
        "mode": run_mode,
        "drive_id": drive_id,
        "sheet_url": sheet_url,
        "sheet_name": resolved_sheet_name,
        "browser_port": browser_port,
        "profile_path": profile_path,
        "credentials_path": credentials_path,
        "runtime_settings": runtime_settings,
        "start_line": int(payload.start_line),
        "force_run_all": bool(payload.force_run_all),
        "only_run_error_rows": bool(payload.only_run_error_rows),
        "capture_five_per_link": bool(payload.capture_five_per_link),
        "highlight_sheet_errors": bool(payload.highlight_sheet_errors),
        "scan_negative_filter": bool(payload.scan_negative_filter),
        "scan_keyword_filter": bool(payload.scan_keyword_filter),
        "target_rows": [],
        "target_block_name": "",
        "mappings": mapping_payload,
    }
    if multi_seeding_blocks:
        request_snapshot["multi_seeding_blocks"] = multi_seeding_blocks
    try:
        resolved_cols = ", ".join(
            sorted(
                {
                    str((m or {}).get("col_url", "")).strip().upper()
                    for m in mapping_payload
                    if str((m or {}).get("col_url", "")).strip()
                }
            )
        ) or "-"
        evidence.write_log(
            f"[INFO] URL resolve mode enabled (web_ui): mapping Link URL column(s)={resolved_cols}; "
            "runtime will parse raw URL, HYPERLINK() formula, and embedded http(s) tokens."
        )
    except Exception:
        pass
    return _enqueue_job(
        owner_email=owner_email,
        request_snapshot=request_snapshot,
        run_mode=run_mode,
        start_line=int(payload.start_line),
        force_run_all=bool(payload.force_run_all),
        only_run_error_rows=bool(payload.only_run_error_rows),
        capture_five_per_link=bool(payload.capture_five_per_link),
        highlight_sheet_errors=bool(payload.highlight_sheet_errors),
        scan_negative_filter=bool(payload.scan_negative_filter),
        scan_keyword_filter=bool(payload.scan_keyword_filter),
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
        root_job_id = str(source_request.get("root_job_id") or source_job.get("id") or job_id).strip()

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
    latest_saved_settings = _read_saved_settings(owner_email)
    runtime_settings = dict(source_request.get("runtime_settings") or {})
    runtime_settings["scan_negative_terms"] = ""
    runtime_settings["scan_keyword_terms"] = ""
    source_request["runtime_settings"] = runtime_settings
    source_request["root_job_id"] = root_job_id
    source_request["source_job_id"] = job_id

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
        highlight_sheet_errors=bool(source_request.get("highlight_sheet_errors")),
        scan_negative_filter=bool(source_request.get("scan_negative_filter")),
        scan_keyword_filter=bool(source_request.get("scan_keyword_filter")),
        detail=detail,
    )


@app.post("/api/jobs/{job_id}/stop")
def stop_job(job_id: str, request: Request):
    owner_email = _require_api_auth(request)
    with JOBS_LOCK:
        job = JOBS.get(job_id)
        if not job or _job_owner_email(job) != owner_email:
            raise HTTPException(status_code=404, detail="Không tìm thấy job")
        current_status = str(job.get("status") or "").strip().lower()
        if current_status not in {"running", "paused", "queued"}:
            raise HTTPException(status_code=400, detail="Job không ở trạng thái có thể dừng")
        adapter: WebAppAdapter | None = job.get("adapter")
        if adapter is not None:
            adapter.is_running = False
        job["status"] = "stopped"
        job["finished_at"] = _utc_now_iso()
        job["detail"] = "Đã dừng thủ công"
        job["ui_status"] = "ĐÃ DỪNG"
        job["ui_color"] = "#f59e0b"
    _persist_jobs(force=True)
    return {"ok": True, "job_id": job_id, "status": "stopped"}


@app.post("/api/jobs/{job_id}/continue")
def continue_job(job_id: str, request: Request):
    owner_email = _require_api_auth(request)
    with JOBS_LOCK:
        source_job = JOBS.get(job_id)
        if not source_job or _job_owner_email(source_job) != owner_email:
            raise HTTPException(status_code=404, detail="Không tìm thấy job nguồn")
        run_mode = _get_job_mode(source_job)
        current_status = str(source_job.get("status") or "").strip().lower()
        if current_status in {"running", "paused", "queued"}:
            raise HTTPException(status_code=400, detail="Chỉ có thể chạy tiếp từ job đã dừng, lỗi hoặc hoàn tất")
        running_id = _any_running_job_for_mode(run_mode, owner_email=owner_email)
        if running_id:
            raise HTTPException(status_code=409, detail=f"Mode {run_mode} đang có job chạy: {running_id}")
        source_request, next_lines = _derive_continue_request_snapshot(source_job)
        latest_saved_settings = _read_saved_settings(owner_email)
        runtime_settings = dict(source_request.get("runtime_settings") or {})
        runtime_settings["scan_negative_terms"] = ""
        runtime_settings["scan_keyword_terms"] = ""
        source_request["runtime_settings"] = runtime_settings
        root_job_id = str(source_request.get("root_job_id") or source_job.get("id") or job_id).strip()

    source_request["root_job_id"] = root_job_id
    source_request["source_job_id"] = job_id

    line_hints = [f"{name} #{line}" for name, line in list(next_lines.items())[:3]]
    detail = "Chạy tiếp"
    if line_hints:
        detail += " từ " + ", ".join(line_hints)

    return _enqueue_job(
        owner_email=owner_email,
        request_snapshot=source_request,
        run_mode=run_mode,
        start_line=int(source_request.get("start_line") or 4),
        force_run_all=bool(source_request.get("force_run_all")),
        only_run_error_rows=False,
        capture_five_per_link=bool(source_request.get("capture_five_per_link")),
        highlight_sheet_errors=bool(source_request.get("highlight_sheet_errors")),
        scan_negative_filter=bool(source_request.get("scan_negative_filter")),
        scan_keyword_filter=bool(source_request.get("scan_keyword_filter")),
        detail=detail,
    )


@app.post("/api/jobs/{job_id}/retry-errors")
def retry_job_errors(job_id: str, request: Request):
    owner_email = _require_api_auth(request)
    with JOBS_LOCK:
        source_job = JOBS.get(job_id)
        if not source_job or _job_owner_email(source_job) != owner_email:
            raise HTTPException(status_code=404, detail="Không tìm thấy job nguồn")
        run_mode = _get_job_mode(source_job)
        current_status = str(source_job.get("status") or "").strip().lower()
        if current_status in {"running", "paused", "queued"}:
            raise HTTPException(status_code=400, detail="Chỉ có thể chạy lỗi từ job đã dừng, lỗi hoặc hoàn tất")
        running_id = _any_running_job_for_mode(run_mode, owner_email=owner_email)
        if running_id:
            raise HTTPException(status_code=409, detail=f"Mode {run_mode} đang có job chạy: {running_id}")
        source_request = json.loads(json.dumps(source_job.get("request") or {}))
        root_job_id = str(source_request.get("root_job_id") or source_job.get("id") or job_id).strip()
        issue_cells = list(source_job.get("issue_cells") or [])

    target_rows: list[int] = []
    for item in issue_cells:
        try:
            kind = str((item or {}).get("kind") or "").strip().lower()
            if kind not in {"failed", "error"}:
                continue
            row_num = int((item or {}).get("row") or 0)
        except Exception:
            continue
        if row_num > 0:
            target_rows.append(row_num)
    target_rows = sorted(set(target_rows))
    if not target_rows:
        raise HTTPException(status_code=400, detail="Job này chưa có dòng lỗi để chạy lại")

    retry_start_line = int(min(target_rows))
    mappings = list(source_request.get("mappings") or [])
    adjusted_mappings = []
    for item in mappings:
        block = dict(item or {})
        try:
            current_start = int(str(block.get("start_line", retry_start_line)).strip() or retry_start_line)
        except Exception:
            current_start = retry_start_line
        block["start_line"] = min(current_start, retry_start_line)
        adjusted_mappings.append(block)
    if adjusted_mappings:
        source_request["mappings"] = adjusted_mappings

    source_request["mode"] = run_mode
    source_request["owner_email"] = owner_email
    source_request["root_job_id"] = root_job_id
    source_request["source_job_id"] = job_id
    source_request["target_rows"] = target_rows
    source_request["target_block_name"] = ""
    source_request["start_line"] = retry_start_line
    source_request["only_run_error_rows"] = False
    source_request["force_run_all"] = True
    latest_saved_settings = _read_saved_settings(owner_email)
    runtime_settings = dict(source_request.get("runtime_settings") or {})
    runtime_settings["scan_negative_terms"] = ""
    runtime_settings["scan_keyword_terms"] = ""
    source_request["runtime_settings"] = runtime_settings

    detail = f"Chạy lỗi {len(target_rows)} dòng"
    return _enqueue_job(
        owner_email=owner_email,
        request_snapshot=source_request,
        run_mode=run_mode,
        start_line=int(source_request.get("start_line") or 4),
        force_run_all=True,
        only_run_error_rows=False,
        capture_five_per_link=bool(source_request.get("capture_five_per_link")),
        highlight_sheet_errors=bool(source_request.get("highlight_sheet_errors")),
        scan_negative_filter=bool(source_request.get("scan_negative_filter")),
        scan_keyword_filter=bool(source_request.get("scan_keyword_filter")),
        detail=detail,
    )


@app.delete("/api/jobs/{job_id}")
def delete_job(job_id: str, request: Request):
    owner_email = _require_api_auth(request)
    can_manage_all = _is_admin_email(owner_email)
    with JOBS_LOCK:
        job = JOBS.get(job_id)
        if not job:
            raise HTTPException(status_code=404, detail="Không tìm thấy job")
        if (not can_manage_all) and (_job_owner_email(job) != owner_email):
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
        if not _can_view_job(job, owner_email):
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
def list_jobs(
    request: Request,
    recent_log_limit: int = JOB_LIST_RECENT_LOG_LIMIT_DEFAULT,
    jobs_limit: int = 0,
    include_recent_logs: int = 1,
    include_issue_details: int = 1,
):
    owner_email = _require_api_auth(request)
    can_view_all = _is_admin_email(owner_email)
    log_limit = max(0, min(int(recent_log_limit), JOB_LIST_RECENT_LOG_LIMIT_MAX))
    max_jobs = max(0, min(int(jobs_limit or 0), 1000))
    want_recent_logs = bool(int(include_recent_logs or 0))
    want_issue_details = bool(int(include_issue_details or 0))
    out = []
    with JOBS_LOCK:
        for job in JOBS.values():
            if not can_view_all and _job_owner_email(job) != owner_email:
                continue
            logs_ref = job.get("logs") or []
            recent_logs = list(logs_ref[-log_limit:]) if (want_recent_logs and log_limit > 0) else []
            issue_rows_ref = job.get("error_rows") or {}
            issue_cells_ref = job.get("issue_cells") or []
            out.append(
                {
                    "id": job["id"],
                    "owner_email": _job_owner_email(job),
                    "mode": _get_job_mode(job),
                    "status": job["status"],
                    "created_at": job["created_at"],
                    "started_at": job["started_at"],
                    "finished_at": job["finished_at"],
                    "summary": job.get("summary"),
                    "detail": job.get("detail"),
                    "request": _compact_request_for_client(job.get("request")),
                    "completion": job.get("completion"),
                    "error_rows": issue_rows_ref if want_issue_details else {},
                    "issue_cells": issue_cells_ref if want_issue_details else [],
                    "error": job.get("error"),
                    "recent_logs": recent_logs,
                    "error_row_count": len(issue_rows_ref),
                    "issue_cell_count": len(issue_cells_ref),
                }
            )
    out.sort(key=lambda x: x["created_at"], reverse=True)
    if max_jobs > 0:
        out = out[:max_jobs]
    return {"jobs": out}


@app.get("/api/jobs/{job_id}")
def get_job(job_id: str, request: Request):
    owner_email = _require_api_auth(request)
    with JOBS_LOCK:
        job = JOBS.get(job_id)
        if not _can_view_job(job, owner_email):
            raise HTTPException(status_code=404, detail="Không tìm thấy job")
        return {
            "id": job["id"],
            "owner_email": _job_owner_email(job),
            "mode": _get_job_mode(job),
            "status": job["status"],
            "created_at": job["created_at"],
            "started_at": job["started_at"],
            "finished_at": job["finished_at"],
            "summary": job.get("summary"),
            "detail": job.get("detail"),
            "request": _compact_request_for_client(job.get("request")),
            "ui_status": job.get("ui_status"),
            "completion": job.get("completion"),
            "error_rows": job.get("error_rows"),
            "issue_cells": job.get("issue_cells"),
            "error": job.get("error"),
        }


@app.get("/api/jobs/{job_id}/logs")
def get_job_logs(job_id: str, request: Request, limit: int = 100, since: int = 0):
    owner_email = _require_api_auth(request)
    lim = max(1, min(int(limit), 1000))
    cursor = max(0, int(since or 0))
    with JOBS_LOCK:
        job = JOBS.get(job_id)
        if not _can_view_job(job, owner_email):
            raise HTTPException(status_code=404, detail="Không tìm thấy job")
        logs_ref = job.get("logs") or []
        total = len(logs_ref)
        reset = False
        if cursor <= 0:
            chunk = list(logs_ref[-lim:])
            next_cursor = total
        else:
            if cursor > total:
                reset = True
                chunk = list(logs_ref[-lim:])
                next_cursor = total
            else:
                end = min(total, cursor + lim)
                chunk = list(logs_ref[cursor:end])
                next_cursor = end
    return {"job_id": job_id, "logs": chunk, "total": total, "next_cursor": next_cursor, "reset": reset}


if __name__ == "__main__":
    import socket
    import uvicorn

    def _can_bind(host_value: str, port_value: int) -> bool:
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                sock.bind((host_value, int(port_value)))
            return True
        except OSError:
            return False

    def _pick_available_port(host_value: str, preferred_port: int, max_tries: int = 100) -> int:
        base = int(preferred_port)
        for candidate in range(base, base + max_tries):
            if _can_bind(host_value, candidate):
                return candidate
        raise RuntimeError(f"Không tìm được cổng trống từ {base} đến {base + max_tries - 1}")

    # Listen on all interfaces by default for dev ergonomics.
    # But for browser access, prefer localhost/127.0.0.1 if running locally.
    host = os.environ.get("HOST", "0.0.0.0")
    requested_port = int(os.environ.get("PORT", "8000"))
    port = _pick_available_port(host, requested_port)

    local_host = "localhost"
    local_addr = "127.0.0.1"
    try:
        private_ip = socket.gethostbyname(socket.gethostname())
    except Exception:
        private_ip = None

    if port != requested_port:
        print(f"Port {requested_port} đang bận, tự chuyển sang cổng trống {port}")
    print(f"Starting web_ui server on {host}:{port}")
    print(f"Open in browser: http://{local_host}:{port} or http://{local_addr}:{port}")
    if private_ip and private_ip not in {"127.0.0.1", "0.0.0.0"}:
        print(f"Also available on local network: http://{private_ip}:{port}")

    uvicorn.run("web_ui:app", host=host, port=port, reload=False)

