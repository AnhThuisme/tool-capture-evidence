import os
import time
import threading
import shutil
import json
import base64
import re
import signal
import calendar
import io
import sys
import socket
import csv
import html as html_lib
import zipfile
from xml.sax.saxutils import escape as xml_escape
from concurrent.futures import ThreadPoolExecutor, as_completed
try:
    import tkinter as tk
    from tkinter import ttk, messagebox, filedialog
except Exception:
    tk = None
    ttk = None
    messagebox = None
    filedialog = None
from datetime import datetime
import unicodedata
import subprocess
import difflib
from urllib.parse import quote, urlparse, parse_qs, urljoin
try:
    import requests
except Exception:
    requests = None

import gspread
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
from webdriver_manager.core.driver_cache import DriverCacheManager
from oauth2client.service_account import ServiceAccountCredentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload, MediaIoBaseDownload
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait

# ================= CONFIG =================
BASE_DIR = os.environ.get("EVIDENCE_BASE_DIR", os.path.dirname(os.path.abspath(__file__)))
APP_DIR = os.path.dirname(sys.executable) if getattr(sys, "frozen", False) else BASE_DIR
TEMP_ROOT = os.path.join(os.environ.get("LOCALAPPDATA", BASE_DIR), "ToolEvidence")
TEMP_DIR = os.path.join(os.environ.get("EVIDENCE_TEMP_DIR", TEMP_ROOT), "temp_screenshots")
FB_PROFILE_PATH = os.path.join(BASE_DIR, "FB_Session")
FB_PROFILE_PATH_ALT = os.path.join(BASE_DIR, "FB_Session_Selenium")
LOCAL_PROFILE_PATH = os.path.join(os.environ.get("LOCALAPPDATA", os.path.join(BASE_DIR, ".local_profile")), "EvidenceTool_Profile")
WDM_ROOT = os.path.join(os.environ.get("LOCALAPPDATA", BASE_DIR), "EvidenceTool_WDM")
LOG_PATH = os.path.join(BASE_DIR, "log.txt")
SETTINGS_PATH = os.path.join(BASE_DIR, "app_settings.json")
ERROR_HISTORY_PATH = os.path.join(BASE_DIR, "error_history.json")

DEFAULT_DRIVE_FOLDER_ID = "1JJuG1ja80ThO_V14XnkOlwBz9Ey-3kmn"
DEFAULT_SHEET_URL = "https://docs.google.com/spreadsheets/d/1wKLirm10BTEhkfVHZJxeo5iR4fjCpesaylUPYlF2UV0"
DEFAULT_SHEET_NAME_TARGET = "Nghiệm thu"
CAPTURE_WINDOW_SIZE = "1920,1400"
CAPTURE_ZOOM_PERCENT = 90
PAGE_READY_TIMEOUT = 3
PAGE_READY_FALLBACK_SLEEP = 0.45
PER_LINK_BASE_WAIT = 0.35
TIKTOK_SCROLL_WAIT_1 = 0.35
TIKTOK_SCROLL_WAIT_2 = 0.5
ZOOM_SETTLE_SLEEP = 0.08
SCREENSHOT_CAPTURE_DELAY = 1.0
MULTI_CAPTURE_INTERVAL_SEC = 5.0
FB_COMMENT_READY_WAIT = 4.0
UI_CLICK_SETTLE_SLEEP = 0.15
UI_SCROLL_SETTLE_SLEEP = 0.1


def get_post_port(post_index: int, base_port: int = 9223) -> int:
    """
    Post 1 -> 9223, Post 2 -> 9324, Post 3 -> 9325, ...
    Keep compatible with existing worker/profile mapping.
    """
    if post_index <= 0:
        return base_port
    return base_port + 100 + post_index


def _bootstrap_env_credentials_path() -> str:
    raw = os.environ.get("GOOGLE_CREDENTIALS_JSON_B64", "").strip()
    if not raw:
        return ""
    target = os.path.join(BASE_DIR, "credentials.env.json")
    try:
        if raw.startswith("{"):
            data = json.loads(raw)
        else:
            padded = raw + ("=" * (-len(raw) % 4))
            decoded = base64.b64decode(padded.encode("utf-8")).decode("utf-8")
            data = json.loads(decoded)
        os.makedirs(os.path.dirname(target), exist_ok=True)
        with open(target, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        return target
    except Exception as exc:
        print(f"[startup-config] failed to materialize GOOGLE_CREDENTIALS_JSON_B64: {exc}")
        return ""


def is_fixed_credentials_path(path: str | None) -> bool:
    raw = str(path or "").strip()
    if not raw:
        return False
    return os.path.basename(raw).lower() in {"credentials.inline.json", "credentials.env.json"}


def resolve_credentials_path() -> str:
    env_path = os.environ.get("GOOGLE_CREDENTIALS_PATH", "").strip()
    if env_path:
        return env_path

    env_b64_path = _bootstrap_env_credentials_path()
    if env_b64_path:
        return env_b64_path

    candidates = [
        os.path.join(APP_DIR, "credentials.inline.json"),  # saved once from web UI / committed fixed file
        os.path.join(APP_DIR, "credentials.json"),     # next to .exe / script
        os.path.join(os.getcwd(), "credentials.inline.json"),
        os.path.join(os.getcwd(), "credentials.json"), # current working directory
        os.path.join(BASE_DIR, "credentials.inline.json"),
        os.path.join(BASE_DIR, "credentials.json"),    # source directory
    ]
    if hasattr(sys, "_MEIPASS"):
        candidates.append(os.path.join(getattr(sys, "_MEIPASS"), "credentials.inline.json"))
        candidates.append(os.path.join(getattr(sys, "_MEIPASS"), "credentials.json"))

    for p in candidates:
        if os.path.exists(p):
            return p

    # default location for error messages when file is missing
    return candidates[0]


JSON_PATH = resolve_credentials_path()


def normalize_sheet_input(sheet_text: str) -> str:
    s = (sheet_text or "").strip()
    if not s:
        return ""
    if "docs.google.com/spreadsheets/" in s:
        return s
    # Accept raw spreadsheet id and normalize to URL.
    if len(s) >= 20 and "/" not in s and " " not in s:
        return f"https://docs.google.com/spreadsheets/d/{s}"
    return s


def normalize_drive_folder_input(folder_text: str) -> str:
    s = (folder_text or "").strip()
    if not s:
        return ""
    if "drive.google.com" not in s:
        return s
    try:
        parsed = urlparse(s)
        parts = [p for p in (parsed.path or "").split("/") if p]
        if "folders" in parts:
            i = parts.index("folders")
            if i + 1 < len(parts):
                cand = parts[i + 1].strip()
                if cand:
                    return cand
        q = parse_qs(parsed.query or "")
        cand = (q.get("id") or [""])[0].strip()
        if cand:
            return cand
    except Exception:
        pass
    return s


def get_default_credentials_input() -> str:
    try:
        with open(JSON_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)
        return json.dumps(data, ensure_ascii=False)
    except Exception:
        return JSON_PATH


def resolve_chromedriver_service() -> Service:
    """
    Resolve chromedriver with a writable cache dir to avoid WinError 5 on locked home dirs.
    """
    local_driver = shutil.which("chromedriver")
    if local_driver:
        write_log(f"[INFO] Use local chromedriver: {local_driver}")
        return Service(local_driver)

    wdm_root = os.environ.get("EVIDENCE_WDM_DIR", "").strip() or WDM_ROOT
    os.makedirs(wdm_root, exist_ok=True)
    write_log(f"[INFO] WebDriver cache dir: {wdm_root}")

    cache_manager = DriverCacheManager(root_dir=wdm_root)
    driver_path = ChromeDriverManager(cache_manager=cache_manager).install()
    write_log(f"[INFO] WebDriver installed: {driver_path}")
    return Service(driver_path)

# ================= HELPERS =================
def get_service_account_email(path: str | None = None):
    """Đọc email service account từ credentials.json để hướng dẫn user chia sẻ Sheet/Drive."""
    try:
        import json
        cred_path = path or JSON_PATH
        with open(cred_path, "r", encoding="utf-8") as f:
            data = json.load(f)
        return (data.get("client_email") or "").strip()
    except Exception:
        return ""


def col_letter_to_index(letter):
    """Convert Excel/Sheets column ref to 1-based index (A, AC, 29, ...)."""
    s = str(letter or "").strip()
    if not s:
        return None

    # Allow numeric column input directly (e.g. "29").
    if s.isdigit():
        idx = int(s)
        return idx if idx > 0 else None

    s = s.upper()
    idx = 0
    for ch in s:
        if not ("A" <= ch <= "Z"):
            return None
        idx = idx * 26 + (ord(ch) - ord("A") + 1)
    return idx


def col_index_to_letter(index: int) -> str:
    """Convert 1-based column index to letter (1->A, 27->AA)."""
    try:
        n = int(index)
    except Exception:
        return ""
    if n <= 0:
        return ""
    out = []
    while n > 0:
        n, rem = divmod(n - 1, 26)
        out.append(chr(ord("A") + rem))
    return "".join(reversed(out))


def extract_url_from_hyperlink_formula(formula_text: str) -> str:
    """
    Extract URL from Sheets formula:
    =HYPERLINK("https://...","label")
    """
    s = str(formula_text or "").strip()
    if not s:
        return ""
    m = re.search(r'^\s*=\s*HYPERLINK\s*\(\s*"((?:[^"]|"")*)"', s, flags=re.IGNORECASE)
    if not m:
        return ""
    return m.group(1).replace('""', '"').strip()


def resolve_links_for_scan(worksheet, col_idx: int, start_row: int = 4) -> list[str]:
    """
    Build effective link list for scan mode.
    If displayed cell value is not an URL, try to read URL from HYPERLINK formula.
    """
    if not col_idx:
        return []

    display_vals = worksheet.col_values(col_idx)
    display_slice = display_vals[start_row - 1 :] if len(display_vals) >= start_row else []

    formula_rows = []
    try:
        col_letter = col_index_to_letter(col_idx)
        if col_letter:
            formula_rows = worksheet.get(
                f"{col_letter}{start_row}:{col_letter}",
                value_render_option="FORMULA",
            ) or []
    except Exception as e:
        write_log(f"[WARN] resolve_links_for_scan formulas read failed: {e}")

    size = max(len(display_slice), len(formula_rows))
    out: list[str] = []
    for i in range(size):
        shown = str(display_slice[i]).strip() if i < len(display_slice) else ""
        shown_norm = normalize_scan_source_url(shown)
        if shown_norm:
            out.append(shown_norm)
            continue
        formula_cell = ""
        if i < len(formula_rows) and formula_rows[i]:
            formula_cell = str(formula_rows[i][0]).strip()
        parsed = extract_url_from_hyperlink_formula(formula_cell)
        out.append(normalize_scan_source_url(parsed or shown))
    return out


def resolve_column_values_aligned(worksheet, col_idx: int, start_row: int = 4, total_rows: int | None = None) -> list[str]:
    """
    Read a column while preserving row alignment and blanks.
    """
    if not col_idx:
        return []
    col_letter = col_index_to_letter(col_idx)
    if not col_letter:
        return []
    if total_rows is not None and total_rows > 0:
        end_row = start_row + total_rows - 1
        rng = f"{col_letter}{start_row}:{col_letter}{end_row}"
    else:
        rng = f"{col_letter}{start_row}:{col_letter}"
    try:
        rows = worksheet.get(rng, value_render_option="UNFORMATTED_VALUE") or []
    except Exception as e:
        write_log(f"[WARN] resolve_column_values_aligned failed: {e}")
        rows = []
    out: list[str] = []
    for r in rows:
        if r and len(r) > 0:
            out.append(str(r[0]).strip())
        else:
            out.append("")
    if total_rows is not None and total_rows > 0 and len(out) < total_rows:
        out.extend([""] * (total_rows - len(out)))
    return out


def normalize_match_text(text: str) -> str:
    s = unicodedata.normalize("NFD", str(text or ""))
    s = "".join(ch for ch in s if unicodedata.category(ch) != "Mn")
    s = s.lower()
    s = re.sub(r"[^a-z0-9\s]", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def extract_drive_file_id(url: str) -> str:
    s = str(url or "").strip()
    if not s:
        return ""
    try:
        if "/file/d/" in s:
            return s.split("/file/d/", 1)[1].split("/", 1)[0].strip()
        parsed = urlparse(s)
        q = parse_qs(parsed.query or "")
        cand = (q.get("id") or [""])[0].strip()
        if cand:
            return cand
    except Exception:
        pass
    return ""


def normalize_scan_source_url(raw_url: str) -> str:
    s = str(raw_url or "").strip()
    if not s:
        return ""
    if s.lower().startswith("http://") or s.lower().startswith("https://"):
        return s
    if "drive.google.com" in s:
        return "https://" + s.lstrip("/")
    fid = extract_drive_file_id(s)
    if fid:
        return f"https://drive.google.com/file/d/{fid}/view"
    return ""


def normalize_web_source_url(raw_url: str) -> str:
    s = str(raw_url or "").strip()
    if not s:
        return ""
    low = s.lower()
    if low.startswith("http://") or low.startswith("https://"):
        return s
    if "://" in s:
        return s
    if "drive.google.com" in low:
        return "https://" + s.lstrip("/")
    if re.match(r"^[a-z0-9][a-z0-9.-]*\.[a-z]{2,}(/.*)?$", low):
        return "https://" + s
    return ""


def build_candidate_image_urls(src_url: str) -> list[str]:
    base = str(src_url or "").strip()
    if not base:
        return []
    out = [base]
    fid = extract_drive_file_id(base)
    if fid:
        out.extend(
            [
                f"https://drive.google.com/uc?export=download&id={fid}",
                f"https://drive.google.com/uc?export=view&id={fid}",
                f"https://lh3.googleusercontent.com/d/{fid}",
            ]
        )
    # preserve order, remove duplicates
    seen = set()
    uniq = []
    for u in out:
        if u not in seen:
            seen.add(u)
            uniq.append(u)
    return uniq


def download_image_bytes_for_scan(url: str, timeout: int = 20, drive_service=None) -> bytes:
    fid = extract_drive_file_id(url)
    if drive_service is not None and fid:
        try:
            req = drive_service.files().get_media(fileId=fid, supportsAllDrives=True)
            buf = io.BytesIO()
            downloader = MediaIoBaseDownload(buf, req)
            done = False
            while not done:
                _, done = downloader.next_chunk()
            content = buf.getvalue() or b""
            if content and (
                content.startswith(b"\x89PNG")
                or content.startswith(b"\xff\xd8")
                or content[:4] == b"RIFF"
            ):
                return content
        except Exception as e:
            write_log(f"[WARN] Drive API download failed for {fid}: {e}")

    headers = {"User-Agent": "Mozilla/5.0"}
    for cand in build_candidate_image_urls(url):
        try:
            if requests is not None:
                r = requests.get(cand, timeout=timeout, headers=headers, allow_redirects=True)
                if r.status_code >= 400:
                    continue
                ctype = (r.headers.get("content-type") or "").lower()
                content = r.content or b""
            else:
                from urllib.request import Request, urlopen
                req = Request(cand, headers=headers)
                with urlopen(req, timeout=timeout) as resp:
                    ctype = str(resp.headers.get("Content-Type", "")).lower()
                    content = resp.read() or b""
            if not content:
                continue
            # Accept typical image content-type or PNG/JPG/WebP bytes signature.
            if (
                "image/" in ctype
                or content.startswith(b"\x89PNG")
                or content.startswith(b"\xff\xd8")
                or content[:4] == b"RIFF"
            ):
                return content
        except Exception:
            continue
    return b""


def ocr_text_from_image_bytes(image_bytes: bytes, expected_text: str = "") -> str:
    if not image_bytes:
        return ""
    try:
        from PIL import Image
        import pytesseract
        img = Image.open(io.BytesIO(image_bytes)).convert("RGB")
        w, h = img.size
        gray = img.convert("L")
        # Fast-first OCR plan; only fallback to slower variants if needed.
        variants = [
            gray,
            img.resize((max(1, w * 2), max(1, h * 2))).convert("L"),
            gray.point(lambda p: 255 if p > 165 else 0),
        ]
        configs = ["--oem 1 --psm 6", "--oem 1 --psm 11"]
        langs = ["vie+eng", "eng"]
        texts = []
        seen = set()
        for v in variants:
            for lang in langs:
                for cfg in configs:
                    try:
                        t = (pytesseract.image_to_string(v, lang=lang, config=cfg) or "").strip()
                        if t and t not in seen:
                            seen.add(t)
                            texts.append(t)
                            # Early stop when already matched -> much faster for positive rows.
                            if expected_text and is_scan_match(expected_text, t):
                                return t
                    except Exception:
                        continue
        if texts:
            return "\n".join(texts)
    except Exception as e:
        write_log(f"[WARN] OCR engine unavailable/failed: {e}")
    return ""


def build_collage_png(image_bytes_list: list[bytes]) -> bytes:
    """
    Merge multiple screenshots into a single collage image so Sheets can render
    all shots in one IMAGE() cell.
    """
    if not image_bytes_list:
        return b""
    try:
        from PIL import Image
    except Exception:
        return b""

    images = []
    for b in image_bytes_list:
        try:
            img = Image.open(io.BytesIO(b)).convert("RGB")
            images.append(img)
        except Exception:
            continue
    if not images:
        return b""
    if len(images) == 1:
        out_buf = io.BytesIO()
        images[0].save(out_buf, format="PNG")
        return out_buf.getvalue()

    cols = min(3, len(images))
    rows = (len(images) + cols - 1) // cols
    tile_w = 360
    tile_h = 260
    pad = 8

    thumbs = []
    for img in images:
        im = img.copy()
        im.thumbnail((tile_w, tile_h))
        thumbs.append(im)

    canvas_w = cols * tile_w + (cols + 1) * pad
    canvas_h = rows * tile_h + (rows + 1) * pad
    canvas = Image.new("RGB", (canvas_w, canvas_h), (245, 246, 250))

    for i, im in enumerate(thumbs):
        r = i // cols
        c = i % cols
        x0 = pad + c * (tile_w + pad)
        y0 = pad + r * (tile_h + pad)
        x = x0 + max(0, (tile_w - im.width) // 2)
        y = y0 + max(0, (tile_h - im.height) // 2)
        canvas.paste(im, (x, y))

    out_buf = io.BytesIO()
    canvas.save(out_buf, format="PNG")
    return out_buf.getvalue()


def check_ocr_dependencies() -> tuple[bool, str]:
    try:
        from PIL import Image  # noqa: F401
    except Exception:
        return False, "Thiếu Pillow. Cài: pip install pillow"
    try:
        import pytesseract
    except Exception:
        return False, "Thiếu pytesseract. Cài: pip install pytesseract"
    try:
        tcmd = getattr(pytesseract.pytesseract, "tesseract_cmd", "") or "tesseract"
        if not shutil.which(str(tcmd)):
            candidates = [
                os.path.join(os.environ.get("LOCALAPPDATA", ""), "Programs", "Tesseract-OCR", "tesseract.exe"),
                os.path.join(os.environ.get("PROGRAMFILES", r"C:\Program Files"), "Tesseract-OCR", "tesseract.exe"),
                os.path.join(os.environ.get("PROGRAMFILES(X86)", r"C:\Program Files (x86)"), "Tesseract-OCR", "tesseract.exe"),
            ]
            for p in candidates:
                if p and os.path.exists(p):
                    pytesseract.pytesseract.tesseract_cmd = p
                    break
    except Exception:
        pass
    try:
        _ = pytesseract.get_tesseract_version()
    except Exception:
        return (
            False,
            "Thiếu Tesseract OCR (binary). Cài Tesseract và thêm vào PATH, rồi mở lại app.",
        )
    return True, ""


def is_scan_match(expected_text: str, ocr_text: str) -> bool:
    expected = normalize_match_text(expected_text)
    got = normalize_match_text(ocr_text)
    if not expected or not got:
        return False
    if expected in got:
        return True
    if got in expected and len(got) >= 18:
        return True
    e_tokens = [t for t in expected.split() if len(t) >= 2]
    g_tokens = set(t for t in got.split() if len(t) >= 2)
    got_pad = f" {got} "
    # Strong signal: a consecutive phrase appears in OCR text.
    if len(e_tokens) >= 3:
        for win in (5, 4, 3):
            if len(e_tokens) >= win:
                for i in range(len(e_tokens) - win + 1):
                    phrase = " " + " ".join(e_tokens[i : i + win]) + " "
                    if phrase in got_pad:
                        return True
    if e_tokens and g_tokens:
        overlap = sum(1 for t in e_tokens if t in g_tokens) / max(1, len(e_tokens))
        if overlap >= 0.42:
            return True
    ratio = difflib.SequenceMatcher(None, expected, got).ratio()
    return ratio >= 0.52


def is_scan_text_strict_match(expected_text: str, source_text: str) -> bool:
    """
    Stricter matching for Scan Only Text mode to reduce false positives.
    Compare against per-line candidate comments, not only whole-page text.
    """
    expected = normalize_match_text(expected_text)
    source = normalize_match_text(source_text)
    if not expected or not source:
        return False

    # Very short expected text is too ambiguous.
    e_tokens = [t for t in expected.split() if len(t) >= 2]
    if len(e_tokens) < 4 or len(expected) < 12:
        return False

    exp_pad = f" {expected} "
    exp_compact = expected.replace(" ", "")
    exp_len = len(expected)

    # Build candidate comment blocks from lines + sliding windows.
    noise_phrases = {
        "like", "reply", "share", "follow", "see more", "view more",
        "xem them", "xem them binh luan", "xem them phan hoi",
        "tat ca binh luan", "binh luan", "phan hoi",
    }
    raw_lines = [str(x).strip() for x in str(source_text or "").splitlines()]
    normalized_lines: list[str] = []
    for ln in raw_lines:
        n = normalize_match_text(ln)
        if len(n) < 10:
            continue
        if n in noise_phrases:
            continue
        normalized_lines.append(n)
    if not normalized_lines:
        normalized_lines = [source]

    candidates: list[str] = []
    seen_cands: set[str] = set()

    def _push_candidate(txt: str):
        t = (txt or "").strip()
        if len(t) < 10:
            return
        if t in seen_cands:
            return
        seen_cands.add(t)
        candidates.append(t)

    for n in normalized_lines:
        _push_candidate(n)

    max_window = 6
    for i in range(len(normalized_lines)):
        merged = normalized_lines[i]
        for w in range(2, max_window + 1):
            j = i + w - 1
            if j >= len(normalized_lines):
                break
            merged = f"{merged} {normalized_lines[j]}".strip()
            if len(merged) > 450:
                break
            _push_candidate(merged)

    if len(candidates) > 5000:
        candidates = candidates[:5000]

    for cand in candidates:
        if not cand:
            continue
        cand_pad = f" {cand} "

        # Primary rule: expected sentence must appear as a contiguous phrase.
        if exp_pad in cand_pad:
            return True

        # Also allow exact-compact containment (for cases where page collapses spaces).
        cand_compact = cand.replace(" ", "")
        if exp_compact and exp_compact in cand_compact:
            return True

        c_tokens = [t for t in cand.split() if len(t) >= 2]
        if not c_tokens:
            continue
        c_set = set(c_tokens)
        overlap = sum(1 for t in e_tokens if t in c_set) / max(1, len(e_tokens))
        ratio = difflib.SequenceMatcher(None, expected, cand).ratio()

        # Ordered token coverage: robust for line breaks / emoji / minor OCR-like distortions.
        j = 0
        ordered_hits = 0
        for tok in e_tokens:
            while j < len(c_tokens) and c_tokens[j] != tok:
                j += 1
            if j < len(c_tokens):
                ordered_hits += 1
                j += 1
        order_cov = ordered_hits / max(1, len(e_tokens))

        # Fuzzy fallback on one candidate only (not whole-page), to reduce false matches.
        len_gap = abs(len(cand) - exp_len) / max(1, exp_len)
        if exp_len >= 90:
            need_order, need_overlap, need_ratio, need_gap = 0.80, 0.82, 0.78, 0.68
        elif exp_len >= 55:
            need_order, need_overlap, need_ratio, need_gap = 0.84, 0.84, 0.82, 0.55
        else:
            need_order, need_overlap, need_ratio, need_gap = 0.90, 0.90, 0.88, 0.38

        if order_cov >= need_order and overlap >= need_overlap and len_gap <= need_gap:
            return True
        if overlap >= need_overlap and ratio >= need_ratio and len_gap <= need_gap:
            return True

    return False


def to_mbasic_facebook_url(raw_url: str) -> str:
    u = str(raw_url or "").strip()
    if not u:
        return ""
    low = u.lower()
    if "facebook.com" not in low and "fb.watch" not in low:
        return ""
    try:
        if "fb.watch/" in low:
            tail = u.split("fb.watch/", 1)[1].strip("/")
            if tail:
                return f"https://mbasic.facebook.com/watch/?v={tail}"
        parsed = urlparse(u)
        path = parsed.path or "/"
        query = parsed.query or ""
        frag = parsed.fragment or ""
        base = f"https://mbasic.facebook.com{path}"
        if query:
            base += f"?{query}"
        if frag:
            base += f"#{frag}"
        return base
    except Exception:
        return ""


def _collect_mbasic_visible_text(driver) -> str:
    try:
        txt = driver.execute_script(
            """
            const sels = [
              'div[data-ft]',
              'article',
              'div[role="article"]',
              'div[id*="ufi"]',
              'h3 + div',
              'p'
            ];
            const out = [];
            const seen = new Set();
            for (const s of sels) {
              const nodes = document.querySelectorAll(s);
              for (const n of nodes) {
                const t = (n && n.innerText) ? n.innerText.trim() : '';
                if (!t || t.length < 6) continue;
                if (seen.has(t)) continue;
                seen.add(t);
                out.push(t);
                if (out.length >= 1200) break;
              }
              if (out.length >= 1200) break;
            }
            if (!out.length && document.body && document.body.innerText) {
              out.push(document.body.innerText);
            }
            return out.join('\\n');
            """
        ) or ""
        return str(txt).strip()
    except Exception:
        return ""


def _html_to_plain_text(html: str) -> str:
    s = str(html or "")
    if not s:
        return ""
    # Remove non-content blocks.
    s = re.sub(r"(?is)<script[^>]*>.*?</script>", " ", s)
    s = re.sub(r"(?is)<style[^>]*>.*?</style>", " ", s)
    s = re.sub(r"(?is)<!--.*?-->", " ", s)
    # Keep line breaks around common block tags.
    s = re.sub(r"(?is)<\s*br\s*/?\s*>", "\n", s)
    s = re.sub(r"(?is)</\s*(p|div|li|tr|h[1-6]|article|section)\s*>", "\n", s)
    # Drop remaining tags.
    s = re.sub(r"(?is)<[^>]+>", " ", s)
    s = html_lib.unescape(s)
    s = s.replace("\r", "\n")
    s = re.sub(r"[ \t]+", " ", s)
    s = re.sub(r"\n{2,}", "\n", s)
    return s.strip()


def _extract_follow_links_mbasic_html(html: str) -> list[str]:
    out: list[str] = []
    if not html:
        return out
    words = [
        "xem them",
        "xem them binh luan",
        "xem them phan hoi",
        "tat ca binh luan",
        "see more",
        "more comments",
        "view more",
        "replies",
        "more replies",
        "all comments",
        "load more",
        "view previous comments",
    ]
    seen: set[str] = set()
    for m in re.finditer(r'(?is)<a[^>]+href="([^"]+)"[^>]*>(.*?)</a>', html):
        href = html_lib.unescape(str(m.group(1) or "").strip())
        label_raw = _html_to_plain_text(m.group(2) or "")
        label = normalize_match_text(label_raw)
        if not href or not label:
            continue
        if not any(w in label for w in words):
            continue
        full = urljoin("https://mbasic.facebook.com", href)
        if full in seen:
            continue
        seen.add(full)
        out.append(full)
        if len(out) >= 160:
            break
    return out


def extract_fb_comments_via_mbasic(driver, src_url: str, max_hops: int = 28) -> str:
    """
    Crawl mbasic Facebook pages to expand and collect comments text without relying on
    heavy dynamic UI interactions on the normal Facebook surface.
    """
    mbasic_url = to_mbasic_facebook_url(src_url)
    if not mbasic_url:
        return ""

    # 1) Prefer requests + browser cookies (more stable than dynamic clicking).
    if requests is not None:
        try:
            session = requests.Session()
            session.headers.update(
                {
                    "User-Agent": (
                        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                        "AppleWebKit/537.36 (KHTML, like Gecko) "
                        "Chrome/122.0.0.0 Safari/537.36"
                    ),
                    "Accept-Language": "vi,en-US;q=0.9,en;q=0.8",
                }
            )
            try:
                for ck in (driver.get_cookies() or []):
                    n = str((ck or {}).get("name", "")).strip()
                    v = str((ck or {}).get("value", "")).strip()
                    d = str((ck or {}).get("domain", "")).strip() or ".facebook.com"
                    p = str((ck or {}).get("path", "")).strip() or "/"
                    if n:
                        session.cookies.set(n, v, domain=d, path=p)
            except Exception:
                pass

            rq_chunks: list[str] = []
            rq_visited: set[str] = set()
            rq_queue: list[str] = [mbasic_url]
            rq_hop = 0
            while rq_queue and rq_hop < max_hops:
                rq_hop += 1
                cur = rq_queue.pop(0)
                if not cur or cur in rq_visited:
                    continue
                rq_visited.add(cur)
                try:
                    resp = session.get(cur, timeout=20, allow_redirects=True)
                except Exception:
                    continue
                if not resp or resp.status_code >= 400:
                    continue
                html = str(resp.text or "")
                txt = _html_to_plain_text(html)
                if txt:
                    rq_chunks.append(txt[:120000])
                for nxt in _extract_follow_links_mbasic_html(html):
                    if nxt not in rq_visited and nxt not in rq_queue:
                        rq_queue.append(nxt)

            if rq_chunks:
                merged_rq = "\n".join(rq_chunks)
                write_log(f"[SCAN_TEXT] mbasic(requests) collected: pages={len(rq_visited)} chars={len(merged_rq)}")
                if len(merged_rq) >= 500:
                    return merged_rq
                write_log("[SCAN_TEXT][WARN] mbasic(requests) text short, fallback to selenium mbasic.")
        except Exception as e:
            write_log(f"[SCAN_TEXT][WARN] mbasic(requests) failed: {e}")

    # 2) Fallback: selenium-based mbasic traversal.
    chunks: list[str] = []
    visited: set[str] = set()
    queue: list[str] = [mbasic_url]
    hop = 0

    while queue and hop < max_hops:
        hop += 1
        cur = queue.pop(0)
        if not cur or cur in visited:
            continue
        visited.add(cur)
        try:
            driver.get(cur)
            time.sleep(0.7)
        except Exception:
            continue

        txt = _collect_mbasic_visible_text(driver)
        if txt:
            chunks.append(txt[:120000])

        try:
            found = driver.execute_script(
                """
                const words = [
                  'xem them binh luan', 'xem thêm bình luận',
                  'xem them phan hoi', 'xem thêm phản hồi',
                  'xem them', 'xem thêm',
                  'see more comments', 'view more comments',
                  'more comments', 'more replies', 'replies', 'view previous comments',
                  'all comments', 'load more'
                ];
                const norm = (s) => (s || '')
                  .toLowerCase()
                  .normalize('NFD')
                  .replace(/[\\u0300-\\u036f]/g, '')
                  .replace(/\\s+/g, ' ')
                  .trim();
                const out = [];
                const seen = new Set();
                const as = document.querySelectorAll('a[href]');
                for (const a of as) {
                  const t = norm(a.innerText || a.textContent || '');
                  if (!t) continue;
                  let ok = false;
                  for (const w of words) {
                    if (t.includes(norm(w))) { ok = true; break; }
                  }
                  if (!ok) continue;
                  const href = a.getAttribute('href') || '';
                  if (!href) continue;
                  if (seen.has(href)) continue;
                  seen.add(href);
                  out.push({href: href, text: t});
                  if (out.length >= 120) break;
                }
                return out;
                """
            ) or []
        except Exception:
            found = []

        for item in found:
            try:
                href = str((item or {}).get("href", "")).strip()
            except Exception:
                href = ""
            if not href:
                continue
            nxt = urljoin("https://mbasic.facebook.com", href)
            if nxt not in visited and nxt not in queue:
                queue.append(nxt)

    if chunks:
        merged = "\n".join(chunks)
        write_log(f"[SCAN_TEXT] mbasic collected: pages={len(visited)} chars={len(merged)}")
        return merged
    write_log(f"[SCAN_TEXT] mbasic collected empty from: {mbasic_url}")
    return ""


def extract_text_from_link_for_scan(driver, url: str) -> str:
    """
    Extract main textual content from a post/article page for Scan Only Text mode.
    """
    if "facebook.com" in (url or "").lower() or "fb.watch" in (url or "").lower():
        try:
            mbasic_text = extract_fb_comments_via_mbasic(driver, url)
            if len(mbasic_text) >= 500:
                return mbasic_text
            if mbasic_text:
                write_log(
                    f"[SCAN_TEXT][WARN] mbasic text short (len={len(mbasic_text)}), fallback dynamic extraction."
                )
        except Exception as e:
            write_log(f"[SCAN_TEXT][WARN] mbasic extraction failed: {e}")

    try:
        # Expand all "xem them/see more/comments/replies" first, then scan text.
        expand_script = """
            const words = [
              'xem them', 'xem thêm', 'xem them binh luan', 'xem thêm bình luận',
              'xem them phan hoi', 'xem thêm phản hồi', 'tat ca binh luan', 'tất cả bình luận',
              'see more', 'more comments', 'view more', 'view more comments',
              'replies', 'more replies', 'all comments', 'load more'
            ];
            const deny = ['thich', 'like', 'share', 'chia se', 'follow', 'theo doi'];
            const norm = (s) => (s || '')
              .toLowerCase()
              .normalize('NFD')
              .replace(/[\\u0300-\\u036f]/g, '')
              .replace(/\\s+/g, ' ')
              .trim();
            const mayClick = (el) => {
              if (!el) return false;
              const raw = el.innerText || el.textContent || '';
              const t = norm(raw);
              if (!t || t.length < 3 || t.length > 220) return false;
              for (const d of deny) {
                if (t === d || t.startsWith(d + ' ')) return false;
              }
              let hit = false;
              for (const w of words) {
                const wn = norm(w);
                if (t.includes(wn)) { hit = true; break; }
              }
              if (!hit) return false;
              try { el.scrollIntoView({block: 'center'}); } catch (_) {}
              try { el.click(); return true; } catch (_) {}
              try {
                const evt = new MouseEvent('click', {bubbles: true, cancelable: true, view: window});
                el.dispatchEvent(evt);
                return true;
              } catch (_) {}
              return false;
            };
            const nodes = Array.from(document.querySelectorAll('a,button,[role="button"],div,span'));
            let clicked = 0;
            for (const n of nodes) {
              if (clicked >= 240) break;
              if (mayClick(n)) clicked++;
            }
            return clicked;
        """
        total_clicked = 0
        no_click_rounds = 0
        max_rounds = 36
        for _ in range(max_rounds):
            # Click at current viewport first.
            try:
                clicked_1 = int(driver.execute_script(expand_script) or 0)
            except Exception:
                clicked_1 = 0
            total_clicked += max(0, clicked_1)
            if clicked_1 > 0:
                no_click_rounds = 0
            else:
                no_click_rounds += 1

            time.sleep(0.28)
            try:
                m = driver.execute_script(
                    "return {y:(window.pageYOffset||document.documentElement.scrollTop||0),"
                    "vh:(window.innerHeight||0),h:(document.body&&document.body.scrollHeight)||0};"
                ) or {}
                y = float(m.get("y", 0) or 0)
                vh = float(m.get("vh", 0) or 0)
                h = float(m.get("h", 0) or 0)
                at_bottom = (y + vh) >= (h - 8)
            except Exception:
                at_bottom = False

            if at_bottom:
                # At bottom: one more expand pass; if still no click for a while -> done.
                try:
                    clicked_bottom = int(driver.execute_script(expand_script) or 0)
                except Exception:
                    clicked_bottom = 0
                total_clicked += max(0, clicked_bottom)
                if clicked_bottom > 0:
                    no_click_rounds = 0
                    time.sleep(0.25)
                    continue
                if no_click_rounds >= 3:
                    break
                # Rewind then scan downward again to catch delayed loaded controls.
                try:
                    driver.execute_script("window.scrollTo(0, 0);")
                except Exception:
                    pass
                time.sleep(0.35)
                continue

            # Not at bottom: keep scrolling to force comment lazy-load.
            try:
                driver.execute_script("window.scrollBy(0, Math.max(760, Math.floor(window.innerHeight * 0.95)));")
            except Exception:
                pass
            # Also scroll comment containers (FB often uses inner scroll regions).
            try:
                driver.execute_script(
                    """
                    const els = Array.from(document.querySelectorAll('div,section,main,article'));
                    let moved = 0;
                    for (const el of els) {
                      try {
                        const canScroll = el.scrollHeight > (el.clientHeight + 60);
                        if (!canScroll) continue;
                        const oldTop = el.scrollTop;
                        el.scrollTop = Math.min(el.scrollHeight, oldTop + Math.max(700, Math.floor(el.clientHeight * 0.9)));
                        if (el.scrollTop !== oldTop) moved++;
                      } catch (_) {}
                    }
                    return moved;
                    """
                )
            except Exception:
                pass
            time.sleep(0.35)

        write_log(f"[SCAN_TEXT] expand-all before scan: clicks={total_clicked}, rounds={max_rounds}")
    except Exception:
        pass

    chunks = []
    try:
        if "facebook.com" in (url or "").lower() or "fb.watch" in (url or "").lower():
            _p, cap = get_fb_profile_and_caption(driver, url)
            if cap and cap.strip():
                chunks.append(cap.strip())
    except Exception:
        pass
    try:
        if "youtube.com" in (url or "").lower() or "youtu.be" in (url or "").lower():
            title = (get_youtube_title(driver) or "").strip()
            if title:
                chunks.append(title)
    except Exception:
        pass
    try:
        comment_like_texts = driver.execute_script(
            """
            const selectors = [
              '[aria-label*="comment" i]',
              '[aria-label*="bình luận" i]',
              '[data-testid*="comment" i]',
              '[class*="comment" i]',
              'ytd-comment-thread-renderer',
              '[data-e2e*="comment"]',
              '[class*="Comment" i]',
              'div[role="article"] div[dir="auto"]'
            ];
            const out = [];
            const seen = new Set();
            for (const sel of selectors) {
              const nodes = document.querySelectorAll(sel);
              for (const n of nodes) {
                const t = (n && n.innerText) ? n.innerText.trim() : '';
                if (!t) continue;
                if (t.length < 6) continue;
                if (seen.has(t)) continue;
                seen.add(t);
                out.push(t);
                if (out.length >= 800) break;
              }
              if (out.length >= 800) break;
            }
            return out.join('\\n');
            """
        ) or ""
        comment_like_texts = str(comment_like_texts).strip()
        if comment_like_texts:
            chunks.append(comment_like_texts[:180000])
    except Exception:
        pass
    try:
        body_text = (
            driver.execute_script(
                "return (document.body && document.body.innerText) ? document.body.innerText : '';"
            )
            or ""
        )
        body_text = str(body_text).strip()
        if body_text:
            chunks.append(body_text[:180000])
    except Exception:
        pass
    if not chunks:
        return ""
    # De-dup while preserving order.
    out = []
    seen = set()
    for c in chunks:
        key = normalize_match_text(c)[:300]
        if key and key not in seen:
            seen.add(key)
            out.append(c)
    return "\n".join(out)


# ================= LOG =================
def write_log(msg):
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    try:
        with open(LOG_PATH, "a", encoding="utf-8") as f:
            f.write(f"[{ts}] {msg}\n")
    except:
        pass


def _normalize_sheet_key(sheet_url: str) -> str:
    u = (sheet_url or "").strip()
    if not u:
        return ""
    # Prefer stable spreadsheet id from URL: /spreadsheets/d/<id>/
    try:
        parts = u.split("/spreadsheets/d/")
        if len(parts) > 1:
            tail = parts[1]
            sid = tail.split("/", 1)[0].strip()
            if sid:
                return f"sheet_id:{sid}"
    except Exception:
        pass
    return f"sheet_url:{u}"


def _sheet_history_key(sheet_url: str, sheet_name: str = "") -> str:
    # History is tracked by Sheet link (not worksheet name).
    return _normalize_sheet_key(sheet_url)


def load_error_history() -> dict:
    try:
        if not os.path.exists(ERROR_HISTORY_PATH):
            return {}
        with open(ERROR_HISTORY_PATH, "r", encoding="utf-8") as f:
            data = json.load(f) or {}
        if isinstance(data, dict):
            return data
    except Exception as e:
        write_log(f"[WARN] Load error history failed: {e}")
    return {}


def save_error_history(data: dict):
    try:
        with open(ERROR_HISTORY_PATH, "w", encoding="utf-8") as f:
            json.dump(data or {}, f, ensure_ascii=False, indent=2)
    except Exception as e:
        write_log(f"[WARN] Save error history failed: {e}")


def get_error_rows_for_sheet(sheet_url: str, sheet_name: str = "") -> set[int]:
    key = _sheet_history_key(sheet_url, sheet_name)
    db = load_error_history()
    item = db.get(key) or {}
    rows = item.get("rows") or []

    # Backward compatibility: merge old keys that included sheet_name suffix.
    if not rows:
        legacy_prefix = f"{(sheet_url or '').strip()}|"
        merged = []
        for k, v in db.items():
            if isinstance(k, str) and k.startswith(legacy_prefix):
                merged.extend((v or {}).get("rows") or [])
        rows = merged
    out = set()
    for r in rows:
        try:
            rv = int(r)
            if rv >= 1:
                out.add(rv)
        except Exception:
            continue
    return out


def get_error_details_for_sheet(sheet_url: str, sheet_name: str = "") -> dict[int, str]:
    key = _sheet_history_key(sheet_url, sheet_name)
    db = load_error_history()
    item = db.get(key) or {}
    raw = item.get("details") or {}
    out: dict[int, str] = {}
    if isinstance(raw, dict):
        for k, v in raw.items():
            try:
                rk = int(k)
                if rk >= 1:
                    out[rk] = str(v or "").strip()
            except Exception:
                continue
    return out


def set_error_rows_for_sheet(
    sheet_url: str,
    sheet_name: str = "",
    rows: set[int] = None,
    details: dict[int, str] | None = None,
):
    rows = rows or set()
    details = details or {}
    key = _sheet_history_key(sheet_url, sheet_name)
    db = load_error_history()
    sorted_rows = sorted({int(r) for r in rows if int(r) >= 1})
    details_clean = {}
    for r in sorted_rows:
        msg = str(details.get(r, "")).strip()
        if msg:
            details_clean[str(r)] = msg[:220]
    if sorted_rows:
        db[key] = {
            "sheet_url": (sheet_url or "").strip(),
            "sheet_name": (sheet_name or "").strip(),
            "rows": sorted_rows,
            "details": details_clean,
            "updated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        }
    else:
        db.pop(key, None)
    save_error_history(db)


def list_saved_error_sheets() -> list[dict]:
    db = load_error_history()
    items = []
    for _, v in db.items():
        if not isinstance(v, dict):
            continue
        url = str(v.get("sheet_url", "")).strip()
        if not url:
            continue
        rows = v.get("rows") or []
        updated_at = str(v.get("updated_at", "")).strip()
        sheet_name = str(v.get("sheet_name", "")).strip()
        items.append(
            {
                "sheet_url": url,
                "sheet_name": sheet_name,
                "rows_count": len(rows),
                "updated_at": updated_at,
            }
        )
    # Newest first (timestamp format YYYY-MM-DD HH:MM:SS is lexicographically sortable)
    items.sort(key=lambda x: x.get("updated_at", ""), reverse=True)
    return items

# ================= FB COMMENT PARSE =================
def extract_comment_id(url):
    comment_id = None
    if "comment_id=" in url:
        start = url.find("comment_id=") + len("comment_id=")
        end = url.find("&", start)
        if end == -1:
            comment_id = url[start:]
        else:
            comment_id = url[start:end]
    elif "reply_comment_id=" in url:
        start = url.find("reply_comment_id=") + len("reply_comment_id=")
        end = url.find("&", start)
        if end == -1:
            comment_id = url[start:]
        else:
            comment_id = url[start:end]
    return comment_id

def get_highlighted_fb_comment(driver, url):
    time.sleep(FB_COMMENT_READY_WAIT)
    
    comment_id = extract_comment_id(url)
    if comment_id:
        try:
            # Find the exact comment element by comment_id
            comment_element = driver.find_element(By.XPATH, f"//div[contains(@data-ft, '\"comment_id\":\"{comment_id}\"')]")
            # Find the text content within the comment
            text_elements = comment_element.find_elements(By.XPATH, ".//div[@dir='auto']")
            text = ""
            for elem in text_elements:
                t = elem.text.strip()
                if len(t) > 5:
                    text = t
                    break
            if text:
                return text
        except:
            pass
    
    # Fallback: Look for comment by checking highlighted/focused elements
    try:
        # Try to find the most prominent comment text
        all_comment_divs = driver.find_elements(By.XPATH, "//div[@data-testid='comment']")
        if all_comment_divs:
            # Get the first visible comment's text
            for comment_div in all_comment_divs[:3]:  # Check first 3 comments
                try:
                    text_elem = comment_div.find_element(By.XPATH, ".//div[@dir='auto']")
                    text = text_elem.text.strip()
                    if len(text) > 5:
                        return text
                except:
                    continue
    except:
        pass

    # Position-based detection as last resort
    candidates = driver.find_elements(By.XPATH, "//div[@dir='auto']")
    best_text = ""

    for c in candidates:
        try:
            text = c.text.strip()

            if len(text) < 8:
                continue

            # loÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â¡ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚ÂºÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¡i text rÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¡c
            if text.lower() in ["thÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â­ch", "trÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â¡ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚ÂºÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â£ lÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â¡ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â»ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Âi", "xem thÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Âªm", "tiÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â¡ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚ÂºÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¿p tÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â¡ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â»ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¥c", "tÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â¡ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚ÂºÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â£i thÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Âªm"]:
                continue

            rect = driver.execute_script("""
                const r = arguments[0].getBoundingClientRect();
                return {top: r.top, bottom: r.bottom};
            """, c)

            # comment ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¾ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬Ãƒâ€¹Ã…â€œÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã‚Â ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â°ÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â¡ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â»ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â£c highlight thÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã‚Â ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â°ÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â¡ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â»ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Âng nÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â¡ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚ÂºÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â±m gÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â¡ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚ÂºÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â§n ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¾ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬Ãƒâ€¹Ã…â€œÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â¡ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚ÂºÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â§u viewport
            if rect["top"] > 120 and rect["top"] < 450:
                best_text = text
                break

        except:
            continue

    return best_text

# ================= TIKTOK CAPTION =================
def get_tiktok_caption(driver):
    caption = ""
    
    # Try meta tags first (og:description) - TikTok should have this
    try:
        metas = driver.find_elements(By.TAG_NAME, "meta")
        for m in metas:
            prop = m.get_attribute("property")
            if prop == "og:description":
                caption = m.get_attribute("content") or ""
                if caption:
                    return caption.strip()
    except:
        pass
    
    # If not found from meta, try to get from DOM
    if not caption:
        try:
            # Look for description text in TikTok DOM - multiple selectors
            selectors = [
                "//span[@data-e2e='video-desc']",
                "//div[@data-testid='video-desc']//span",
                "//h2//span",
                "//h1//span",
            ]
            for selector in selectors:
                desc_elements = driver.find_elements(By.XPATH, selector)
                for elem in desc_elements:
                    text = elem.text.strip()
                    if len(text) > 5 and len(text) < 10000:  # TikTok captions can be long
                        caption = text
                        return caption.strip()
        except:
            pass
    
    return caption


def get_tiktok_profile_name(driver, source_url: str = "") -> str:
    # Resolve handle from final URL/source URL first.
    expected_handle = ""
    try:
        current_url = (driver.current_url or "").strip()
    except Exception:
        current_url = ""
    for candidate_url in [current_url, source_url]:
        h = extract_account_name_from_url(candidate_url)
        if h:
            expected_handle = h.strip()
            break

    # 1) Try JSON-LD metadata (usually ties directly to current video author).
    try:
        scripts = driver.find_elements(By.XPATH, "//script[@type='application/ld+json']")
        for s in scripts:
            raw = (s.get_attribute("textContent") or "").strip()
            if not raw:
                continue
            try:
                data = json.loads(raw)
            except Exception:
                continue

            objs = data if isinstance(data, list) else [data]
            for obj in objs:
                if not isinstance(obj, dict):
                    continue
                author = obj.get("author")
                if isinstance(author, list):
                    author = author[0] if author else {}
                if not isinstance(author, dict):
                    continue

                display = clean_account_name_candidate(str(author.get("name", "")).strip())
                alt = str(author.get("alternateName", "")).strip()
                alt_handle = alt if alt.startswith("@") else ""
                if not alt_handle and alt and alt.startswith("@"):
                    alt_handle = alt

                # If we know expected handle, only trust matching author blocks.
                if expected_handle and alt_handle and alt_handle != expected_handle:
                    continue

                if display and not display.startswith("@") and is_likely_account_name(display):
                    return display
    except Exception:
        pass

    # 2) Prefer display name (nickname) but only when paired with expected handle.
    try:
        selectors = [
            "//*[@data-e2e='video-author-nickname']",
            "//*[@data-e2e='browse-user-nickname']",
            "//h3[contains(@data-e2e,'nickname')]",
            "//h2[contains(@data-e2e,'nickname')]",
        ]
        for sel in selectors:
            elems = driver.find_elements(By.XPATH, sel)
            for e in elems:
                txt = clean_account_name_candidate((e.text or "").strip())
                if not (txt and not txt.startswith("@") and is_likely_account_name(txt)):
                    continue

                if expected_handle:
                    try:
                        nearby = driver.execute_script(
                            """
                            const el = arguments[0];
                            const root = el.closest('article, section, div') || el.parentElement;
                            return (root && root.innerText) ? root.innerText : '';
                            """,
                            e,
                        ) or ""
                        if expected_handle not in nearby:
                            continue
                    except Exception:
                        continue

                    return txt
    except Exception:
        pass

    # 3) Meta fallback: "<display name> on TikTok"
    try:
        metas = driver.find_elements(By.TAG_NAME, "meta")
        for m in metas:
            if m.get_attribute("property") == "og:title":
                title = (m.get_attribute("content") or "").strip()
                if title:
                    lowered = title.lower()
                    marker = " on tiktok"
                    if marker in lowered:
                        title = title[:lowered.find(marker)].strip()
                    txt = clean_account_name_candidate(title)
                    if txt and not txt.startswith("@") and is_likely_account_name(txt):
                        return txt
    except Exception:
        pass

    # 4) Fallback: resolved URL after redirects (@handle)
    for candidate_url in [current_url, source_url]:
        uname = extract_account_name_from_url(candidate_url)
        if uname:
            return uname.strip()

    # 5) DOM fallback: profile links that point to /@username
    try:
        links = driver.find_elements(By.XPATH, "//a[contains(@href, '/@')]")
        for a in links:
            href = (a.get_attribute("href") or "").strip()
            uname = extract_account_name_from_url(href)
            if uname:
                return uname.strip()
            txt = (a.text or "").strip()
            if txt.startswith("@") and len(txt) > 1:
                return txt
    except Exception:
        pass

    return ""


# ================= YOUTUBE TITLE =================
def get_youtube_title(driver):
    title = ""
    try:
        # Try og:title meta
        metas = driver.find_elements(By.TAG_NAME, "meta")
        for m in metas:
            if m.get_attribute("property") == "og:title":
                title = m.get_attribute("content") or ""
                if title:
                    return title.strip()
    except:
        pass

    try:
        # Fallback to document title
        t = driver.title or driver.execute_script("return document.title")
        if t:
            return t.strip()
    except:
        pass

    return title


def get_youtube_channel(driver):
    channel = ""
    try:
        # Try meta article:author first (often has channel name)
        metas = driver.find_elements(By.TAG_NAME, "meta")
        for m in metas:
            prop = m.get_attribute("property") or m.get_attribute("name") or ""
            if "author" in prop.lower():
                channel = m.get_attribute("content") or ""
                if channel and len(channel) > 2:
                    return channel.strip()
    except:
        pass

    try:
        # Try og:site_name
        metas = driver.find_elements(By.TAG_NAME, "meta")
        for m in metas:
            if m.get_attribute("property") == "og:site_name" or m.get_attribute("name") == "og:site_name":
                channel = m.get_attribute("content") or ""
                if channel and len(channel) > 2:
                    return channel.strip()
    except:
        pass

    try:
        # Try to extract from page title (usually has channel: | Uploaded by channel)
        title = driver.title or ""
        if " - " in title:
            parts = title.split(" - ")
            if len(parts) > 1:
                potential_channel = parts[-1].strip()
                if len(potential_channel) > 2 and len(potential_channel) < 100:
                    return potential_channel
    except:
        pass

    try:
        # Fallback: look for channel link/button in header
        channel_link = driver.find_element(By.XPATH, "//a[contains(@href, 'youtube.com/@') or contains(@href, '/channel/') or contains(@href, '/user/')][1]")
        t = channel_link.text.strip()
        if t and len(t) > 2:
            return t
    except:
        pass

    return channel


# ================= NAME CLEAN =================
def clean_fb_profile_name(name: str) -> str:
    if not name:
        return name
    n = name.strip()

    def _strip_phrase(original: str, phrase: str) -> str:
        # Normalize to ASCII for matching while keeping original for slicing
        norm = ""
        mapping = []
        for i, ch in enumerate(original):
            decomp = unicodedata.normalize("NFD", ch)
            for dc in decomp:
                if unicodedata.category(dc) == "Mn":
                    continue
                norm += dc
                mapping.append(i)
        norm_lower = norm.lower()
        phrase_lower = phrase.lower()
        idx = norm_lower.find(phrase_lower)
        if idx != -1:
            end_norm = idx + len(phrase_lower)
            end_orig = mapping[end_norm - 1] + 1
            return original[end_orig:].strip(" :-")
        return original

    n2 = _strip_phrase(n, "bai viet cua")
    if n2 != n:
        return n2.strip()

    return n


def is_likely_account_name(text: str) -> bool:
    if not text:
        return False
    t = text.strip()
    if not t:
        return False
    if "\n" in t or "\r" in t:
        return False
    if len(t) > 80:
        return False

    lower = t.lower()
    noise_markers = [
        " views", " view", " reactions", " reaction", " comments", " comment",
        " shares", " share", " like", " thich", " binh luan", " xem them",
        "http://", "https://", "www.", "#"
    ]
    if any(m in lower for m in noise_markers):
        return False
    return True


def clean_account_name_candidate(text: str) -> str:
    t = (text or "").strip()
    if not t:
        return ""
    t = t.splitlines()[0].strip()
    for sep in [" · ", "Â·", " Â· ", "â€¢", "•", "|", " - "]:
        if sep in t:
            t = t.split(sep, 1)[0].strip()
    t = t.strip(":- ")
    t = clean_fb_profile_name(t)
    return t.strip()


def is_numeric_like_account_name(name: str) -> bool:
    t = (name or "").strip().lstrip("@")
    if not t:
        return False
    if t.startswith("profile_") and t[8:].isdigit():
        return True
    return t.isdigit() and len(t) >= 5


def extract_account_name_from_title(title: str) -> str:
    t = clean_account_name_candidate(title)
    if not t:
        return ""
    if t.lower() in {"facebook", "instagram", "tiktok", "youtube"}:
        return ""
    return t


def extract_account_name_from_url(url: str) -> str:
    if not url:
        return ""
    try:
        parsed = urlparse(url)
    except Exception:
        return ""

    host = (parsed.netloc or "").lower()
    path_parts = [p for p in (parsed.path or "").split("/") if p]
    query = parse_qs(parsed.query or "")

    # TikTok: /@username/video/...
    if "tiktok.com" in host:
        for p in path_parts:
            if p.startswith("@") and len(p) > 1:
                return p

    # Instagram: /username/p/... or /username/reel/...
    if "instagram.com" in host or "instagr.am" in host:
        if path_parts:
            first = path_parts[0]
            if first not in {"p", "reel", "tv", "stories", "explore"}:
                return first

    # YouTube: /@handle, /channel/<id>, /user/<name>, /c/<name>
    if "youtube.com" in host or "youtu.be" in host:
        if path_parts:
            first = path_parts[0]
            if first.startswith("@") and len(first) > 1:
                return first
            if first in {"channel", "user", "c"} and len(path_parts) > 1:
                return path_parts[1]

    # Facebook common paths
    if "facebook.com" in host or "fb.watch" in host:
        target = (query.get("u") or [""])[0].strip()
        if target:
            nested = extract_account_name_from_url(target)
            if nested:
                return nested

        if path_parts:
            first = path_parts[0]
            reserved = {
                "watch", "reel", "reels", "story.php", "permalink.php",
                "photo", "photos", "photo.php", "groups", "events", "share", "plugins",
                "login", "hashtag"
            }
            if first == "profile.php":
                pid = (query.get("id") or [""])[0].strip()
                if pid:
                    return f"profile_{pid}"
            if first == "people" and len(path_parts) > 1:
                return path_parts[1]
            if first not in reserved:
                return first

    return ""


def normalize_account_name(name: str, url: str) -> str:
    n = clean_account_name_candidate(name)
    if n and not is_likely_account_name(n):
        n = ""
    if n and is_numeric_like_account_name(n):
        n = ""
    if not n:
        n = clean_account_name_candidate(extract_account_name_from_url(url))
    if n and is_numeric_like_account_name(n):
        n = ""
    return n.strip()


def get_post_caption(driver):

    caption = ""

    # Try to expand "See more" / "Xem thÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Âªm" buttons to reveal full caption
    try:
        buttons = driver.find_elements(By.XPATH, "//div[contains(text(), 'Xem') or contains(text(), 'See more')]")
        for b in buttons:
            try:
                driver.execute_script("arguments[0].click();", b)
                time.sleep(UI_CLICK_SETTLE_SLEEP)
            except:
                continue
    except:
        pass

    try:
        driver.execute_script("window.scrollTo(0, 600);")
        time.sleep(UI_SCROLL_SETTLE_SLEEP)
    except:
        pass


    # Strategy 0: Facebook main caption container (data-ad-preview="message")
    try:
        elems = driver.find_elements(By.XPATH, "//div[@data-ad-preview='message']//div[@dir='auto'] | //div[@data-ad-preview='message']")
        for elem in elems:
            text = elem.text.strip()
            if len(text) > 5 and len(text) < 5000:
                return text
    except:
        pass

    # Strategy 0.5: Reel / main caption areas
    try:
        selectors = [
            "//div[@data-testid='post_message']//div[@dir='auto']",
            "//div[@data-pagelet='Reel']//div[@dir='auto']",
            "//div[@role='main']//div[@dir='auto']",
        ]
        noise_words = ["thich", "tra loi", "xem", "tiep tuc", "chia se", "binh luan", "tai xuong",
                       "like", "comment", "see more", "show more", "share", "download"]
        for selector in selectors:
            elems = driver.find_elements(By.XPATH, selector)
            for elem in elems:
                text = elem.text.strip()
                text_lower = text.lower()
                if (len(text) > 10 and len(text) < 5000 and text_lower not in noise_words and
                    "facebook.com" not in text_lower and not text.startswith("http") and not text.startswith("www.")):
                    return text
    except:
        pass

    # Strategy 1: Look for shared article/link content first (for shared posts)
    try:
        shared_selectors = [
            "//div[@data-testid='share_content']//div[@dir='auto']",
            "//div[contains(@class, 'xwib8y')]//div[@dir='auto']",
            "//div[contains(@class, 'x1iyjqo2')]//h2//span | //div[contains(@class, 'x1iyjqo2')]//div[@dir='auto']",
            "//div[@role='article']//a[contains(@href, 'facebook.com') or contains(@href, 'l.facebook.com')]//parent::*//*[@dir='auto']",
            "//div[@data-testid='message']//div[@dir='auto']",
        ]

        for selector in shared_selectors:
            try:
                elems = driver.find_elements(By.XPATH, selector)
                for elem in elems:
                    text = elem.text.strip()
                    if len(text) > 10 and len(text) < 5000:
                        caption = text
                        break
                if caption:
                    return caption
            except:
                continue
    except:
        pass

    # Strategy 2: Extract from the main article container text
    try:
        article = None
        # try several article selectors
        try:
            article = driver.find_element(By.XPATH, "//article")
        except:
            try:
                article = driver.find_element(By.XPATH, "//div[@role='article']")
            except:
                article = None

        if article:
            full_text = article.text or ""
            lines = [l.strip() for l in full_text.split('\n') if l.strip()]
            # filter noise and pick the longest reasonable line
            candidates = [l for l in lines if len(l) > 10 and 'facebook.com' not in l.lower() and 'see more' not in l.lower()]
            if candidates:
                caption = max(candidates, key=len)
                return caption
    except:
        pass

    # Strategy 3: Look for all div[@dir='auto'] and filter intelligently
    try:
        all_divs = driver.find_elements(By.XPATH, "//div[@dir='auto']")
        candidates = []
        noise_words = ["thÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â­ch", "trÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â¡ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚ÂºÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â£ lÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â¡ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â»ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Âi", "xem thÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Âªm", "tiÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â¡ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚ÂºÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¿p tÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â¡ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â»ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¥c", "chia sÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â¡ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚ÂºÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â»", "bÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¬nh luÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â¡ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚ÂºÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â­n", "tÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â¡ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚ÂºÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â£i xuÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â¡ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â»ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬Ãƒâ€¹Ã…â€œng",
                       "like", "comment", "see more", "show more", "share", "download"]
        for div in all_divs:
            text = div.text.strip()
            text_lower = text.lower()
            if (len(text) > 10 and len(text) < 5000 and text_lower not in noise_words and
                "facebook.com" not in text_lower and not text.startswith("http") and not text.startswith("www.")):
                candidates.append(text)

        if candidates:
            candidates.sort(key=len, reverse=True)
            for c in candidates:
                if 12 < len(c) < 2000:
                    caption = c
                    return caption
    except:
        pass

    # Strategy 4: Fallback to meta tags
    try:
        metas = driver.find_elements(By.TAG_NAME, "meta")
        for m in metas:
            prop = m.get_attribute("property")
            if prop == "og:description":
                caption = m.get_attribute("content") or ""
                if caption:
                    return caption
    except:
        pass

    return caption


# ================= INSTAGRAM PARSE =================
def get_instagram_profile_and_caption(driver, url):
    name = ""
    caption = ""
    og_title = ""
    og_desc = ""

    try:
        metas = driver.find_elements(By.TAG_NAME, "meta")
        for m in metas:
            prop = m.get_attribute("property") or m.get_attribute("name") or ""
            if prop == "og:title":
                og_title = m.get_attribute("content") or ""
            if prop in ("og:description", "description"):
                og_desc = m.get_attribute("content") or ""
    except:
        pass

    # Try to parse profile from URL path
    try:
        if "instagram.com/" in url:
            path_part = url.split("instagram.com/", 1)[1]
            path_part = path_part.split("?", 1)[0]
            first = path_part.strip("/").split("/")[0]
            if first and first not in ("p", "reel", "tv", "stories", "explore"):
                name = first
    except:
        pass

    # Try to parse display name from og:title
    if not name and og_title:
        t = og_title
        if " on Instagram" in t:
            name = t.split(" on Instagram", 1)[0].strip()
        elif "Instagram:" in t:
            name = t.split("Instagram:", 1)[0].strip()
        elif "Instagram" in t:
            name_part = t.split("Instagram", 1)[0].strip(" -|")
            name = name_part.split("(", 1)[0].strip() or name_part

    # Caption from og:description
    if og_desc:
        c = og_desc
        if "on Instagram:" in c:
            c = c.split("on Instagram:", 1)[1].strip()
        elif "Instagram:" in c:
            c = c.split("Instagram:", 1)[1].strip()
        caption = c.strip("\"' " )

    name = normalize_account_name(name, url)
    return name, caption.strip()

# ================= FB PARSE =================
def get_facebook_actor_name(driver) -> str:
    candidates = []
    selectors = [
        # Common actor/title link areas on Facebook posts/reels
        "//h2//a | //h3//a | //strong//a",
        "//div[@role='article']//a[contains(@href,'facebook.com') or contains(@href,'/profile.php') or contains(@href,'/people/') or contains(@href,'/reel/')]/span",
        "//a[contains(@href,'/profile.php') or contains(@href,'/people/')]/span",
    ]
    for xp in selectors:
        try:
            elems = driver.find_elements(By.XPATH, xp)
            for e in elems:
                t = (e.text or "").strip()
                if t:
                    candidates.append(t)
        except:
            pass

    # aria-label fallback
    try:
        elems = driver.find_elements(By.XPATH, "//a[@aria-label]")
        for e in elems[:80]:
            t = (e.get_attribute("aria-label") or "").strip()
            if t:
                candidates.append(t)
    except:
        pass

    noise = {"Thích", "Bình luận", "Chia sẻ", "Like", "Comment", "Share", "Follow", "Theo dõi"}
    for c in candidates:
        cc = clean_account_name_candidate(c)
        if 2 < len(cc) < 80 and cc not in noise and is_likely_account_name(cc):
            return cc
    return ""


def get_fb_profile_and_caption(driver, url):
    name = ""
    caption = ""

    url_l = (url or "").lower()
    # Only treat as comment when link contains the word "comment"
    is_comment = "comment" in url_l
    is_tiktok = "tiktok.com" in url_l or "vt.tiktok.com" in url_l
    is_instagram = "instagram.com" in url_l or "instagr.am" in url_l
    is_facebook = ("facebook.com" in url_l) or ("fb.watch" in url_l) or ("m.facebook.com" in url_l)

    # ===== TIKTOK MODE =====
    if is_tiktok:
        caption = get_tiktok_caption(driver)
        # Prefer resolved URL/profile link over broad "@..." DOM text to avoid wrong mentions.
        name = get_tiktok_profile_name(driver, url)

    # ===== INSTAGRAM MODE =====
    elif is_instagram:
        name, caption = get_instagram_profile_and_caption(driver, url)

    # ===== FACEBOOK MODE =====
    elif is_facebook:
        name = get_facebook_actor_name(driver)
        if is_comment:
            caption = get_highlighted_fb_comment(driver, url)
        else:
            caption = get_post_caption(driver)

    # ===== FALLBACK MODE =====
    if not caption:
        try:
            metas = driver.find_elements(By.TAG_NAME, "meta")
            for m in metas:
                prop = m.get_attribute("property")
                if prop == "og:description" and not caption:
                    caption = m.get_attribute("content") or ""
                if prop == "og:title" and not name:
                    name = m.get_attribute("content") or ""
        except:
            pass

    # ===== PROFILE NAME FALLBACK =====
    if not name:
        try:
            elems = driver.find_elements(By.XPATH, "//h2//span | //strong//span")
            for e in elems:
                t = e.text.strip()
                if 2 < len(t) < 60:
                    name = t
                    break
        except:
            pass

    if not name and is_facebook:
        name = get_facebook_actor_name(driver)

    if not name:
        try:
            name = extract_account_name_from_title(driver.title or "")
        except:
            pass

    name = normalize_account_name(name, url)
    return name.strip(), caption.strip()


def get_fb_post_datetime(driver):
    post_time = ""

    try:
        elems = driver.find_elements(By.XPATH, "//abbr | //time")
        for e in elems:
            dt_attr = (e.get_attribute("datetime") or "").strip()
            if dt_attr:
                post_time = dt_attr
                break
            t = e.get_attribute("title")
            if t:
                post_time = t
                break
    except:
        pass

    if not post_time:
        try:
            metas = driver.find_elements(By.TAG_NAME, "meta")
            for m in metas:
                prop = (m.get_attribute("property") or "").strip().lower()
                name = (m.get_attribute("name") or "").strip().lower()
                content = (m.get_attribute("content") or "").strip()
                if not content:
                    continue
                if prop in {"article:published_time", "og:updated_time"}:
                    post_time = content
                    break
                if name in {"pubdate", "publishdate", "date", "datepublished"}:
                    post_time = content
                    break
        except:
            pass

    return post_time.strip()


def get_air_date_token(post_time: str) -> str:
    raw = (post_time or "").strip()
    if not raw:
        return ""

    try:
        dt = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        return dt.strftime("%Y%m%d")
    except Exception:
        pass

    m = re.search(r"(20\d{2})[/-](\d{1,2})[/-](\d{1,2})", raw)
    if m:
        y, mo, d = int(m.group(1)), int(m.group(2)), int(m.group(3))
        try:
            return datetime(y, mo, d).strftime("%Y%m%d")
        except Exception:
            pass

    m = re.search(r"(\d{1,2})[/-](\d{1,2})[/-](20\d{2})", raw)
    if m:
        d, mo, y = int(m.group(1)), int(m.group(2)), int(m.group(3))
        try:
            return datetime(y, mo, d).strftime("%Y%m%d")
        except Exception:
            pass

    return ""


def detect_platform_label(url: str) -> str:
    u = (url or "").lower()
    if "tiktok.com" in u or "vt.tiktok.com" in u:
        return "TikTok"
    if "instagram.com" in u or "instagr.am" in u:
        return "Instagram"
    if "youtube.com" in u or "youtu.be" in u:
        return "YouTube"
    if "facebook.com" in u or "fb.watch" in u or "m.facebook.com" in u:
        return "Facebook"
    return "Other"


def sanitize_filename_token(text: str, fallback: str = "Unknown", max_len: int = 64) -> str:
    t = (text or "").strip()
    if not t:
        return fallback
    t = re.sub(r'[\\/:*?"<>|]+', "_", t)
    t = re.sub(r"\s+", "_", t)
    t = t.strip("._- ")
    if not t:
        return fallback
    return t[:max_len]


def is_unavailable_content_page(driver, source_url: str = "") -> bool:
    """
    Detect pages that opened but content is unavailable/private/deleted.
    This prevents saving blank/blocked screenshots as successful rows.
    """
    try:
        txt_raw = (driver.execute_script("return (document.body && document.body.innerText) ? document.body.innerText : ''") or "")
    except Exception:
        txt_raw = ""
    txt = str(txt_raw or "").lower()
    txt_norm = normalize_match_text(txt_raw or "")
    try:
        cur = (driver.current_url or "").lower()
    except Exception:
        cur = ""
    url = (source_url or "").lower()

    markers_raw = [
        "bạn hiện không xem được nội dung này",
        "không xem được nội dung này",
        "nội dung này hiện không khả dụng",
        "nội dung không khả dụng",
        "không có nội dung",
        "bài viết này hiện không còn",
        "bài viết này không còn khả dụng",
        "trang này hiện không khả dụng",
        "liên kết này có thể đã bị hỏng",
        "nội dung này đã bị gỡ",
        "video này hiện không khả dụng",
        "khong xem duoc noi dung nay",
        "noi dung khong kha dung",
        "khong co noi dung",
        "this content isn't available",
        "this page isn't available",
        "content isn't available right now",
        "this post is no longer available",
        "the page isn't available",
        "this video is unavailable",
        "no content available",
        "you cannot view this content",
    ]
    markers_norm = [normalize_match_text(m) for m in markers_raw]
    if any((m in txt) or (normalize_match_text(m) in txt_norm) for m in markers_raw):
        return True
    if any(mn and mn in txt_norm for mn in markers_norm):
        return True

    # Common Facebook dead-end routes.
    if "facebook.com" in (url + cur):
        dead_routes = ["/checkpoint/", "/login/", "/recover/"]
        if any(r in cur for r in dead_routes):
            return True

    return False


def write_colored_xlsx_builtin(path: str, headers: list[str], rows_with_tags: list[tuple[list, list]]):
    """
    Create a minimal .xlsx with row background colors using only stdlib.
    Styles:
    - 0: default
    - 1: ok
    - 2: fail
    - 3: unavailable
    """
    def col_name(idx: int) -> str:
        n = idx + 1
        out = ""
        while n > 0:
            n, r = divmod(n - 1, 26)
            out = chr(65 + r) + out
        return out

    def style_id_for_row(vals: list, tags: list) -> int:
        tag_set = set(tags or [])
        state = str(vals[3]).strip().upper() if len(vals) > 3 else ""
        msg = str(vals[4]).lower() if len(vals) > 4 else ""
        if "fail" in tag_set or state == "FAIL":
            return 2
        if "unavailable" in tag_set or "nội dung không khả dụng" in msg:
            return 3
        if "ok" in tag_set or state == "OK":
            return 1
        return 0

    rows_xml = []
    # Header row
    header_cells = []
    for c, h in enumerate(headers):
        ref = f"{col_name(c)}1"
        header_cells.append(f'<c r="{ref}" t="inlineStr"><is><t>{xml_escape(str(h))}</t></is></c>')
    rows_xml.append(f'<row r="1">{"".join(header_cells)}</row>')

    # Data rows
    for i, (vals, tags) in enumerate(rows_with_tags, start=2):
        s_id = style_id_for_row(vals, tags)
        cells = []
        for c, v in enumerate(vals):
            ref = f"{col_name(c)}{i}"
            txt = xml_escape(str(v))
            if s_id > 0:
                cells.append(f'<c r="{ref}" s="{s_id}" t="inlineStr"><is><t>{txt}</t></is></c>')
            else:
                cells.append(f'<c r="{ref}" t="inlineStr"><is><t>{txt}</t></is></c>')
        rows_xml.append(f'<row r="{i}">{"".join(cells)}</row>')

    sheet_xml = (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main">'
        f'<sheetData>{"".join(rows_xml)}</sheetData>'
        '</worksheet>'
    )

    styles_xml = """<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<styleSheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main">
  <fonts count="1"><font><sz val="11"/><name val="Calibri"/></font></fonts>
  <fills count="5">
    <fill><patternFill patternType="none"/></fill>
    <fill><patternFill patternType="gray125"/></fill>
    <fill><patternFill patternType="solid"><fgColor rgb="FFD8F3DC"/><bgColor indexed="64"/></patternFill></fill>
    <fill><patternFill patternType="solid"><fgColor rgb="FFFFD9D9"/><bgColor indexed="64"/></patternFill></fill>
    <fill><patternFill patternType="solid"><fgColor rgb="FFFFE6C7"/><bgColor indexed="64"/></patternFill></fill>
  </fills>
  <borders count="1"><border><left/><right/><top/><bottom/><diagonal/></border></borders>
  <cellStyleXfs count="1"><xf numFmtId="0" fontId="0" fillId="0" borderId="0"/></cellStyleXfs>
  <cellXfs count="4">
    <xf numFmtId="0" fontId="0" fillId="0" borderId="0" xfId="0"/>
    <xf numFmtId="0" fontId="0" fillId="2" borderId="0" xfId="0" applyFill="1"/>
    <xf numFmtId="0" fontId="0" fillId="3" borderId="0" xfId="0" applyFill="1"/>
    <xf numFmtId="0" fontId="0" fillId="4" borderId="0" xfId="0" applyFill="1"/>
  </cellXfs>
  <cellStyles count="1"><cellStyle name="Normal" xfId="0" builtinId="0"/></cellStyles>
</styleSheet>
"""

    workbook_xml = """<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<workbook xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main" xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships">
  <sheets><sheet name="Log" sheetId="1" r:id="rId1"/></sheets>
</workbook>
"""

    rels_root = """<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">
  <Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument" Target="xl/workbook.xml"/>
</Relationships>
"""

    rels_workbook = """<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">
  <Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet" Target="worksheets/sheet1.xml"/>
  <Relationship Id="rId2" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/styles" Target="styles.xml"/>
</Relationships>
"""

    content_types = """<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types">
  <Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/>
  <Default Extension="xml" ContentType="application/xml"/>
  <Override PartName="/xl/workbook.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml"/>
  <Override PartName="/xl/worksheets/sheet1.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml"/>
  <Override PartName="/xl/styles.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.styles+xml"/>
</Types>
"""

    with zipfile.ZipFile(path, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("[Content_Types].xml", content_types)
        zf.writestr("_rels/.rels", rels_root)
        zf.writestr("xl/workbook.xml", workbook_xml)
        zf.writestr("xl/_rels/workbook.xml.rels", rels_workbook)
        zf.writestr("xl/styles.xml", styles_xml)
        zf.writestr("xl/worksheets/sheet1.xml", sheet_xml)

# ================= UI =================
class ProgressApp:
    def __init__(self, root):
        self.root = root
        self.root.title("Evidence Tool")
        self.root.geometry(self._get_initial_geometry())
        screen_w = self.root.winfo_screenwidth()
        screen_h = self.root.winfo_screenheight()
        min_w = min(1220, max(980, screen_w - 140))
        min_h = min(860, max(720, screen_h - 180))
        self.root.minsize(min_w, min_h)
        self.root.configure(bg="#f3f4f7")
        self.is_running = True
        self.is_paused = False
        self.driver = None

        self.main_canvas = tk.Canvas(self.root, bg="#f3f4f7", highlightthickness=0)
        self.v_scrollbar = ttk.Scrollbar(self.root, orient="vertical", command=self.main_canvas.yview)
        self.main_canvas.configure(yscrollcommand=self.v_scrollbar.set)
        self.v_scrollbar.pack(side="right", fill="y")
        self.main_canvas.pack(side="left", fill="both", expand=True)

        self.main_frame = tk.Frame(self.main_canvas, bg="#ffffff")
        self.canvas_window = self.main_canvas.create_window((0, 0), window=self.main_frame, anchor="nw")
        self.main_frame.bind(
            "<Configure>",
            lambda e: self.main_canvas.configure(scrollregion=self.main_canvas.bbox("all"))
        )
        self.main_canvas.bind("<Configure>", self._on_canvas_configure)
        self._bind_scroll_events()

        self.force_run_all = tk.BooleanVar(value=False)
        self.only_run_error_rows = tk.BooleanVar(value=False)
        self.auto_launch_chrome = tk.BooleanVar(value=True)
        self.capture_five_per_link = tk.BooleanVar(value=False)
        self.mapping_mode_var = tk.StringVar(value="Seeding")
        self.sheet_url_var = tk.StringVar(value=DEFAULT_SHEET_URL)
        self.sheet_name_var = tk.StringVar(value=DEFAULT_SHEET_NAME_TARGET)
        self.drive_id_var = tk.StringVar(value=DEFAULT_DRIVE_FOLDER_ID)
        self.credentials_path_var = tk.StringVar(value=get_default_credentials_input())
        self.mapping_blocks = []
        self.mapping_blocks_by_mode: dict[str, list[dict]] = {}
        self._active_mapping_mode = "Seeding"
        self._is_loading_settings = False
        self.mapping_entries = []
        self.mapping_remove_buttons = []
        self.mapping_launch_buttons = []
        self.chk_capture5 = None
        self.btn_add_block = None
        self.load_settings()
        if not self.mapping_blocks:
            self._ensure_default_mapping_blocks()
        self._build_menu()
        self.main_frame.configure(bg="#f3f4f7")

        # Header
        header = tk.Frame(self.main_frame, bg="#f7f7fa", relief="ridge", bd=1, padx=10, pady=6)
        header.pack(fill="x", padx=12, pady=(8, 8))

        self.label_status = tk.Label(
            header, text="● STATUS: READY",
            font=("Arial", 10, "bold"),
            bg="#f7f7fa", fg="#2e7d32", anchor="w"
        )
        self.label_status.pack(side="left")

        self.reload_btn = tk.Button(
            header, text="⟳", command=self.reload_app,
            width=4, bg="#eeeeee", fg="#444444"
        )
        self.reload_btn.pack(side="right", padx=(4, 0))

        self.pause_btn = tk.Button(
            header, text="⏸", command=self.toggle_pause,
            width=4, bg="#fff3cd", fg="#ff6b6b", state="disabled"
        )
        self.pause_btn.pack(side="right", padx=(4, 0))

        self.save_btn = tk.Button(
            header, text="Save Config", command=self.save_settings,
            width=10, bg="#e6f4ea", fg="#137333"
        )
        self.save_btn.pack(side="right", padx=(4, 0))

        # Content area
        content = tk.Frame(self.main_frame, bg="#f3f4f7")
        content.pack(fill="x", padx=12, pady=4)
        content.grid_columnconfigure(0, weight=1)
        content.grid_columnconfigure(1, weight=1)

        left_card = tk.LabelFrame(content, text="DATA SOURCE", bg="#f7f7fa", fg="#4a4a4a", padx=8, pady=8)
        left_card.grid(row=0, column=0, sticky="nsew", padx=(0, 6))
        right_card = tk.LabelFrame(content, text="COLUMN MAPPING", bg="#f7f7fa", fg="#4a4a4a", padx=8, pady=8)
        right_card.grid(row=0, column=1, sticky="nsew", padx=(6, 0))

        def add_source_row(parent, r, label, text_var, btn_var_name=None):
            tk.Label(parent, text=label, bg="#f7f7fa", anchor="w", width=12).grid(row=r, column=0, sticky="w", pady=2)
            ent = tk.Entry(parent, textvariable=text_var, width=34)
            ent.grid(row=r, column=1, sticky="ew", padx=4, pady=2)
            if btn_var_name:
                btn = tk.Button(parent, text="DÁN", width=6, command=lambda v=text_var: self.paste_to(v))
                btn.grid(row=r, column=2, pady=2)
                setattr(self, btn_var_name, btn)
            return ent

        left_card.grid_columnconfigure(1, weight=1)
        self.entry_sheet_url = add_source_row(left_card, 0, "Sheet URL", self.sheet_url_var, "btn_paste_sheet_url")
        self.entry_sheet_name = add_source_row(left_card, 1, "Sheet Name", self.sheet_name_var, "btn_paste_sheet_name")
        self.entry_drive_id = add_source_row(left_card, 2, "Drive Folder", self.drive_id_var, "btn_paste_drive_id")
        self.entry_credentials_path = add_source_row(left_card, 3, "Credentials", self.credentials_path_var, None)

        mode_row = tk.Frame(right_card, bg="#f7f7fa")
        mode_row.pack(fill="x", pady=(0, 4))
        tk.Label(mode_row, text="Mode:", bg="#f7f7fa", anchor="w", width=12).pack(side="left")
        self.mapping_mode_combo = ttk.Combobox(
            mode_row,
            textvariable=self.mapping_mode_var,
            values=("Seeding", "Booking", "Scan"),
            state="readonly",
            width=14,
        )
        self.mapping_mode_combo.pack(side="left")
        self.mapping_mode_combo.bind("<<ComboboxSelected>>", self._on_mode_changed)
        self.mapping_header = tk.Frame(right_card, bg="#f7f7fa")
        self.mapping_header.pack(fill="x", pady=(0, 6))
        self.mapping_grid = tk.Frame(right_card, bg="#f7f7fa")
        self.mapping_grid.pack(fill="x")
        self._render_mapping_blocks()

        run_mode = tk.LabelFrame(content, text="RUN MODE", bg="#f7f7fa", fg="#4a4a4a", padx=8, pady=8)
        run_mode.grid(row=1, column=0, columnspan=2, sticky="ew", pady=(10, 2), padx=0)
        self.checkbox = tk.Checkbutton(
            run_mode, text="Run All (overwrite)", variable=self.force_run_all,
            bg="#f7f7fa", anchor="w"
        )
        self.checkbox.pack(anchor="w")
        self.checkbox_errors_only = tk.Checkbutton(
            run_mode, text="Retry Failed Only", variable=self.only_run_error_rows,
            bg="#f7f7fa", anchor="w"
        )
        self.checkbox_errors_only.pack(anchor="w")

        self.action_row = tk.Frame(run_mode, bg="#f7f7fa")
        self.action_row.pack(fill="x", pady=(8, 0))

        self.start_btn = tk.Button(
            self.action_row, text="▶ RUN", command=self.start_processing,
            width=11,
            bg="#2f80ed",
            fg="#ffffff",
            activebackground="#2f80ed",
            activeforeground="#ffffff",
            disabledforeground="#ffffff",
            relief="raised",
            overrelief="raised",
            bd=1,
            highlightthickness=0,
        )
        self.start_btn.pack(side="left", padx=2)

        self.export_log_btn = tk.Button(
            self.action_row, text="Export Log", command=self.export_live_log_excel,
            width=10, bg="#e8f0fe", fg="#1a3d8f"
        )
        self.export_log_btn.pack(side="left", padx=2)

        # Share + Progress
        share_frame = tk.Frame(self.main_frame, bg="#eef3ff", relief="ridge", bd=1, padx=8, pady=6)
        share_frame.pack(fill="x", pady=8, padx=12)
        tk.Label(
            share_frame, text="Chia sẻ Sheet & Drive folder cho (quyền Editor):",
            bg="#eef3ff", fg="#1a73e8", font=("Arial", 9, "bold"), anchor="w"
        ).pack(anchor="w")
        email = get_service_account_email(self.credentials_path_var.get().strip()) or "link-verification@hazel-tea-485816-u3.iam.gserviceaccount.com"
        self.share_email_var = tk.StringVar(value=email)
        row_share = tk.Frame(share_frame, bg="#eef3ff")
        row_share.pack(fill="x", pady=4)
        self.entry_share_email = tk.Entry(row_share, textvariable=self.share_email_var, state="readonly")
        self.entry_share_email.pack(side="left", fill="x", expand=True, padx=(0, 4))
        tk.Button(row_share, text="Copy", width=6, command=lambda: self.copy_share_email()).pack(side="left")

        self.error_card = tk.Frame(self.main_frame, bg="#f7f7fa", relief="ridge", bd=1)
        self.error_card.pack(fill="x", padx=12, pady=(0, 8))

        self.error_card_header = tk.Frame(self.error_card, bg="#f7f7fa")
        self.error_card_header.pack(fill="x", padx=8, pady=(6, 4))

        self.error_header_var = tk.StringVar(value="⚠ Lỗi theo link Sheet: chưa có")
        self.error_header_label = tk.Label(
            self.error_card_header,
            textvariable=self.error_header_var,
            font=("Arial", 11, "bold"),
            bg="#f7f7fa",
            fg="#2f3b52",
            anchor="w",
        )
        self.error_header_label.pack(side="left", fill="x", expand=True)

        self.error_card_save_btn = tk.Button(
            self.error_card_header,
            text="💾 Save",
            width=9,
            command=self._save_error_history_current_sheet,
            bg="#eef2ff",
            fg="#2f3b52",
        )
        self.error_card_save_btn.pack(side="right")

        self.error_card_clear_btn = tk.Button(
            self.error_card_header,
            text="🗑 Xóa",
            width=9,
            command=self._clear_current_sheet_error_history,
            bg="#ffe9e9",
            fg="#8a1c1c",
        )
        self.error_card_clear_btn.pack(side="right", padx=(0, 6))

        self.error_rows_var = tk.StringVar(value="• Chưa có dòng lỗi đã lưu.")
        self.error_rows_frame = tk.Frame(self.error_card, bg="#f7f7fa")
        self.error_rows_frame.pack(fill="x", padx=14, pady=(0, 6))
        self.error_rows_text = tk.Text(
            self.error_rows_frame,
            height=5,
            wrap="word",
            font=("Arial", 10),
            bg="#f7f7fa",
            fg="#5f6673",
            relief="flat",
            borderwidth=0,
            highlightthickness=0,
        )
        self.error_rows_text.pack(side="left", fill="both", expand=True)
        self.error_rows_scroll = ttk.Scrollbar(
            self.error_rows_frame,
            orient="vertical",
            command=self.error_rows_text.yview,
        )
        self.error_rows_scroll.pack(side="right", fill="y")
        self.error_rows_text.configure(yscrollcommand=self.error_rows_scroll.set)
        self.error_rows_text.insert("1.0", self.error_rows_var.get())
        self.error_rows_text.config(state="disabled")

        self.error_sep = tk.Frame(self.error_card, bg="#d9dde7", height=1)
        self.error_sep.pack(fill="x", padx=10, pady=(0, 6))

        self.progress_summary_var = tk.StringVar(value="✔ Progress: 0/0 | Success: 0 | Failed: 0 | ETA: ---")
        self.progress_summary_label = tk.Label(
            self.error_card,
            textvariable=self.progress_summary_var,
            font=("Arial", 10, "bold"),
            bg="#f7f7fa",
            fg="#2f3b52",
            anchor="w",
        )
        self.progress_summary_label.pack(fill="x", padx=14, pady=(0, 4))

        self.progress = ttk.Progressbar(self.error_card, orient="horizontal", length=560, mode="determinate")
        self.progress.pack(pady=(0, 8), padx=12, fill="x")

        self.live_log_frame = tk.Frame(self.main_frame, bg="#f3f4f7")
        self.live_log_frame.pack(fill="both", padx=12, pady=(6, 6), expand=False)
        self.live_log_table = ttk.Treeview(
            self.live_log_frame,
            columns=("time", "row", "s1", "s2", "msg"),
            show="headings",
            height=6,
        )
        self.live_log_table.heading("time", text="Time")
        self.live_log_table.heading("row", text="#")
        self.live_log_table.heading("s1", text="State")
        self.live_log_table.heading("s2", text="Result")
        self.live_log_table.heading("msg", text="Message")
        self.live_log_table.column("time", width=74, anchor="w")
        self.live_log_table.column("row", width=44, anchor="center")
        self.live_log_table.column("s1", width=72, anchor="center")
        self.live_log_table.column("s2", width=72, anchor="center")
        self.live_log_table.column("msg", width=360, anchor="w")
        self.live_log_table.tag_configure("start")
        self.live_log_table.tag_configure("ok", background="#d8f3dc", foreground="#1f3a2a")
        self.live_log_table.tag_configure("unavailable", background="#ffe6c7", foreground="#4a3820")
        self.live_log_table.tag_configure("fail", background="#ffd9d9", foreground="#4a1f1f")
        self.live_log_table.pack(side="left", fill="x", expand=True)
        self.live_log_scroll = ttk.Scrollbar(self.live_log_frame, orient="vertical", command=self.live_log_table.yview)
        self.live_log_table.configure(yscrollcommand=self.live_log_scroll.set)
        self.live_log_scroll.pack(side="right", fill="y")

        self.label_detail = tk.Label(
            self.main_frame, text="No activity yet...",
            font=("Arial", 9),
            bg="#f3f4f7", fg="#777777", wraplength=560, anchor="w", justify="left"
        )
        self.label_detail.pack(fill="x", padx=12, pady=(0, 8))

        self.exit_btn = tk.Button(self.main_frame, text="THOÁT", command=self.exit_app, width=12, bg="#f0f0f0")
        self.exit_btn.pack(pady=8)

        # Keep this list for menu history actions
        self._history_sheet_items = []
        self.live_error_details: dict[int, str] = {}

        self.sheet_url_var.trace_add("write", lambda *_: self.refresh_error_history_ui())
        self.sheet_name_var.trace_add("write", lambda *_: self.refresh_error_history_ui())
        self.refresh_error_history_ui()
        self.refresh_saved_sheets_list()

    def _new_block_vars(self, data: dict | None = None) -> dict:
        d = data or {}
        return {
            "name_var": tk.StringVar(value=str(d.get("name", "Post")).strip() or "Post"),
            "manual_link_var": tk.StringVar(value=str(d.get("manual_link", "")).strip()),
            "col_profile_var": tk.StringVar(value=str(d.get("col_profile", "")).strip().upper()),
            "col_content_var": tk.StringVar(value=str(d.get("col_content", "")).strip().upper()),
            "col_url_var": tk.StringVar(value=str(d.get("col_url", "")).strip().upper()),
            "col_drive_var": tk.StringVar(value=str(d.get("col_drive", "")).strip().upper()),
            "col_screenshot_var": tk.StringVar(value=str(d.get("col_screenshot", "")).strip().upper()),
            "col_air_date_var": tk.StringVar(value=str(d.get("col_air_date", "")).strip().upper()),
            "start_line_var": tk.StringVar(value=str(d.get("start_line", "4")).strip() or "4"),
        }

    def _ensure_default_mapping_blocks(self):
        defaults = [
            {"name": "Post 1", "col_profile": "C", "col_content": "D", "col_url": "E", "col_drive": "F", "col_screenshot": "G", "col_air_date": "", "start_line": "4"},
        ]
        self.mapping_blocks = [self._new_block_vars(x) for x in defaults]

    def _normalize_mode_name(self, mode_text: str) -> str:
        s = str(mode_text or "").strip().lower()
        if s in ("scan only text", "scan_only_text", "scan text", "text scan"):
            return "Scan"
        if s == "scan":
            return "Scan"
        if s == "booking":
            return "Booking"
        return "Seeding"

    def _default_mapping_configs_for_mode(self, mode_name: str) -> list[dict]:
        mode = self._normalize_mode_name(mode_name)
        if mode == "Scan":
            return [
                {
                    "name": "Scan 1",
                    "manual_link": "",
                    "col_profile": "",
                    "col_content": "E",
                    "col_url": "F",
                    "col_drive": "G",
                    "col_screenshot": "",
                    "col_air_date": "",
                    "start_line": "4",
                }
            ]
        return [
            {
                "name": "Post 1",
                "col_profile": "C",
                "col_content": "D",
                "col_url": "E",
                "col_drive": "F",
                "col_screenshot": "G",
                "col_air_date": "",
                "start_line": "4",
            }
        ]

    def _snapshot_current_mode_configs(self):
        mode = self._normalize_mode_name(getattr(self, "_active_mapping_mode", self.mapping_mode_var.get()))
        self.mapping_blocks_by_mode[mode] = self.get_mapping_configs()

    def _on_mode_changed(self, _event=None):
        new_mode = self._normalize_mode_name(self.mapping_mode_var.get())
        if self._is_loading_settings:
            self._active_mapping_mode = new_mode
            self._render_mapping_blocks()
            return
        self._snapshot_current_mode_configs()
        if not self.mapping_blocks_by_mode.get(new_mode):
            self.mapping_blocks_by_mode[new_mode] = self._default_mapping_configs_for_mode(new_mode)
        self._active_mapping_mode = new_mode
        self._load_mapping_blocks(self.mapping_blocks_by_mode.get(new_mode) or [], render=True)

    def _add_mapping_block(self, seed: dict | None = None):
        idx = len(self.mapping_blocks) + 1
        mode_name = self._normalize_mode_name(self.mapping_mode_var.get())
        if mode_name == "Scan":
            block_seed = {
                "name": f"Scan {idx}",
                "manual_link": "",
                "col_profile": "",
                "col_content": "",
                "col_url": "",
                "col_drive": "",
                "col_screenshot": "",
                "col_air_date": "",
                "start_line": "4",
            }
        else:
            block_seed = {"name": f"Post {idx}", "start_line": "4"}
        if seed:
            block_seed.update(seed)
        self.mapping_blocks.append(self._new_block_vars(block_seed))
        self._render_mapping_blocks()

    def _get_block_port(self, idx: int) -> int:
        return get_post_port(idx, 9223)

    def _get_block_profile(self, idx: int) -> str:
        if idx <= 0:
            return LOCAL_PROFILE_PATH
        return os.path.join(TEMP_DIR, f"chrome_profile_worker_{idx}")

    def launch_chrome_for_block(self, idx: int):
        port = self._get_block_port(idx)
        profile = self._get_block_profile(idx)
        try:
            os.makedirs(profile, exist_ok=True)
        except Exception:
            pass
        ok, info = launch_chrome_for_login(port, profile_path=profile)
        block_name = f"Post {idx + 1}"
        if 0 <= idx < len(self.mapping_blocks):
            block_name = (self.mapping_blocks[idx]["name_var"].get() or block_name).strip() or block_name
        if ok:
            messagebox.showinfo(
                "Chrome đã mở",
                f"{block_name} mở Chrome ở port {port}.\n\nBạn đăng nhập xong rồi bấm RUN."
            )
        else:
            messagebox.showerror("Lỗi", f"Không mở được Chrome cho {block_name}: {info}")

    def _remove_mapping_block(self, idx: int):
        if len(self.mapping_blocks) <= 1:
            return
        if 0 <= idx < len(self.mapping_blocks):
            self.mapping_blocks.pop(idx)
        self._render_mapping_blocks()

    def _pick_air_date_for_block(self, idx: int, anchor_widget=None):
        if tk is None:
            return
        if not (0 <= idx < len(self.mapping_blocks)):
            return
        block = self.mapping_blocks[idx]
        var = block.get("col_air_date_var")
        if var is None:
            return

        now = datetime.now()
        current = str(var.get() or "").strip()
        parsed = get_air_date_token(current)
        selected_token = ""
        if parsed and len(parsed) == 8 and parsed.isdigit():
            try:
                now = datetime(int(parsed[:4]), int(parsed[4:6]), int(parsed[6:8]))
                selected_token = parsed
            except Exception:
                pass
        if not selected_token:
            selected_token = now.strftime("%Y%m%d")

        win = tk.Toplevel(self.root)
        win.title("Chọn Air Date")
        win.resizable(False, False)
        win.transient(self.root)

        frm = tk.Frame(win, padx=10, pady=10, bg="#ffffff")
        frm.pack(fill="both", expand=True)

        cursor_year = now.year
        cursor_month = now.month
        month_text = tk.StringVar()

        header = tk.Frame(frm, bg="#ffffff")
        header.pack(fill="x")

        weekday_row = tk.Frame(frm, bg="#ffffff")
        weekday_row.pack(fill="x", pady=(6, 2))

        days_frame = tk.Frame(frm, bg="#ffffff")
        days_frame.pack(fill="both", expand=True)

        footer = tk.Frame(frm, bg="#ffffff")
        footer.pack(fill="x", pady=(8, 0))

        def _set_date(y: int, m: int, d: int):
            var.set(f"{y:04d}-{m:02d}-{d:02d}")
            win.destroy()

        def _clear():
            var.set("")
            win.destroy()

        def _today():
            dt = datetime.now()
            _set_date(dt.year, dt.month, dt.day)

        def _render_calendar():
            month_text.set(f"Tháng {cursor_month:02d}/{cursor_year}")

            for c in days_frame.winfo_children():
                c.destroy()

            first_weekday, days_in_month = calendar.monthrange(cursor_year, cursor_month)
            day = 1
            for r in range(6):
                for c in range(7):
                    cell = r * 7 + c
                    if cell < first_weekday or day > days_in_month:
                        tk.Label(days_frame, text=" ", width=4, bg="#ffffff").grid(row=r, column=c, padx=1, pady=1)
                    else:
                        token = f"{cursor_year:04d}{cursor_month:02d}{day:02d}"
                        bg = "#2f80ed" if token == selected_token else "#f5f5f5"
                        fg = "#ffffff" if token == selected_token else "#222222"
                        btn = tk.Button(
                            days_frame,
                            text=str(day),
                            width=4,
                            bg=bg,
                            fg=fg,
                            relief="flat",
                            command=lambda dd=day: _set_date(cursor_year, cursor_month, dd),
                        )
                        btn.grid(row=r, column=c, padx=1, pady=1)
                        day += 1

        def _prev_month():
            nonlocal cursor_year, cursor_month
            cursor_month -= 1
            if cursor_month < 1:
                cursor_month = 12
                cursor_year -= 1
            _render_calendar()

        def _next_month():
            nonlocal cursor_year, cursor_month
            cursor_month += 1
            if cursor_month > 12:
                cursor_month = 1
                cursor_year += 1
            _render_calendar()

        tk.Button(header, text="◀", width=4, command=_prev_month).pack(side="left")
        tk.Label(header, textvariable=month_text, bg="#ffffff", font=("Arial", 10, "bold")).pack(side="left", expand=True)
        tk.Button(header, text="▶", width=4, command=_next_month).pack(side="right")

        for i, wd in enumerate(["T2", "T3", "T4", "T5", "T6", "T7", "CN"]):
            tk.Label(weekday_row, text=wd, width=4, bg="#ffffff", fg="#555555").grid(row=0, column=i, padx=1)

        tk.Button(footer, text="Xóa", width=8, command=_clear).pack(side="left")
        tk.Button(footer, text="Hôm nay", width=8, command=_today).pack(side="left", padx=(6, 0))
        tk.Button(footer, text="Đóng", width=8, command=win.destroy).pack(side="right")

        _render_calendar()

        # Place popup next to clicked button for quicker date picking.
        try:
            win.update_idletasks()
            popup_w = max(260, win.winfo_width())
            popup_h = max(220, win.winfo_height())
            screen_w = win.winfo_screenwidth()
            screen_h = win.winfo_screenheight()

            if anchor_widget is not None and anchor_widget.winfo_exists():
                ax = anchor_widget.winfo_rootx()
                ay = anchor_widget.winfo_rooty()
                aw = anchor_widget.winfo_width()
                ah = anchor_widget.winfo_height()
                x = ax + aw + 6
                y = ay
                # If right edge overflows, show on the left side of button.
                if x + popup_w > screen_w - 8:
                    x = max(8, ax - popup_w - 6)
                # Keep popup inside vertical bounds.
                if y + popup_h > screen_h - 8:
                    y = max(8, ay + ah - popup_h)
            else:
                # Fallback to cursor-near placement.
                x = min(max(8, self.root.winfo_pointerx() + 8), max(8, screen_w - popup_w - 8))
                y = min(max(8, self.root.winfo_pointery() - 10), max(8, screen_h - popup_h - 8))

            win.geometry(f"+{x}+{y}")
        except Exception:
            pass

        win.grab_set()

    def _render_mapping_blocks(self):
        for child in self.mapping_header.winfo_children():
            child.destroy()
        for child in self.mapping_grid.winfo_children():
            child.destroy()

        # Reset persisted grid column layout from previous mode renders
        # (especially Scan mode, which uses weighted columns).
        for i in range(0, 24):
            try:
                self.mapping_header.grid_columnconfigure(i, minsize=0, weight=0, uniform="")
            except Exception:
                pass
            try:
                self.mapping_grid.grid_columnconfigure(i, minsize=0, weight=0, uniform="")
            except Exception:
                pass

        self.mapping_entries = []
        self.mapping_remove_buttons = []
        self.mapping_launch_buttons = []
        self.chk_capture5 = None
        self.btn_add_block = None
        mode_name = (self.mapping_mode_var.get() or "Seeding").strip().lower()
        is_scan_mode = mode_name == "scan"
        is_scan_like_mode = is_scan_mode
        render_blocks = [(i, b) for i, b in enumerate(self.mapping_blocks)]

        self.mapping_header.grid_columnconfigure(0, minsize=96)
        self.mapping_grid.grid_columnconfigure(0, minsize=96)
        for col_idx in range(len(render_blocks)):
            self.mapping_header.grid_columnconfigure(col_idx + 1, minsize=96)
            self.mapping_grid.grid_columnconfigure(col_idx + 1, minsize=96)

        if is_scan_like_mode:
            self.mapping_header.pack_forget()
            cards_per_row = 2
            for c in range(cards_per_row):
                self.mapping_grid.grid_columnconfigure(c, weight=1, uniform="scan_cards")
            for view_idx, (block_idx, block) in enumerate(render_blocks):
                old_result_col = (block["col_screenshot_var"].get() or "").strip().upper()
                new_result_col = (block["col_drive_var"].get() or "").strip().upper()
                if (not new_result_col) and old_result_col:
                    block["col_drive_var"].set(old_result_col)

                card = tk.LabelFrame(
                    self.mapping_grid,
                    text=f"Scan {view_idx + 1}",
                    bg="#f7f7fa",
                    fg="#4a4a4a",
                    padx=8,
                    pady=6,
                )
                grid_row = view_idx // cards_per_row
                grid_col = view_idx % cards_per_row
                card.grid(row=grid_row, column=grid_col, sticky="nsew", padx=(0, 4), pady=(0, 6))
                card.grid_columnconfigure(0, minsize=88)
                card.grid_columnconfigure(1, minsize=130)

                labels = [
                    ("Tên Post", "name_var"),
                    ("Text Column", "col_content_var"),
                    ("Image Column", "col_url_var"),
                    ("Result Column", "col_drive_var"),
                    ("Start Line", "start_line_var"),
                ]
                for row_idx, (label_text, key_name) in enumerate(labels):
                    tk.Label(card, text=label_text, bg="#f7f7fa", anchor="w", width=12).grid(
                        row=row_idx, column=0, sticky="w", padx=(2, 4), pady=2
                    )
                    ent = tk.Entry(card, textvariable=block[key_name], width=12)
                    ent.grid(row=row_idx, column=1, sticky="w", padx=(0, 4), pady=2)
                    self.mapping_entries.append(ent)

                if len(render_blocks) > 1:
                    rm_btn = tk.Button(
                        card,
                        text="−",
                        width=2,
                        bg="#f8d7da",
                        fg="#9d2026",
                        command=lambda x=block_idx: self._remove_mapping_block(x),
                    )
                    rm_btn.place(relx=1.0, rely=0.0, anchor="ne", x=-6, y=6)
                    self.mapping_remove_buttons.append(rm_btn)

            self.btn_add_block = tk.Button(
                self.mapping_grid,
                text="+ Thêm Block",
                width=14,
                bg="#d9edf7",
                command=self._add_mapping_block,
            )
            button_row = (len(render_blocks) + cards_per_row - 1) // cards_per_row
            self.btn_add_block.grid(row=button_row, column=0, sticky="w", pady=(0, 2))
            return
        else:
            self.mapping_header.pack_forget()
            target_grid = tk.Frame(
                self.mapping_grid,
                bg="#f7f7fa",
                relief="groove",
                bd=1,
                padx=8,
                pady=6,
            )
            target_grid.grid(row=0, column=0, sticky="w", pady=(0, 4))
            target_grid.grid_columnconfigure(0, minsize=96)
            for col_idx in range(len(render_blocks)):
                target_grid.grid_columnconfigure(col_idx + 1, minsize=96)
            tk.Label(target_grid, text="Tên Post", bg="#f7f7fa", anchor="w", width=12).grid(
                row=0, column=0, sticky="w", pady=2
            )
            for block_idx, block in render_blocks:
                col_frame = tk.Frame(target_grid, bg="#f7f7fa")
                col_frame.grid(row=0, column=block_idx + 1, sticky="w", padx=2, pady=2)
                name_entry = tk.Entry(col_frame, textvariable=block["name_var"], width=12, justify="center")
                name_entry.grid(row=0, column=1, padx=(0, 2))
                rm_btn = tk.Button(
                    col_frame,
                    text="−",
                    width=2,
                    bg="#f8d7da",
                    fg="#9d2026",
                    command=lambda x=block_idx: self._remove_mapping_block(x),
                )
                rm_btn.grid(row=0, column=2)
                self.mapping_entries.append(name_entry)
                self.mapping_remove_buttons.append(rm_btn)

            self.btn_add_block = tk.Button(
                self.mapping_grid,
                text="+ Thêm Block",
                width=14,
                bg="#d9edf7",
                command=self._add_mapping_block,
            )
            self.btn_add_block.grid(row=1, column=0, sticky="w", pady=(0, 2))
            if mode_name == "booking":
                self.mapping_grid.grid_columnconfigure(1, weight=1)
                self.chk_capture5 = tk.Checkbutton(
                    self.mapping_grid,
                    text="Chụp 5 tấm / 1 link",
                    variable=self.capture_five_per_link,
                    bg="#f7f7fa",
                    anchor="w",
                )
                self.chk_capture5.grid(row=1, column=1, sticky="e", padx=(6, 2), pady=(0, 2))
            if mode_name == "seeding":
                labels = [
                    ("Air Date", "col_air_date_var"),
                    ("Link URL", "col_url_var"),
                    ("Drive URL", "col_drive_var"),
                    ("Screenshot", "col_screenshot_var"),
                    ("Start Line", "start_line_var"),
                ]
            else:
                labels = [
                    ("Air Date", "col_air_date_var"),
                    ("Profile", "col_profile_var"),
                    ("Content", "col_content_var"),
                    ("Link URL", "col_url_var"),
                    ("Drive URL", "col_drive_var"),
                    ("Screenshot", "col_screenshot_var"),
                    ("Start Line", "start_line_var"),
                ]
            show_chrome_row = True
            row_offset = 1

        for row_idx, (label_text, key_name) in enumerate(labels):
            grid_row = row_idx + row_offset
            tk.Label(target_grid, text=label_text, bg="#f7f7fa", anchor="w", width=12).grid(row=grid_row, column=0, sticky="w", pady=2)
            for view_col, (block_idx, block) in enumerate(render_blocks, start=1):
                if key_name == "col_air_date_var":
                    cell = tk.Frame(target_grid, bg="#f7f7fa")
                    cell.grid(row=grid_row, column=view_col, sticky="w", padx=2, pady=2)
                    ent = tk.Entry(cell, textvariable=block[key_name], width=11)
                    ent.pack(side="left")
                    btn = tk.Button(cell, text="...", width=3)
                    btn.configure(command=lambda x=block_idx, w=btn: self._pick_air_date_for_block(x, w))
                    btn.pack(side="left", padx=(2, 0))
                    self.mapping_entries.append(ent)
                    self.mapping_entries.append(btn)
                else:
                    ent = tk.Entry(target_grid, textvariable=block[key_name], width=11)
                    ent.grid(row=grid_row, column=view_col, sticky="w", padx=2, pady=2)
                    self.mapping_entries.append(ent)

        if show_chrome_row:
            chrome_row_idx = len(labels) + row_offset
            tk.Label(target_grid, text="Chrome", bg="#f7f7fa", anchor="w", width=12).grid(
                row=chrome_row_idx, column=0, sticky="w", pady=2
            )
            for view_col, (block_idx, _block) in enumerate(render_blocks, start=1):
                launch_btn = tk.Button(
                    target_grid,
                    text=f"Chrome {self._get_block_port(block_idx)}",
                    width=12,
                    bg="#d9edf7",
                    command=lambda x=block_idx: self.launch_chrome_for_block(x),
                )
                launch_btn.grid(row=chrome_row_idx, column=view_col, sticky="w", padx=2, pady=2)
                self.mapping_launch_buttons.append(launch_btn)

    def _build_menu(self):
        menubar = tk.Menu(self.root)

        menu_file = tk.Menu(menubar, tearoff=0)
        menu_file.add_command(label="Lưu cấu hình", command=self.save_settings)
        menu_file.add_command(label="Tải lại app", command=self.reload_app)
        menu_file.add_separator()
        menu_file.add_command(label="Thoát", command=self.exit_app)
        menubar.add_cascade(label="Tệp", menu=menu_file)

        menu_run = tk.Menu(menubar, tearoff=0)
        menu_run.add_command(label="Bắt đầu", command=self.start_processing)
        menu_run.add_command(label="Chạy lại các dòng lỗi", command=self.start_processing_error_rows)
        menu_run.add_command(label="Tạm dừng / Tiếp tục", command=self.toggle_pause)
        menu_run.add_command(label="Launch Chrome", command=self.launch_chrome_for_login)
        menubar.add_cascade(label="Chạy", menu=menu_run)

        menu_error = tk.Menu(menubar, tearoff=0)
        menu_error.add_checkbutton(
            label="Chỉ chạy các dòng lỗi đã lưu",
            variable=self.only_run_error_rows,
            onvalue=True,
            offvalue=False,
        )
        menu_error.add_command(label="Lưu lịch sử dòng lỗi", command=self._save_error_history_current_sheet)
        menu_error.add_command(label="Xuất bảng log ra Excel", command=self.export_live_log_excel)
        self.menu_error_saved_sheets = tk.Menu(menu_error, tearoff=0)
        menu_error.add_cascade(label="Danh sách sheet lỗi đã lưu", menu=self.menu_error_saved_sheets)
        menu_error.add_separator()
        menu_error.add_command(label="Xóa lịch sử dòng lỗi", command=self._clear_error_history)
        menubar.add_cascade(label="Lỗi", menu=menu_error)

        menu_help = tk.Menu(menubar, tearoff=0)
        menu_help.add_command(label="Mở log.txt", command=lambda: self._open_path(LOG_PATH))
        menu_help.add_command(label="Mở app_settings.json", command=lambda: self._open_path(SETTINGS_PATH))
        menu_help.add_command(label="Mở error_history.json", command=lambda: self._open_path(ERROR_HISTORY_PATH))
        menubar.add_cascade(label="Hỗ trợ", menu=menu_help)

        self.root.config(menu=menubar)
        self.menubar = menubar
        self.refresh_saved_sheets_list()

    def _open_path(self, path: str):
        try:
            if not os.path.exists(path):
                with open(path, "a", encoding="utf-8"):
                    pass
            os.startfile(path)  # type: ignore[attr-defined]
        except Exception as e:
            if messagebox:
                messagebox.showerror("Lỗi mở file", str(e))
            write_log(f"[WARN] Open path failed: {path} -> {e}")

    def _clear_error_history(self):
        try:
            save_error_history({})
            self.refresh_error_history_ui()
            if messagebox:
                messagebox.showinfo("Đã xóa", "Đã xóa lịch sử các dòng lỗi.")
        except Exception as e:
            if messagebox:
                messagebox.showerror("Lỗi", str(e))

    def _clear_current_sheet_error_history(self):
        try:
            sheet_url = self.sheet_url_var.get().strip()
            sheet_name = self.sheet_name_var.get().strip()
            if not sheet_url:
                if messagebox:
                    messagebox.showwarning("Thiếu Sheet URL", "Bạn chưa nhập Sheet URL.")
                return
            set_error_rows_for_sheet(sheet_url, sheet_name=sheet_name, rows=set(), details={})
            self.live_error_details = {}
            self.refresh_error_history_ui()
            if messagebox:
                messagebox.showinfo("Đã xóa", "Đã xóa lịch sử lỗi của sheet hiện tại.")
        except Exception as e:
            if messagebox:
                messagebox.showerror("Lỗi", str(e))

    def _save_error_history_current_sheet(self):
        try:
            sheet_url = self.sheet_url_var.get().strip()
            sheet_name = self.sheet_name_var.get().strip()
            if not sheet_url:
                if messagebox:
                    messagebox.showwarning("Thiếu Sheet URL", "Bạn chưa nhập Sheet URL.")
                return
            rows = get_error_rows_for_sheet(sheet_url)
            details = get_error_details_for_sheet(sheet_url)
            if getattr(self, "live_error_details", None):
                details.update({int(k): str(v) for k, v in self.live_error_details.items()})
                rows = set(rows) | set(details.keys())
            # If app history is empty, scan current sheet for ERR markers.
            if not rows:
                rows = self._collect_error_rows_from_sheet()
                details = self._collect_error_details_from_sheet(rows)
            # Force write/update timestamp for current sheet key.
            set_error_rows_for_sheet(sheet_url, sheet_name=sheet_name, rows=rows, details=details)
            self.refresh_error_history_ui()
            if messagebox:
                messagebox.showinfo("Đã lưu", f"Đã lưu lịch sử lỗi cho link Sheet hiện tại ({len(rows)} dòng).")
        except Exception as e:
            if messagebox:
                messagebox.showerror("Lỗi", str(e))

    def _collect_error_rows_from_sheet(self) -> set[int]:
        rows: set[int] = set()
        try:
            sheet_url = self.sheet_url_var.get().strip()
            sheet_name = self.sheet_name_var.get().strip()
            if not sheet_url or not sheet_name:
                return rows

            mapping_blocks = self.get_mapping_configs()
            scan_cols = []
            for block in mapping_blocks:
                idx_content = col_letter_to_index((block.get("col_content") or "").strip().upper())
                idx_drive = col_letter_to_index((block.get("col_drive") or "").strip().upper())
                for c in [idx_content, idx_drive]:
                    if c:
                        scan_cols.append(c)
            scan_cols = sorted(set(scan_cols))
            if not scan_cols:
                return rows

            try:
                starts = []
                for block in mapping_blocks:
                    try:
                        starts.append(int(str(block.get("start_line", "4")).strip() or "4"))
                    except Exception:
                        continue
                start_line = min(starts) if starts else 4
            except Exception:
                start_line = 4

            if not os.path.exists(JSON_PATH):
                write_log(f"[WARN] _collect_error_rows_from_sheet: credentials not found at {JSON_PATH}")
                return rows

            creds = ServiceAccountCredentials.from_json_keyfile_name(
                JSON_PATH,
                [
                    "https://spreadsheets.google.com/feeds",
                    "https://www.googleapis.com/auth/drive",
                ],
            )
            client = gspread.authorize(creds)
            worksheet = client.open_by_url(sheet_url).worksheet(sheet_name)

            for col_idx in scan_cols:
                vals = worksheet.col_values(col_idx)
                for r in range(start_line, len(vals) + 1):
                    v = str(vals[r - 1]).strip().upper()
                    if v.startswith("ERR"):
                        rows.add(r)
        except Exception as e:
            write_log(f"[WARN] _collect_error_rows_from_sheet failed: {e}")
        return rows

    def _collect_error_details_from_sheet(self, rows: set[int]) -> dict[int, str]:
        details: dict[int, str] = {}
        try:
            if not rows:
                return details
            sheet_url = self.sheet_url_var.get().strip()
            sheet_name = self.sheet_name_var.get().strip()
            if not sheet_url or not sheet_name:
                return details

            mapping_blocks = self.get_mapping_configs()
            scan_cols = []
            for block in mapping_blocks:
                idx_content = col_letter_to_index((block.get("col_content") or "").strip().upper())
                idx_drive = col_letter_to_index((block.get("col_drive") or "").strip().upper())
                for c in [idx_content, idx_drive]:
                    if c:
                        scan_cols.append(c)
            scan_cols = sorted(set(scan_cols))
            if not scan_cols:
                return details

            if not os.path.exists(JSON_PATH):
                return details

            creds = ServiceAccountCredentials.from_json_keyfile_name(
                JSON_PATH,
                [
                    "https://spreadsheets.google.com/feeds",
                    "https://www.googleapis.com/auth/drive",
                ],
            )
            client = gspread.authorize(creds)
            worksheet = client.open_by_url(sheet_url).worksheet(sheet_name)

            cols_data = {}
            for col_idx in scan_cols:
                try:
                    cols_data[col_idx] = worksheet.col_values(col_idx)
                except Exception:
                    cols_data[col_idx] = []

            for r in sorted(rows):
                msg = ""
                for col_idx in scan_cols:
                    vals = cols_data.get(col_idx, [])
                    if r - 1 < len(vals):
                        v = str(vals[r - 1]).strip()
                        if v.upper().startswith("ERR"):
                            msg = v
                            break
                if msg:
                    details[r] = msg
        except Exception as e:
            write_log(f"[WARN] _collect_error_details_from_sheet failed: {e}")
        return details

    def refresh_error_history_ui(self):
        try:
            sheet_url = self.sheet_url_var.get().strip()
            rows = sorted(get_error_rows_for_sheet(sheet_url))
            details = get_error_details_for_sheet(sheet_url)
            # Initialize live view from saved history when switching sheet/reloading.
            self.live_error_details = {int(r): str(details.get(r, "")).strip() for r in rows}
            self._render_error_history_card(self.live_error_details)
            self.refresh_saved_sheets_list()
        except Exception as e:
            write_log(f"[WARN] refresh_error_history_ui failed: {e}")

    def _render_error_history_card(self, details_map: dict[int, str]):
        rows = sorted(details_map.keys())
        if not rows:
            header_text = "⚠ Lỗi theo link Sheet: chưa có"
            list_text = "• Chưa có dòng lỗi đã lưu."
        else:
            header_text = f"⚠ Lỗi theo link Sheet: {len(rows)} dòng"
            lines = []
            for r in rows:
                msg = (details_map.get(r) or "Dòng lỗi đã lưu").strip()
                if ":" in msg:
                    left, right = msg.split(":", 1)
                    left = left.strip()
                    right = right.strip()
                    if left and right:
                        lines.append(f"• #{r}  [{left}] {right}")
                    elif left:
                        lines.append(f"• #{r}  [{left}]")
                    else:
                        lines.append(f"• #{r}  {right or 'Dòng lỗi đã lưu'}")
                else:
                    lines.append(f"• #{r}  {msg}")
            list_text = "\n".join(lines)
        if hasattr(self, "error_header_var"):
            self.error_header_var.set(header_text)
        if hasattr(self, "error_rows_var"):
            self.error_rows_var.set(list_text)
        if hasattr(self, "error_rows_text"):
            try:
                self.error_rows_text.config(state="normal")
                self.error_rows_text.delete("1.0", "end")
                self.error_rows_text.insert("1.0", list_text)
                self.error_rows_text.config(state="disabled")
                self.error_rows_text.yview_moveto(0.0)
            except Exception:
                pass

    def update_error_row_live(self, row: int, message: str = "", is_fail: bool = False):
        try:
            r = int(row)
            if r <= 0:
                return
            if is_fail:
                msg = (message or "Có lỗi trong quá trình xử lý").strip()
                self.live_error_details[r] = msg[:220]
            else:
                self.live_error_details.pop(r, None)
            self._render_error_history_card(self.live_error_details)
        except Exception as e:
            write_log(f"[WARN] update_error_row_live failed: {e}")

    def refresh_saved_sheets_list(self):
        try:
            items = list_saved_error_sheets()
            self._history_sheet_items = items
            menu_obj = getattr(self, "menu_error_saved_sheets", None)
            if menu_obj is None:
                return

            menu_obj.delete(0, "end")
            if not items:
                menu_obj.add_command(label="(Chưa có sheet nào)", state="disabled")
                return

            current_url = self.sheet_url_var.get().strip()
            for it in items:
                sname = (it.get("sheet_name") or "").strip()
                if sname:
                    label = f"{sname} | {it['sheet_url']} | lỗi:{it['rows_count']}"
                else:
                    label = f"{it['sheet_url']} | lỗi:{it['rows_count']}"
                if it.get("updated_at"):
                    label += f" | {it['updated_at']}"
                if it["sheet_url"] == current_url:
                    label = "• " + label
                menu_obj.add_command(
                    label=label,
                    command=lambda u=it["sheet_url"], n=(it.get("sheet_name") or ""): self.load_sheet_from_history(u, n),
                )
        except Exception as e:
            write_log(f"[WARN] refresh_saved_sheets_list failed: {e}")

    def load_sheet_from_history(self, sheet_url: str, sheet_name: str = ""):
        try:
            target_url = (sheet_url or "").strip()
            if not target_url:
                return
            self.sheet_url_var.set(target_url)
            if (sheet_name or "").strip():
                self.sheet_name_var.set((sheet_name or "").strip())
            self.refresh_error_history_ui()
        except Exception as e:
            if messagebox:
                messagebox.showerror("Lỗi", str(e))

    def start_processing_error_rows(self):
        sheet_url = self.sheet_url_var.get().strip()
        rows = sorted(get_error_rows_for_sheet(sheet_url))
        if not rows:
            if messagebox:
                messagebox.showinfo("Không có lỗi", "Link Sheet hiện tại chưa có lịch sử dòng lỗi để chạy lại.")
            return
        self.only_run_error_rows.set(True)
        self.start_processing()

    def _get_initial_geometry(self) -> str:
        screen_w = self.root.winfo_screenwidth()
        screen_h = self.root.winfo_screenheight()
        preferred_w = 1380
        preferred_h = 940
        width = min(preferred_w, max(1120, screen_w - 40))
        height = min(preferred_h, max(780, screen_h - 80))
        x = max(20, (screen_w - width) // 2)
        y = max(20, (screen_h - height) // 2)
        return f"{width}x{height}+{x}+{y}"

    def _on_canvas_configure(self, event):
        target_h = max(event.height, self.main_frame.winfo_reqheight())
        self.main_canvas.itemconfigure(self.canvas_window, width=event.width, height=target_h)
        if hasattr(self, "progress"):
            self.progress.configure(length=max(320, event.width - 80))
        if hasattr(self, "label_detail"):
            self.label_detail.configure(wraplength=max(320, event.width - 40))
        if hasattr(self, "error_rows_text"):
            try:
                self.error_rows_text.configure(width=max(40, (event.width - 90) // 7))
            except Exception:
                pass

    def _on_mousewheel(self, event):
        self.main_canvas.yview_scroll(int(-1 * (event.delta / 120)), "units")

    def _bind_scroll_events(self):
        self.main_canvas.bind_all("<MouseWheel>", self._on_mousewheel)

    def _get_settings_payload(self) -> dict:
        self._snapshot_current_mode_configs()
        mode_key = self._normalize_mode_name(self.mapping_mode_var.get())
        return {
            "sheet_url": self.sheet_url_var.get().strip(),
            "sheet_name": self.sheet_name_var.get().strip(),
            "drive_id": self.drive_id_var.get().strip(),
            "credentials_path": self.credentials_path_var.get().strip(),
            "mapping_mode": mode_key,
            "mapping_blocks": self.mapping_blocks_by_mode.get(mode_key, self.get_mapping_configs()),
            "mapping_blocks_by_mode": self.mapping_blocks_by_mode,
            "capture_five_per_link": bool(self.capture_five_per_link.get()),
            "force_run_all": bool(self.force_run_all.get()),
            "only_run_error_rows": bool(self.only_run_error_rows.get()),
            "auto_launch_chrome": bool(self.auto_launch_chrome.get()),
        }

    def get_mapping_configs(self) -> list[dict]:
        out = []
        mode_name = (self.mapping_mode_var.get() or "Seeding").strip().lower()
        for i, block in enumerate(self.mapping_blocks):
            item = {
                "name": (block["name_var"].get() or f"Post {i + 1}").strip() or f"Post {i + 1}",
                "manual_link": (block["manual_link_var"].get() or "").strip(),
                "col_profile": (block["col_profile_var"].get() or "").strip().upper(),
                "col_content": (block["col_content_var"].get() or "").strip().upper(),
                "col_url": (block["col_url_var"].get() or "").strip().upper(),
                "col_drive": (block["col_drive_var"].get() or "").strip().upper(),
                "col_screenshot": (block["col_screenshot_var"].get() or "").strip().upper(),
                "col_air_date": (block["col_air_date_var"].get() or "").strip().upper(),
                "start_line": (block["start_line_var"].get() or "4").strip() or "4",
            }
            if mode_name == "seeding":
                item["col_profile"] = ""
                item["col_content"] = ""
            elif mode_name == "scan":
                item["col_profile"] = ""
                item["col_screenshot"] = ""
                item["col_air_date"] = ""
            out.append(item)
        return out

    def _load_mapping_blocks(self, blocks_data: list[dict] | None, render: bool = True):
        self.mapping_blocks = []
        for raw in blocks_data or []:
            if not isinstance(raw, dict):
                continue
            self.mapping_blocks.append(self._new_block_vars(raw))
        if not self.mapping_blocks:
            self._ensure_default_mapping_blocks()
        if render and hasattr(self, "mapping_grid"):
            self._render_mapping_blocks()

    def load_settings(self):
        try:
            if not os.path.exists(SETTINGS_PATH):
                return
            self._is_loading_settings = True
            with open(SETTINGS_PATH, "r", encoding="utf-8") as f:
                data = json.load(f) or {}
            self.sheet_url_var.set(str(data.get("sheet_url", self.sheet_url_var.get())).strip())
            self.sheet_name_var.set(str(data.get("sheet_name", self.sheet_name_var.get())).strip())
            self.drive_id_var.set(str(data.get("drive_id", self.drive_id_var.get())).strip())
            saved_credentials_path = str(data.get("credentials_path", self.credentials_path_var.get())).strip()
            if saved_credentials_path:
                # Keep compatibility with older configs where folder text might be mangled,
                # but filename remains valid.
                norm_cred = os.path.normpath(saved_credentials_path)
                if not os.path.exists(norm_cred):
                    candidate = os.path.join(APP_DIR, os.path.basename(norm_cred))
                    if os.path.exists(candidate):
                        norm_cred = candidate
                self.credentials_path_var.set(norm_cred)
            mode_value = self._normalize_mode_name(str(data.get("mapping_mode", self.mapping_mode_var.get())).strip() or "Seeding")
            blocks_data = data.get("mapping_blocks")
            if not isinstance(blocks_data, list):
                # Backward compatibility with old single-block config.
                blocks_data = [
                    {
                        "name": "Post 1",
                        "start_line": str(data.get("start_line", "4")).strip() or "4",
                        "col_url": str(data.get("col_url", "K")).strip().upper(),
                        "col_profile": str(data.get("col_profile", "B")).strip().upper(),
                        "col_content": str(data.get("col_content", "I")).strip().upper(),
                        "col_screenshot": str(data.get("col_screenshot", "J")).strip().upper(),
                        "col_drive": str(data.get("col_drive", "L")).strip().upper(),
                        "col_air_date": str(data.get("col_air_date", "")).strip().upper(),
                    }
                ]
            mode_map_raw = data.get("mapping_blocks_by_mode")
            mode_map: dict[str, list[dict]] = {}
            if isinstance(mode_map_raw, dict):
                for k, v in mode_map_raw.items():
                    mk = self._normalize_mode_name(k)
                    if isinstance(v, list):
                        mode_map[mk] = [x for x in v if isinstance(x, dict)]
            if mode_value not in mode_map or not mode_map.get(mode_value):
                mode_map[mode_value] = [x for x in blocks_data if isinstance(x, dict)]
            for mk in ("Seeding", "Booking", "Scan"):
                if not mode_map.get(mk):
                    mode_map[mk] = self._default_mapping_configs_for_mode(mk)
            self.mapping_blocks_by_mode = mode_map
            self.mapping_mode_var.set(mode_value)
            self._active_mapping_mode = mode_value
            self._load_mapping_blocks(self.mapping_blocks_by_mode.get(mode_value) or [], render=False)
            if hasattr(self, "mapping_grid"):
                self._render_mapping_blocks()
            self.force_run_all.set(bool(data.get("force_run_all", self.force_run_all.get())))
            self.only_run_error_rows.set(bool(data.get("only_run_error_rows", self.only_run_error_rows.get())))
            self.auto_launch_chrome.set(bool(data.get("auto_launch_chrome", self.auto_launch_chrome.get())))
            self.capture_five_per_link.set(bool(data.get("capture_five_per_link", self.capture_five_per_link.get())))
            if hasattr(self, "share_email_var"):
                email = get_service_account_email(self.credentials_path_var.get().strip()) or "link-verification@hazel-tea-485816-u3.iam.gserviceaccount.com"
                self.share_email_var.set(email)
            write_log(f"[INFO] Loaded settings from {SETTINGS_PATH}")
        except Exception as e:
            write_log(f"[WARN] Load settings failed: {e}")
        finally:
            self._is_loading_settings = False

    def save_settings(self, silent: bool = False):
        try:
            payload = self._get_settings_payload()
            with open(SETTINGS_PATH, "w", encoding="utf-8") as f:
                json.dump(payload, f, ensure_ascii=False, indent=2)
            write_log(f"[INFO] Saved settings to {SETTINGS_PATH}")
            if (not silent) and messagebox:
                messagebox.showinfo("Đã lưu", f"Đã lưu cấu hình vào:\n{SETTINGS_PATH}")
        except Exception as e:
            write_log(f"[ERROR] Save settings failed: {e}")
            if (not silent) and messagebox:
                messagebox.showerror("Lỗi", f"Không lưu được cấu hình:\n{e}")

    def set_inputs_enabled(self, enabled: bool):
        state = "normal" if enabled else "disabled"
        self.entry_sheet_url.config(state=state)
        self.entry_sheet_name.config(state=state)
        self.entry_drive_id.config(state=state)
        self.entry_credentials_path.config(state=state)
        for ent in getattr(self, "mapping_entries", []):
            try:
                ent.config(state=state)
            except Exception:
                pass
        for btn in getattr(self, "mapping_remove_buttons", []):
            try:
                btn.config(state=state)
            except Exception:
                pass
        # Keep per-post Chrome launch buttons enabled even while running,
        # so user can reopen login windows on demand.
        for btn in getattr(self, "mapping_launch_buttons", []):
            try:
                btn.config(state="normal")
            except Exception:
                pass
        if getattr(self, "btn_add_block", None):
            try:
                self.btn_add_block.config(state=state)
            except Exception:
                pass
        self.start_btn.config(state=state)
        if hasattr(self, "error_card_save_btn"):
            self.error_card_save_btn.config(state=state)
        if hasattr(self, "error_card_clear_btn"):
            self.error_card_clear_btn.config(state=state)
        self.export_log_btn.config(state=state)
        self.save_btn.config(state=state)
        self.reload_btn.config(state=state)
        self.checkbox.config(state=state)
        self.checkbox_errors_only.config(state=state)
        if hasattr(self, "chk_capture5"):
            try:
                if self.chk_capture5:
                    self.chk_capture5.config(state=state)
            except Exception:
                pass
        if hasattr(self, "mapping_mode_combo"):
            self.mapping_mode_combo.config(state="readonly" if enabled else "disabled")
        if hasattr(self, "btn_launch_chrome"):
            self.btn_launch_chrome.config(state=state)
        self.btn_paste_sheet_url.config(state=state)
        self.btn_paste_sheet_name.config(state=state)
        self.btn_paste_drive_id.config(state=state)
        if not enabled:
            self.pause_btn.config(state="normal")
        else:
            self.pause_btn.config(state="disabled")

    def toggle_pause(self):
        self.is_paused = not self.is_paused
        if self.is_paused:
            self.pause_btn.config(text="▶", bg="#c6e3b5")
            self.label_status.config(text="TẠM DỪNG", fg="#ff6b6b")
            # While paused, allow utility controls.
            self.reload_btn.config(state="normal")
            self.save_btn.config(state="normal")
            self.export_log_btn.config(state="normal")
            self.checkbox.config(state="normal")
            self.checkbox_errors_only.config(state="normal")
            if hasattr(self, "chk_capture5"):
                try:
                    if self.chk_capture5:
                        self.chk_capture5.config(state="normal")
                except Exception:
                    pass
            if hasattr(self, "mapping_mode_combo"):
                self.mapping_mode_combo.config(state="readonly")
            if hasattr(self, "btn_launch_chrome"):
                self.btn_launch_chrome.config(state="normal")
            self.btn_paste_sheet_url.config(state="normal")
            self.btn_paste_sheet_name.config(state="normal")
            self.btn_paste_drive_id.config(state="normal")
            for btn in getattr(self, "mapping_launch_buttons", []):
                try:
                    btn.config(state="normal")
                except Exception:
                    pass
            if getattr(self, "btn_add_block", None):
                try:
                    self.btn_add_block.config(state="normal")
                except Exception:
                    pass
            for btn in getattr(self, "mapping_remove_buttons", []):
                try:
                    btn.config(state="normal")
                except Exception:
                    pass
        else:
            self.pause_btn.config(text="⏸", bg="#fff3cd")
            self.label_status.config(text="ĐANG CHẠY", fg="#1877F2")
            # Resume running lock-state.
            self.reload_btn.config(state="disabled")
            self.save_btn.config(state="disabled")
            self.export_log_btn.config(state="disabled")
            self.checkbox.config(state="disabled")
            self.checkbox_errors_only.config(state="disabled")
            if hasattr(self, "chk_capture5"):
                try:
                    if self.chk_capture5:
                        self.chk_capture5.config(state="disabled")
                except Exception:
                    pass
            if hasattr(self, "mapping_mode_combo"):
                self.mapping_mode_combo.config(state="disabled")
            if hasattr(self, "btn_launch_chrome"):
                self.btn_launch_chrome.config(state="disabled")
            self.btn_paste_sheet_url.config(state="disabled")
            self.btn_paste_sheet_name.config(state="disabled")
            self.btn_paste_drive_id.config(state="disabled")
            if getattr(self, "btn_add_block", None):
                try:
                    self.btn_add_block.config(state="disabled")
                except Exception:
                    pass
            for btn in getattr(self, "mapping_remove_buttons", []):
                try:
                    btn.config(state="disabled")
                except Exception:
                    pass

    def paste_to(self, target_var):
        try:
            text = self.root.clipboard_get()
        except Exception:
            messagebox.showerror("Clipboard trống", "Không đọc được dữ liệu từ clipboard.")
            return
        target_var.set(text.strip())

    def copy_share_email(self):
        email = self.share_email_var.get().strip()
        if not email or email.startswith("Không đọc"):
            return
        try:
            self.root.clipboard_clear()
            self.root.clipboard_append(email)
            self.root.update_idletasks()
            messagebox.showinfo("Đã copy", "Đã copy email vào clipboard. Chia sẻ Sheet và folder Drive với email này, chọn quyền Editor.")
        except Exception:
            messagebox.showerror("Lỗi", "Không copy được vào clipboard.")

    def export_live_log_excel(self):
        if filedialog is None:
            if messagebox:
                messagebox.showerror("Lỗi", "Môi trường hiện tại không hỗ trợ hộp thoại lưu file.")
            return
        if not hasattr(self, "live_log_table"):
            return

        rows = []
        for iid in self.live_log_table.get_children():
            vals = self.live_log_table.item(iid, "values")
            tags = self.live_log_table.item(iid, "tags") or ()
            if vals:
                rows.append((list(vals), list(tags)))
        if not rows:
            if messagebox:
                messagebox.showinfo("Không có dữ liệu", "Bảng log hiện đang trống.")
            return

        out_path = filedialog.asksaveasfilename(
            title="Lưu bảng log",
            defaultextension=".xlsx",
            initialfile=f"evidence_log_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx",
            filetypes=[("Excel Workbook", "*.xlsx"), ("CSV", "*.csv"), ("All files", "*.*")],
        )
        if not out_path:
            return

        headers = ["Time", "#", "State", "Result", "Message"]
        try:
            if out_path.lower().endswith(".csv"):
                with open(out_path, "w", newline="", encoding="utf-8-sig") as f:
                    w = csv.writer(f)
                    w.writerow(headers)
                    for r, _tags in rows:
                        w.writerow(r)
            else:
                exported_with_color = False
                # 1) Prefer openpyxl
                try:
                    from openpyxl import Workbook
                    from openpyxl.styles import PatternFill

                    wb = Workbook()
                    ws = wb.active
                    ws.title = "Log"
                    ws.append(headers)
                    fill_ok = PatternFill(fill_type="solid", fgColor="FFD8F3DC")
                    fill_fail = PatternFill(fill_type="solid", fgColor="FFFFD9D9")
                    fill_unavailable = PatternFill(fill_type="solid", fgColor="FFFFE6C7")

                    for r, tags in rows:
                        ws.append(r)
                        row_idx = ws.max_row
                        tag_set = set(tags or [])
                        state = str(r[3]).strip().upper() if len(r) > 3 else ""
                        msg = str(r[4]).lower() if len(r) > 4 else ""
                        row_fill = None
                        if "fail" in tag_set or state == "FAIL":
                            row_fill = fill_fail
                        elif "unavailable" in tag_set or "nội dung không khả dụng" in msg:
                            row_fill = fill_unavailable
                        elif "ok" in tag_set or state == "OK":
                            row_fill = fill_ok
                        if row_fill:
                            for col in range(1, 6):
                                ws.cell(row=row_idx, column=col).fill = row_fill
                    wb.save(out_path)
                    exported_with_color = True
                except Exception:
                    exported_with_color = False

                # 2) Fallback to xlsxwriter (also keeps colors)
                if not exported_with_color:
                    try:
                        import xlsxwriter

                        wb = xlsxwriter.Workbook(out_path)
                        ws = wb.add_worksheet("Log")
                        fmt_ok = wb.add_format({"bg_color": "#D8F3DC"})
                        fmt_fail = wb.add_format({"bg_color": "#FFD9D9"})
                        fmt_unavailable = wb.add_format({"bg_color": "#FFE6C7"})
                        for c, h in enumerate(headers):
                            ws.write(0, c, h)
                        for i, (r, tags) in enumerate(rows, start=1):
                            tag_set = set(tags or [])
                            state = str(r[3]).strip().upper() if len(r) > 3 else ""
                            msg = str(r[4]).lower() if len(r) > 4 else ""
                            fmt = None
                            if "fail" in tag_set or state == "FAIL":
                                fmt = fmt_fail
                            elif "unavailable" in tag_set or "nội dung không khả dụng" in msg:
                                fmt = fmt_unavailable
                            elif "ok" in tag_set or state == "OK":
                                fmt = fmt_ok
                            for c, v in enumerate(r):
                                ws.write(i, c, v, fmt)
                        wb.close()
                        exported_with_color = True
                    except Exception:
                        exported_with_color = False

                # 3) Last fallback: HTML table saved as .xls (opens in Excel with colors)
                if not exported_with_color:
                    try:
                        native_xlsx = out_path if out_path.lower().endswith(".xlsx") else out_path + ".xlsx"
                        write_colored_xlsx_builtin(native_xlsx, headers, rows)
                        out_path = native_xlsx
                        exported_with_color = True
                    except Exception:
                        exported_with_color = False

                # 4) Last-resort fallback: HTML .xls with colors
                if not exported_with_color:
                    fallback = out_path
                    if not fallback.lower().endswith(".xls"):
                        fallback = fallback + ".xls"
                    css = """
                    <style>
                    table { border-collapse: collapse; font-family: Arial, sans-serif; font-size: 11pt; }
                    th, td { border: 1px solid #d0d0d0; padding: 4px 6px; }
                    .ok { background: #D8F3DC; }
                    .fail { background: #FFD9D9; }
                    .unavailable { background: #FFE6C7; }
                    </style>
                    """
                    lines = [
                        "<html><head><meta charset='utf-8'>",
                        css,
                        "</head><body><table>",
                        "<tr>" + "".join(f"<th>{html_lib.escape(h)}</th>" for h in headers) + "</tr>",
                    ]
                    for r, tags in rows:
                        tag_set = set(tags or [])
                        state = str(r[3]).strip().upper() if len(r) > 3 else ""
                        msg = str(r[4]).lower() if len(r) > 4 else ""
                        cls = ""
                        if "fail" in tag_set or state == "FAIL":
                            cls = "fail"
                        elif "unavailable" in tag_set or "nội dung không khả dụng" in msg:
                            cls = "unavailable"
                        elif "ok" in tag_set or state == "OK":
                            cls = "ok"
                        row_cells = "".join(f"<td>{html_lib.escape(str(v))}</td>" for v in r)
                        lines.append(f"<tr class='{cls}'>{row_cells}</tr>")
                    lines.append("</table></body></html>")
                    with open(fallback, "w", encoding="utf-8") as f:
                        f.write("\n".join(lines))
                    out_path = fallback
                    if messagebox:
                        messagebox.showwarning(
                            "Thiếu thư viện Excel",
                            "Máy này thiếu thư viện Excel, đã dùng fallback .xls (HTML) có màu.",
                        )
            if messagebox:
                messagebox.showinfo("Đã xuất", f"Đã xuất bảng log:\n{out_path}")
        except Exception as e:
            if messagebox:
                messagebox.showerror("Lỗi xuất file", str(e))

    def reset_live_log(self):
        try:
            if hasattr(self, "live_log_table"):
                for iid in self.live_log_table.get_children():
                    self.live_log_table.delete(iid)
        except Exception:
            pass
        self.update_progress_summary(0, 0, 0, 0, "---")

    def update_progress_summary(self, done: int, total: int, ok_count: int, fail_count: int, eta_text: str = "---"):
        try:
            self.progress_summary_var.set(
                f"✔ Progress: {done}/{total} | Success: {ok_count} | Failed: {fail_count} | ETA: {eta_text}"
            )
        except Exception:
            pass

    def add_live_log(self, row: int, state_left: str, state_right: str, message: str, tag: str = ""):
        try:
            ts = datetime.now().strftime("%H:%M:%S")
            if not hasattr(self, "live_log_table"):
                return
            self.live_log_table.insert(
                "",
                0,
                values=(ts, f"#{row}", state_left, state_right, message[:240]),
                tags=(tag,) if tag else (),
            )
        except Exception:
            pass

    def show_completion_popup(self, title: str, summary_text: str, severity: str = "info"):
        if tk is None:
            return
        win = tk.Toplevel(self.root)
        win.title(title)
        win.configure(bg="#f7f9ff")
        win.resizable(False, False)
        win.attributes("-topmost", True)
        win.transient(self.root)
        win.grab_set()

        border_color = "#2f80ed"
        icon = "ℹ"
        icon_color = "#2f80ed"
        if severity == "warn":
            border_color = "#e09f00"
            icon = "⚠"
            icon_color = "#e09f00"
        elif severity == "error":
            border_color = "#c0392b"
            icon = "✖"
            icon_color = "#c0392b"

        outer = tk.Frame(win, bg=border_color, padx=2, pady=2)
        outer.pack(fill="both", expand=True)
        body = tk.Frame(outer, bg="#ffffff", padx=16, pady=14)
        body.pack(fill="both", expand=True)

        top_row = tk.Frame(body, bg="#ffffff")
        top_row.pack(fill="x", pady=(0, 8))
        tk.Label(top_row, text=icon, font=("Arial", 20, "bold"), fg=icon_color, bg="#ffffff").pack(side="left")
        tk.Label(top_row, text=title, font=("Arial", 12, "bold"), fg="#1d2a44", bg="#ffffff").pack(side="left", padx=(8, 0))

        tk.Label(
            body,
            text=summary_text,
            justify="left",
            anchor="w",
            font=("Arial", 11, "bold"),
            fg="#1f2d4d",
            bg="#ffffff",
        ).pack(fill="x", pady=(0, 12))

        btn = tk.Button(body, text="OK", width=10, command=win.destroy, bg="#2f80ed", fg="#ffffff")
        btn.pack(anchor="e")
        btn.focus_set()

        try:
            win.update_idletasks()
            sw = win.winfo_screenwidth()
            sh = win.winfo_screenheight()
            ww = win.winfo_width()
            wh = win.winfo_height()
            x = max(0, (sw - ww) // 2)
            y = max(0, (sh - wh) // 2)
            win.geometry(f"+{x}+{y}")
        except Exception:
            pass

    def launch_chrome_for_login(self):
        browser_port = 9223
        
        ok, info = launch_chrome_for_login(browser_port)
        if ok:
            messagebox.showinfo(
                "Chrome đã mở",
                f"Chrome mở trên port {browser_port}.\n\nBây giờ bạn có thể:\n"
                f"1. Đăng nhập Facebook, TikTok, Instagram, YouTube\n"
                f"2. Sau đó bấm BẮT ĐẦU để chạy xử lý\n"
                f"3. Chrome sẽ nhớ tất cả đăng nhập"
            )
        else:
            messagebox.showerror("Lỗi", f"Không mở được Chrome: {info}")

    def start_processing(self):
        global JSON_PATH
        run_mode = (self.mapping_mode_var.get() or "Seeding").strip().lower()
        is_scan_image_mode = run_mode == "scan"
        is_scan_like_mode = is_scan_image_mode
        sheet_url = normalize_sheet_input(self.sheet_url_var.get().strip())
        sheet_name = self.sheet_name_var.get().strip()
        drive_id = normalize_drive_folder_input(self.drive_id_var.get().strip())
        cred_input = self.credentials_path_var.get().strip()
        if sheet_url:
            self.sheet_url_var.set(sheet_url)
        if drive_id:
            self.drive_id_var.set(drive_id)

        if not sheet_url or not sheet_name or ((not is_scan_like_mode) and not drive_id):
            required_text = (
                "Vui lòng nhập đầy đủ Sheet URL, Sheet Name."
                if is_scan_like_mode
                else "Vui lòng nhập đầy đủ Sheet URL, Sheet Name và Drive Folder ID."
            )
            messagebox.showerror(
                "Thiếu thông tin",
                required_text
            )
            return
        if not cred_input:
            messagebox.showerror(
                "Thiếu credentials",
                "Hãy nhập đường dẫn credentials.json hoặc dán nội dung JSON vào ô Credentials JSON."
            )
            return

        if os.path.exists(cred_input):
            JSON_PATH = cred_input
        else:
            # Allow pasting raw JSON directly in Credentials input.
            try:
                data = json.loads(cred_input)
            except Exception:
                messagebox.showerror(
                    "Credentials không hợp lệ",
                    "Ô Credentials JSON không phải đường dẫn file và cũng không phải JSON hợp lệ.\n"
                    "Hãy dán trực tiếp nội dung JSON vào ô Credentials JSON."
                )
                return

            required = ["type", "client_email", "private_key"]
            missing = [k for k in required if not str(data.get(k, "")).strip()]
            if missing:
                messagebox.showerror("Thiếu trường", f"JSON thiếu trường bắt buộc: {', '.join(missing)}")
                return

            out_path = os.path.join(BASE_DIR, "credentials.inline.json")
            try:
                with open(out_path, "w", encoding="utf-8") as f:
                    json.dump(data, f, ensure_ascii=False, indent=2)
            except Exception as e:
                messagebox.showerror("Lỗi lưu file", str(e))
                return
            JSON_PATH = out_path
            self.credentials_path_var.set(out_path)
            self.share_email_var.set(str(data.get("client_email", "")).strip() or "link-verification@hazel-tea-485816-u3.iam.gserviceaccount.com")

        block_configs = self.get_mapping_configs()

        mappings = []
        for i, block in enumerate(block_configs):
            col_url = (block.get("col_url") or "").strip().upper()
            if not col_url:
                continue
            if col_url and (not col_letter_to_index(col_url)):
                messagebox.showerror("Lỗi dữ liệu", f"Block {i+1}: Cột Link URL không hợp lệ ({col_url}).")
                return
            col_profile = (block.get("col_profile") or "").strip().upper()
            col_content = (block.get("col_content") or "").strip().upper()
            col_screenshot = (block.get("col_screenshot") or "").strip().upper()
            col_drive = (block.get("col_drive") or "").strip().upper()
            col_air_date_raw = (block.get("col_air_date") or "").strip()
            col_air_date = col_air_date_raw.upper()
            optional_cols = [
                ("Profile", col_profile),
                ("Content", col_content),
                ("Screenshot", col_screenshot),
                ("Drive URL", col_drive),
            ]
            for label, col_ref in optional_cols:
                if col_ref and not col_letter_to_index(col_ref):
                    messagebox.showerror("Lỗi dữ liệu", f"Block {i+1}: Cột {label} không hợp lệ ({col_ref}).")
                    return
            if is_scan_like_mode:
                if not col_content:
                    messagebox.showerror("Lỗi dữ liệu", f"Block {i+1}: Text Column không được để trống.")
                    return
                if not col_drive:
                    messagebox.showerror("Lỗi dữ liệu", f"Block {i+1}: Result Column không được để trống.")
                    return
            fixed_air_date = ""
            if col_air_date_raw:
                if col_letter_to_index(col_air_date):
                    pass
                else:
                    fixed_air_date = get_air_date_token(col_air_date_raw)
                    if not fixed_air_date:
                        messagebox.showerror(
                            "Lỗi dữ liệu",
                            f"Block {i+1}: Air Date phải là ký tự cột (vd: H) hoặc ngày hợp lệ (vd: 2026-03-10).",
                        )
                        return
                    col_air_date = ""
            try:
                start_line = int(str(block.get("start_line", "4")).strip() or "4")
            except ValueError:
                messagebox.showerror("Lỗi dữ liệu", f"Block {i+1}: Start Line phải là số.")
                return
            mappings.append(
                {
                    "name": (
                        (
                            (block.get("name") or "").strip()
                            or (f"Scan {i+1}")
                        )
                        if is_scan_like_mode
                        else (block.get("name") or f"Post {i+1}").strip() or f"Post {i+1}"
                    ),
                    "start_line": start_line,
                    "col_url": col_url,
                    "col_profile": col_profile,
                    "col_content": col_content,
                    "col_screenshot": col_screenshot,
                    "col_drive": col_drive,
                    "col_air_date": col_air_date,
                    "fixed_air_date": fixed_air_date,
                    "mode": run_mode,
                }
            )

        if not mappings:
            messagebox.showerror("Thiếu cấu hình", "Cần ít nhất 1 block hợp lệ để chạy.")
            return
        if is_scan_image_mode:
            ocr_ok, ocr_msg = check_ocr_dependencies()
            if not ocr_ok:
                messagebox.showerror(
                    "Thiếu OCR",
                    f"Scan cần OCR để đọc chữ trong ảnh.\n\n{ocr_msg}",
                )
                return
        browser_port = 9223
        if self.auto_launch_chrome.get() and run_mode != "scan":
            # Auto-prepare Chrome for all mapped posts, not only Post 1.
            for i in range(len(mappings)):
                p = self._get_block_port(i)
                prof = self._get_block_profile(i)
                ok, info = launch_chrome_for_login(p, profile_path=prof)
                if not ok:
                    write_log(f"[WARN] Auto launch Chrome failed (Post {i+1}, port {p}): {info}")

        self.is_running = True
        self.is_paused = False
        self.save_settings(silent=True)
        self.set_inputs_enabled(False)
        self.label_status.config(text="ĐANG CHẠY", fg="#1877F2")
        self.label_detail.config(text="Bắt đầu xử lý...")
        self.reset_live_log()

        threading.Thread(
            target=lambda: main_logic(self, drive_id, sheet_url, sheet_name, mappings=mappings, browser_port=browser_port),
            daemon=True
        ).start()

    def reload_app(self):
        self.is_running = False
        self.is_paused = False
        if self.driver:
            try:
                self.driver.quit()
            except:
                pass
        self.driver = None
        self.load_settings()
        self.progress["value"] = 0
        self.pause_btn.config(text="⏸", bg="#fff3cd")
        self.label_status.config(text="Sẵn sàng", fg="#1877F2")
        self.label_detail.config(text="Đã load lại app.")
        self.reset_live_log()
        self.set_inputs_enabled(True)

    def exit_app(self):
        self.is_running = False
        self.main_canvas.unbind_all("<MouseWheel>")
        if self.driver:
            try:
                self.driver.quit()
            except:
                pass
        self.root.destroy()
        os._exit(0)

# ================= CORE =================
def main_logic(app: ProgressApp, drive_id: str, sheet_url: str, sheet_name: str, start_line: int = 4, col_url_letter: str = "K", col_profile_letter: str = "B", col_content_letter: str = "I", col_screenshot_letter: str = "J", col_drive_letter: str = "L", browser_port: int = 9223, mappings: list[dict] | None = None, primary_profile_path: str | None = None, target_rows: list[int] | set[int] | tuple[int, ...] | None = None, target_block_name: str | None = None):
    def ui_call(fn, *args, **kwargs):
        """
        Run UI actions on Tk main thread to avoid random crashes on some machines.
        """
        try:
            root = getattr(app, "root", None)
            if root is not None and hasattr(root, "after") and threading.current_thread() is not threading.main_thread():
                root.after(0, lambda: fn(*args, **kwargs))
            else:
                fn(*args, **kwargs)
        except Exception as e:
            write_log(f"[WARN] UI call failed: {e}")

    def ui_set_progress(value: int):
        app.progress["value"] = value

    def ui_set_detail(text: str):
        app.label_detail.config(text=text)

    def ui_set_done():
        app.label_status.config(text="HOÀN TẤT", fg="#34C759")

    def ui_update_summary(done: int, total: int, ok_count: int, fail_count: int, eta_text: str):
        if hasattr(app, "update_progress_summary"):
            app.update_progress_summary(done, total, ok_count, fail_count, eta_text)

    def ui_add_log(row: int, state_left: str, state_right: str, message: str, tag: str):
        if hasattr(app, "add_live_log"):
            app.add_live_log(row, state_left, state_right, message, tag)

    tracked_error_rows: set[int] = set()
    tracked_error_details: dict[int, str] = {}
    history_ready = False

    try:
        write_log("=== START FINAL TOOL v2.2 ===")

        if not os.path.exists(TEMP_DIR):
            os.makedirs(TEMP_DIR)

        if not os.path.exists(JSON_PATH):
            raise FileNotFoundError(
                "Khong tim thay credentials.json.\n"
                f"Duong dan dang tim: {JSON_PATH}\n"
                "Hay dat credentials.json canh file .exe hoac set env GOOGLE_CREDENTIALS_PATH."
            )

        creds = ServiceAccountCredentials.from_json_keyfile_name(
            JSON_PATH,
            [
                "https://spreadsheets.google.com/feeds",
                "https://www.googleapis.com/auth/drive"
            ]
        )
        client = gspread.authorize(creds)
        drive_service = build("drive", "v3", credentials=creds)
        sheets_service = build("sheets", "v4", credentials=creds)

        spreadsheet = client.open_by_url(sheet_url)
        spreadsheet_id = spreadsheet.id
        worksheet = spreadsheet.worksheet(sheet_name)
        sheet_id = worksheet.id

        def wait_page_ready(driver, timeout: int = PAGE_READY_TIMEOUT):
            try:
                WebDriverWait(driver, timeout).until(
                    lambda d: d.execute_script("return document.readyState") in ("interactive", "complete")
                )
            except Exception:
                time.sleep(PAGE_READY_FALLBACK_SLEEP)

        def load_existing_drive_files():
            files_by_name = {}
            page_token = None
            while True:
                resp = drive_service.files().list(
                    q=f"'{drive_id}' in parents and trashed = false",
                    fields="nextPageToken, files(id,name)",
                    supportsAllDrives=True,
                    includeItemsFromAllDrives=True,
                    corpora="allDrives",
                    pageSize=1000,
                    pageToken=page_token,
                ).execute()
                for f in resp.get("files", []):
                    n = f.get("name")
                    fid = f.get("id")
                    if n and fid:
                        files_by_name.setdefault(n, []).append(fid)
                page_token = resp.get("nextPageToken")
                if not page_token:
                    break
            return files_by_name

        def build_chrome_options(user_data_dir: str, headless: bool, debug_port: int) -> Options:
            options = Options()
            if user_data_dir:
                options.add_argument(f"--user-data-dir={user_data_dir}")
            options.add_argument(f"--window-size={CAPTURE_WINDOW_SIZE}")
            options.add_argument("--disable-gpu")
            options.add_argument("--no-sandbox")
            options.add_argument("--disable-dev-shm-usage")
            options.add_argument(f"--remote-debugging-port={debug_port}")
            options.add_argument("--force-device-scale-factor=1")
            options.add_argument("--high-dpi-support=1")
            options.add_argument("--remote-allow-origins=*")
            options.add_argument("--no-first-run")
            options.add_argument("--no-default-browser-check")
            options.add_argument("--disable-extensions")
            options.add_argument("--disable-background-networking")
            options.add_argument("--disable-sync")
            options.add_argument("--disable-features=TranslateUI")
            options.add_argument("--disable-blink-features=AutomationControlled")
            options.page_load_strategy = "eager"
            if headless:
                options.add_argument("--headless=new")
            return options

        scan_only_request = bool(mappings) and all(
            str((m or {}).get("mode", "seeding")).strip().lower() == "scan"
            for m in mappings
        )

        service = None
        def seed_profile_if_needed(target_profile: str):
            if not target_profile:
                return
            try:
                os.makedirs(target_profile, exist_ok=True)
                if os.path.isdir(os.path.join(target_profile, "Default")):
                    return
                seed_profile = ""
                if target_profile != LOCAL_PROFILE_PATH and os.path.isdir(LOCAL_PROFILE_PATH):
                    seed_profile = LOCAL_PROFILE_PATH
                if not seed_profile and os.path.isdir(FB_PROFILE_PATH):
                    seed_profile = FB_PROFILE_PATH
                if not seed_profile and os.path.isdir(FB_PROFILE_PATH_ALT):
                    seed_profile = FB_PROFILE_PATH_ALT
                if not seed_profile or os.path.abspath(seed_profile) == os.path.abspath(target_profile):
                    return
                shutil.copytree(
                    seed_profile,
                    target_profile,
                    dirs_exist_ok=True,
                    ignore=shutil.ignore_patterns(
                        "Cache",
                        "Code Cache",
                        "GPUCache",
                        "GrShaderCache",
                        "ShaderCache",
                        "Crashpad",
                        "Singleton*",
                        "lockfile",
                        "*.tmp",
                    ),
                )
                write_log(f"[INFO] Seeded profile '{target_profile}' from '{seed_profile}'")
            except Exception as e:
                write_log(f"[WARN] Profile seed failed ({target_profile}): {e}")

        profile_candidates = []
        for cand in [
            primary_profile_path,
            LOCAL_PROFILE_PATH,
            FB_PROFILE_PATH,
            FB_PROFILE_PATH_ALT,
            os.path.join(TEMP_DIR, "chrome_profile_temp"),
        ]:
            cand = str(cand or "").strip()
            if cand and cand not in profile_candidates:
                profile_candidates.append(cand)
        def find_chrome_binary() -> str | None:
            candidates = []
            if os.name == "nt":
                candidates.extend(
                    [
                        os.path.join(os.environ.get("PROGRAMFILES", r"C:\Program Files"), "Google", "Chrome", "Application", "chrome.exe"),
                        os.path.join(os.environ.get("PROGRAMFILES(X86)", r"C:\Program Files (x86)"), "Google", "Chrome", "Application", "chrome.exe"),
                        os.path.join(os.environ.get("LOCALAPPDATA", ""), "Google", "Chrome", "Application", "chrome.exe"),
                        os.path.join(os.environ.get("PROGRAMFILES", r"C:\Program Files"), "Chromium", "Application", "chrome.exe"),
                        os.path.join(os.environ.get("PROGRAMFILES(X86)", r"C:\Program Files (x86)"), "Chromium", "Application", "chrome.exe"),
                        os.path.join(os.environ.get("PROGRAMFILES", r"C:\Program Files"), "Microsoft", "Edge", "Application", "msedge.exe"),
                        os.path.join(os.environ.get("PROGRAMFILES(X86)", r"C:\Program Files (x86)"), "Microsoft", "Edge", "Application", "msedge.exe"),
                    ]
                )
            elif sys.platform == "darwin":
                candidates.extend(
                    [
                        "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome",
                        "/Applications/Chromium.app/Contents/MacOS/Chromium",
                        "/Applications/Microsoft Edge.app/Contents/MacOS/Microsoft Edge",
                    ]
                )
            else:
                for cmd in ["google-chrome", "google-chrome-stable", "chromium", "chromium-browser", "microsoft-edge", "microsoft-edge-stable"]:
                    p = shutil.which(cmd)
                    if p:
                        candidates.append(p)

            for cmd in ["chrome", "chrome.exe", "google-chrome", "chromium", "msedge", "msedge.exe"]:
                p = shutil.which(cmd)
                if p:
                    candidates.append(p)
            for p in candidates:
                if p and os.path.exists(p):
                    return p
            try:
                import winreg
                for root in (winreg.HKEY_CURRENT_USER, winreg.HKEY_LOCAL_MACHINE):
                    try:
                        with winreg.OpenKey(root, r"SOFTWARE\Microsoft\Windows\CurrentVersion\App Paths\chrome.exe") as key:
                            val, _ = winreg.QueryValueEx(key, None)
                            if val and os.path.exists(val):
                                return val
                    except Exception:
                        continue
            except Exception:
                pass
            return None

        if not scan_only_request:
            service = resolve_chromedriver_service()
            started = False
            last_err = None

            # 1) Prefer attaching to an already launched Chrome (from "Launch Chrome").
            try:
                attach_opts = Options()
                attach_opts.add_experimental_option("debuggerAddress", f"127.0.0.1:{browser_port}")
                app.driver = webdriver.Chrome(service=service, options=attach_opts)
                write_log(f"[INFO] Attached to existing Chrome debug session on port {browser_port}")
                started = True
            except Exception as e:
                last_err = e
                write_log(f"[INFO] No attachable Chrome on port {browser_port}: {e}")

            # 2) If not attachable, start new Chrome with profile candidates.
            if not started:
                for profile in profile_candidates:
                    seed_profile_if_needed(profile)
                    for headless in (True, False):
                        try:
                            app.driver = webdriver.Chrome(
                                service=service,
                                options=build_chrome_options(user_data_dir=profile, headless=headless, debug_port=browser_port)
                            )
                            write_log(f"[INFO] Chrome started with profile='{profile}', headless={headless}")
                            started = True
                            break
                        except Exception as e:
                            last_err = e
                            write_log(f"[WARN] Chrome start failed (profile='{profile}', headless={headless}): {e}")
                            try:
                                if app.driver:
                                    app.driver.quit()
                            except:
                                pass
                    if started:
                        break
            if not started:
                chrome_path = find_chrome_binary()
                if chrome_path:
                    debug_profile = primary_profile_path or LOCAL_PROFILE_PATH
                    seed_profile_if_needed(debug_profile)
                    for port in [browser_port]:
                        try:
                            args = [
                                chrome_path,
                                f"--remote-debugging-port={port}",
                                f"--user-data-dir={debug_profile}",
                                f"--window-size={CAPTURE_WINDOW_SIZE}",
                                "--force-device-scale-factor=1",
                                "--high-dpi-support=1",
                                "--no-first-run",
                                "--no-default-browser-check",
                                "--disable-extensions",
                                "--disable-background-networking",
                                "--disable-sync",
                                "--disable-gpu",
                            ]
                            subprocess.Popen(args, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
                            time.sleep(2)
                            attach_opts = Options()
                            attach_opts.add_experimental_option("debuggerAddress", f"127.0.0.1:{port}")
                            app.driver = webdriver.Chrome(service=service, options=attach_opts)
                            write_log(f"[INFO] Attached to debug Chrome on port {port}")
                            started = True
                            break
                        except Exception as e:
                            last_err = e
                            write_log(f"[WARN] Attach debug Chrome failed (port={port}): {e}")
                if not started:
                    raise Exception(f"CHROME_START_FAILED: {last_err}")
        else:
            write_log("[INFO] Scan mode: skip Selenium/Chrome startup.")

        requested_rows: set[int] = set()
        for item in target_rows or []:
            try:
                row_value = int(item)
            except Exception:
                continue
            if row_value >= 1:
                requested_rows.add(row_value)
        requested_block_name = str(target_block_name or "").strip().lower()

        mapping_list = mappings or [
            {
                "name": "Post 1",
                "start_line": start_line,
                "col_url": col_url_letter,
                "col_profile": col_profile_letter,
                "col_content": col_content_letter,
                "col_screenshot": col_screenshot_letter,
                "col_drive": col_drive_letter,
                "col_air_date": "",
                "mode": "seeding",
            }
        ]
        normalized_mappings = []
        for i, m in enumerate(mapping_list):
            m_name = str((m or {}).get("name", f"Post {i+1}")).strip() or f"Post {i+1}"
            m_mode = str((m or {}).get("mode", "seeding")).strip().lower() or "seeding"
            if m_mode == "scan only text":
                write_log(f"[INFO] '{m_name}': mode 'Scan Only Text' đã bị gỡ, tự chuyển sang 'Scan'.")
                m_mode = "scan"
            m_col_url = str((m or {}).get("col_url", "")).strip().upper()
            if not col_letter_to_index(m_col_url):
                write_log(f"[WARN] Skip {m_name}: invalid Link URL column '{m_col_url}'")
                continue
            m_col_profile = str((m or {}).get("col_profile", "")).strip().upper()
            m_col_content = str((m or {}).get("col_content", "")).strip().upper()
            m_col_screenshot = str((m or {}).get("col_screenshot", "")).strip().upper()
            m_col_drive = str((m or {}).get("col_drive", "")).strip().upper()
            m_col_air_date_raw = str((m or {}).get("col_air_date", "")).strip()
            m_col_air_date = m_col_air_date_raw.upper()
            m_fixed_air_date = str((m or {}).get("fixed_air_date", "")).strip()
            optional_pairs = [
                ("Profile", m_col_profile),
                ("Content", m_col_content),
                ("Screenshot", m_col_screenshot),
                ("Drive", m_col_drive),
            ]
            sanitized_optional = {}
            for label, col_ref in optional_pairs:
                if col_ref and not col_letter_to_index(col_ref):
                    write_log(f"[WARN] {m_name}: invalid {label} column '{col_ref}', set empty.")
                    sanitized_optional[label] = ""
                else:
                    sanitized_optional[label] = col_ref
            if m_mode == "seeding":
                sanitized_optional["Profile"] = ""
                sanitized_optional["Content"] = ""
            elif m_mode == "scan":
                sanitized_optional["Profile"] = ""
                sanitized_optional["Screenshot"] = ""
                m_col_air_date = ""
                m_fixed_air_date = ""
            if m_col_air_date and not col_letter_to_index(m_col_air_date):
                parsed_fixed = get_air_date_token(m_col_air_date_raw)
                if parsed_fixed:
                    m_fixed_air_date = parsed_fixed
                    m_col_air_date = ""
                else:
                    write_log(f"[WARN] {m_name}: invalid AirDate '{m_col_air_date_raw}', ignore.")
                    m_col_air_date = ""
            try:
                m_start = int(str((m or {}).get("start_line", "4")).strip() or "4")
            except Exception:
                m_start = 4
            normalized_mappings.append(
                {
                    "name": m_name,
                    "start_line": m_start,
                    "col_url": m_col_url,
                    "col_profile": sanitized_optional["Profile"],
                    "col_content": sanitized_optional["Content"],
                    "col_screenshot": sanitized_optional["Screenshot"],
                    "col_drive": sanitized_optional["Drive"],
                    "col_air_date": m_col_air_date,
                    "fixed_air_date": m_fixed_air_date,
                    "mode": m_mode,
                }
            )
        if requested_block_name:
            filtered_mappings = [m for m in normalized_mappings if str(m.get("name", "")).strip().lower() == requested_block_name]
            if filtered_mappings:
                normalized_mappings = filtered_mappings
        if not normalized_mappings:
            raise Exception("KHONG_CO_BLOCK_HOP_LE")

        prepared_blocks = []
        need_upload = False
        max_rows = 0
        for m in normalized_mappings:
            idx_url = col_letter_to_index(m["col_url"])
            idx_profile = col_letter_to_index(m["col_profile"]) if m["col_profile"] else None
            idx_content = col_letter_to_index(m["col_content"]) if m["col_content"] else None
            idx_drive = col_letter_to_index(m["col_drive"]) if m["col_drive"] else None
            idx_screenshot = col_letter_to_index(m["col_screenshot"]) if m["col_screenshot"] else None
            idx_air_date = col_letter_to_index(m["col_air_date"]) if m["col_air_date"] else None
            mode_name = str(m.get("mode", "seeding")).strip().lower()
            if mode_name == "scan":
                links = resolve_links_for_scan(worksheet, idx_url, start_row=4)
                row_numbers = list(range(4, 4 + len(links)))
                scan_expected_texts = resolve_column_values_aligned(
                    worksheet,
                    idx_content,
                    start_row=4,
                    total_rows=len(links),
                ) if idx_content else []
            else:
                links = worksheet.col_values(idx_url)[3:] if idx_url else []
                row_numbers = list(range(4, 4 + len(links)))
                scan_expected_texts = []
            results = worksheet.col_values(idx_drive)[3:] if idx_drive else []
            captions_existing = worksheet.col_values(idx_content)[3:] if idx_content else []
            air_dates = worksheet.col_values(idx_air_date)[3:] if idx_air_date else []
            prepared_blocks.append(
                {
                    "name": m["name"],
                    "start_line": m["start_line"],
                    "col_url": m["col_url"],
                    "col_profile": m["col_profile"],
                    "col_content": m["col_content"],
                    "col_screenshot": m["col_screenshot"],
                    "col_drive": m["col_drive"],
                    "col_air_date": m["col_air_date"],
                    "idx_profile": idx_profile,
                    "idx_content": idx_content,
                    "idx_drive": idx_drive,
                    "idx_screenshot": idx_screenshot,
                    "idx_air_date": idx_air_date,
                    "fixed_air_date": m.get("fixed_air_date", ""),
                    "mode": mode_name or "seeding",
                    "links": links,
                    "row_numbers": row_numbers,
                    "scan_expected_texts": scan_expected_texts,
                    "results": results,
                    "captions_existing": captions_existing,
                    "air_dates": air_dates,
                }
            )
            if (str(m.get("mode", "seeding")).strip().lower() not in ("scan",)) and (idx_drive or idx_screenshot):
                need_upload = True
            max_rows = max(max_rows, len(links))

        write_log(f"[DEBUG] Using Drive Folder ID: {drive_id}")
        existing_files_by_name = load_existing_drive_files() if need_upload else {}
        if need_upload:
            write_log(f"[INFO] Drive folder preload complete: {len(existing_files_by_name)} distinct names")
        else:
            write_log("[INFO] Skip upload step (Drive/Screenshot column not configured).")

        try:
            only_error_mode = bool(getattr(app, "only_run_error_rows", None).get())
        except Exception:
            only_error_mode = False

        tracked_error_rows = get_error_rows_for_sheet(sheet_url)
        tracked_error_details = get_error_details_for_sheet(sheet_url)
        history_ready = True
        if only_error_mode:
            if tracked_error_rows:
                write_log(f"[INFO] Error-only mode: loaded {len(tracked_error_rows)} rows from history")
            else:
                write_log("[INFO] Error-only mode: no stored error rows for this sheet")

        def _is_target_row(start_at: int, row_num: int, link_val: str, mode_key: str = "") -> bool:
            if row_num < start_at:
                return False
            if requested_rows and row_num not in requested_rows:
                return False
            if only_error_mode and row_num not in tracked_error_rows:
                return False
            mk = str(mode_key or "").strip().lower()
            if mk == "scan":
                return bool(normalize_scan_source_url(link_val))
            return str(link_val).strip().startswith("http")

        target_total = 0
        for block in prepared_blocks:
            block_mode_key = str(block.get("mode", "seeding")).strip().lower()
            row_nums = block.get("row_numbers") or []
            for i, lnk in enumerate(block["links"]):
                r = row_nums[i] if i < len(row_nums) else (i + 4)
                if _is_target_row(block["start_line"], r, str(lnk), mode_key=block_mode_key):
                    target_total += 1
        if target_total == 0:
            write_log("[WARN] target_total=0: no eligible links found to process.")
            ui_call(ui_set_detail, "Không có dữ liệu hợp lệ để xử lý. Kiểm tra Image Column và Start Line.")

        run_started_at = time.time()
        started_count = 0
        processed_count = 0
        success_count = 0
        fail_count = 0
        ui_call(ui_update_summary, 0, target_total, 0, 0, "---")

        counter_lock = threading.Lock()
        error_lock = threading.Lock()
        drive_cache_lock = threading.Lock()
        sheet_write_lock = threading.Lock()
        last_sheet_write_ts = [0.0]

        def _is_quota_error(exc: Exception) -> bool:
            s = str(exc).lower()
            return ("429" in s) or ("quota exceeded" in s) or ("rate limit" in s)

        def safe_sheet_write(write_fn, op_desc: str = "sheet_write", max_retry: int = 8):
            """
            Serialize + throttle writes to avoid Google Sheets write quota bursts
            when multiple blocks/workers run in parallel.
            """
            base_wait = 1.1  # ~54 writes/minute global
            last_err = None
            for attempt in range(max_retry):
                try:
                    with sheet_write_lock:
                        now = time.time()
                        wait_more = base_wait - (now - last_sheet_write_ts[0])
                        if wait_more > 0:
                            time.sleep(wait_more)
                        out = write_fn()
                        last_sheet_write_ts[0] = time.time()
                        return out
                except Exception as e:
                    last_err = e
                    if not _is_quota_error(e):
                        raise
                    sleep_s = min(20.0, (1.3 ** attempt) * 1.2)
                    write_log(
                        f"[WARN] {op_desc} quota hit (attempt {attempt + 1}/{max_retry}), sleep {sleep_s:.1f}s: {e}"
                    )
                    time.sleep(sleep_s)
            if last_err:
                raise last_err

        def _calc_eta(done_count: int) -> str:
            elapsed = max(0.0, time.time() - run_started_at)
            if done_count > 0 and done_count < target_total:
                avg = elapsed / done_count
                remain = int(avg * (target_total - done_count))
                return f"{remain}s"
            return "---"

        def _start_row(block_name: str, row: int, url: str):
            nonlocal started_count
            with counter_lock:
                started_count += 1
                done = processed_count
                okv = success_count
                failv = fail_count
                percent = int((done / max(1, target_total)) * 100)
                eta = _calc_eta(done)
            ui_call(ui_set_progress, percent)
            ui_call(ui_set_detail, f"{block_name} - hàng {row}")
            ui_call(ui_update_summary, done, target_total, okv, failv, eta)
            ui_call(ui_add_log, row, "START", "START", f"{block_name}: Link {url[:110]}", "start")
            return eta

        def _finish_row_ok(
            block_name: str,
            row: int,
            url: str,
            eta: str,
            msg: str | None = None,
            log_tag: str = "ok",
        ):
            nonlocal processed_count, success_count, fail_count
            with error_lock:
                tracked_error_rows.discard(row)
                tracked_error_details.pop(row, None)
            with counter_lock:
                processed_count += 1
                success_count += 1
                fail_count = len(tracked_error_rows)
                done = processed_count
                okv = success_count
                failv = fail_count
                percent = int((done / max(1, target_total)) * 100)
                eta = _calc_eta(done)
            text = msg if msg else f"{block_name}: {url[:110]}"
            ui_call(ui_add_log, row, "OK", "OK", text, log_tag)
            if hasattr(app, "update_error_row_live"):
                ui_call(app.update_error_row_live, row, "", False)
            ui_call(ui_set_progress, percent)
            ui_call(ui_update_summary, done, target_total, okv, failv, eta)

        def _finish_row_fail(block_name: str, row: int, err: str, eta: str):
            nonlocal processed_count, fail_count
            with error_lock:
                tracked_error_rows.add(row)
                err_text = str(err).strip()
                if not err_text:
                    err_text = "Lỗi xử lý"
                if err_text.lower().startswith(str(block_name).strip().lower() + ":"):
                    msg_store = err_text
                else:
                    msg_store = f"{block_name}: {err_text}"
                tracked_error_details[row] = msg_store[:220]
            with counter_lock:
                processed_count += 1
                fail_count = len(tracked_error_rows)
                done = processed_count
                okv = success_count
                failv = fail_count
                percent = int((done / max(1, target_total)) * 100)
                eta = _calc_eta(done)
            ui_call(ui_add_log, row, "FAIL", "FAIL", f"{block_name}: {err}", "fail")
            if hasattr(app, "update_error_row_live"):
                ui_call(app.update_error_row_live, row, msg_store, True)
            ui_call(ui_set_progress, percent)
            ui_call(ui_update_summary, done, target_total, okv, failv, eta)

        def _start_worker_driver(worker_idx: int):
            if scan_only_request:
                return None
            if worker_idx == 0 and app.driver:
                return app.driver
            worker_profile = os.path.join(TEMP_DIR, f"chrome_profile_worker_{worker_idx}")
            os.makedirs(worker_profile, exist_ok=True)
            # Copy login session from main profile so parallel workers don't ask login again.
            try:
                has_local_session = os.path.isdir(os.path.join(worker_profile, "Default"))
                seed_profile = LOCAL_PROFILE_PATH if os.path.isdir(LOCAL_PROFILE_PATH) else ""
                if not seed_profile and os.path.isdir(FB_PROFILE_PATH):
                    seed_profile = FB_PROFILE_PATH
                if not seed_profile and os.path.isdir(FB_PROFILE_PATH_ALT):
                    seed_profile = FB_PROFILE_PATH_ALT
                if seed_profile and not has_local_session:
                    shutil.copytree(
                        seed_profile,
                        worker_profile,
                        dirs_exist_ok=True,
                        ignore=shutil.ignore_patterns(
                            "Cache",
                            "Code Cache",
                            "GPUCache",
                            "GrShaderCache",
                            "ShaderCache",
                            "Crashpad",
                            "Singleton*",
                            "lockfile",
                            "*.tmp",
                        ),
                    )
            except Exception as e:
                write_log(f"[WARN] Worker profile seed failed ({worker_idx}): {e}")
            worker_port = get_post_port(worker_idx, browser_port)
            last = None
            for headless in (True, False):
                try:
                    return webdriver.Chrome(
                        service=service,
                        options=build_chrome_options(user_data_dir=worker_profile, headless=headless, debug_port=worker_port),
                    )
                except Exception as e:
                    last = e
            raise Exception(f"WORKER_CHROME_START_FAILED[{worker_idx}]: {last}")

        def _run_block(block: dict, worker_idx: int):
            worker_driver = _start_worker_driver(worker_idx)
            local_is_main_driver = bool(worker_driver) and (worker_driver is app.driver)
            try:
                # Each worker gets its own Google API clients to avoid SSL/socket corruption
                # when sharing httplib2 transport across threads.
                local_creds = ServiceAccountCredentials.from_json_keyfile_name(
                    JSON_PATH,
                    [
                        "https://spreadsheets.google.com/feeds",
                        "https://www.googleapis.com/auth/drive",
                    ],
                )
                local_client = gspread.authorize(local_creds)
                local_spreadsheet = local_client.open_by_url(sheet_url)
                local_worksheet = local_spreadsheet.worksheet(sheet_name)
                local_drive_service = build("drive", "v3", credentials=local_creds)
                block_name = block["name"]
                idx_profile = block["idx_profile"]
                idx_content = block["idx_content"]
                idx_drive = block["idx_drive"]
                idx_screenshot = block["idx_screenshot"]
                idx_air_date = block["idx_air_date"]
                fixed_air_date = str(block.get("fixed_air_date", "")).strip()
                block_mode = str(block.get("mode", "seeding")).strip().lower()
                is_scan_mode = block_mode == "scan"
                log_block_name = block_name
                links = block["links"]
                row_numbers = block.get("row_numbers") or []
                scan_expected_texts = block.get("scan_expected_texts", [])
                results = block["results"]
                captions_existing = block["captions_existing"]
                air_dates = block["air_dates"]
                start_at = block["start_line"]
                col_profile_letter = block["col_profile"]
                col_content_letter = block["col_content"]
                col_screenshot_letter = block["col_screenshot"]
                col_drive_letter = block["col_drive"]
                try:
                    multi_capture_enabled = bool(getattr(app, "capture_five_per_link", None).get())
                except Exception:
                    multi_capture_enabled = False
                captures_per_link = 5 if (multi_capture_enabled and block_mode == "booking") else 1

                for idx, url in enumerate(links):
                    if not app.is_running:
                        break
                    while getattr(app, "is_paused", False):
                        time.sleep(0.5)

                    row = row_numbers[idx] if idx < len(row_numbers) else (idx + 4)
                    if row < start_at:
                        continue
                    if requested_rows and row not in requested_rows:
                        continue
                    if only_error_mode and row not in tracked_error_rows:
                        continue

                    url = str(url).strip()
                    if is_scan_mode:
                        url = normalize_scan_source_url(url)
                        if not url:
                            continue
                    else:
                        if not url.startswith("http"):
                            continue

                    expected_scan_text = ""
                    if is_scan_mode and idx < len(scan_expected_texts):
                        expected_scan_text = str(scan_expected_texts[idx]).strip()

                    eta_text = _start_row(log_block_name, row, url)

                    if (not is_scan_mode) and (not app.force_run_all.get()) and idx_drive and idx_content:
                        if idx < len(results) and "drive.google.com" in str(results[idx]):
                            if idx < len(captions_existing) and captions_existing[idx].strip():
                                _finish_row_ok(log_block_name, row, url, eta_text)
                                continue

                    try:
                        unavailable = False
                        profile_name = ""
                        caption = ""
                        _post_time = ""
                        ocr_text = ""
                        text_scan_source = ""
                        if is_scan_mode:
                            try:
                                image_bytes = download_image_bytes_for_scan(url, drive_service=local_drive_service)
                                if image_bytes:
                                    ocr_text = ocr_text_from_image_bytes(image_bytes, expected_text=expected_scan_text)
                            except Exception as ocr_e:
                                write_log(f"[WARN] OCR failed row {row}: {ocr_e}")
                                ocr_text = ""
                        else:
                            worker_driver.get(url)
                            wait_page_ready(worker_driver, timeout=PAGE_READY_TIMEOUT)

                            url_lower = url.lower()
                            is_tiktok = "tiktok.com" in url_lower or "vt.tiktok.com" in url_lower
                            if is_tiktok:
                                try:
                                    worker_driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                                    time.sleep(TIKTOK_SCROLL_WAIT_1)
                                    worker_driver.execute_script("window.scrollTo(0, window.innerHeight / 2);")
                                    time.sleep(TIKTOK_SCROLL_WAIT_2)
                                except Exception:
                                    pass
                            time.sleep(PER_LINK_BASE_WAIT)

                            unavailable = is_unavailable_content_page(worker_driver, url)
                            if unavailable:
                                profile_name, caption = "", "Nội dung không khả dụng"
                                _post_time = ""
                            else:
                                profile_name, caption = get_fb_profile_and_caption(worker_driver, url)
                                _post_time = get_fb_post_datetime(worker_driver)
                            try:
                                worker_driver.execute_cdp_cmd(
                                    "Emulation.setPageScaleFactor",
                                    {"pageScaleFactor": CAPTURE_ZOOM_PERCENT / 100.0},
                                )
                            except Exception:
                                pass
                            try:
                                worker_driver.execute_script(f"document.body.style.zoom='{CAPTURE_ZOOM_PERCENT}%'")
                                time.sleep(ZOOM_SETTLE_SLEEP)
                            except Exception:
                                pass

                        link_drive = ""
                        direct_url = ""
                        if (not is_scan_mode) and (idx_drive or idx_screenshot):
                            effective_captures = 1 if unavailable else captures_per_link
                            sheet_air_raw = str(air_dates[idx]).strip() if (idx_air_date and idx < len(air_dates)) else ""
                            air_date = get_air_date_token(sheet_air_raw) or fixed_air_date or get_air_date_token(_post_time)
                            platform_token = sanitize_filename_token(detect_platform_label(url), fallback="Other", max_len=24)
                            kol_token = sanitize_filename_token(profile_name, fallback="UnknownKOL", max_len=60)
                            date_token = sanitize_filename_token(air_date, fallback="NoDate", max_len=16)
                            base_name = f"Post_{platform_token}_{kol_token}_{date_token}_Row_{row}"
                            captured_pngs: list[bytes] = []

                            def _upload_png_as(file_name: str, png_data: bytes):
                                media = MediaIoBaseUpload(io.BytesIO(png_data), mimetype="image/png", resumable=False)
                                with drive_cache_lock:
                                    existing_files = list(existing_files_by_name.get(file_name, []))

                                if existing_files:
                                    is_new_file_local = False
                                    file_id_local = existing_files[0]
                                    if len(existing_files) > 1:
                                        for dup in existing_files[1:]:
                                            try:
                                                local_drive_service.files().delete(fileId=dup, supportsAllDrives=True).execute()
                                            except Exception:
                                                pass
                                        with drive_cache_lock:
                                            existing_files_by_name[file_name] = [file_id_local]
                                    local_drive_service.files().update(
                                        fileId=file_id_local,
                                        media_body=media,
                                        supportsAllDrives=True,
                                    ).execute()
                                else:
                                    is_new_file_local = True
                                    file_meta_local = {"name": file_name, "parents": [drive_id]}
                                    uploaded_local = local_drive_service.files().create(
                                        body=file_meta_local,
                                        media_body=media,
                                        fields="id",
                                        supportsAllDrives=True,
                                    ).execute()
                                    file_id_local = uploaded_local.get("id")
                                    if not file_id_local:
                                        raise Exception("UPLOAD_FAIL")
                                    with drive_cache_lock:
                                        existing_files_by_name[file_name] = [file_id_local]

                                if is_new_file_local:
                                    local_drive_service.permissions().create(
                                        fileId=file_id_local,
                                        body={"type": "anyone", "role": "reader"},
                                        supportsAllDrives=True,
                                    ).execute()
                                file_info_local = local_drive_service.files().get(
                                    fileId=file_id_local,
                                    fields="webViewLink",
                                    supportsAllDrives=True,
                                ).execute()
                                web_link_local = file_info_local.get("webViewLink")
                                direct_local = f"https://drive.google.com/uc?export=view&id={file_id_local}&ts={int(time.time())}"
                                return file_id_local, web_link_local, direct_local

                            for shot_idx in range(1, effective_captures + 1):
                                if shot_idx == 1:
                                    time.sleep(SCREENSHOT_CAPTURE_DELAY)
                                else:
                                    time.sleep(MULTI_CAPTURE_INTERVAL_SEC)
                                png_bytes = worker_driver.get_screenshot_as_png()
                                captured_pngs.append(png_bytes)
                                if effective_captures > 1:
                                    file_name = f"{base_name}_S{shot_idx}.png"
                                else:
                                    file_name = f"{base_name}.png"
                                file_id, web_link, direct_link = _upload_png_as(file_name, png_bytes)
                                if shot_idx == 1:
                                    link_drive = web_link
                                    direct_url = direct_link
                                ui_call(
                                    ui_add_log,
                                    row,
                                    "INFO",
                                    "SHOT",
                                    f"{block_name}: Đã chụp {shot_idx}/{effective_captures}",
                                    "start",
                                )
                            if effective_captures > 1 and idx_screenshot:
                                collage_png = build_collage_png(captured_pngs)
                                if collage_png:
                                    collage_name = f"{base_name}_ALL.png"
                                    _fid_all, web_all, direct_all = _upload_png_as(collage_name, collage_png)
                                    link_drive = web_all or link_drive
                                    direct_url = direct_all or direct_url
                                    ui_call(
                                        ui_add_log,
                                        row,
                                        "INFO",
                                        "SHOT",
                                        f"{block_name}: Đã gộp {captures_per_link} ảnh vào 1 ô",
                                        "start",
                                    )

                        is_youtube = ("youtube.com" in url) or ("youtu.be" in url)
                        is_facebook = ("facebook.com" in url) or ("fb.watch" in url) or ("m.facebook.com" in url)
                        if unavailable:
                            col_i = "Nội dung không khả dụng"
                        elif is_youtube:
                            profile_name = get_youtube_channel(worker_driver) or profile_name
                            col_i = (get_youtube_title(worker_driver) or "").strip()
                        else:
                            if is_facebook:
                                profile_name = clean_fb_profile_name(profile_name)
                            col_i = caption.strip() if caption else ""
                        profile_name = normalize_account_name(profile_name, url)
                        if (not unavailable) and is_facebook and (not profile_name) and (not col_i.strip()):
                            unavailable = True
                            col_i = "Nội dung không khả dụng"

                        updates = []
                        scan_ok = False
                        if is_scan_mode:
                            scan_ok = is_scan_match(expected_scan_text, ocr_text)
                            e_norm = normalize_match_text(expected_scan_text)
                            o_norm = normalize_match_text(ocr_text)
                            ratio_dbg = difflib.SequenceMatcher(None, e_norm, o_norm).ratio() if e_norm and o_norm else 0.0
                            write_log(
                                f"[SCAN] row={row} match={int(scan_ok)} ratio={ratio_dbg:.2f} "
                                f"exp='{e_norm[:90]}' ocr='{o_norm[:90]}'"
                            )
                            if idx_drive:
                                updates.append({"range": f"{col_drive_letter}{row}", "values": [["1" if scan_ok else "0"]]})
                        else:
                            if idx_profile and profile_name:
                                updates.append({"range": f"{col_profile_letter}{row}", "values": [[profile_name]]})
                            if idx_drive:
                                updates.append({"range": f"{col_drive_letter}{row}", "values": [[link_drive]]})
                            if idx_screenshot and direct_url:
                                updates.append({"range": f"{col_screenshot_letter}{row}", "values": [[f'=IMAGE(\"{direct_url}\")']]})
                            if idx_content and col_i:
                                updates.append({"range": f"{col_content_letter}{row}", "values": [[col_i]]})

                        if updates:
                            safe_sheet_write(
                                lambda: local_worksheet.batch_update(updates, value_input_option="USER_ENTERED"),
                                op_desc=f"batch_update_row_{row}",
                            )
                        if is_scan_mode:
                            if scan_ok:
                                _finish_row_ok(
                                    log_block_name,
                                    row,
                                    url,
                                    eta_text,
                                    msg=f"{log_block_name}: MATCH",
                                    log_tag="ok",
                                )
                            else:
                                _finish_row_fail(log_block_name, row, "NO_MATCH", eta_text)
                        elif unavailable:
                            _finish_row_ok(
                                log_block_name,
                                row,
                                url,
                                eta_text,
                                msg=f"{log_block_name}: Nội dung không khả dụng",
                                log_tag="unavailable",
                            )
                        else:
                            _finish_row_ok(log_block_name, row, url, eta_text)
                    except Exception as e:
                        write_log(f"{log_block_name} row {row} ERROR: {e}")
                        _finish_row_fail(log_block_name, row, str(e), eta_text)
                        if is_scan_mode and idx_drive:
                            try:
                                safe_sheet_write(
                                    lambda: local_worksheet.update_acell(f"{col_drive_letter}{row}", "0"),
                                    op_desc=f"update_result_0_row_{row}",
                                )
                            except Exception:
                                pass
                        if (not is_scan_mode) and idx_drive:
                            try:
                                safe_sheet_write(
                                    lambda: local_worksheet.update_acell(f"{col_drive_letter}{row}", f"ERR: {str(e)[:80]}"),
                                    op_desc=f"update_drive_err_row_{row}",
                                )
                            except Exception:
                                pass
                        if (not is_scan_mode) and idx_content:
                            try:
                                safe_sheet_write(
                                    lambda: local_worksheet.update_acell(f"{col_content_letter}{row}", f"ERR_CAPTION: {str(e)[:80]}"),
                                    op_desc=f"update_caption_err_row_{row}",
                                )
                            except Exception:
                                pass
            finally:
                if (not local_is_main_driver) and worker_driver:
                    try:
                        worker_driver.quit()
                    except Exception:
                        pass

        # Run all configured posts in parallel (no fixed upper limit).
        worker_total = max(1, len(prepared_blocks))
        if len(prepared_blocks) > 1:
            with ThreadPoolExecutor(max_workers=worker_total) as ex:
                futures = [ex.submit(_run_block, b, i) for i, b in enumerate(prepared_blocks)]
                for fu in as_completed(futures):
                    fu.result()
        elif prepared_blocks:
            _run_block(prepared_blocks[0], 0)

        if history_ready:
            set_error_rows_for_sheet(
                sheet_url,
                sheet_name=sheet_name,
                rows=tracked_error_rows,
                details=tracked_error_details,
            )
            write_log(f"[INFO] Error history saved: {len(tracked_error_rows)} row(s) pending")
            if hasattr(app, "refresh_error_history_ui"):
                ui_call(app.refresh_error_history_ui)

        # Force-sync UI error panel from the same runtime source used for final summary,
        # so "Lỗi theo link Sheet" and "Failed" never diverge after run completion.
        try:
            if hasattr(app, "live_error_details"):
                app.live_error_details = {int(k): str(v) for k, v in tracked_error_details.items()}
            if hasattr(app, "_render_error_history_card"):
                ui_call(app._render_error_history_card, dict(tracked_error_details))
        except Exception:
            pass

        final_fail_count = len(tracked_error_details)
        fail_count = final_fail_count
        ui_call(ui_update_summary, processed_count, target_total, success_count, final_fail_count, "---")
        ui_call(ui_set_done)
        if messagebox:
            try:
                stopped_early = (not getattr(app, "is_running", True)) and (processed_count < target_total)
                summary_text = (
                    f"Đã xử lý: {processed_count}/{target_total}\n"
                    f"Success: {success_count}\n"
                    f"Failed: {final_fail_count}"
                )
                if stopped_early:
                    if hasattr(app, "show_completion_popup"):
                        ui_call(app.show_completion_popup, "Đã dừng", f"Tiến trình đã dừng giữa chừng.\n\n{summary_text}", "warn")
                    else:
                        ui_call(messagebox.showwarning, "Đã dừng", f"Tiến trình đã dừng giữa chừng.\n\n{summary_text}")
                elif fail_count > 0:
                    if hasattr(app, "show_completion_popup"):
                        ui_call(app.show_completion_popup, "Hoàn tất (có lỗi)", summary_text, "warn")
                    else:
                        ui_call(messagebox.showwarning, "Hoàn tất (có lỗi)", summary_text)
                else:
                    if hasattr(app, "show_completion_popup"):
                        ui_call(app.show_completion_popup, "Hoàn tất", summary_text, "info")
                    else:
                        ui_call(messagebox.showinfo, "Hoàn tất", summary_text)
            except Exception:
                pass
        ui_call(app.set_inputs_enabled, True)

    except Exception as e:
        write_log(f"FATAL: {e}")
        if history_ready:
            set_error_rows_for_sheet(
                sheet_url,
                sheet_name=sheet_name,
                rows=tracked_error_rows,
                details=tracked_error_details,
            )
            if hasattr(app, "refresh_error_history_ui"):
                ui_call(app.refresh_error_history_ui)
        if messagebox:
            ui_call(messagebox.showerror, "Lỗi hệ thống", str(e))
        else:
            print(f"FATAL: {e}")
        ui_call(app.set_inputs_enabled, True)
    finally:
        if app.driver:
            try:
                app.driver.quit()
            except:
                pass

# ================= RUN =================
def run_headless(drive_id: str, sheet_url: str, sheet_name: str, start_line: int = 4, force_run_all: bool = False, browser_port: int = 9223):
    class _Flag:
        def __init__(self, v=False):
            self._v = bool(v)

        def get(self):
            return self._v

    class _LabelStub:
        def config(self, **kwargs):
            # no-op for headless
            return

    class _RootStub:
        def update(self):
            return

    class _AppStub:
        def __init__(self):
            self.is_running = True
            self.driver = None
            self.start_line = start_line
            self.force_run_all = _Flag(force_run_all)
            self.only_run_error_rows = _Flag(False)
            self.progress = {"value": 0}
            self.label_detail = _LabelStub()
            self.label_status = _LabelStub()
            self.root = _RootStub()

        def set_inputs_enabled(self, enabled: bool):
            return

    app = _AppStub()

    # Run in a separate thread to avoid blocking callers
    def _target():
        try:
            main_logic(app, drive_id, sheet_url, sheet_name, start_line=start_line, browser_port=browser_port)
        except Exception as e:
            write_log(f"run_headless ERROR: {e}")

    t = threading.Thread(target=_target, daemon=True)
    t.start()
    return t

def launch_chrome_for_login(browser_port: int = 9223, profile_path: str | None = None) -> tuple[bool, str]:
    """
    Launch Chrome on specified port for user to login to sites.
    Returns (success: bool, profile_info: str)
    """
    try:
        replaced_headless = False
        has_desktop_session = (
            os.name == "nt"
            or sys.platform == "darwin"
            or bool(os.environ.get("DISPLAY") or os.environ.get("WAYLAND_DISPLAY"))
        )

        def is_port_open(port: int, timeout_sec: float = 0.8) -> bool:
            try:
                with socket.create_connection(("127.0.0.1", port), timeout=timeout_sec):
                    return True
            except Exception:
                return False

        def get_debugger_version(port: int) -> dict:
            endpoint = f"http://127.0.0.1:{port}/json/version"
            try:
                if requests is not None:
                    resp = requests.get(endpoint, timeout=2)
                    if resp.ok:
                        return resp.json() or {}
                    return {}
                import urllib.request
                with urllib.request.urlopen(endpoint, timeout=2) as resp:
                    return json.loads(resp.read().decode("utf-8", errors="ignore") or "{}")
            except Exception as e:
                write_log(f"[WARN] Failed to read debugger version on port {port}: {e}")
                return {}

        def debugger_is_headless(port: int) -> bool:
            meta = get_debugger_version(port)
            browser_text = str(meta.get("Browser") or "")
            user_agent = str(meta.get("User-Agent") or "")
            return "HeadlessChrome" in browser_text or "HeadlessChrome" in user_agent

        def find_listener_pid(port: int) -> int | None:
            try:
                if os.name == "nt":
                    result = subprocess.run(
                        ["netstat", "-ano", "-p", "tcp"],
                        capture_output=True,
                        text=True,
                        timeout=6,
                        check=False,
                    )
                    for raw in (result.stdout or "").splitlines():
                        line = raw.strip()
                        if not line.upper().startswith("TCP"):
                            continue
                        parts = line.split()
                        if len(parts) < 5:
                            continue
                        local_addr = parts[1]
                        state = parts[3].upper()
                        pid_text = parts[4]
                        if state != "LISTENING":
                            continue
                        local_port = local_addr.rsplit(":", 1)[-1].strip("[]")
                        if local_port == str(port) and pid_text.isdigit():
                            return int(pid_text)
                    return None
                result = subprocess.run(
                    ["lsof", "-nPi", f"TCP:{port}", "-sTCP:LISTEN"],
                    capture_output=True,
                    text=True,
                    timeout=6,
                    check=False,
                )
                for raw in (result.stdout or "").splitlines()[1:]:
                    parts = raw.split()
                    if len(parts) > 1 and parts[1].isdigit():
                        return int(parts[1])
            except Exception as e:
                write_log(f"[WARN] Failed to resolve pid for port {port}: {e}")
            return None

        def terminate_process_tree(pid: int) -> bool:
            if pid <= 0:
                return False
            try:
                if os.name == "nt":
                    result = subprocess.run(
                        ["taskkill", "/PID", str(pid), "/F", "/T"],
                        stdout=subprocess.DEVNULL,
                        stderr=subprocess.DEVNULL,
                        timeout=10,
                        check=False,
                    )
                    return result.returncode == 0
                os.kill(pid, signal.SIGTERM)
                return True
            except Exception as e:
                write_log(f"[WARN] Failed to terminate pid {pid}: {e}")
                return False

        def wait_for_port_closed(port: int, timeout_sec: float = 8.0) -> bool:
            deadline = time.time() + timeout_sec
            while time.time() < deadline:
                if not is_port_open(port, timeout_sec=0.4):
                    return True
                time.sleep(0.25)
            return not is_port_open(port, timeout_sec=0.4)

        def find_chrome_binary() -> str | None:
            candidates = []
            if os.name == "nt":
                candidates.extend(
                    [
                        os.path.join(os.environ.get("PROGRAMFILES", r"C:\Program Files"), "Google", "Chrome", "Application", "chrome.exe"),
                        os.path.join(os.environ.get("PROGRAMFILES(X86)", r"C:\Program Files (x86)"), "Google", "Chrome", "Application", "chrome.exe"),
                        os.path.join(os.environ.get("LOCALAPPDATA", ""), "Google", "Chrome", "Application", "chrome.exe"),
                        os.path.join(os.environ.get("PROGRAMFILES", r"C:\Program Files"), "Chromium", "Application", "chrome.exe"),
                        os.path.join(os.environ.get("PROGRAMFILES(X86)", r"C:\Program Files (x86)"), "Chromium", "Application", "chrome.exe"),
                        os.path.join(os.environ.get("PROGRAMFILES", r"C:\Program Files"), "Microsoft", "Edge", "Application", "msedge.exe"),
                        os.path.join(os.environ.get("PROGRAMFILES(X86)", r"C:\Program Files (x86)"), "Microsoft", "Edge", "Application", "msedge.exe"),
                    ]
                )
            elif sys.platform == "darwin":
                candidates.extend(
                    [
                        "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome",
                        "/Applications/Chromium.app/Contents/MacOS/Chromium",
                        "/Applications/Microsoft Edge.app/Contents/MacOS/Microsoft Edge",
                    ]
                )
            else:
                for cmd in ["google-chrome", "google-chrome-stable", "chromium", "chromium-browser", "microsoft-edge", "microsoft-edge-stable"]:
                    p = shutil.which(cmd)
                    if p:
                        candidates.append(p)

            for cmd in ["chrome", "chrome.exe", "google-chrome", "chromium", "msedge", "msedge.exe"]:
                p = shutil.which(cmd)
                if p:
                    candidates.append(p)
            for p in candidates:
                if p and os.path.exists(p):
                    return p
            try:
                import winreg
                for root in (winreg.HKEY_CURRENT_USER, winreg.HKEY_LOCAL_MACHINE):
                    try:
                        with winreg.OpenKey(root, r"SOFTWARE\Microsoft\Windows\CurrentVersion\App Paths\chrome.exe") as key:
                            val, _ = winreg.QueryValueEx(key, None)
                            if val and os.path.exists(val):
                                return val
                    except Exception:
                        continue
            except Exception:
                pass
            return None

        def open_visible_window(chrome_path: str, profile: str):
            args_visible = [
                chrome_path,
                f"--remote-debugging-port={browser_port}",
                "--remote-debugging-address=127.0.0.1",
                f"--user-data-dir={profile}",
                "--new-window",
                "--window-size=1200,900",
                "about:blank",
            ]
            subprocess.Popen(args_visible, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

        def open_tab_in_existing_debugger(target_url: str = "about:blank") -> bool:
            try:
                endpoint = f"http://127.0.0.1:{browser_port}/json/new?{quote(target_url, safe=':/?=&,%#')}"
                if requests is not None:
                    resp = requests.put(endpoint, timeout=2)
                    return bool(resp.ok)
                import urllib.request
                req = urllib.request.Request(endpoint, method="PUT")
                with urllib.request.urlopen(req, timeout=2):
                    return True
            except Exception as e:
                write_log(f"[WARN] Failed to open tab via debugger port {browser_port}: {e}")
                return False

        def focus_existing_browser_window(title_hint: str | None = None) -> bool:
            if os.name != "nt":
                return False
            title_hint = str(title_hint or "").strip()
            title_expr = title_hint.replace("'", "''")
            script = """
$ws = New-Object -ComObject WScript.Shell
$targets = @()
if ('__TITLE__'.Length -gt 0) {
  $targets += '__TITLE__'
}
$targets += @('Google Chrome', 'Chrome', 'Microsoft Edge', 'Edge')
foreach ($t in $targets) {
  try {
    if ($ws.AppActivate($t)) { exit 0 }
  } catch {}
}
exit 1
"""
            script = script.replace("__TITLE__", title_expr)
            try:
                result = subprocess.run(
                    ["powershell", "-NoProfile", "-Command", script],
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL,
                    timeout=4,
                    check=False,
                )
                return result.returncode == 0
            except Exception as e:
                write_log(f"[WARN] Failed to focus existing browser window: {e}")
                return False

        def open_focus_marker_tab() -> tuple[bool, str]:
            marker_title = f"Tool Evidence Chrome {browser_port}"
            marker_html = (
                "<html><head>"
                f"<title>{marker_title}</title>"
                "</head><body style='font-family:Segoe UI,Arial,sans-serif;"
                "background:#0f172a;color:#dbeafe;display:grid;place-items:center;"
                "height:100vh;margin:0'>"
                f"<div>Chrome debugger port {browser_port}</div>"
                "</body></html>"
            )
            target_url = "data:text/html," + marker_html
            opened = open_tab_in_existing_debugger(target_url)
            if opened:
                time.sleep(0.35)
            focused = focus_existing_browser_window(marker_title)
            return opened, marker_title if focused else marker_title

        chrome_path = find_chrome_binary()
        if not chrome_path:
            return False, "Chrome not found (missing chrome.exe)"

        if not has_desktop_session:
            return (
                False,
                "Môi trường web deploy không có giao diện desktop để mở Chrome. Hãy dùng local web/app nếu cần Chrome 9223 trên máy của bạn.",
            )

        profile = profile_path or LOCAL_PROFILE_PATH
        os.makedirs(profile, exist_ok=True)
        if os.path.abspath(profile) != os.path.abspath(LOCAL_PROFILE_PATH):
            try:
                if not os.path.isdir(os.path.join(profile, "Default")) and os.path.isdir(LOCAL_PROFILE_PATH):
                    shutil.copytree(
                        LOCAL_PROFILE_PATH,
                        profile,
                        dirs_exist_ok=True,
                        ignore=shutil.ignore_patterns(
                            "Cache",
                            "Code Cache",
                            "GPUCache",
                            "GrShaderCache",
                            "ShaderCache",
                            "Crashpad",
                            "Singleton*",
                            "lockfile",
                            "*.tmp",
                        ),
                    )
                    write_log(f"[INFO] Seeded login profile '{profile}' from LOCAL profile")
            except Exception as e:
                write_log(f"[WARN] Failed to seed login profile '{profile}': {e}")
        # Cleanup stale lock files that can block opening profile window.
        for fn in ["SingletonLock", "SingletonCookie", "SingletonSocket", "lockfile"]:
            try:
                p = os.path.join(profile, fn)
                if os.path.exists(p):
                    os.remove(p)
            except Exception:
                pass

        # If debug port is already alive, still force opening a visible Chrome window.
        if is_port_open(browser_port):
            if debugger_is_headless(browser_port):
                pid = find_listener_pid(browser_port)
                if pid is None:
                    return False, f"Port {browser_port} đang bị HeadlessChrome chiếm nhưng không tìm được PID để mở lại window"
                if not terminate_process_tree(pid):
                    return False, f"Port {browser_port} đang bị HeadlessChrome chiếm và không thể dừng process {pid}"
                if not wait_for_port_closed(browser_port):
                    return False, f"Port {browser_port} vẫn còn bị chiếm sau khi dừng HeadlessChrome (PID {pid})"
                replaced_headless = True
                write_log(f"[INFO] Replaced headless Chrome on port {browser_port} (pid={pid}) with visible window request")
            else:
                opened_tab, marker_title = open_focus_marker_tab()
                focused = focus_existing_browser_window(marker_title)
                try:
                    open_visible_window(chrome_path, profile)
                except Exception as e:
                    write_log(f"[WARN] Port {browser_port} is open but failed to open visible window: {e}")
                    if os.name == "nt":
                        try:
                            fallback_cmd = (
                                f'start "" "{chrome_path}" --remote-debugging-port={browser_port} '
                                f'--remote-debugging-address=127.0.0.1 --user-data-dir="{profile}" --new-window about:blank'
                            )
                            subprocess.Popen(["cmd", "/c", fallback_cmd], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
                        except Exception:
                            pass
                if not focused:
                    focused = focus_existing_browser_window(marker_title)
                write_log(
                    f"[INFO] Chrome debug port {browser_port} already active. "
                    f"opened_tab={opened_tab}, focused={focused}, marker_title={marker_title}"
                )
                if opened_tab or focused:
                    return True, f"Port {browser_port} already active; opened existing Chrome session"
                return True, f"Port {browser_port} already active; check the existing Chrome window"

        args = [
            chrome_path,
            f"--remote-debugging-port={browser_port}",
            f"--user-data-dir={profile}",
            "--new-window",
            f"--window-size={CAPTURE_WINDOW_SIZE}",
            "--force-device-scale-factor=1",
            "--high-dpi-support=1",
            "--no-first-run",
            "--no-default-browser-check",
            "--disable-background-networking",
            "--disable-sync",
            "about:blank",
        ]

        write_log(f"[INFO] Launch Chrome cmd: {args[0]}")
        subprocess.Popen(args, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        for _ in range(12):
            if is_port_open(browser_port):
                write_log(f"[INFO] Chrome launched on port {browser_port} for login. Profile: {profile}")
                if replaced_headless:
                    return True, f"Port {browser_port} đã được mở lại thành window thật. Profile: {os.path.basename(profile)}"
                return True, f"Port {browser_port}, Profile: {os.path.basename(profile)}"
            time.sleep(0.5)

        # Fallback launch via shell alias (some machines only resolve chrome via App Paths/PATH).
        if os.name == "nt":
            fallback_cmd = (
                f'start "" chrome --remote-debugging-port={browser_port} '
                f'--user-data-dir="{profile}" --new-window about:blank'
            )
            subprocess.Popen(["cmd", "/c", fallback_cmd], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            for _ in range(8):
                if is_port_open(browser_port):
                    write_log(f"[INFO] Chrome launched via fallback shell command on port {browser_port}.")
                    if replaced_headless:
                        return True, f"Port {browser_port} đã được mở lại thành window thật. Profile: {os.path.basename(profile)}"
                    return True, f"Port {browser_port} (fallback), Profile: {os.path.basename(profile)}"
                time.sleep(0.5)

        write_log(f"[ERROR] Chrome started but port {browser_port} is not reachable after direct + fallback launch.")
        return False, f"Chrome did not expose debug port {browser_port}"
    except Exception as e:
        write_log(f"[ERROR] Failed to launch Chrome for login: {e}")
        return False, str(e)

if __name__ == "__main__":
    if tk is None:
        raise RuntimeError("Tkinter is unavailable in this environment. Use web_ui.py for web mode.")
    root = tk.Tk()
    app = ProgressApp(root)
    root.mainloop()


