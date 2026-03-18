from __future__ import annotations

import os
import sys
from urllib.parse import parse_qs, urlparse

try:
    import tkinter as tk
    from tkinter import messagebox
except Exception:
    tk = None
    messagebox = None

import evidence

MODE_BASE_PORTS = {
    "seeding": 9223,
    "booking": 9423,
    "scan": 9623,
}


def _normalize_mode(value: str | None) -> str:
    mode = str(value or "seeding").strip().lower()
    return mode if mode in MODE_BASE_PORTS else "seeding"


def _get_mode_profile(run_mode: str | None, block_index: int = 0) -> str:
    mode = _normalize_mode(run_mode)
    idx = max(0, int(block_index or 0))
    if mode == "seeding":
        return evidence.LOCAL_PROFILE_PATH if idx <= 0 else os.path.join(evidence.TEMP_DIR, f"chrome_profile_worker_{idx}")
    suffix = f"{mode}_{idx}" if idx > 0 else f"{mode}_main"
    return os.path.join(evidence.TEMP_DIR, f"chrome_profile_{suffix}")


def _notify(title: str, text: str, *, error: bool = False) -> None:
    if tk is None or messagebox is None:
        print(text)
        return
    try:
        root = tk.Tk()
        root.withdraw()
        if error:
            messagebox.showerror(title, text)
        else:
            messagebox.showinfo(title, text)
        root.destroy()
    except Exception:
        print(text)


def main() -> int:
    raw = sys.argv[1] if len(sys.argv) > 1 else ""
    if not raw:
        _notify("Tool Evidence", "Kh?ng c? l?nh Chrome ???c g?i t? web.", error=True)
        return 1

    parsed = urlparse(raw)
    if parsed.scheme != "tool-evidence":
        _notify("Tool Evidence", f"Protocol kh?ng h?p l?: {parsed.scheme}", error=True)
        return 1

    params = parse_qs(parsed.query)
    run_mode = _normalize_mode((params.get("mode") or ["seeding"])[0])
    block_index = max(0, int((params.get("block") or ["0"])[0] or 0))
    default_port = evidence.get_post_port(block_index, MODE_BASE_PORTS[run_mode])
    browser_port = int((params.get("port") or [str(default_port)])[0] or default_port)
    profile_path = ((params.get("profile_path") or [""])[0] or "").strip() or _get_mode_profile(run_mode, block_index)

    ok, info = evidence.launch_chrome_for_login(browser_port=browser_port, profile_path=profile_path)
    if not ok:
        _notify("Tool Evidence", info, error=True)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
