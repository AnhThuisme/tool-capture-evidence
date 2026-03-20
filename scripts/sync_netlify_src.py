from __future__ import annotations

import re
from pathlib import Path


ROOT = Path(__file__).resolve().parent.parent
WEB_UI = ROOT / "web_ui.py"
NETLIFY_SRC = ROOT / "netlify_src"


def _extract_block(pattern: str, text: str, label: str) -> str:
    match = re.search(pattern, text, flags=re.DOTALL)
    if not match:
        raise RuntimeError(f"Could not extract {label} from web_ui.py")
    return match.group(1)


def main() -> int:
    source = WEB_UI.read_text(encoding="utf-8")

    login_html = _extract_block(
        r'LOGIN_PAGE_HTML\s*=\s*"""(.*?)"""',
        source,
        "LOGIN_PAGE_HTML",
    )
    root_html = _extract_block(
        r'return HTMLResponse\(\s*"""(.*?)"""\s*\.replace\(',
        source,
        "home_page template",
    )

    replacements = {
        "__AUTH_EMAIL_TITLE__": "",
        "__AUTH_EMAIL__": "",
        "__AUTH_EMAIL_DISPLAY__": "-",
        "__AUTH_ROLE_CLASS__": "user",
        "__AUTH_ROLE__": "user",
        "__AUTH_ROLE_DISPLAY__": "User",
        "__ADMIN_NAV_STYLE__": "display:none",
        "__ADMIN_SECTION_STYLE__": "display:none",
        "__SETTINGS_NAV_STYLE__": "",
        "__SETTINGS_SECTION_STYLE__": "",
        "__AUTH_IS_ADMIN__": "false",
    }
    for old, new in replacements.items():
        root_html = root_html.replace(old, new)

    NETLIFY_SRC.mkdir(parents=True, exist_ok=True)
    (NETLIFY_SRC / "index.html").write_text(root_html, encoding="utf-8", newline="\n")
    (NETLIFY_SRC / "login.html").write_text(login_html, encoding="utf-8", newline="\n")
    print(f"Synced Netlify source from {WEB_UI} -> {NETLIFY_SRC}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
