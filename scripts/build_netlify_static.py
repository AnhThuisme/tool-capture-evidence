from __future__ import annotations

import os
import shutil
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(Path(__file__).resolve().parent))
from sync_netlify_src import main as sync_netlify_src_main

SRC = ROOT / "netlify_src"
DIST = ROOT / "netlify_dist"


def _copy_tree(src: Path, dst: Path) -> None:
    if dst.exists():
        shutil.rmtree(dst)
    dst.mkdir(parents=True, exist_ok=True)
    for item in src.iterdir():
        target = dst / item.name
        if item.is_dir():
            shutil.copytree(item, target)
        else:
            shutil.copy2(item, target)


def _normalize_origin(value: str) -> str:
    return value.strip().rstrip("/")


def main() -> int:
    sync_netlify_src_main()

    backend_origin = _normalize_origin(os.getenv("NETLIFY_BACKEND_ORIGIN", ""))
    if not backend_origin:
        print("Missing NETLIFY_BACKEND_ORIGIN. Example: https://your-backend.example.com", file=sys.stderr)
        return 1

    if not SRC.exists():
        print(f"Missing source folder: {SRC}", file=sys.stderr)
        return 1

    _copy_tree(SRC, DIST)

    redirects = "\n".join(
        [
            f"/api/*  {backend_origin}/api/:splat  200",
            f"/health  {backend_origin}/health  200",
            "/login  /login.html  200",
            "/  /index.html  200",
            "/*  /index.html  200",
            "",
        ]
    )
    (DIST / "_redirects").write_text(redirects, encoding="utf-8")
    print(f"Netlify static build ready in {DIST}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
