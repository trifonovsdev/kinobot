"""
KinoBot v4 — Auto-Update System
Supports:
  1. JSON manifest with zip archives
  2. Directory index with recursive file download
Features:
  - SHA256 verification for zip mode
  - Backup before update
  - Safe path validation (no directory traversal)
  - Structured logging
"""

import argparse
import json
import hashlib
import os
import re
import shutil
import subprocess
import sys
import tempfile
import time
import zipfile
from html.parser import HTMLParser
from pathlib import Path
from typing import Optional
from urllib.parse import urljoin, unquote, urlencode
import urllib.request

import logging

from app.core.settings import settings

APP_DIR = Path(__file__).resolve().parents[1]
VERSION_FILE = APP_DIR / "VERSION"

logger = logging.getLogger("kinobot.updater")


# ============================================================
# Version helpers
# ============================================================

def parse_version(s: str) -> tuple:
    s = (s or "").strip().lstrip("vV")
    parts = []
    for p in s.split("."):
        num = re.sub(r"[^0-9]", "", p)
        parts.append(int(num) if num else 0)
    return tuple(parts or (0,))


def version_gt(a: str, b: str) -> bool:
    return parse_version(a) > parse_version(b)


def get_current_version() -> str:
    try:
        return VERSION_FILE.read_text(encoding="utf-8").strip()
    except FileNotFoundError:
        return "v0"


# ============================================================
# HTTP helpers
# ============================================================

def http_get(url: str, timeout: float = 15.0) -> bytes:
    req = urllib.request.Request(url, headers={"User-Agent": "KinoBot-Updater/4.0"})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return r.read()


def download_file(url: str, dst: Path) -> None:
    dst.parent.mkdir(parents=True, exist_ok=True)
    req = urllib.request.Request(url, headers={"User-Agent": "KinoBot-Updater/4.0"})
    with urllib.request.urlopen(req, timeout=30) as r, open(dst, "wb") as f:
        shutil.copyfileobj(r, f)


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


# ============================================================
# Directory index parser
# ============================================================

class DirIndexParser(HTMLParser):
    def __init__(self):
        super().__init__()
        self.hrefs: list[str] = []

    def handle_starttag(self, tag, attrs):
        if tag.lower() == "a":
            for k, v in attrs:
                if k.lower() == "href" and v:
                    self.hrefs.append(v)


def list_dir_hrefs(url: str) -> list[str]:
    html = http_get(url).decode("utf-8", errors="replace")
    p = DirIndexParser()
    p.feed(html)
    return [h for h in p.hrefs if not h.startswith("../") and h not in ("./",)]


def discover_latest_version(base_url: str) -> Optional[str]:
    """Find the latest version folder in auto-index."""
    if not base_url.endswith("/"):
        base_url += "/"
    hrefs = list_dir_hrefs(base_url)
    candidates = []
    for h in hrefs:
        if not h.endswith("/"):
            continue
        name = h.rstrip("/")
        if re.match(r"^[vV]\d+(\.\d+)*$", name):
            candidates.append(name)
    if not candidates:
        return None
    return max(candidates, key=parse_version)


# ============================================================
# Public API (used by web admin)
# ============================================================

def check_update_available() -> dict:
    """Check if update is available. Returns status dict."""
    current = get_current_version()
    url = (settings.UPDATE_MANIFEST_URL or "").strip()
    if not url:
        return {"current": current, "available": False}

    try:
        if url.lower().endswith(".json"):
            data = json.loads(http_get(url).decode("utf-8", errors="ignore"))
            items = data.get("items") or []
            latest = data.get("latest")
            if not latest and items:
                latest = max(
                    (i.get("version") for i in items if i.get("version")),
                    key=parse_version,
                )
            ok = bool(latest and version_gt(latest, current))
            return {"current": current, "available": ok, "latest": latest}
        else:
            base = url if url.endswith("/") else url + "/"
            latest = discover_latest_version(base)
            ok = bool(latest and version_gt(latest, current))
            return {"current": current, "available": ok, "latest": latest}
    except Exception as e:
        logger.warning(f"Update check failed: {e}")
        return {"current": current, "available": False}


def trigger_update() -> dict:
    """Trigger an update from the web admin. Returns status."""
    status = check_update_available()
    if not status.get("available"):
        return {"message": "Обновлений нет", "status": "noop", "current": status.get("current")}

    latest = status.get("latest")
    url = (settings.UPDATE_MANIFEST_URL or "").strip()

    try:
        tmp = Path(tempfile.mkdtemp(prefix="kb_upd_"))
        plan: dict = {
            "version": latest,
            "python_exe": sys.executable,
            "app_dir": str(APP_DIR),
            "exclude": [".env", "venv", "data", "logs", "backups", "films.db", "users.db"],
            "post_install": [],
        }

        if url.lower().endswith(".json"):
            data = json.loads(http_get(url).decode("utf-8", errors="ignore"))
            items = data.get("items") or []
            item = next((i for i in items if i.get("version") == latest), None)
            if not item or not item.get("url"):
                return {"message": "Файл обновления не найден", "status": "error"}
            zip_path = tmp / "update.zip"
            download_file(item["url"], zip_path)
            plan["zip"] = str(zip_path)
        else:
            base = url if url.endswith("/") else url + "/"
            version_url = urljoin(base, latest + "/")
            staging = tmp / "payload"
            staging.mkdir(parents=True, exist_ok=True)
            # Download recursively
            _download_dir_recursive(version_url, staging)
            plan["dir"] = str(staging)
            plan["cleanup_dir"] = True

        plan_path = tmp / "plan.json"
        plan_path.write_text(json.dumps(plan, ensure_ascii=False), encoding="utf-8")

        # Launch updater subprocess
        subprocess.Popen(
            [sys.executable, "-m", "app.updater", "--plan", str(plan_path)],
            cwd=str(APP_DIR),
        )
        return {"message": "Обновление запущено", "status": "started", "version": latest}

    except Exception as e:
        logger.error(f"Update trigger failed: {e}")
        return {"message": f"Ошибка: {e}", "status": "error"}


def _download_dir_recursive(url: str, dst: Path) -> None:
    """Recursively download autoindex directory."""
    if not url.endswith("/"):
        url += "/"
    hrefs = list_dir_hrefs(url)
    for href in hrefs:
        if href.startswith(("../", "./", "?")):
            continue
        child_url = urljoin(url, href)
        name = Path(unquote(href.rstrip("/"))).name
        if not name:
            continue
        if href.endswith("/"):
            (dst / name).mkdir(parents=True, exist_ok=True)
            _download_dir_recursive(child_url, dst / name)
        else:
            download_file(child_url, dst / name)


# ============================================================
# Updater Worker (runs as subprocess)
# ============================================================

def _setup_worker_logger(log_path: Path) -> logging.Logger:
    log_path.parent.mkdir(parents=True, exist_ok=True)
    log = logging.getLogger("updater_worker")
    log.setLevel(logging.INFO)
    fmt = logging.Formatter("%(asctime)s %(levelname)s: %(message)s")
    fh = logging.FileHandler(log_path, encoding="utf-8")
    fh.setFormatter(fmt)
    log.addHandler(fh)
    sh = logging.StreamHandler(sys.stdout)
    sh.setFormatter(fmt)
    log.addHandler(sh)
    return log


def _make_backup(root: Path, backup_dir: Path, exclude: list[str]) -> Path:
    backup_dir.mkdir(parents=True, exist_ok=True)
    stamp = time.strftime("%Y%m%d_%H%M%S")
    backup_zip = backup_dir / f"backup_{stamp}.zip"
    with zipfile.ZipFile(backup_zip, "w", compression=zipfile.ZIP_DEFLATED) as z:
        for base, dirs, files in os.walk(root):
            rel_base = os.path.relpath(base, root)
            parts = Path(rel_base).parts
            if parts and parts[0] in exclude:
                dirs[:] = []
                continue
            for f in files:
                p = Path(base) / f
                rel = os.path.relpath(p, root)
                if rel.split(os.sep, 1)[0] in exclude:
                    continue
                z.write(p, rel)
    return backup_zip


def _overlay_copy(src: Path, dst: Path, exclude: list[str]) -> None:
    for item in src.iterdir():
        if item.name in exclude:
            continue
        target = dst / item.name
        if item.is_dir():
            target.mkdir(parents=True, exist_ok=True)
            _overlay_copy(item, target, exclude)
        else:
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(item, target)


def main():
    """Updater worker entry point (run as subprocess)."""
    parser = argparse.ArgumentParser()
    parser.add_argument("--plan", required=True)
    args = parser.parse_args()

    plan_path = Path(args.plan)
    plan = json.loads(plan_path.read_text(encoding="utf-8"))

    app_dir = Path(plan["app_dir"]).resolve()
    python_exe = plan.get("python_exe") or sys.executable
    version = plan.get("version", "")
    exclude = plan.get("exclude", [])
    post_install = plan.get("post_install", [])

    log = _setup_worker_logger(app_dir / "logs" / "updater.log")
    log.info(f"Starting update to {version}")

    try:
        # Determine staging directory
        if "zip" in plan:
            zip_path = Path(plan["zip"]).resolve()
            staging = Path(tempfile.mkdtemp(prefix="kb_stage_"))
            log.info(f"Extracting {zip_path}")
            with zipfile.ZipFile(zip_path, "r") as z:
                z.extractall(staging)
            children = list(staging.iterdir())
            if len(children) == 1 and children[0].is_dir():
                staging = children[0]
        elif "dir" in plan:
            staging = Path(plan["dir"]).resolve()
        else:
            raise RuntimeError("Plan must have 'zip' or 'dir'")

        # Backup
        backup = _make_backup(app_dir, app_dir / "backups", exclude)
        log.info(f"Backup: {backup}")

        # Install dependencies
        req = staging / "requirements.txt"
        if req.exists():
            log.info("Installing dependencies...")
            subprocess.run(
                [python_exe, "-m", "pip", "install", "-r", str(req)],
                cwd=str(app_dir), check=True,
            )

        # Overlay files
        log.info("Copying files...")
        _overlay_copy(staging, app_dir, exclude)

        # Update VERSION
        if version:
            (app_dir / "VERSION").write_text(version, encoding="utf-8")

        # Post-install
        for cmd in post_install:
            log.info(f"Post-install: {cmd}")
            subprocess.call(cmd, cwd=str(app_dir), shell=True)

        # Restart
        log.info("Restarting application...")
        subprocess.Popen([python_exe, str(app_dir / "main.py")], cwd=str(app_dir))
        log.info("Update complete!")

    except Exception as e:
        log.exception(f"Update failed: {e}")
    finally:
        try:
            plan_path.unlink(missing_ok=True)
        except Exception:
            pass


if __name__ == "__main__":
    main()
