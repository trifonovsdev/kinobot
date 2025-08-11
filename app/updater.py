import argparse
import json
import os
import shutil
import subprocess
import sys
import tempfile
import time
import zipfile
from pathlib import Path

import logging


def setup_logger(log_path: Path) -> logging.Logger:
    log_path.parent.mkdir(parents=True, exist_ok=True)
    logger = logging.getLogger("updater")
    logger.setLevel(logging.INFO)
    fmt = logging.Formatter("%(asctime)s %(levelname)s: %(message)s")
    fh = logging.FileHandler(log_path, encoding="utf-8")
    fh.setFormatter(fmt)
    logger.addHandler(fh)
    sh = logging.StreamHandler(sys.stdout)
    sh.setFormatter(fmt)
    logger.addHandler(sh)
    return logger


def unzip(zip_path: Path, dst_dir: Path):
    with zipfile.ZipFile(zip_path, 'r') as z:
        z.extractall(dst_dir)


def make_backup(root: Path, backup_dir: Path, exclude: list[str]) -> Path:
    backup_dir.mkdir(parents=True, exist_ok=True)
    stamp = time.strftime("%Y%m%d_%H%M%S")
    backup_zip = backup_dir / f"backup_{stamp}.zip"
    with zipfile.ZipFile(backup_zip, 'w', compression=zipfile.ZIP_DEFLATED) as z:
        for base, dirs, files in os.walk(root):
            rel_base = os.path.relpath(base, root)
            # skip excluded top-levels
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


def overlay_copy(src_root: Path, dst_root: Path, exclude: list[str]):
    # Копируем поверх, пропуская исключения. Старые лишние файлы не удаляем.
    for item in src_root.iterdir():
        if item.name in exclude:
            continue
        dst = dst_root / item.name
        # не затираем сам апдейтер, пока он работает
        try:
            if dst.resolve() == Path(__file__).resolve():
                continue
        except Exception:
            pass
        if item.is_dir():
            if dst.exists() and not dst.is_dir():
                dst.unlink()
            dst.mkdir(parents=True, exist_ok=True)
            overlay_copy(item, dst, exclude)
        else:
            if dst.exists() and dst.is_dir():
                shutil.rmtree(dst)
            dst.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(item, dst)


def run_cmd(logger: logging.Logger, args: list[str], cwd: Path) -> int:
    logger.info("RUN: %s", " ".join(args))
    proc = subprocess.Popen(args, cwd=str(cwd))
    return proc.wait()


def run_post_install(logger: logging.Logger, cmds: list[str], cwd: Path) -> None:
    for cmd in cmds:
        logger.info("POST_INSTALL: %s", cmd)
        rc = subprocess.call(cmd, cwd=str(cwd), shell=True)
        if rc != 0:
            raise RuntimeError(f"post_install failed: {cmd} (rc={rc})")


def process_delete_list(logger: logging.Logger, app_dir: Path, staging: Path) -> None:
    """Удаляет файлы/папки, перечисленные в файле 'delete' в staging.

    Формат файла:
      - по одной записи в строке, относительный путь от корня приложения
      - поддерживаются и прямые, и обратные слеши
      - пустые строки и строки, начинающиеся с '#' — игнорируются
    Пример строки: app/web/updater.py
    """
    delete_file = staging / "delete"
    if not delete_file.exists():
        return
    try:
        lines = delete_file.read_text(encoding="utf-8").splitlines()
    except Exception:
        try:
            lines = delete_file.read_text(encoding="cp1251").splitlines()
        except Exception as e:
            logger.warning("Cannot read delete list: %s", e)
            return

    for raw in lines:
        s = (raw or "").strip()
        if not s or s.startswith("#"):
            continue
        # Нормализуем разделители
        rel = Path(s.replace("\\", "/"))
        # Не позволяем абсолютные пути
        if rel.is_absolute():
            logger.warning("Skip absolute path in delete list: %s", s)
            continue
        target = (app_dir / rel).resolve()
        # Безопасность: гарантируем, что цель внутри app_dir
        try:
            app_root = app_dir.resolve()
        except Exception:
            app_root = app_dir
        if app_root not in target.parents and target != app_root:
            logger.warning("Skip outside-of-root path: %s -> %s", s, target)
            continue
        try:
            if target.exists() or target.is_symlink():
                if target.is_dir() and not target.is_symlink():
                    shutil.rmtree(target)
                    logger.info("Deleted directory: %s", target)
                else:
                    target.unlink()
                    logger.info("Deleted file: %s", target)
            else:
                logger.info("Delete path not found (skip): %s", target)
        except Exception as e:
            logger.warning("Failed to delete %s: %s", target, e)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--plan", required=True)
    args = parser.parse_args()

    plan_path = Path(args.plan)
    plan = json.loads(plan_path.read_text(encoding="utf-8"))

    app_dir = Path(plan["app_dir"]).resolve()
    python_exe = plan.get("python_exe") or sys.executable
    zip_path = Path(plan["zip"]).resolve() if "zip" in plan else None
    dir_path = Path(plan["dir"]).resolve() if "dir" in plan else None
    version = plan.get("version", "")
    exclude = plan.get("exclude") or [
        ".env",
        "venv",
        "data",
        "posters",
        "logs",
        "backups",
        "films.db",
        "users.db",
    ]
    post_install = plan.get("post_install") or []

    log = setup_logger(app_dir / "logs" / "updater.log")
    lock_path = app_dir / ".kb_updating.lock"
    log.info("Starting updater. Plan: %s", plan_path)

    staging = None
    try:
        # 0) Установить lock, чтобы supervisor/systemd могли подождать
        try:
            lock_path.write_text("1", encoding="utf-8")
        except Exception:
            pass

        # 1) Подготовить staging
        if zip_path is not None:
            staging = Path(tempfile.mkdtemp(prefix="kb_stage_"))
            log.info("Unzipping %s to %s", zip_path, staging)
            unzip(zip_path, staging)
            # Если архив содержит корневую папку, а не файлы — сдвинем корень
            children = list(staging.iterdir())
            if len(children) == 1 and children[0].is_dir():
                staging = children[0]
                log.info("Detected single-root folder inside zip: %s", staging)
        elif dir_path is not None:
            staging = dir_path
            log.info("Using pre-staged directory: %s", staging)
        else:
            raise RuntimeError("Update plan must contain either 'zip' or 'dir'")

        # 2) Бэкап текущей установки
        backup_dir = app_dir / "backups"
        backup_zip = make_backup(app_dir, backup_dir, exclude)
        log.info("Backup created: %s", backup_zip)

        # 3) Поставить зависимости (если есть новый requirements.txt в staging)
        req_staging = staging / "requirements.txt"
        if req_staging.exists():
            rc = run_cmd(log, [python_exe, "-m", "pip", "install", "-U", "pip"], app_dir)
            if rc != 0:
                raise RuntimeError("pip upgrade failed")
            rc = run_cmd(log, [python_exe, "-m", "pip", "install", "-r", str(req_staging)], app_dir)
            if rc != 0:
                raise RuntimeError("pip install -r failed")

        # 4) Перекопировать файлы поверх (исключая исключения)
        log.info("Overlay copying files to %s", app_dir)
        overlay_copy(staging, app_dir, exclude)

        # 5) Удалить файлы из списка delete (если присутствует)
        process_delete_list(log, app_dir, staging)

        # 6) Обновить VERSION
        version_file = app_dir / "VERSION"
        if version:
            version_file.write_text(version, encoding="utf-8")
            log.info("VERSION updated to %s", version)

        # 7) post_install команды
        if post_install:
            run_post_install(log, post_install, app_dir)

        # 8) Рестарт приложения
        spawn = os.getenv("UPDATER_SPAWN", "1").lower() in {"1", "true", "yes", "on"}
        if spawn:
            log.info("Restarting app (spawn): %s main.py", python_exe)
            subprocess.Popen([python_exe, str(app_dir / "main.py")], cwd=str(app_dir))
        else:
            log.info("Skipping spawn due to UPDATER_SPAWN=0 (expecting supervisor/systemd to restart)")
        log.info("Updater finished OK")
    except Exception as e:
        log.exception("Updater failed: %s", e)
        # При ошибке можно попытаться откатить из последнего бэкапа вручную.
    finally:
        try:
            if plan_path.exists():
                plan_path.unlink()
        except Exception:
            pass

        # Чистим временные каталоги/файлы, если возможно
        if zip_path is not None:
            try:
                if zip_path.exists():
                    zip_path.unlink()
            except Exception:
                pass

        # staging может не удалиться, если внутри файлы в использовании — допустимо
        # Удаляем staging, если он временный: при zip он всегда временный;
        # при dir — только если в плане явно указано cleanup_dir=true
        cleanup_dir = bool(plan.get("cleanup_dir", zip_path is not None))
        try:
            if cleanup_dir and staging is not None and staging.exists() and staging.is_dir():
                shutil.rmtree(staging)
        except Exception:
            pass

        # Снять lock
        try:
            if lock_path.exists():
                lock_path.unlink()
        except Exception:
            pass


if __name__ == "__main__":
    main()
