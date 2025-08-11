import os
from app.core.settings import settings


def uploads_path() -> str:
    path = settings.UPLOAD_FOLDER
    os.makedirs(path, exist_ok=True)
    return path


def allowed_file(filename: str) -> bool:
    return "." in filename and filename.rsplit(".", 1)[1].lower() in settings.ALLOWED_EXTENSIONS
