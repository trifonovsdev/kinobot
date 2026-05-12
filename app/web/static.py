"""
Static file helpers.
"""

import os
from app.core.settings import settings


def uploads_path() -> str:
    """Get uploads directory, creating it if needed."""
    path = settings.UPLOAD_FOLDER
    os.makedirs(path, exist_ok=True)
    return path


def allowed_file(filename: str) -> bool:
    """Check if file extension is allowed."""
    return "." in filename and filename.rsplit(".", 1)[1].lower() in settings.ALLOWED_EXTENSIONS
