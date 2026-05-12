"""
Security utilities: password hashing, token generation, rate limiting.
"""

import hashlib
import hmac
import secrets
import time
from collections import defaultdict
from typing import Optional

from app.core.settings import settings


# ============================================================
# Password hashing (simple PBKDF2 — no external deps needed)
# ============================================================

def hash_password(password: str, salt: Optional[str] = None) -> str:
    """Hash password with PBKDF2-SHA256. Returns 'salt$hash'."""
    if salt is None:
        salt = secrets.token_hex(16)
    dk = hashlib.pbkdf2_hmac("sha256", password.encode(), salt.encode(), 100_000)
    return f"{salt}${dk.hex()}"


def verify_password(password: str, stored: str) -> bool:
    """Verify password against stored 'salt$hash'."""
    if "$" not in stored:
        # Legacy plain-text comparison (for migration)
        return hmac.compare_digest(password, stored)
    salt, _ = stored.split("$", 1)
    return hmac.compare_digest(hash_password(password, salt), stored)


# ============================================================
# Rate Limiter (in-memory, per-IP)
# ============================================================

class RateLimiter:
    """Simple in-memory sliding-window rate limiter."""

    def __init__(self, max_requests: int = 5, window_seconds: int = 60):
        self.max_requests = max_requests
        self.window = window_seconds
        self._hits: dict[str, list[float]] = defaultdict(list)

    def is_allowed(self, key: str) -> bool:
        """Check if request is allowed. Returns False if rate exceeded."""
        now = time.time()
        window_start = now - self.window
        # Clean old entries
        self._hits[key] = [t for t in self._hits[key] if t > window_start]
        if len(self._hits[key]) >= self.max_requests:
            return False
        self._hits[key].append(now)
        return True

    def reset(self, key: str) -> None:
        """Reset rate limit for a key (e.g., after successful login)."""
        self._hits.pop(key, None)


# Global rate limiter instance for login
login_limiter = RateLimiter(
    max_requests=settings.LOGIN_RATE_LIMIT,
    window_seconds=60,
)


# ============================================================
# CSRF Token
# ============================================================

def generate_csrf_token() -> str:
    """Generate a CSRF token."""
    return secrets.token_hex(32)


def validate_csrf_token(session_token: str, form_token: str) -> bool:
    """Validate CSRF token from form matches session."""
    if not session_token or not form_token:
        return False
    return hmac.compare_digest(session_token, form_token)
