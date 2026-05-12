"""User domain models."""

from pydantic import BaseModel
from typing import Optional


class User(BaseModel):
    id: int
    name: Optional[str] = None
    tg_id: int
    admin: int = 0
    referral_code: Optional[str] = None
    referred_by: Optional[str] = None
    banned: int = 0

    class Config:
        from_attributes = True


class UserPublic(BaseModel):
    """Public-facing user info (safe to expose)."""
    id: int
    name: Optional[str] = None
    tg_id: int
    admin: int = 0
    banned: int = 0
