"""Film domain models."""

from pydantic import BaseModel, Field
from typing import Optional


class FilmBase(BaseModel):
    name: str = Field(..., min_length=1, max_length=256)
    description: str = ""
    genre: str = ""
    site: str = ""


class FilmCreate(FilmBase):
    pass


class FilmUpdate(FilmBase):
    pass


class Film(FilmBase):
    id: int
    code: Optional[str] = None
    photo_status: int = 0
    photo_id: Optional[str] = None
    activate: int = 1
    external_source: Optional[str] = None
    external_id: Optional[str] = None

    class Config:
        from_attributes = True
