"""ASGI entrypoint: ``uvicorn app.asgi:app``."""

from .main import app

__all__ = ["app"]
