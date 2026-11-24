"""Runtime configuration for the gateway log aggregator."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


def _env_bool(name: str, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


@dataclass(frozen=True)
class Settings:
    """Filesystem and feature settings.

    ``data_dir`` is the only directory jobs may read source shards from.
    ``work_dir`` holds per-job temporary part files and manifests.
    """

    data_dir: Path
    work_dir: Path
    allow_simulation: bool = False

    def validate(self) -> None:
        if not str(self.data_dir):
            raise ValueError("data_dir must not be empty")
        if not str(self.work_dir):
            raise ValueError("work_dir must not be empty")


def load_settings() -> Settings:
    settings = Settings(
        data_dir=Path(os.getenv("LOG_DATA_DIR", "/data")).expanduser().resolve(),
        work_dir=Path(os.getenv("WORK_DIR", "/work")).expanduser().resolve(),
        allow_simulation=_env_bool("ALLOW_SIMULATION", False),
    )
    settings.validate()
    return settings
