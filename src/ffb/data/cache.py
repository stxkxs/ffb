"""Local file cache for NFL data with TTL-based freshness."""

import json
import logging
import shutil
import time
from pathlib import Path

import polars as pl

log = logging.getLogger(__name__)

CACHE_DIR = Path.home() / ".fantasy" / "cache"
DEFAULT_TTL = 6 * 3600  # 6 hours


def _meta_path() -> Path:
    return CACHE_DIR / "_meta.json"


def _read_meta() -> dict[str, dict]:
    path = _meta_path()
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text())
    except (json.JSONDecodeError, OSError) as e:
        log.warning("corrupt cache metadata, resetting: %s", e)
        path.unlink(missing_ok=True)
        return {}


def _write_meta(meta: dict[str, dict]) -> None:
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    _meta_path().write_text(json.dumps(meta, indent=2))


def get(key: str, ttl: int = DEFAULT_TTL) -> pl.DataFrame | None:
    """Return cached DataFrame if fresh, None if stale or missing."""
    meta = _read_meta()
    entry = meta.get(key)
    if entry is None:
        return None
    if time.time() - entry.get("timestamp", 0) > ttl:
        return None
    path = CACHE_DIR / f"{key}.parquet"
    if not path.exists():
        return None
    try:
        return pl.read_parquet(path)
    except Exception as e:
        log.warning("corrupt cache entry '%s', invalidating: %s", key, e)
        invalidate(key)
        return None


def put(key: str, df: pl.DataFrame) -> None:
    """Write DataFrame to cache."""
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    path = CACHE_DIR / f"{key}.parquet"
    df.write_parquet(path)
    meta = _read_meta()
    meta[key] = {"timestamp": time.time()}
    _write_meta(meta)


def invalidate(key: str | None = None) -> None:
    """Remove a cache entry, or all entries if key is None."""
    if key is None:
        if CACHE_DIR.exists():
            shutil.rmtree(CACHE_DIR)
        return
    meta = _read_meta()
    meta.pop(key, None)
    _write_meta(meta)
    path = CACHE_DIR / f"{key}.parquet"
    if path.exists():
        path.unlink()
