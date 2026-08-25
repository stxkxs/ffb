"""Local file cache for NFL data with TTL-based freshness and an LRU size ceiling."""

import json
import logging
import os
import shutil
import tempfile
import time
from pathlib import Path
from typing import Any

import polars as pl

log = logging.getLogger(__name__)

CACHE_DIR = Path.home() / ".fantasy" / "cache"
DEFAULT_TTL = 6 * 3600  # 6 hours
# Play-by-play for a pair of seasons lands around 100 MB, so this holds a working
# set of many season tuples while bounding what the cache can take from the disk.
MAX_CACHE_BYTES = 2 * 1024**3  # 2 GiB

# Partial writes carry this suffix, which keeps them out of the "*.parquet" glob
# that orphan reaping deletes from.
_TMP_SUFFIX = ".part"


def _meta_path() -> Path:
    return CACHE_DIR / "_meta.json"


def _entry_path(key: str) -> Path:
    return CACHE_DIR / f"{key}.parquet"


def _accessed(entry: dict[str, Any]) -> float:
    """Last-access time of an entry, falling back to its write time."""
    value = entry.get("accessed", entry.get("timestamp", 0.0))
    return value if isinstance(value, int | float) else 0.0


def _read_meta() -> dict[str, dict[str, Any]]:
    path = _meta_path()
    if not path.exists():
        return {}
    try:
        raw = json.loads(path.read_text())
    except (json.JSONDecodeError, OSError) as e:
        log.warning("corrupt cache metadata, resetting: %s", e)
        path.unlink(missing_ok=True)
        return {}
    if not isinstance(raw, dict):
        log.warning("corrupt cache metadata, resetting: root is not an object")
        path.unlink(missing_ok=True)
        return {}
    return {key: record for key, record in raw.items() if isinstance(record, dict)}


def _replace_atomically(data: bytes, dest: Path) -> None:
    """Rename a fully written temporary file over dest.

    A rename within one filesystem is atomic, so a crash or a concurrent process
    leaves either the whole previous file or the whole new one, never a mix.
    """
    dest.parent.mkdir(parents=True, exist_ok=True)
    fd, name = tempfile.mkstemp(dir=dest.parent, prefix=".tmp-", suffix=_TMP_SUFFIX)
    tmp = Path(name)
    try:
        with os.fdopen(fd, "wb") as fh:
            fh.write(data)
            fh.flush()
            os.fsync(fh.fileno())
        os.replace(tmp, dest)
    finally:
        tmp.unlink(missing_ok=True)


def _write_parquet_atomically(df: pl.DataFrame, dest: Path) -> None:
    """Serialize df to a temporary file and rename it over dest."""
    dest.parent.mkdir(parents=True, exist_ok=True)
    fd, name = tempfile.mkstemp(dir=dest.parent, prefix=".tmp-", suffix=_TMP_SUFFIX)
    os.close(fd)
    tmp = Path(name)
    try:
        df.write_parquet(tmp)
        os.replace(tmp, dest)
    finally:
        tmp.unlink(missing_ok=True)


def _write_meta(meta: dict[str, dict[str, Any]]) -> None:
    _replace_atomically(json.dumps(meta, indent=2).encode(), _meta_path())


def _reap_orphans(meta: dict[str, dict[str, Any]]) -> None:
    """Drop records whose parquet is gone and parquet files that no record claims.

    Mutates meta in place; the caller persists it.
    """
    for key in [key for key in meta if not _entry_path(key).exists()]:
        log.debug("reaping cache record with no data file: %s", key)
        del meta[key]
    if not CACHE_DIR.exists():
        return
    for path in CACHE_DIR.glob("*.parquet"):
        if path.stem not in meta:
            log.debug("reaping cache data file with no record: %s", path.name)
            path.unlink(missing_ok=True)


def _evict_lru(meta: dict[str, dict[str, Any]]) -> None:
    """Delete least-recently-accessed entries until the cache fits MAX_CACHE_BYTES.

    Mutates meta in place; the caller persists it.
    """
    sizes: dict[str, int] = {}
    for key in meta:
        try:
            sizes[key] = _entry_path(key).stat().st_size
        except OSError:
            sizes[key] = 0
    total = sum(sizes.values())
    for key in sorted(meta, key=lambda k: _accessed(meta[k])):
        if total <= MAX_CACHE_BYTES:
            return
        log.debug("evicting cache entry '%s' (%d bytes)", key, sizes[key])
        _entry_path(key).unlink(missing_ok=True)
        del meta[key]
        total -= sizes[key]


def get(key: str, ttl: int = DEFAULT_TTL) -> pl.DataFrame | None:
    """Return cached DataFrame if fresh, None if stale or missing."""
    meta = _read_meta()
    entry = meta.get(key)
    if entry is None:
        return None
    if time.time() - entry.get("timestamp", 0) > ttl:
        return None
    path = _entry_path(key)
    if not path.exists():
        return None
    try:
        df = pl.read_parquet(path)
    except Exception as e:
        log.warning("corrupt cache entry '%s', invalidating: %s", key, e)
        invalidate(key)
        return None
    entry["accessed"] = time.time()
    try:
        _write_meta(meta)
    except OSError as e:
        # An unwritable cache directory costs eviction ordering accuracy, not the read.
        log.debug("access time for '%s' not recorded: %s", key, e)
    return df


def put(key: str, df: pl.DataFrame) -> None:
    """Write DataFrame to cache, reaping orphans and evicting down to the ceiling."""
    _write_parquet_atomically(df, _entry_path(key))
    meta = _read_meta()
    written = time.time()
    meta[key] = {"timestamp": written, "accessed": written}
    _reap_orphans(meta)
    _evict_lru(meta)
    _write_meta(meta)


def invalidate(key: str | None = None) -> None:
    """Remove a cache entry, or all entries if key is None."""
    if key is None:
        if CACHE_DIR.exists():
            shutil.rmtree(CACHE_DIR)
        return
    meta = _read_meta()
    if meta.pop(key, None) is not None:
        _write_meta(meta)
    _entry_path(key).unlink(missing_ok=True)
