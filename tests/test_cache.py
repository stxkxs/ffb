"""Tests for the data cache layer."""

import json
import polars as pl
from pathlib import Path

from ffb.data import cache


def test_put_and_get(tmp_path, monkeypatch):
    monkeypatch.setattr(cache, "CACHE_DIR", tmp_path)
    df = pl.DataFrame({"a": [1, 2, 3]})
    cache.put("test_key", df)
    result = cache.get("test_key")
    assert result is not None
    assert result.shape == (3, 1)


def test_get_stale_returns_none(tmp_path, monkeypatch):
    monkeypatch.setattr(cache, "CACHE_DIR", tmp_path)
    df = pl.DataFrame({"a": [1]})
    cache.put("test_key", df)
    # Should return None with 0 TTL
    assert cache.get("test_key", ttl=0) is None


def test_invalidate_key(tmp_path, monkeypatch):
    monkeypatch.setattr(cache, "CACHE_DIR", tmp_path)
    df = pl.DataFrame({"a": [1]})
    cache.put("test_key", df)
    cache.invalidate("test_key")
    assert cache.get("test_key") is None


def test_invalidate_all(tmp_path, monkeypatch):
    monkeypatch.setattr(cache, "CACHE_DIR", tmp_path)
    cache.put("k1", pl.DataFrame({"a": [1]}))
    cache.put("k2", pl.DataFrame({"b": [2]}))
    cache.invalidate(None)
    assert not tmp_path.exists()


def test_corrupt_meta_recovers(tmp_path, monkeypatch):
    monkeypatch.setattr(cache, "CACHE_DIR", tmp_path)
    meta_path = tmp_path / "_meta.json"
    tmp_path.mkdir(parents=True, exist_ok=True)
    meta_path.write_text("NOT VALID JSON{{{")
    # Should recover gracefully, not crash
    result = cache.get("anything")
    assert result is None


def test_corrupt_parquet_self_heals(tmp_path, monkeypatch):
    monkeypatch.setattr(cache, "CACHE_DIR", tmp_path)
    # Write a valid entry
    df = pl.DataFrame({"a": [1]})
    cache.put("test_key", df)
    # Corrupt the parquet file
    parquet_path = tmp_path / "test_key.parquet"
    parquet_path.write_text("NOT A PARQUET FILE")
    # get() should return None and self-heal
    result = cache.get("test_key")
    assert result is None
    # Meta should be cleaned up
    meta = json.loads((tmp_path / "_meta.json").read_text())
    assert "test_key" not in meta
