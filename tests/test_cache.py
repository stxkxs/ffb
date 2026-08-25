"""Tests for the data cache layer."""

import json
from pathlib import Path

import polars as pl
import pytest

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


# ── Fixtures and helpers ─────────────────────────────────────────────────────


class Clock:
    """Stand-in for the wall clock the cache stamps entries with."""

    def __init__(self, start: float = 1_000_000.0) -> None:
        self.value = start

    def time(self) -> float:
        return self.value

    def advance(self, seconds: float = 1.0) -> float:
        self.value += seconds
        return self.value


@pytest.fixture()
def clock(monkeypatch):
    """Freeze the cache's clock so write and access times are exact."""
    fake = Clock()
    monkeypatch.setattr(cache, "time", fake)
    return fake


@pytest.fixture()
def cache_dir(tmp_path, monkeypatch):
    """Point the cache at a directory of this test's own."""
    monkeypatch.setattr(cache, "CACHE_DIR", tmp_path)
    return tmp_path


def read_meta(cache_dir: Path) -> dict:
    return json.loads((cache_dir / "_meta.json").read_text())


def entry(marker: int) -> pl.DataFrame:
    """A one-row frame; every entry built this way serializes to the same size."""
    return pl.DataFrame({"marker": [marker]})


def fail_midway(self, path, *args, **kwargs) -> None:
    """Write part of the parquet, then fail as a full disk does."""
    Path(path).write_bytes(b"PAR1 partial")
    raise OSError("no space left on device")


# ── Access time ──────────────────────────────────────────────────────────────


def test_a_read_records_the_time_it_read_at(cache_dir, clock):
    cache.put("k", entry(1))
    reading = clock.advance(60)
    cache.get("k")
    assert read_meta(cache_dir)["k"]["accessed"] == reading


def test_a_read_leaves_the_write_time_alone(cache_dir, clock):
    written = clock.value
    cache.put("k", entry(1))
    clock.advance(60)
    cache.get("k")
    assert read_meta(cache_dir)["k"]["timestamp"] == written


def test_an_entry_expires_the_ttl_after_it_was_written_not_after_it_was_read(cache_dir, clock):
    cache.put("k", entry(1))
    clock.advance(cache.DEFAULT_TTL - 1)
    cache.get("k")
    clock.advance(2)
    assert cache.get("k") is None


def test_an_entry_survives_to_the_end_of_its_ttl(cache_dir, clock):
    cache.put("k", entry(1))
    clock.advance(cache.DEFAULT_TTL)
    assert cache.get("k") is not None


def test_an_entry_expires_one_second_past_its_ttl(cache_dir, clock):
    cache.put("k", entry(1))
    clock.advance(cache.DEFAULT_TTL + 1)
    assert cache.get("k") is None


# ── Size ceiling ─────────────────────────────────────────────────────────────


def fill_three_and_read_b(cache_dir, clock, monkeypatch) -> None:
    """Store a, b and c, then read b, leaving `a` least recently accessed.

    The ceiling is set to what the three occupy, so storing a fourth entry of the
    same size puts the cache one entry over.
    """
    for key in ("a", "b", "c"):
        clock.advance()
        cache.put(key, entry(1))
    clock.advance()
    cache.get("b")
    resident = sum((cache_dir / f"{key}.parquet").stat().st_size for key in "abc")
    monkeypatch.setattr(cache, "MAX_CACHE_BYTES", resident)
    clock.advance()


def test_the_ceiling_evicts_the_least_recently_accessed_entry(cache_dir, clock, monkeypatch):
    fill_three_and_read_b(cache_dir, clock, monkeypatch)
    cache.put("d", entry(1))
    assert cache.get("a") is None


def test_an_evicted_entry_leaves_no_data_file(cache_dir, clock, monkeypatch):
    fill_three_and_read_b(cache_dir, clock, monkeypatch)
    cache.put("d", entry(1))
    assert not (cache_dir / "a.parquet").exists()


def test_an_evicted_entry_leaves_no_record(cache_dir, clock, monkeypatch):
    fill_three_and_read_b(cache_dir, clock, monkeypatch)
    cache.put("d", entry(1))
    assert "a" not in read_meta(cache_dir)


def test_eviction_stops_once_the_cache_fits(cache_dir, clock, monkeypatch):
    fill_three_and_read_b(cache_dir, clock, monkeypatch)
    cache.put("d", entry(1))
    assert [key for key in ("b", "c", "d") if cache.get(key) is None] == []


def test_a_read_promotes_an_entry_past_one_written_after_it(cache_dir, clock, monkeypatch):
    fill_three_and_read_b(cache_dir, clock, monkeypatch)
    cache.get("a")
    clock.advance()
    cache.put("d", entry(1))
    assert [key for key in ("a", "b", "d") if cache.get(key) is None] == []


def test_a_read_promoted_entry_outlives_the_entry_it_overtook(cache_dir, clock, monkeypatch):
    fill_three_and_read_b(cache_dir, clock, monkeypatch)
    cache.get("a")
    clock.advance()
    cache.put("d", entry(1))
    assert cache.get("c") is None


# ── Atomic writes ────────────────────────────────────────────────────────────


def test_a_write_that_fails_partway_leaves_the_stored_entry_readable(cache_dir, monkeypatch):
    cache.put("k", entry(1))
    monkeypatch.setattr(pl.DataFrame, "write_parquet", fail_midway)
    with pytest.raises(OSError):
        cache.put("k", entry(2))
    assert cache.get("k")["marker"].to_list() == [1]


def test_a_write_that_fails_partway_stores_no_entry(cache_dir, monkeypatch):
    monkeypatch.setattr(pl.DataFrame, "write_parquet", fail_midway)
    with pytest.raises(OSError):
        cache.put("k", entry(1))
    assert cache.get("k") is None


def test_a_write_that_fails_partway_leaves_no_data_file(cache_dir, monkeypatch):
    monkeypatch.setattr(pl.DataFrame, "write_parquet", fail_midway)
    with pytest.raises(OSError):
        cache.put("k", entry(1))
    assert not (cache_dir / "k.parquet").exists()


def test_a_write_that_fails_partway_leaves_no_temporary_file(cache_dir, monkeypatch):
    cache.put("k", entry(1))
    monkeypatch.setattr(pl.DataFrame, "write_parquet", fail_midway)
    with pytest.raises(OSError):
        cache.put("k", entry(2))
    assert sorted(path.name for path in cache_dir.iterdir()) == [
        "_meta.json",
        "k.parquet",
    ]


def test_a_stored_entry_leaves_no_temporary_file(cache_dir):
    cache.put("k", entry(1))
    assert list(cache_dir.glob(".tmp-*")) == []


# ── Orphan reaping ───────────────────────────────────────────────────────────


def test_a_record_whose_data_file_is_gone_is_reaped(cache_dir):
    cache.put("a", entry(1))
    (cache_dir / "a.parquet").unlink()
    cache.put("b", entry(2))
    assert "a" not in read_meta(cache_dir)


def test_a_data_file_no_record_claims_is_reaped(cache_dir):
    cache.put("a", entry(1))
    stray = cache_dir / "stray.parquet"
    stray.write_bytes(b"unclaimed")
    cache.put("b", entry(2))
    assert not stray.exists()


def test_reaping_leaves_the_recorded_entries_alone(cache_dir):
    cache.put("a", entry(1))
    (cache_dir / "stray.parquet").write_bytes(b"unclaimed")
    cache.put("b", entry(2))
    assert cache.get("a")["marker"].to_list() == [1]


def test_a_reaped_record_stops_being_served(cache_dir):
    cache.put("a", entry(1))
    (cache_dir / "a.parquet").unlink()
    cache.put("b", entry(2))
    assert cache.get("a") is None


def test_invalidating_one_key_leaves_the_others_readable(cache_dir):
    cache.put("a", entry(1))
    cache.put("b", entry(2))
    cache.invalidate("a")
    assert cache.get("b")["marker"].to_list() == [2]


def test_invalidating_one_key_removes_its_data_file(cache_dir):
    cache.put("a", entry(1))
    cache.invalidate("a")
    assert not (cache_dir / "a.parquet").exists()
