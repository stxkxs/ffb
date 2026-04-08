"""Tests for snap share trend engine."""

from ffb.snap_share.engine import compute_trends


def test_compute_trends_basic(snap_counts):
    trends = compute_trends(snap_counts)
    assert trends.shape[0] > 0
    assert set(trends.columns) >= {
        "player", "position", "team", "season", "week",
        "snap_pct", "rolling_avg", "delta", "velocity", "trend",
    }


def test_compute_trends_filters_to_offensive(snap_counts):
    trends = compute_trends(snap_counts)
    positions = trends["position"].unique().to_list()
    assert all(p in ("QB", "RB", "WR", "TE") for p in positions)


def test_compute_trends_drops_early_weeks(snap_counts):
    """Weeks without enough history for velocity should be dropped."""
    trends = compute_trends(snap_counts, window=3)
    assert trends["rolling_avg"].null_count() == 0
    assert trends["velocity"].null_count() == 0


def test_compute_trends_rising_player(snap_counts):
    """Alpha Player has increasing snap% — should be flagged as rising."""
    trends = compute_trends(snap_counts, window=3)
    alpha = trends.filter(trends["player"] == "Alpha Player")
    assert alpha.shape[0] > 0
    # Alpha's snap% increases every week, so velocity should be positive
    latest = alpha.sort("week").tail(1)
    assert latest["velocity"][0] > 0


def test_compute_trends_window_guard():
    """Window < 2 should raise."""
    import polars as pl
    import pytest

    with pytest.raises(ValueError, match="window must be >= 2"):
        compute_trends(pl.DataFrame(), window=1)
