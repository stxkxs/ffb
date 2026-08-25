"""Tests for snap share trend engine."""

import polars as pl
import pytest

from ffb.snap_share.engine import compute_trends
from tests.conftest import COLLIDING_FALLING_PFR_ID, COLLIDING_RISING_PFR_ID


def test_compute_trends_basic(snap_counts):
    trends = compute_trends(snap_counts)
    assert trends.shape[0] > 0
    assert set(trends.columns) >= {
        "player",
        "position",
        "team",
        "season",
        "week",
        "snap_pct",
        "rolling_avg",
        "delta",
        "velocity",
        "trend",
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


def _by_week(trends: pl.DataFrame, pfr_id: str, column: str) -> dict:
    """Value of `column` in every trend row `pfr_id` holds, keyed by week."""
    rows = trends.filter(pl.col("pfr_player_id") == pfr_id).sort("week")
    return dict(zip(rows["week"].to_list(), rows[column].to_list(), strict=True))


def test_colliding_names_hold_separate_rolling_averages(colliding_name_snaps):
    """A rolling average covers one player's weeks, not every player wearing their name.

    The rising share reads 30, 40, 50, 60, 70, 80 and the falling one mirrors it. A
    three-week window over the preceding weeks therefore averages 30 and 40 in week 3,
    then 30, 40 and 50 in week 4, and the falling player averages 80 and 70, then 80,
    70 and 60.
    """
    trends = compute_trends(colliding_name_snaps, window=3)
    assert _by_week(trends, COLLIDING_RISING_PFR_ID, "rolling_avg") == pytest.approx(
        {3: 35.0, 4: 40.0, 5: 50.0, 6: 60.0}
    )
    assert _by_week(trends, COLLIDING_FALLING_PFR_ID, "rolling_avg") == pytest.approx(
        {3: 75.0, 4: 70.0, 5: 60.0, 6: 50.0}
    )


def test_colliding_names_hold_separate_deltas(colliding_name_snaps):
    """A delta measures a player's share against their own rolling average.

    The rising player is 15 points over their week-3 average of 35 and 20 points over
    every later one; the falling player is under theirs by the same margins.
    """
    trends = compute_trends(colliding_name_snaps, window=3)
    assert _by_week(trends, COLLIDING_RISING_PFR_ID, "delta") == pytest.approx(
        {3: 15.0, 4: 20.0, 5: 20.0, 6: 20.0}
    )
    assert _by_week(trends, COLLIDING_FALLING_PFR_ID, "delta") == pytest.approx(
        {3: -15.0, 4: -20.0, 5: -20.0, 6: -20.0}
    )


def test_colliding_names_hold_separate_velocities(colliding_name_snaps):
    """A velocity is the slope of one player's own shares across the window.

    Each series moves ten points a week, so the two-week slope is +10 for the rising
    player and -10 for the falling one in every week the window covers.
    """
    trends = compute_trends(colliding_name_snaps, window=3)
    assert _by_week(trends, COLLIDING_RISING_PFR_ID, "velocity") == pytest.approx(
        dict.fromkeys(range(3, 7), 10.0)
    )
    assert _by_week(trends, COLLIDING_FALLING_PFR_ID, "velocity") == pytest.approx(
        dict.fromkeys(range(3, 7), -10.0)
    )


def test_colliding_names_classify_in_opposite_directions(colliding_name_snaps):
    """Each player wearing one name lands on the trend their own shares describe."""
    trends = compute_trends(colliding_name_snaps, window=3)
    assert _by_week(trends, COLLIDING_RISING_PFR_ID, "trend") == dict.fromkeys(
        range(3, 7), "rising"
    )
    assert _by_week(trends, COLLIDING_FALLING_PFR_ID, "trend") == dict.fromkeys(
        range(3, 7), "falling"
    )


def test_a_shared_display_name_yields_one_series_per_identity(colliding_name_snaps):
    """Two players wearing one name produce two windows, not one spliced series."""
    trends = compute_trends(colliding_name_snaps, window=3)
    assert trends["player"].unique().to_list() == ["Casey Rivers"]
    assert sorted(trends["pfr_player_id"].unique().to_list()) == sorted(
        [COLLIDING_RISING_PFR_ID, COLLIDING_FALLING_PFR_ID]
    )
