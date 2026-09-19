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


def test_the_week_opening_a_season_keeps_its_share_and_names_no_trend(colliding_name_snaps):
    """A share is a measurement; a trend is a comparison, and week 1 has nothing to compare to.

    Dropping the week instead would take a season's opening weeks out of the frame the
    filters are built from, so the season itself would stop being offered until its
    third week.
    """
    trends = compute_trends(colliding_name_snaps, window=3)
    opening = trends.filter(
        (pl.col("pfr_player_id") == COLLIDING_RISING_PFR_ID) & (pl.col("week") == 1)
    )
    assert opening["snap_pct"].to_list() == [30.0]
    assert opening["rolling_avg"].to_list() == [None]
    assert opening["delta"].to_list() == [None]
    assert opening["velocity"].to_list() == [None]
    assert opening["trend"].to_list() == [None]


def test_the_second_week_slopes_against_the_only_week_behind_it(colliding_name_snaps):
    """A window wider than the season is short-changed, not refused.

    The rising share climbs ten points a week, so week 2 slopes at ten over one week
    just as the later weeks slope at ten over two.
    """
    trends = compute_trends(colliding_name_snaps, window=3)
    assert _by_week(trends, COLLIDING_RISING_PFR_ID, "velocity")[2] == pytest.approx(10.0)
    assert _by_week(trends, COLLIDING_FALLING_PFR_ID, "velocity")[2] == pytest.approx(-10.0)


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


def _measured(trends: pl.DataFrame) -> pl.DataFrame:
    """The rows a trend was measured on — every week but the one opening a player's season."""
    return trends.drop_nulls(subset=["velocity"])


def test_a_week_beyond_the_window_from_the_last_one_played_carries_no_trend(
    colliding_name_snaps,
):
    """A player back from a long absence has no recent series to slope across.

    `shift` counts appearances, so without a check on the weeks between them the row
    returning from an absence would slope against whatever week came before it and
    divide by one, reporting a whole absence's worth of change as one week of movement.
    """
    gapped = colliding_name_snaps.filter(~pl.col("week").is_in([2, 3]))
    trends = compute_trends(gapped, window=3)
    returning = trends.filter(
        (pl.col("pfr_player_id") == COLLIDING_RISING_PFR_ID) & (pl.col("week") == 4)
    )
    assert returning["snap_pct"].to_list() == [60.0]
    assert returning["velocity"].to_list() == [None]
    assert returning["trend"].to_list() == [None]


def test_a_slope_divides_by_the_weeks_it_spans_not_the_rows(colliding_name_snaps):
    """Two rows a week apart and two rows two weeks apart describe different speeds.

    The rising share climbs ten points a week, so every span reads +10 whether it is
    measured across one week or two.
    """
    gapped = colliding_name_snaps.filter(~pl.col("week").is_in([2, 3]))
    velocities = _by_week(compute_trends(gapped, window=3), COLLIDING_RISING_PFR_ID, "velocity")
    assert velocities[5] == pytest.approx(10.0)
    assert velocities[6] == pytest.approx(10.0)


def test_colliding_names_hold_separate_rolling_averages(colliding_name_snaps):
    """A rolling average covers one player's weeks, not every player wearing their name.

    The rising share reads 30, 40, 50, 60, 70, 80 and the falling one mirrors it. A
    three-week window over the preceding weeks therefore averages 30 and 40 in week 3,
    then 30, 40 and 50 in week 4, and the falling player averages 80 and 70, then 80,
    70 and 60.
    """
    trends = _measured(compute_trends(colliding_name_snaps, window=3))
    assert _by_week(trends, COLLIDING_RISING_PFR_ID, "rolling_avg") == pytest.approx(
        {2: 30.0, 3: 35.0, 4: 40.0, 5: 50.0, 6: 60.0}
    )
    assert _by_week(trends, COLLIDING_FALLING_PFR_ID, "rolling_avg") == pytest.approx(
        {2: 80.0, 3: 75.0, 4: 70.0, 5: 60.0, 6: 50.0}
    )


def test_colliding_names_hold_separate_deltas(colliding_name_snaps):
    """A delta measures a player's share against their own rolling average.

    The rising player is 10 points over their week-2 average of 30, 15 points over
    their week-3 average of 35 and 20 points over every later one; the falling player
    is under theirs by the same margins.
    """
    trends = _measured(compute_trends(colliding_name_snaps, window=3))
    assert _by_week(trends, COLLIDING_RISING_PFR_ID, "delta") == pytest.approx(
        {2: 10.0, 3: 15.0, 4: 20.0, 5: 20.0, 6: 20.0}
    )
    assert _by_week(trends, COLLIDING_FALLING_PFR_ID, "delta") == pytest.approx(
        {2: -10.0, 3: -15.0, 4: -20.0, 5: -20.0, 6: -20.0}
    )


def test_colliding_names_hold_separate_velocities(colliding_name_snaps):
    """A velocity is the slope of one player's own shares across the window.

    Each series moves ten points a week, so the slope is +10 for the rising player and
    -10 for the falling one in every week the window covers, whether it spans one week
    or the full two.
    """
    trends = _measured(compute_trends(colliding_name_snaps, window=3))
    assert _by_week(trends, COLLIDING_RISING_PFR_ID, "velocity") == pytest.approx(
        dict.fromkeys(range(2, 7), 10.0)
    )
    assert _by_week(trends, COLLIDING_FALLING_PFR_ID, "velocity") == pytest.approx(
        dict.fromkeys(range(2, 7), -10.0)
    )


def test_colliding_names_classify_in_opposite_directions(colliding_name_snaps):
    """Each player wearing one name lands on the trend their own shares describe."""
    trends = _measured(compute_trends(colliding_name_snaps, window=3))
    assert _by_week(trends, COLLIDING_RISING_PFR_ID, "trend") == dict.fromkeys(
        range(2, 7), "rising"
    )
    assert _by_week(trends, COLLIDING_FALLING_PFR_ID, "trend") == dict.fromkeys(
        range(2, 7), "falling"
    )


def test_a_shared_display_name_yields_one_series_per_identity(colliding_name_snaps):
    """Two players wearing one name produce two windows, not one spliced series."""
    trends = compute_trends(colliding_name_snaps, window=3)
    assert trends["player"].unique().to_list() == ["Casey Rivers"]
    assert sorted(trends["pfr_player_id"].unique().to_list()) == sorted(
        [COLLIDING_RISING_PFR_ID, COLLIDING_FALLING_PFR_ID]
    )
