"""Tests for waiver wire usage trend engine."""

import polars as pl
import pytest

from ffb.waiver_wire.engine import compute_usage_trends
from tests.conftest import (
    COLLIDING_FALLING_GSIS_ID,
    COLLIDING_NAME,
    COLLIDING_RISING_GSIS_ID,
)


def test_compute_usage_trends_basic(snap_counts, weekly_stats, player_ids):
    trends = compute_usage_trends(snap_counts, weekly_stats, player_ids)
    assert trends.shape[0] > 0
    assert "usage_score" in trends.columns
    assert "velocity" in trends.columns
    assert "trend" in trends.columns


def test_usage_score_components(snap_counts, weekly_stats, player_ids):
    """Usage score should combine snap%, target share, and touch share."""
    trends = compute_usage_trends(snap_counts, weekly_stats, player_ids)
    assert "snap_pct" in trends.columns
    assert "tgt_share" in trends.columns
    assert "touch_share" in trends.columns
    # All components should be non-negative
    assert trends["snap_pct"].min() >= 0
    assert trends["tgt_share"].min() >= 0
    assert trends["touch_share"].min() >= 0


def _by_week(trends: pl.DataFrame, gsis_id: str, column: str) -> dict:
    """Value of `column` in every trend row `gsis_id` holds, keyed by week."""
    rows = trends.filter(pl.col("gsis_id") == gsis_id).sort("week")
    return dict(zip(rows["week"].to_list(), rows[column].to_list(), strict=True))


def _colliding_trends(snaps, stats, ids):
    return compute_usage_trends(snaps, stats, ids, window=3)


def test_colliding_names_hold_separate_usage_scores(
    colliding_name_snaps, colliding_name_weekly_stats, player_ids
):
    """A usage score weighs one player's own snap share, targets and touches.

    Each is the only player on their team, so the rising receiver holds a 100 target
    share and a 100 touch share and the falling back holds 0 and 100. Their scores are
    therefore ``snap_pct * 0.40 + 60`` and ``snap_pct * 0.40 + 25`` against snap shares
    that climb and fall by ten points a week.
    """
    trends = _colliding_trends(colliding_name_snaps, colliding_name_weekly_stats, player_ids)
    assert _by_week(trends, COLLIDING_RISING_GSIS_ID, "usage_score") == pytest.approx(
        {3: 80.0, 4: 84.0, 5: 88.0, 6: 92.0}
    )
    assert _by_week(trends, COLLIDING_FALLING_GSIS_ID, "usage_score") == pytest.approx(
        {3: 49.0, 4: 45.0, 5: 41.0, 6: 37.0}
    )


def test_colliding_names_hold_separate_rolling_averages(
    colliding_name_snaps, colliding_name_weekly_stats, player_ids
):
    """A rolling average covers one player's weeks, not every player wearing their name.

    The rising scores read 72, 76, 80, 84, 88, 92, so the average of the preceding
    weeks is 74 in week 3 and 76 in week 4. The falling scores read 57, 53, 49, 45,
    41, 37, giving 55 and 53.
    """
    trends = _colliding_trends(colliding_name_snaps, colliding_name_weekly_stats, player_ids)
    assert _by_week(trends, COLLIDING_RISING_GSIS_ID, "rolling_avg") == pytest.approx(
        {3: 74.0, 4: 76.0, 5: 80.0, 6: 84.0}
    )
    assert _by_week(trends, COLLIDING_FALLING_GSIS_ID, "rolling_avg") == pytest.approx(
        {3: 55.0, 4: 53.0, 5: 49.0, 6: 45.0}
    )


def test_colliding_names_hold_separate_deltas(
    colliding_name_snaps, colliding_name_weekly_stats, player_ids
):
    """A delta measures a player's score against their own rolling average.

    The rising player is 6 points over their week-3 average of 74 and 8 points over
    every later one; the falling player is under theirs by the same margins.
    """
    trends = _colliding_trends(colliding_name_snaps, colliding_name_weekly_stats, player_ids)
    assert _by_week(trends, COLLIDING_RISING_GSIS_ID, "delta") == pytest.approx(
        {3: 6.0, 4: 8.0, 5: 8.0, 6: 8.0}
    )
    assert _by_week(trends, COLLIDING_FALLING_GSIS_ID, "delta") == pytest.approx(
        {3: -6.0, 4: -8.0, 5: -8.0, 6: -8.0}
    )


def test_colliding_names_hold_separate_velocities(
    colliding_name_snaps, colliding_name_weekly_stats, player_ids
):
    """A velocity is the slope of one player's own scores across the window.

    Each score moves four points a week, so the two-week slope is +4 for the rising
    player and -4 for the falling one in every week the window covers.
    """
    trends = _colliding_trends(colliding_name_snaps, colliding_name_weekly_stats, player_ids)
    assert _by_week(trends, COLLIDING_RISING_GSIS_ID, "velocity") == pytest.approx(
        dict.fromkeys(range(3, 7), 4.0)
    )
    assert _by_week(trends, COLLIDING_FALLING_GSIS_ID, "velocity") == pytest.approx(
        dict.fromkeys(range(3, 7), -4.0)
    )


def test_colliding_names_classify_in_opposite_directions(
    colliding_name_snaps, colliding_name_weekly_stats, player_ids
):
    """Each player wearing one name lands on the trend their own usage describes."""
    trends = _colliding_trends(colliding_name_snaps, colliding_name_weekly_stats, player_ids)
    assert _by_week(trends, COLLIDING_RISING_GSIS_ID, "trend") == dict.fromkeys(
        range(3, 7), "rising"
    )
    assert _by_week(trends, COLLIDING_FALLING_GSIS_ID, "trend") == dict.fromkeys(
        range(3, 7), "falling"
    )


def test_a_shared_display_name_yields_one_series_per_identity(
    colliding_name_snaps, colliding_name_weekly_stats, player_ids
):
    """Two players wearing one name produce two windows, not one spliced series."""
    trends = _colliding_trends(colliding_name_snaps, colliding_name_weekly_stats, player_ids)
    assert trends["player"].unique().to_list() == [COLLIDING_NAME]
    assert sorted(trends["gsis_id"].unique().to_list()) == sorted(
        [COLLIDING_RISING_GSIS_ID, COLLIDING_FALLING_GSIS_ID]
    )


def test_a_zero_target_team_gives_its_players_no_target_share(
    colliding_name_snaps, colliding_name_weekly_stats, player_ids
):
    """A share over a zero team total reads zero rather than dividing by nothing."""
    trends = _colliding_trends(colliding_name_snaps, colliding_name_weekly_stats, player_ids)
    assert _by_week(trends, COLLIDING_FALLING_GSIS_ID, "tgt_share") == pytest.approx(
        dict.fromkeys(range(3, 7), 0.0)
    )
