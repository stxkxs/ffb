"""Tests for waiver wire usage trend engine."""

from ffb.waiver_wire.engine import compute_usage_trends


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
