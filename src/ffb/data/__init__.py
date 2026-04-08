"""Shared data layer constants and helpers."""

import polars as pl

OFFENSIVE_POSITIONS = frozenset({"QB", "RB", "WR", "TE"})


def build_id_crosswalk(player_ids: pl.DataFrame) -> pl.DataFrame:
    """Build pfr_id → gsis_id lookup from player ID table."""
    return (
        player_ids.select("pfr_id", "gsis_id")
        .drop_nulls()
        .unique(subset=["pfr_id"], keep="last")
    )
