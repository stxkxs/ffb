"""Snap share trend computation — pure polars, no I/O."""

import polars as pl

from ffb.data import OFFENSIVE_POSITIONS


def compute_trends(
    snaps: pl.DataFrame,
    window: int = 3,
    delta_threshold: float = 5.0,
) -> pl.DataFrame:
    """Compute snap share trends per player per season.

    Returns a DataFrame with columns: pfr_player_id, player, position, team,
    season, week, snap_pct, rolling_avg, delta, velocity, trend.

    Every rolling window groups by ``pfr_player_id``. Display names collide
    across the league — multiple active players share a name in any given
    season — so a name-keyed window splices two players into one series.
    A snap row carrying no ``pfr_player_id`` cannot be attributed to a player
    and is excluded.
    """
    if window < 2:
        raise ValueError(f"window must be >= 2, got {window}")

    df = (
        snaps.filter(pl.col("position").is_in(OFFENSIVE_POSITIONS) & (pl.col("game_type") == "REG"))
        .select("pfr_player_id", "player", "position", "team", "season", "week", "offense_pct")
        .drop_nulls(subset=["offense_pct", "pfr_player_id"])
        .sort("pfr_player_id", "season", "week")
    )

    # Normalize to 0-100 if stored as 0-1 fraction
    max_pct = df["offense_pct"].max()
    if isinstance(max_pct, (int, float)) and max_pct <= 1.0:
        df = df.with_columns(pl.col("offense_pct") * 100)

    df = df.rename({"offense_pct": "snap_pct"})

    # Rolling average of *previous* weeks (shift first so current week excluded)
    df = df.with_columns(
        pl.col("snap_pct")
        .shift(1)
        .rolling_mean(window_size=window, min_samples=1)
        .over("pfr_player_id", "season")
        .alias("rolling_avg")
    )

    # Delta: current week vs prior rolling average
    df = df.with_columns((pl.col("snap_pct") - pl.col("rolling_avg")).alias("delta"))

    # Velocity: simplified OLS slope = (current - lag(window-1)) / (window-1)
    df = df.with_columns(
        (
            (
                pl.col("snap_pct")
                - pl.col("snap_pct").shift(window - 1).over("pfr_player_id", "season")
            )
            / (window - 1)
        ).alias("velocity")
    )

    # Breakout: crossed from <50% to >60% within the window
    breakout = (pl.col("snap_pct") > 60) & (
        pl.col("snap_pct").shift(window - 1).over("pfr_player_id", "season") < 50
    )

    # Classify trend
    df = df.with_columns(
        pl.when(breakout)
        .then(pl.lit("breakout"))
        .when((pl.col("delta") > delta_threshold) & (pl.col("velocity") > 0))
        .then(pl.lit("rising"))
        .when((pl.col("delta") < -delta_threshold) & (pl.col("velocity") < 0))
        .then(pl.lit("falling"))
        .otherwise(pl.lit("stable"))
        .alias("trend")
    )

    # Drop weeks without enough history for trend calculation
    return df.drop_nulls(subset=["rolling_avg", "velocity"])
