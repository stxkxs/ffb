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

    Every week a player has a snap share for is a row, including the weeks that open
    a season. A trend needs earlier weeks to measure against, so the opening week of
    a player's season carries a snap share and a null rolling average, delta,
    velocity and trend; the weeks after it slope against the earliest week still inside
    the window, per week elapsed. A week whose player was absent for longer than the
    window carries no velocity and no trend either: there is no recent series under it.

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

    # `shift` counts a player's appearances, not weeks, so a lag reaches a week whose
    # distance depends on which weeks the player was active. Each candidate lag is
    # admitted only where the weeks it spans fall inside the window, and the slope
    # divides by those weeks: a player back from an absence longer than the window has
    # no series to slope across, and one back from a shorter absence slopes across the
    # weeks that passed rather than the rows.
    lags = range(window - 1, 0, -1)

    def earlier(lag: int) -> pl.Expr:
        return pl.col("snap_pct").shift(lag).over("pfr_player_id", "season")

    def span(lag: int) -> pl.Expr:
        return pl.col("week") - pl.col("week").shift(lag).over("pfr_player_id", "season")

    def inside(lag: int) -> pl.Expr:
        return (span(lag) > 0) & (span(lag) <= window - 1)

    # Velocity: simplified OLS slope = (current - earlier) / weeks between them, taken
    # across the widest span the window holds.
    df = df.with_columns(
        pl.coalesce(
            [
                pl.when(inside(lag)).then((pl.col("snap_pct") - earlier(lag)) / span(lag))
                for lag in lags
            ]
        ).alias("velocity")
    )

    # The share the window opens on, against which a breakout is measured.
    opening = pl.coalesce([pl.when(inside(lag)).then(earlier(lag)) for lag in lags])

    # Breakout: crossed from <50% to >60% within the window
    breakout = (pl.col("snap_pct") > 60) & (opening < 50)

    # Classify trend. A week with nothing behind it has no direction to name, and a
    # null says so where any label would assert movement that was never measured.
    df = df.with_columns(
        pl.when(pl.col("velocity").is_null())
        .then(pl.lit(None, dtype=pl.String))
        .when(breakout)
        .then(pl.lit("breakout"))
        .when((pl.col("delta") > delta_threshold) & (pl.col("velocity") > 0))
        .then(pl.lit("rising"))
        .when((pl.col("delta") < -delta_threshold) & (pl.col("velocity") < 0))
        .then(pl.lit("falling"))
        .otherwise(pl.lit("stable"))
        .alias("trend")
    )

    return df
