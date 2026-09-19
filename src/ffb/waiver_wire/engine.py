"""Waiver wire trend computation — pure polars, no I/O.

Ranks players by a composite usage score (snap%, target share, touch share)
and flags those with rising 3-week trends.
"""

import polars as pl

from ffb.data import OFFENSIVE_POSITIONS, build_id_crosswalk


def compute_usage_trends(
    snaps: pl.DataFrame,
    weekly_stats: pl.DataFrame,
    player_ids: pl.DataFrame,
    window: int = 3,
) -> pl.DataFrame:
    """Compute composite usage scores and trends per player per week.

    Returns: gsis_id, player, position, team, season, week, snap_pct,
    tgt_share, touch_share, usage_score, rolling_avg, delta, velocity, trend.

    Every week a player has usage for is a row, including the weeks that open a
    season. A trend needs earlier weeks to measure against, so the opening week of a
    player's season carries a usage score and a null rolling average, delta, velocity
    and trend; the weeks after it slope against the earliest week still inside the
    window, per week elapsed. A week whose player was absent for longer than the window
    carries no velocity and no trend either: there is no recent series under it.

    Every rolling window groups by ``gsis_id``. Display names collide across
    the league — multiple active players share a name in any given season —
    so a name-keyed window splices two players into one series.
    """
    if window < 2:
        raise ValueError(f"window must be >= 2, got {window}")

    # ── Snap data ────────────────────────────────────────────
    snap_df = (
        snaps.filter(pl.col("position").is_in(OFFENSIVE_POSITIONS) & (pl.col("game_type") == "REG"))
        .select("pfr_player_id", "player", "position", "team", "season", "week", "offense_pct")
        .drop_nulls(subset=["offense_pct", "pfr_player_id"])
    )

    # Normalize to 0-100
    max_pct = snap_df["offense_pct"].max()
    if isinstance(max_pct, (int, float)) and max_pct <= 1.0:
        snap_df = snap_df.with_columns(pl.col("offense_pct") * 100)

    # ── Map pfr_id → gsis_id ────────────────────────────────
    id_map = build_id_crosswalk(player_ids)

    snap_df = snap_df.join(id_map, left_on="pfr_player_id", right_on="pfr_id", how="inner")

    # ── Weekly stats (targets, carries) ──────────────────────
    stats = (
        weekly_stats.filter(pl.col("season_type") == "REG")
        .select("player_id", "season", "week", "targets", "carries")
        .with_columns(
            pl.col("targets").fill_null(0),
            pl.col("carries").fill_null(0),
        )
        .with_columns((pl.col("targets") + pl.col("carries")).alias("touches"))
    )

    # ── Join snap data with stats ────────────────────────────
    df = snap_df.join(
        stats,
        left_on=["gsis_id", "season", "week"],
        right_on=["player_id", "season", "week"],
        how="inner",
    )

    # ── Team totals per week (for share calculations) ────────
    team_totals = df.group_by("team", "season", "week").agg(
        pl.col("targets").sum().alias("team_targets"),
        pl.col("touches").sum().alias("team_touches"),
    )

    df = df.join(team_totals, on=["team", "season", "week"], how="left")

    # ── Compute shares (0-100 scale) ────────────────────────
    df = df.with_columns(
        pl.when(pl.col("team_targets") > 0)
        .then(pl.col("targets") / pl.col("team_targets") * 100)
        .otherwise(0.0)
        .alias("tgt_share"),
        pl.when(pl.col("team_touches") > 0)
        .then(pl.col("touches") / pl.col("team_touches") * 100)
        .otherwise(0.0)
        .alias("touch_share"),
    )

    df = df.rename({"offense_pct": "snap_pct"})

    # ── Composite usage score ────────────────────────────────
    df = df.with_columns(
        (
            pl.col("snap_pct") * 0.4 + pl.col("tgt_share") * 0.35 + pl.col("touch_share") * 0.25
        ).alias("usage_score")
    )

    # ── Trend computation (same pattern as snap_share) ───────
    df = df.sort("gsis_id", "season", "week")

    # Rolling average of previous weeks
    df = df.with_columns(
        pl.col("usage_score")
        .shift(1)
        .rolling_mean(window_size=window, min_samples=1)
        .over("gsis_id", "season")
        .alias("rolling_avg")
    )

    # Delta
    df = df.with_columns((pl.col("usage_score") - pl.col("rolling_avg")).alias("delta"))

    # `shift` counts a player's appearances, not weeks, so a lag reaches a week whose
    # distance depends on which weeks the player was active. Each candidate lag is
    # admitted only where the weeks it spans fall inside the window, and the slope
    # divides by those weeks: a player back from an absence longer than the window has
    # no series to slope across, and one back from a shorter absence slopes across the
    # weeks that passed rather than the rows.
    lags = range(window - 1, 0, -1)

    def earlier(lag: int) -> pl.Expr:
        return pl.col("usage_score").shift(lag).over("gsis_id", "season")

    def span(lag: int) -> pl.Expr:
        return pl.col("week") - pl.col("week").shift(lag).over("gsis_id", "season")

    # Velocity: slope per week elapsed, across the widest span the window holds.
    df = df.with_columns(
        pl.coalesce(
            [
                pl.when((span(lag) > 0) & (span(lag) <= window - 1)).then(
                    (pl.col("usage_score") - earlier(lag)) / span(lag)
                )
                for lag in lags
            ]
        ).alias("velocity")
    )

    # Trend classification. A week with nothing behind it has no direction to name, and
    # a null says so where any label would assert movement that was never measured.
    df = df.with_columns(
        pl.when(pl.col("velocity").is_null())
        .then(pl.lit(None, dtype=pl.String))
        .when((pl.col("delta") > 2) & (pl.col("velocity") > 0))
        .then(pl.lit("rising"))
        .when((pl.col("delta") < -2) & (pl.col("velocity") < 0))
        .then(pl.lit("falling"))
        .otherwise(pl.lit("stable"))
        .alias("trend")
    )

    return df.select(
        "gsis_id",
        "player",
        "position",
        "team",
        "season",
        "week",
        "snap_pct",
        "tgt_share",
        "touch_share",
        "usage_score",
        "rolling_avg",
        "delta",
        "velocity",
        "trend",
    )
