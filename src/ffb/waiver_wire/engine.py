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

    Returns: player, position, team, season, week, snap_pct, tgt_share,
    touch_share, usage_score, rolling_avg, delta, velocity, trend.
    """
    if window < 2:
        raise ValueError(f"window must be >= 2, got {window}")

    # ── Snap data ────────────────────────────────────────────
    snap_df = (
        snaps.filter(
            pl.col("position").is_in(OFFENSIVE_POSITIONS)
            & (pl.col("game_type") == "REG")
        )
        .select("pfr_player_id", "player", "position", "team", "season", "week", "offense_pct")
        .drop_nulls(subset=["offense_pct", "pfr_player_id"])
    )

    # Normalize to 0-100
    if snap_df["offense_pct"].max() is not None and snap_df["offense_pct"].max() <= 1.0:
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
        .with_columns(
            (pl.col("targets") + pl.col("carries")).alias("touches")
        )
    )

    # ── Join snap data with stats ────────────────────────────
    df = snap_df.join(
        stats,
        left_on=["gsis_id", "season", "week"],
        right_on=["player_id", "season", "week"],
        how="inner",
    )

    # ── Team totals per week (for share calculations) ────────
    team_totals = (
        df.group_by("team", "season", "week")
        .agg(
            pl.col("targets").sum().alias("team_targets"),
            pl.col("touches").sum().alias("team_touches"),
        )
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
            pl.col("snap_pct") * 0.4
            + pl.col("tgt_share") * 0.35
            + pl.col("touch_share") * 0.25
        ).alias("usage_score")
    )

    # ── Trend computation (same pattern as snap_share) ───────
    df = df.sort("player", "season", "week")

    # Rolling average of previous weeks
    df = df.with_columns(
        pl.col("usage_score")
        .shift(1)
        .rolling_mean(window_size=window, min_samples=1)
        .over("player", "season")
        .alias("rolling_avg")
    )

    # Delta
    df = df.with_columns(
        (pl.col("usage_score") - pl.col("rolling_avg")).alias("delta")
    )

    # Velocity (slope)
    df = df.with_columns(
        (
            (
                pl.col("usage_score")
                - pl.col("usage_score").shift(window - 1).over("player", "season")
            )
            / (window - 1)
        ).alias("velocity")
    )

    # Trend classification
    df = df.with_columns(
        pl.when((pl.col("delta") > 2) & (pl.col("velocity") > 0))
        .then(pl.lit("rising"))
        .when((pl.col("delta") < -2) & (pl.col("velocity") < 0))
        .then(pl.lit("falling"))
        .otherwise(pl.lit("stable"))
        .alias("trend")
    )

    # Drop incomplete rows and select output columns
    return (
        df.drop_nulls(subset=["rolling_avg", "velocity"])
        .select(
            "player", "position", "team", "season", "week",
            "snap_pct", "tgt_share", "touch_share", "usage_score",
            "rolling_avg", "delta", "velocity", "trend",
        )
    )
