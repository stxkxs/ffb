"""Trade value computation — pure polars, no I/O.

Normalizes player value from production, remaining schedule strength,
usage, injury history, and bye weeks into a 0-100 trade value score.
"""

import polars as pl

from ffb.data import OFFENSIVE_POSITIONS, build_id_crosswalk

REGULAR_SEASON_WEEKS = 18


def _team_bye_weeks(schedules: pl.DataFrame, season: int) -> pl.DataFrame:
    """Determine bye week for each team in a season."""
    reg = schedules.filter(
        (pl.col("season") == season) & (pl.col("game_type") == "REG")
    )
    all_weeks = set(range(1, REGULAR_SEASON_WEEKS + 1))

    rows = []
    for team_col in ["home_team", "away_team"]:
        team_weeks = reg.select(pl.col(team_col).alias("team"), "week")
        rows.append(team_weeks)

    played = pl.concat(rows).group_by("team").agg(
        pl.col("week").unique().alias("weeks_played")
    )

    bye_rows = []
    for row in played.iter_rows(named=True):
        missing = all_weeks - set(row["weeks_played"])
        if missing:
            bye_rows.append({"team": row["team"], "bye_week": min(missing)})

    if not bye_rows:
        return pl.DataFrame({"team": [], "bye_week": []}).cast({"bye_week": pl.Int64})

    return pl.DataFrame(bye_rows)


def _defensive_strength(
    weekly_stats: pl.DataFrame,
    season: int,
    through_week: int,
) -> pl.DataFrame:
    """Avg fantasy points allowed by position — higher = weaker defense."""
    df = weekly_stats.filter(
        (pl.col("season") == season)
        & (pl.col("season_type") == "REG")
        & (pl.col("week") <= through_week)
        & pl.col("opponent_team").is_not_null()
        & pl.col("position").is_in(OFFENSIVE_POSITIONS)
    )

    per_game = (
        df.group_by("opponent_team", "position", "week")
        .agg(pl.col("fantasy_points_ppr").sum().alias("fpts"))
    )

    return (
        per_game.group_by("opponent_team", "position")
        .agg(pl.col("fpts").mean().alias("fpts_allowed"))
    )


def _remaining_schedule_factor(
    schedules: pl.DataFrame,
    def_strength: pl.DataFrame,
    season: int,
    current_week: int,
) -> pl.DataFrame:
    """Avg opponent defensive weakness for each team's remaining games.

    Returns: team, position, sched_factor (higher = easier remaining schedule).
    """
    reg = schedules.filter(
        (pl.col("season") == season)
        & (pl.col("game_type") == "REG")
        & (pl.col("week") > current_week)
    )

    # Build team → remaining opponents
    home = reg.select(pl.col("home_team").alias("team"), pl.col("away_team").alias("opponent"))
    away = reg.select(pl.col("away_team").alias("team"), pl.col("home_team").alias("opponent"))
    remaining = pl.concat([home, away])

    # Join with defensive strength
    remaining = remaining.join(
        def_strength,
        left_on="opponent",
        right_on="opponent_team",
        how="inner",
    )

    # Average opponent fpts_allowed per position per team
    return (
        remaining.group_by("team", "position")
        .agg(pl.col("fpts_allowed").mean().alias("sched_factor"))
    )


def _health_discount(
    injuries: pl.DataFrame,
) -> pl.DataFrame:
    """Injury risk factor per player. More 'Out' weeks = higher risk.

    Returns: gsis_id, health (0.0-1.0, where 1.0 = fully healthy history).
    """
    outs = injuries.filter(
        (pl.col("report_status") == "Out")
        & (pl.col("season_type") == "REG")
    )

    out_counts = (
        outs.group_by("gsis_id")
        .agg(pl.len().alias("weeks_out"))
    )

    # Total possible weeks per player
    total = injuries.filter(pl.col("season_type") == "REG").group_by("gsis_id").agg(
        (pl.col("season").n_unique() * REGULAR_SEASON_WEEKS).alias("total_weeks")
    )

    health = out_counts.join(total, on="gsis_id", how="left").with_columns(
        (1.0 - (pl.col("weeks_out") / pl.col("total_weeks")).clip(0.0, 0.5)).alias("health")
    )

    return health.select("gsis_id", "health")


def compute_trade_values(
    weekly_stats: pl.DataFrame,
    snap_counts: pl.DataFrame,
    schedules: pl.DataFrame,
    injuries: pl.DataFrame,
    player_ids: pl.DataFrame,
    season: int,
    current_week: int,
) -> pl.DataFrame:
    """Compute trade value scores (0-100) for all offensive players.

    Components (weighted):
    - Production (40%): season PPG × games remaining
    - Schedule (20%): avg opponent defensive weakness for remaining games
    - Usage (20%): snap% × target/touch share
    - Health (10%): injury history discount
    - Bye (10%): penalty if bye week is still upcoming
    """
    games_remaining = REGULAR_SEASON_WEEKS - current_week
    if games_remaining <= 0 or current_week < 3:
        return pl.DataFrame()

    # ── Player baselines ─────────────────────────────────────
    baselines = (
        weekly_stats.filter(
            (pl.col("season") == season)
            & (pl.col("season_type") == "REG")
            & (pl.col("week") <= current_week)
            & pl.col("position").is_in(OFFENSIVE_POSITIONS)
        )
        .group_by("player_id", "player_display_name", "position", "recent_team")
        .agg(
            pl.col("fantasy_points_ppr").mean().alias("ppg"),
            pl.col("fantasy_points_ppr").len().alias("games_played"),
        )
        .filter(pl.col("games_played") >= 3)
        .rename({"player_display_name": "player", "recent_team": "team"})
    )

    # ── Production score (raw, not normalized yet) ───────────
    baselines = baselines.with_columns(
        (pl.col("ppg") * games_remaining).alias("production_raw")
    )

    # ── Schedule factor ──────────────────────────────────────
    def_str = _defensive_strength(weekly_stats, season, current_week)
    league_avg = def_str.group_by("position").agg(
        pl.col("fpts_allowed").mean().alias("league_avg")
    )
    sched = _remaining_schedule_factor(schedules, def_str, season, current_week)

    # Normalize: sched_factor / league_avg → multiplier around 1.0
    sched = sched.join(league_avg, on="position", how="left").with_columns(
        (pl.col("sched_factor") / pl.col("league_avg")).alias("sched_mult")
    ).select("team", "position", "sched_mult")

    baselines = baselines.join(sched, on=["team", "position"], how="left").with_columns(
        pl.col("sched_mult").fill_null(1.0)
    )

    # ── Usage score ──────────────────────────────────────────
    id_map = build_id_crosswalk(player_ids)

    snap_usage = (
        snap_counts.filter(
            (pl.col("season") == season)
            & (pl.col("game_type") == "REG")
            & (pl.col("week") <= current_week)
            & pl.col("position").is_in(OFFENSIVE_POSITIONS)
        )
        .group_by("pfr_player_id")
        .agg(pl.col("offense_pct").mean().alias("avg_snap_pct"))
    )

    # Normalize snap_pct to 0-1
    max_snap = snap_usage["avg_snap_pct"].max()
    if max_snap is not None and max_snap <= 1.0:
        snap_usage = snap_usage.with_columns(pl.col("avg_snap_pct") * 100)

    snap_usage = snap_usage.join(id_map, left_on="pfr_player_id", right_on="pfr_id", how="inner")

    baselines = baselines.join(
        snap_usage.select("gsis_id", "avg_snap_pct"),
        left_on="player_id", right_on="gsis_id", how="left",
    ).with_columns(pl.col("avg_snap_pct").fill_null(50.0))

    # ── Health discount ──────────────────────────────────────
    health = _health_discount(injuries)
    baselines = baselines.join(
        health, left_on="player_id", right_on="gsis_id", how="left"
    ).with_columns(pl.col("health").fill_null(1.0))

    # ── Bye penalty ──────────────────────────────────────────
    byes = _team_bye_weeks(schedules, season)
    baselines = baselines.join(byes, on="team", how="left").with_columns(
        pl.when(pl.col("bye_week").is_not_null() & (pl.col("bye_week") > current_week))
        .then(pl.lit(0.0))
        .otherwise(pl.lit(1.0))
        .alias("bye_factor")
    )

    # ── Composite trade value ────────────────────────────────
    # Normalize each component to 0-1 range, then weight
    baselines = baselines.with_columns(
        # Production: normalize by max
        (pl.col("production_raw") / pl.col("production_raw").max()).alias("prod_norm"),
        # Schedule: already around 1.0, normalize to 0-1
        ((pl.col("sched_mult") - pl.col("sched_mult").min())
         / (pl.col("sched_mult").max() - pl.col("sched_mult").min() + 1e-9)).alias("sched_norm"),
        # Usage: snap_pct / 100
        (pl.col("avg_snap_pct") / 100).alias("usage_norm"),
    )

    baselines = baselines.with_columns(
        (
            pl.col("prod_norm") * 0.40
            + pl.col("sched_norm") * 0.20
            + pl.col("usage_norm") * 0.20
            + pl.col("health") * 0.10
            + pl.col("bye_factor") * 0.10
        ).alias("trade_value_raw")
    )

    # Scale to 0-100
    tv_max = baselines["trade_value_raw"].max()
    if tv_max is not None and tv_max > 0:
        baselines = baselines.with_columns(
            (pl.col("trade_value_raw") / tv_max * 100).round(1).alias("trade_value")
        )
    else:
        baselines = baselines.with_columns(pl.lit(0.0).alias("trade_value"))

    return (
        baselines.select(
            "player", "position", "team", "ppg", "games_played",
            "production_raw", "sched_mult", "avg_snap_pct", "health",
            "bye_week", "trade_value",
        )
        .sort("trade_value", descending=True)
    )
