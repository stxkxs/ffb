"""Trade value computation — pure polars, no I/O.

Normalizes player value from production, remaining schedule strength, usage,
and injury history into a 0-100 trade value score. A bye week enters through
the count of games a player has left to play, not as a separate penalty.
"""

import polars as pl

from ffb.data import OFFENSIVE_POSITIONS, build_id_crosswalk

REGULAR_SEASON_WEEKS = 18

# Columns and dtypes every compute_trade_values result carries, in order.
# Callers filter and index the result by name, so an empty result must be
# shaped identically to a populated one.
OUTPUT_SCHEMA = pl.Schema(
    {
        "player": pl.String(),
        "position": pl.String(),
        "team": pl.String(),
        "ppg": pl.Float64(),
        "games_played": pl.UInt32(),
        "production_raw": pl.Float64(),
        "sched_mult": pl.Float64(),
        "avg_snap_pct": pl.Float64(),
        "health": pl.Float64(),
        "bye_week": pl.Int64(),
        "trade_value": pl.Float64(),
    }
)


def _empty_result() -> pl.DataFrame:
    """Zero-row frame carrying OUTPUT_SCHEMA."""
    return pl.DataFrame(schema=OUTPUT_SCHEMA)


def _team_bye_weeks(schedules: pl.DataFrame, season: int) -> pl.DataFrame:
    """Determine bye week for each team in a season."""
    reg = schedules.filter((pl.col("season") == season) & (pl.col("game_type") == "REG"))
    all_weeks = set(range(1, REGULAR_SEASON_WEEKS + 1))

    rows = []
    for team_col in ["home_team", "away_team"]:
        team_weeks = reg.select(pl.col(team_col).alias("team"), "week")
        rows.append(team_weeks)

    played = pl.concat(rows).group_by("team").agg(pl.col("week").unique().alias("weeks_played"))

    bye_rows = []
    for row in played.iter_rows(named=True):
        missing = all_weeks - set(row["weeks_played"])
        if missing:
            bye_rows.append({"team": row["team"], "bye_week": min(missing)})

    if not bye_rows:
        return pl.DataFrame(schema={"team": pl.String(), "bye_week": pl.Int64()})

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

    per_game = df.group_by("opponent_team", "position", "week").agg(
        pl.col("fantasy_points_ppr").sum().alias("fpts")
    )

    return per_game.group_by("opponent_team", "position").agg(
        pl.col("fpts").mean().alias("fpts_allowed")
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
    return remaining.group_by("team", "position").agg(
        pl.col("fpts_allowed").mean().alias("sched_factor")
    )


def _health_discount(
    injuries: pl.DataFrame,
) -> pl.DataFrame:
    """Injury risk factor per player. More 'Out' weeks = higher risk.

    Weeks reported Out come off one slate of REGULAR_SEASON_WEEKS for each
    regular season the frame carries. That slate is the denominator for every
    player in the frame, so two players reported Out as many weeks as each
    other score the same health however their reports fall across seasons. The
    missed share is capped at half the slate, flooring health at 0.5.

    Returns: gsis_id, health (0.0-1.0, where 1.0 = fully healthy history).
    """
    regular = injuries.filter(pl.col("season_type") == "REG")
    total_weeks = regular["season"].n_unique() * REGULAR_SEASON_WEEKS
    if total_weeks == 0:
        return pl.DataFrame(schema={"gsis_id": pl.String(), "health": pl.Float64()})

    return (
        regular.filter(pl.col("report_status") == "Out")
        .group_by("gsis_id")
        .agg(pl.len().alias("weeks_out"))
        .select(
            "gsis_id",
            (1.0 - (pl.col("weeks_out") / total_weeks).clip(0.0, 0.5)).alias("health"),
        )
    )


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
    - Production (50%): season PPG × games left, where games left is the weeks
      remaining in the regular season minus one for a bye still ahead
    - Schedule (20%): avg opponent defensive weakness over the remaining
      schedule, relative to the league average allowed at the position
    - Usage (20%): avg offensive snap share
    - Health (10%): share of the weeks available across the regular seasons the
      injury frame carries, floored at 0.5

    Returns a frame carrying OUTPUT_SCHEMA, empty when the season has fewer
    than three weeks of results or no weeks left to play.
    """
    weeks_left = REGULAR_SEASON_WEEKS - current_week
    if weeks_left <= 0 or current_week < 3:
        return _empty_result()

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

    # ── Games left, bye-aware ────────────────────────────────
    # A bye is the only thing that varies games left across players, and so the
    # only thing that keeps the production component from cancelling out of the
    # max-normalization below.
    byes = _team_bye_weeks(schedules, season)
    baselines = baselines.join(byes, on="team", how="left").with_columns(
        (
            weeks_left
            - pl.when(pl.col("bye_week").is_not_null() & (pl.col("bye_week") > current_week))
            .then(1)
            .otherwise(0)
        ).alias("games_left")
    )

    # ── Production score (raw, not normalized yet) ───────────
    baselines = baselines.with_columns(
        (pl.col("ppg") * pl.col("games_left")).alias("production_raw")
    )

    # ── Schedule factor ──────────────────────────────────────
    def_str = _defensive_strength(weekly_stats, season, current_week)
    league_avg = def_str.group_by("position").agg(pl.col("fpts_allowed").mean().alias("league_avg"))
    sched = _remaining_schedule_factor(schedules, def_str, season, current_week)

    # Normalize: sched_factor / league_avg → multiplier around 1.0
    sched = (
        sched.join(league_avg, on="position", how="left")
        .with_columns((pl.col("sched_factor") / pl.col("league_avg")).alias("sched_mult"))
        .select("team", "position", "sched_mult")
    )

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

    # offense_pct arrives as either a 0-1 fraction or 0-100 points; scale
    # fractions to points so the usage component divides by a fixed 100.
    max_snap = snap_usage["avg_snap_pct"].max()
    if isinstance(max_snap, (int, float)) and max_snap <= 1.0:
        snap_usage = snap_usage.with_columns(pl.col("avg_snap_pct") * 100)

    snap_usage = snap_usage.join(id_map, left_on="pfr_player_id", right_on="pfr_id", how="inner")

    baselines = baselines.join(
        snap_usage.select("gsis_id", "avg_snap_pct"),
        left_on="player_id",
        right_on="gsis_id",
        how="left",
    ).with_columns(pl.col("avg_snap_pct").fill_null(50.0))

    # ── Health discount ──────────────────────────────────────
    health = _health_discount(injuries)
    baselines = baselines.join(
        health, left_on="player_id", right_on="gsis_id", how="left"
    ).with_columns(pl.col("health").fill_null(1.0))

    # ── Composite trade value ────────────────────────────────
    # Normalize each component to 0-1 range, then weight
    baselines = baselines.with_columns(
        # Production: normalize by max
        (pl.col("production_raw") / pl.col("production_raw").max()).alias("prod_norm"),
        # Schedule: already around 1.0, normalize to 0-1
        (
            (pl.col("sched_mult") - pl.col("sched_mult").min())
            / (pl.col("sched_mult").max() - pl.col("sched_mult").min() + 1e-9)
        ).alias("sched_norm"),
        # Usage: snap_pct / 100
        (pl.col("avg_snap_pct") / 100).alias("usage_norm"),
    )

    baselines = baselines.with_columns(
        (
            pl.col("prod_norm") * 0.50
            + pl.col("sched_norm") * 0.20
            + pl.col("usage_norm") * 0.20
            + pl.col("health") * 0.10
        ).alias("trade_value_raw")
    )

    # Scale to 0-100
    tv_max = baselines["trade_value_raw"].max()
    if isinstance(tv_max, (int, float)) and tv_max > 0:
        baselines = baselines.with_columns(
            (pl.col("trade_value_raw") / tv_max * 100).round(1).alias("trade_value")
        )
    else:
        baselines = baselines.with_columns(pl.lit(0.0).alias("trade_value"))

    return baselines.select(*OUTPUT_SCHEMA).cast(OUTPUT_SCHEMA).sort("trade_value", descending=True)
