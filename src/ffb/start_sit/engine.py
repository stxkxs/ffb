"""Start/sit matchup projection engine — pure polars, no I/O.

Cross-references player baselines against opponent defensive rankings
by position to produce matchup-adjusted fantasy projections.
"""

import polars as pl

from ffb.data import OFFENSIVE_POSITIONS

# Weeks of data a defense needs before its points-allowed average can scale a
# projection; a one-game sample swings the multiplier by more than the matchup
# it claims to describe.
MIN_DEFENSIVE_GAMES = 3

# Games a player needs before their per-game average is treated as a baseline.
MIN_BASELINE_GAMES = 3

# Columns and dtypes every compute_start_sit result carries, in order. Callers
# filter and index the result by name, so an empty result must be shaped
# identically to a populated one.
OUTPUT_SCHEMA = pl.Schema(
    {
        "player": pl.String(),
        "position": pl.String(),
        "team": pl.String(),
        "opponent": pl.String(),
        "baseline_fpts": pl.Float64(),
        "fpts_allowed": pl.Float64(),
        "league_avg": pl.Float64(),
        "matchup_mult": pl.Float64(),
        "projected_fpts": pl.Float64(),
        "confidence": pl.String(),
    }
)


def _empty_result() -> pl.DataFrame:
    """Zero-row frame carrying OUTPUT_SCHEMA."""
    return pl.DataFrame(schema=OUTPUT_SCHEMA)


def _defensive_rankings(
    weekly_stats: pl.DataFrame,
    season: int,
    through_week: int,
) -> pl.DataFrame:
    """Avg fantasy points allowed per game by defense and position.

    Defenses with fewer than MIN_DEFENSIVE_GAMES games of data are excluded:
    they distort the league average and any projection scaled against them.

    Returns: opponent_team, position, fpts_allowed (avg per game).
    """
    df = weekly_stats.filter(
        (pl.col("season") == season)
        & (pl.col("season_type") == "REG")
        & (pl.col("week") <= through_week)
        & pl.col("opponent_team").is_not_null()
        & pl.col("position").is_in(OFFENSIVE_POSITIONS)
    )

    # Sum fantasy points per (opponent defense, position, week)
    per_game = df.group_by("opponent_team", "position", "week").agg(
        pl.col("fantasy_points_ppr").sum().alias("fpts_scored")
    )

    # Average across weeks, holding each defense to a minimum sample
    return (
        per_game.group_by("opponent_team", "position")
        .agg(
            pl.col("fpts_scored").mean().alias("fpts_allowed"),
            pl.col("fpts_scored").len().alias("games"),
        )
        .filter(pl.col("games") >= MIN_DEFENSIVE_GAMES)
        .select("opponent_team", "position", "fpts_allowed")
    )


def _league_avg_by_position(def_ranks: pl.DataFrame) -> pl.DataFrame:
    """League-wide average fantasy points per position per game."""
    return def_ranks.group_by("position").agg(pl.col("fpts_allowed").mean().alias("league_avg"))


def _player_baselines(
    weekly_stats: pl.DataFrame,
    season: int,
    through_week: int,
) -> pl.DataFrame:
    """Per-player season average fantasy points per game.

    Players with fewer than `MIN_BASELINE_GAMES` regular-season games are
    dropped, so a single outlier week never stands in for a season average.
    """
    df = weekly_stats.filter(
        (pl.col("season") == season)
        & (pl.col("season_type") == "REG")
        & (pl.col("week") <= through_week)
        & pl.col("position").is_in(OFFENSIVE_POSITIONS)
    )

    return (
        df.group_by("player_id", "player_display_name", "position", "recent_team")
        .agg(
            pl.col("fantasy_points_ppr").mean().alias("baseline_fpts"),
            pl.col("fantasy_points_ppr").len().alias("games_played"),
        )
        .filter(pl.col("games_played") >= MIN_BASELINE_GAMES)
        .rename({"player_display_name": "player", "recent_team": "team"})
    )


def _matchups_for_week(
    schedules: pl.DataFrame,
    season: int,
    week: int,
) -> pl.DataFrame:
    """Build team → opponent mapping for a given week.

    Returns: team, opponent.
    """
    games = schedules.filter(
        (pl.col("season") == season) & (pl.col("week") == week) & (pl.col("game_type") == "REG")
    )

    # Each game produces two rows: home vs away and away vs home
    home = games.select(
        pl.col("home_team").alias("team"),
        pl.col("away_team").alias("opponent"),
    )
    away = games.select(
        pl.col("away_team").alias("team"),
        pl.col("home_team").alias("opponent"),
    )

    return pl.concat([home, away])


def compute_start_sit(
    weekly_stats: pl.DataFrame,
    schedules: pl.DataFrame,
    season: int,
    week: int,
) -> pl.DataFrame:
    """Produce matchup-adjusted projections for a given week.

    Returns a frame carrying OUTPUT_SCHEMA, empty when fewer than
    MIN_BASELINE_GAMES weeks precede the requested week or no regular-season
    games are scheduled for it.
    """
    through_week = week - 1
    # No player clears MIN_BASELINE_GAMES games with fewer weeks than that behind
    # them, so the opening weeks of a season carry no projection. Answering at the
    # boundary holds that contract whatever thresholds the baseline and defensive
    # sample floors below are set to.
    if through_week < MIN_BASELINE_GAMES:
        return _empty_result()

    # Core computations
    baselines = _player_baselines(weekly_stats, season, through_week)
    def_ranks = _defensive_rankings(weekly_stats, season, through_week)
    league_avgs = _league_avg_by_position(def_ranks)
    matchups = _matchups_for_week(schedules, season, week)

    if matchups.is_empty():
        return _empty_result()

    # Join: player → their matchup opponent. A team on a bye holds no matchup
    # row, so an inner join leaves its starters out of the projection here. The
    # null drop at the end answers a separate state: an opponent whose defense
    # carries no ranking.
    df = baselines.join(matchups, on="team", how="inner")

    # Join: opponent defensive ranking for this position
    df = df.join(
        def_ranks.select("opponent_team", "position", "fpts_allowed"),
        left_on=["opponent", "position"],
        right_on=["opponent_team", "position"],
        how="left",
    )

    # Join: league average for this position
    df = df.join(league_avgs, on="position", how="left")

    # Matchup multiplier and projection
    df = df.with_columns(
        (pl.col("fpts_allowed") / pl.col("league_avg")).alias("matchup_mult")
    ).with_columns((pl.col("baseline_fpts") * pl.col("matchup_mult")).alias("projected_fpts"))

    # Confidence tier (check from highest threshold down)
    df = df.with_columns(
        pl.when(pl.col("matchup_mult") >= 1.20)
        .then(pl.lit("Strong Start"))
        .when(pl.col("matchup_mult") >= 1.08)
        .then(pl.lit("Lean Start"))
        .when(pl.col("matchup_mult") >= 0.92)
        .then(pl.lit("Neutral"))
        .when(pl.col("matchup_mult") >= 0.80)
        .then(pl.lit("Lean Sit"))
        .otherwise(pl.lit("Strong Sit"))
        .alias("confidence")
    )

    return (
        df.select(*OUTPUT_SCHEMA)
        .cast(OUTPUT_SCHEMA)
        .drop_nulls(subset=["projected_fpts"])
        .sort("projected_fpts", descending=True)
    )
