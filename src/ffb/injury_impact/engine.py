"""Injury impact computation — pure polars, no I/O.

Determines how teammate fantasy production changes when a given player
is active vs inactive.
"""

from typing import TypedDict

import polars as pl

from ffb.data import OFFENSIVE_POSITIONS, build_id_crosswalk


class PlayerInfo(TypedDict):
    """The searched player, described by one team-season of absences."""

    name: str
    position: str
    team: str
    season: int
    games_missed: int


def get_searchable_players(snaps: pl.DataFrame) -> list[str]:
    """Return sorted unique player names for search autocomplete."""
    return (
        snaps.filter(pl.col("position").is_in(OFFENSIVE_POSITIONS) & (pl.col("game_type") == "REG"))
        .select("player")
        .unique()
        .sort("player")
        .to_series()
        .to_list()
    )


def compute_injury_impact(
    player_name: str,
    snaps: pl.DataFrame,
    weekly_stats: pl.DataFrame,
    player_ids: pl.DataFrame,
    min_games_missed: int = 2,
) -> tuple[PlayerInfo | None, pl.DataFrame]:
    """Compute teammate fantasy impact when a player misses games.

    A teammate's name and position come from their last week of snaps with the team;
    two listings inside that week resolve on the position label, then the name, in
    ascending order.

    Returns (player_info, teammates_df).
    player_info is None if the player isn't found or has insufficient data.
    """
    id_map = build_id_crosswalk(player_ids)

    # Filter to regular season offensive snaps
    reg_snaps = snaps.filter(
        (pl.col("game_type") == "REG") & pl.col("position").is_in(OFFENSIVE_POSITIONS)
    )

    # Find the target player
    player_rows = reg_snaps.filter(pl.col("player") == player_name)
    if player_rows.is_empty():
        return None, pl.DataFrame()

    # Get player's team-seasons. `unique` does not preserve input order, so
    # sort explicitly to keep the concatenated result order reproducible.
    player_team_seasons = (
        player_rows.select("pfr_player_id", "position", "team", "season")
        .unique()
        .sort(
            ["season", "team", "position", "pfr_player_id"],
            descending=[True, False, False, False],
        )
    )

    all_results = []
    info_candidates: list[PlayerInfo] = []

    for row in player_team_seasons.iter_rows(named=True):
        pfr_id = row["pfr_player_id"]
        team = row["team"]
        season = row["season"]
        position = row["position"]

        # All weeks this team played
        team_weeks = (
            reg_snaps.filter((pl.col("team") == team) & (pl.col("season") == season))
            .select("week")
            .unique()
            .to_series()
        )

        # Weeks the player was active (offense_snaps > 0)
        active_weeks = (
            player_rows.filter(
                (pl.col("team") == team)
                & (pl.col("season") == season)
                & (pl.col("offense_snaps") > 0)
            )
            .select("week")
            .to_series()
        )

        inactive_weeks = team_weeks.filter(~team_weeks.is_in(active_weeks.implode()))

        if len(inactive_weeks) < min_games_missed:
            continue

        # Every teammate on this team-season, the target player aside. The sort
        # keys order the rows of a teammate completely, so the one kept — and the
        # name and position it carries — follows from the data, not from row order.
        teammates = (
            reg_snaps.filter(
                (pl.col("team") == team)
                & (pl.col("season") == season)
                & (pl.col("pfr_player_id") != pfr_id)
            )
            .select("pfr_player_id", "player", "position", "week")
            .sort(["week", "position", "player"], descending=[True, False, False])
            .unique(subset=["pfr_player_id"], keep="first", maintain_order=True)
            .drop("week")
        )

        # Map teammate pfr_id → gsis_id
        teammates_with_gsis = teammates.join(
            id_map, left_on="pfr_player_id", right_on="pfr_id", how="inner"
        )

        if teammates_with_gsis.is_empty():
            continue

        # Get weekly stats for these teammates in this season
        teammate_gsis_ids = teammates_with_gsis.select("gsis_id").to_series()
        tm_stats = weekly_stats.filter(
            (pl.col("player_id").is_in(teammate_gsis_ids.implode()))
            & (pl.col("season") == season)
            & (pl.col("season_type") == "REG")
        ).select("player_id", "week", "fantasy_points_ppr", "targets", "carries")

        if tm_stats.is_empty():
            continue

        # Tag each week as active or inactive
        tm_stats = tm_stats.with_columns(
            pl.col("week").is_in(active_weeks.implode()).alias("player_active")
        ).with_columns(
            (pl.col("targets").fill_null(0) + pl.col("carries").fill_null(0)).alias("touches")
        )

        # Aggregate: avg stats per teammate per split
        agg = tm_stats.group_by("player_id", "player_active").agg(
            pl.len().alias("games"),
            pl.col("fantasy_points_ppr").mean().alias("avg_fpts"),
            pl.col("targets").mean().alias("avg_tgt"),
            pl.col("touches").mean().alias("avg_touches"),
        )

        # Pivot active/inactive into separate columns
        with_stats = agg.filter(pl.col("player_active")).drop("player_active")
        without_stats = agg.filter(~pl.col("player_active")).drop("player_active")

        merged = with_stats.join(without_stats, on="player_id", how="inner", suffix="_wo")

        if merged.is_empty():
            continue

        # Compute deltas
        merged = merged.with_columns(
            (pl.col("avg_fpts_wo") - pl.col("avg_fpts")).alias("delta_fpts"),
            (pl.col("avg_tgt_wo") - pl.col("avg_tgt")).alias("delta_tgt"),
            (pl.col("avg_touches_wo") - pl.col("avg_touches")).alias("delta_touches"),
        )

        # Join back teammate names and positions
        merged = merged.join(
            teammates_with_gsis.select("gsis_id", "player", "position"),
            left_on="player_id",
            right_on="gsis_id",
            how="left",
        )

        # Add context columns
        merged = merged.with_columns(
            pl.lit(team).alias("team"),
            pl.lit(season).alias("season"),
            pl.when(pl.col("games_wo") >= 7)
            .then(pl.lit("High"))
            .when(pl.col("games_wo") >= 4)
            .then(pl.lit("Med"))
            .otherwise(pl.lit("Low"))
            .alias("confidence"),
        )

        result = merged.select(
            pl.col("player").alias("teammate"),
            "position",
            "team",
            "season",
            pl.col("games").alias("games_with"),
            pl.col("games_wo").alias("games_without"),
            pl.col("avg_fpts").alias("fpts_with"),
            pl.col("avg_fpts_wo").alias("fpts_without"),
            "delta_fpts",
            pl.col("avg_tgt").alias("tgt_with"),
            pl.col("avg_tgt_wo").alias("tgt_without"),
            pl.col("avg_touches").alias("touches_with"),
            pl.col("avg_touches_wo").alias("touches_without"),
            "confidence",
        )

        all_results.append(result)

        info_candidates.append(
            {
                "name": player_name,
                "position": position,
                "team": team,
                "season": season,
                "games_missed": len(inactive_weeks),
            }
        )

    if not all_results:
        return None, pl.DataFrame()

    # Describe the player by the most recent team-season that produced
    # teammate splits; equal seasons break on team name so the same input
    # always yields the same description.
    player_info = min(info_candidates, key=lambda c: (-c["season"], c["team"]))

    combined = pl.concat(all_results)
    combined = combined.sort(pl.col("delta_fpts").abs(), descending=True)

    return player_info, combined
