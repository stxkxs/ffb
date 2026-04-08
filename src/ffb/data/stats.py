"""Derive weekly player stats from play-by-play data.

Fallback for seasons where nflverse hasn't published the player_stats
parquet file yet. Produces the same columns the injury_impact engine needs.
"""

import polars as pl


def compute_weekly_stats_from_pbp(
    pbp: pl.DataFrame,
    rosters: pl.DataFrame | None = None,
) -> pl.DataFrame:
    """Aggregate PBP into per-player weekly fantasy stats.

    Parameters
    ----------
    pbp : pl.DataFrame
        Play-by-play data.
    rosters : pl.DataFrame, optional
        Seasonal roster data for position lookup. If None, position will be null.
    """
    # Filter to actual plays
    plays = pbp.filter(
        pl.col("play_type").is_in(["pass", "run"])
    )

    # ── Receiving stats ──────────────────────────────────────
    rec = (
        plays.filter(
            (pl.col("pass_attempt") == 1)
            & pl.col("receiver_player_id").is_not_null()
        )
        .group_by(
            "receiver_player_id", "receiver_player_name",
            "posteam", "season", "week", "season_type",
        )
        .agg(
            pl.len().alias("targets"),
            pl.col("complete_pass").sum().alias("receptions"),
            pl.col("receiving_yards").sum().alias("receiving_yards"),
            pl.col("pass_touchdown").sum().alias("receiving_tds"),
        )
        .rename({
            "receiver_player_id": "player_id",
            "receiver_player_name": "player_display_name",
            "posteam": "recent_team",
        })
    )

    # ── Rushing stats ────────────────────────────────────────
    rush = (
        plays.filter(
            (pl.col("rush_attempt") == 1)
            & pl.col("rusher_player_id").is_not_null()
        )
        .group_by(
            "rusher_player_id", "rusher_player_name",
            "posteam", "season", "week", "season_type",
        )
        .agg(
            pl.len().alias("carries"),
            pl.col("rushing_yards").sum().alias("rushing_yards"),
            pl.col("rush_touchdown").sum().alias("rushing_tds"),
        )
        .rename({
            "rusher_player_id": "player_id",
            "rusher_player_name": "player_display_name",
            "posteam": "recent_team",
        })
    )

    # ── Passing stats ────────────────────────────────────────
    passing = (
        plays.filter(
            (pl.col("pass_attempt") == 1)
            & pl.col("passer_player_id").is_not_null()
        )
        .group_by(
            "passer_player_id", "passer_player_name",
            "posteam", "season", "week", "season_type",
        )
        .agg(
            pl.col("passing_yards").sum().alias("passing_yards"),
            pl.col("pass_touchdown").sum().alias("passing_tds"),
            pl.col("interception").sum().alias("interceptions"),
        )
        .rename({
            "passer_player_id": "player_id",
            "passer_player_name": "player_display_name",
            "posteam": "recent_team",
        })
    )

    # ── Fumbles (attributed to the fumbler) ──────────────────
    fumbles = (
        plays.filter(
            (pl.col("fumble_lost") == 1)
            & pl.col("fumbled_1_player_id").is_not_null()
        )
        .group_by(
            "fumbled_1_player_id", "fumbled_1_player_name",
            "posteam", "season", "week", "season_type",
        )
        .agg(pl.col("fumble_lost").sum().alias("fumbles_lost"))
        .rename({
            "fumbled_1_player_id": "player_id",
            "fumbled_1_player_name": "player_display_name",
            "posteam": "recent_team",
        })
    )

    # ── Opponent mapping (one defteam per team per game) ────
    opponent_map = (
        plays.select("posteam", "defteam", "season", "week")
        .drop_nulls()
        .unique()
        .rename({"posteam": "recent_team", "defteam": "opponent_team"})
    )

    # ── Merge all stat lines ─────────────────────────────────
    join_keys = ["player_id", "player_display_name", "recent_team", "season", "week", "season_type"]

    df = rec.join(rush, on=join_keys, how="full", coalesce=True)
    df = df.join(passing, on=join_keys, how="full", coalesce=True)
    df = df.join(fumbles, on=join_keys, how="full", coalesce=True)

    # Fill nulls for numeric columns
    stat_cols = [
        "targets", "receptions", "receiving_yards", "receiving_tds",
        "carries", "rushing_yards", "rushing_tds",
        "passing_yards", "passing_tds", "interceptions", "fumbles_lost",
    ]
    df = df.with_columns([pl.col(c).fill_null(0).cast(pl.Float32) for c in stat_cols])

    # ── PPR fantasy points ───────────────────────────────────
    df = df.with_columns(
        (
            pl.col("receiving_yards") / 10
            + pl.col("receiving_tds") * 6
            + pl.col("receptions") * 1
            + pl.col("rushing_yards") / 10
            + pl.col("rushing_tds") * 6
            + pl.col("passing_yards") / 25
            + pl.col("passing_tds") * 4
            - pl.col("interceptions") * 2
            - pl.col("fumbles_lost") * 2
        ).alias("fantasy_points_ppr")
    )

    # Add opponent_team
    df = df.join(opponent_map, on=["recent_team", "season", "week"], how="left")

    # Add position from rosters
    if rosters is not None:
        pos_map = (
            rosters.select("player_id", "position")
            .drop_nulls()
            .unique(subset=["player_id"], keep="last")
        )
        df = df.join(pos_map, on="player_id", how="left")

    return df.drop_nulls(subset=["player_id"])
