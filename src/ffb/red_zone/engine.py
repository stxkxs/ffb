"""Red zone efficiency computation — pure polars, no I/O."""

import polars as pl


def _rz_plays(pbp: pl.DataFrame) -> pl.DataFrame:
    """Filter to regular-season red zone pass/run plays."""
    return pbp.filter(
        (pl.col("yardline_100") <= 20)
        & (pl.col("season_type") == "REG")
        & pl.col("play_type").is_in(["pass", "run"])
    )


def compute_team_rz(pbp: pl.DataFrame) -> pl.DataFrame:
    """Team-level red zone stats per season.

    Returns: team, season, rz_trips, rz_tds, conv_pct, pass_pct, rush_pct, rz_epa.
    """
    rz = _rz_plays(pbp)

    df = (
        rz.group_by("posteam", "season")
        .agg(
            pl.concat_str(["game_id", "fixed_drive"], separator="_")
            .n_unique()
            .alias("rz_trips"),
            pl.col("touchdown").sum().cast(pl.Int32).alias("rz_tds"),
            pl.col("pass_attempt").sum().cast(pl.Int32).alias("rz_passes"),
            pl.col("rush_attempt").sum().cast(pl.Int32).alias("rz_rushes"),
            pl.len().alias("rz_plays"),
            pl.col("epa").mean().alias("rz_epa"),
        )
        .with_columns(
            (pl.col("rz_tds") / pl.col("rz_trips") * 100).alias("conv_pct"),
            (pl.col("rz_passes") / pl.col("rz_plays") * 100).alias("pass_pct"),
            (pl.col("rz_rushes") / pl.col("rz_plays") * 100).alias("rush_pct"),
        )
        .rename({"posteam": "team"})
        .sort("conv_pct", descending=True)
    )

    return df


def _player_position_map(rosters: pl.DataFrame) -> pl.DataFrame:
    """Build player GSIS ID → position lookup from roster data."""
    return (
        rosters.select("player_id", "position")
        .drop_nulls()
        .unique(subset=["player_id"], keep="last")
    )


def compute_player_rz(
    pbp: pl.DataFrame,
    rosters: pl.DataFrame,
) -> pl.DataFrame:
    """Player-level red zone stats per season.

    Returns: player, position, team, season, rz_targets, rz_tgt_share,
    rz_carries, rz_touches, rz_tds, td_pct.
    """
    rz = _rz_plays(pbp)
    pos_map = _player_position_map(rosters)

    # Receiving red zone stats
    rz_rec = (
        rz.filter(pl.col("pass_attempt") == 1)
        .drop_nulls(subset=["receiver_player_id"])
        .group_by("receiver_player_id", "receiver_player_name", "posteam", "season")
        .agg(
            pl.len().alias("rz_targets"),
            pl.col("pass_touchdown").sum().cast(pl.Int32).alias("rz_rec_tds"),
        )
        .rename({
            "receiver_player_id": "player_id",
            "receiver_player_name": "player",
            "posteam": "team",
        })
    )

    # Team RZ target totals for target share
    team_rz_targets = (
        rz_rec.group_by("team", "season")
        .agg(pl.col("rz_targets").sum().alias("team_rz_targets"))
    )
    rz_rec = rz_rec.join(team_rz_targets, on=["team", "season"], how="left")
    rz_rec = rz_rec.with_columns(
        (pl.col("rz_targets") / pl.col("team_rz_targets") * 100).alias("rz_tgt_share")
    ).drop("team_rz_targets")

    # Rushing red zone stats
    rz_rush = (
        rz.filter(pl.col("rush_attempt") == 1)
        .drop_nulls(subset=["rusher_player_id"])
        .group_by("rusher_player_id", "rusher_player_name", "posteam", "season")
        .agg(
            pl.len().alias("rz_carries"),
            pl.col("rush_touchdown").sum().cast(pl.Int32).alias("rz_rush_tds"),
        )
        .rename({
            "rusher_player_id": "player_id",
            "rusher_player_name": "player",
            "posteam": "team",
        })
    )

    # Merge receiving + rushing
    df = rz_rec.join(
        rz_rush,
        on=["player_id", "player", "team", "season"],
        how="full",
        coalesce=True,
    ).with_columns(
        pl.col("rz_targets").fill_null(0),
        pl.col("rz_rec_tds").fill_null(0),
        pl.col("rz_tgt_share").fill_null(0.0),
        pl.col("rz_carries").fill_null(0),
        pl.col("rz_rush_tds").fill_null(0),
    )

    # Derived columns
    df = df.with_columns(
        (pl.col("rz_targets") + pl.col("rz_carries")).alias("rz_touches"),
        (pl.col("rz_rec_tds") + pl.col("rz_rush_tds")).alias("rz_tds"),
    ).with_columns(
        pl.when(pl.col("rz_touches") > 0)
        .then(pl.col("rz_tds") / pl.col("rz_touches") * 100)
        .otherwise(0.0)
        .alias("td_pct"),
    )

    # Join position from rosters
    df = df.join(pos_map, on="player_id", how="left")

    return df.select(
        "player", "position", "team", "season",
        "rz_targets", "rz_tgt_share", "rz_carries", "rz_touches", "rz_tds", "td_pct",
    ).sort("rz_tds", descending=True)
