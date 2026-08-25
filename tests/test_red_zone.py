"""Property tests for the red zone efficiency engine.

Every expected number is read off the ``pbp`` fixture's construction rule: NYG takes
3 red zone trips over 8 plays split 4 passes to 4 rushes and scores on all 3, DAL
takes 4 trips over 8 plays split 6 passes to 2 rushes and scores once, PHI takes 1
trip and scores, and WAS takes 1 and does not.
"""

import polars as pl
import pytest

from ffb.red_zone.engine import compute_player_rz, compute_team_rz
from tests.conftest import (
    LEAGUE_POSITIONS,
    LEAGUE_TEAMS,
    league_gsis_id,
    league_player_name,
)

#: The columns ``compute_team_rz`` declares. Its docstring is the contract, so a
#: column arriving or leaving without the docstring following it is a break.
TEAM_COLUMNS = frozenset(
    {
        "team",
        "season",
        "rz_trips",
        "rz_tds",
        "conv_pct",
        "pass_pct",
        "rush_pct",
        "rz_epa",
    }
)

#: The columns ``compute_player_rz`` declares.
PLAYER_COLUMNS = frozenset(
    {
        "player",
        "position",
        "team",
        "season",
        "rz_targets",
        "rz_tgt_share",
        "rz_carries",
        "rz_touches",
        "rz_tds",
        "td_pct",
    }
)

#: Distance to the goal line at which a snap is a red zone snap.
RED_ZONE_YARDLINE = 20.0

#: A second display name for one GSIS id. nflverse fills ``receiver_player_name`` and
#: ``rusher_player_name`` from separate source fields, so one player reaches the engine
#: under two spellings.
ALTERNATE_SPELLING = "N. Receiver"


def _red_zone_plays(pbp: pl.DataFrame) -> pl.DataFrame:
    """Regular-season pass and run plays snapped on or inside the twenty."""
    return pbp.filter(
        (pl.col("yardline_100") <= RED_ZONE_YARDLINE)
        & (pl.col("season_type") == "REG")
        & pl.col("play_type").is_in(["pass", "run"])
    )


def _team_row(teams: pl.DataFrame, team: str) -> dict:
    """The single row `teams` holds for `team`."""
    rows = teams.filter(pl.col("team") == team).to_dicts()
    assert len(rows) == 1
    return rows[0]


def _player_row(players: pl.DataFrame, team: str, position: str) -> dict:
    """The single row `players` holds for `team`'s starter at `position`."""
    rows = players.filter(pl.col("player") == league_player_name(team, position)).to_dicts()
    assert len(rows) == 1
    return rows[0]


def _rows_for(players: pl.DataFrame, team: str, position: str) -> list[dict]:
    """Every row `players` holds for `team` at `position`, found without the name."""
    return players.filter((pl.col("team") == team) & (pl.col("position") == position)).to_dicts()


def _moved_downfield(pbp: pl.DataFrame) -> pl.DataFrame:
    """`pbp` with every snap taken from midfield, so no play is a red zone play."""
    return pbp.with_columns(pl.lit(50.0).alias("yardline_100"))


def _replayed_a_season_later(pbp: pl.DataFrame) -> pl.DataFrame:
    """`pbp` alongside a copy of itself played out one season later."""
    return pl.concat([pbp, pbp.with_columns(pl.col("season") + 1)])


def _one_snap_from(pbp: pl.DataFrame, yardline: float) -> pl.DataFrame:
    """One regular-season pass play of `pbp`, snapped `yardline` yards from the goal."""
    one = pbp.filter((pl.col("season_type") == "REG") & (pl.col("play_type") == "pass")).head(1)
    assert one.height == 1
    return one.with_columns(pl.lit(yardline).alias("yardline_100"))


def _hand_off_to(pbp: pl.DataFrame, team: str, position: str) -> pl.DataFrame:
    """`pbp` with `team`'s red zone rushes credited to its starter at `position`."""
    rushed = (
        (pl.col("posteam") == team)
        & (pl.col("rush_attempt") == 1.0)
        & (pl.col("yardline_100") <= RED_ZONE_YARDLINE)
    )
    return pbp.with_columns(
        pl.when(rushed)
        .then(pl.lit(league_gsis_id(team, position)))
        .otherwise(pl.col("rusher_player_id"))
        .alias("rusher_player_id"),
        pl.when(rushed)
        .then(pl.lit(league_player_name(team, position)))
        .otherwise(pl.col("rusher_player_name"))
        .alias("rusher_player_name"),
    )


def _respell_rusher(pbp: pl.DataFrame, player_id: str, spelling: str) -> pl.DataFrame:
    """`pbp` with every rush by `player_id` credited under the display name `spelling`."""
    return pbp.with_columns(
        pl.when(pl.col("rusher_player_id") == player_id)
        .then(pl.lit(spelling))
        .otherwise(pl.col("rusher_player_name"))
        .alias("rusher_player_name"),
    )


def _reordered(pbp: pl.DataFrame) -> pl.DataFrame:
    """The same plays as `pbp`, arriving in the opposite row order."""
    return pbp.reverse()


# ── Declared shape ───────────────────────────────────────────────────────────


def test_team_frame_carries_exactly_the_declared_columns(pbp):
    assert set(compute_team_rz(pbp).columns) == TEAM_COLUMNS


def test_player_frame_carries_exactly_the_declared_columns(pbp, rosters):
    assert set(compute_player_rz(pbp, rosters).columns) == PLAYER_COLUMNS


def test_team_frame_carries_the_declared_columns_when_no_play_reaches_the_red_zone(pbp):
    empty = compute_team_rz(_moved_downfield(pbp))

    assert empty.height == 0
    assert set(empty.columns) == TEAM_COLUMNS


def test_player_frame_carries_the_declared_columns_when_no_play_reaches_the_red_zone(pbp, rosters):
    empty = compute_player_rz(_moved_downfield(pbp), rosters)

    assert empty.height == 0
    assert set(empty.columns) == PLAYER_COLUMNS


# ── Which plays are red zone plays ───────────────────────────────────────────


def test_snap_on_the_twenty_yard_line_is_a_red_zone_snap(pbp):
    on_the_line = _one_snap_from(pbp, RED_ZONE_YARDLINE)

    assert compute_team_rz(on_the_line).height == 1


def test_snap_beyond_the_twenty_yard_line_is_not_a_red_zone_snap(pbp):
    beyond_the_line = _one_snap_from(pbp, RED_ZONE_YARDLINE + 0.5)

    assert compute_team_rz(beyond_the_line).height == 0


def test_snaps_taken_outside_the_twenty_reach_no_team_total(pbp):
    assert compute_team_rz(_moved_downfield(pbp)).height == 0


def test_postseason_snaps_reach_no_team_total(pbp):
    postseason = pbp.filter(pl.col("season_type") != "REG")

    assert postseason.height > 0
    assert compute_team_rz(postseason).height == 0


def test_team_touchdowns_count_only_regular_season_red_zone_scores(pbp):
    postseason_scores = pbp.filter(
        (pl.col("season_type") != "REG")
        & (pl.col("yardline_100") <= RED_ZONE_YARDLINE)
        & (pl.col("touchdown") == 1.0)
    )

    assert postseason_scores.height == 1
    assert _team_row(compute_team_rz(pbp), "NYG")["rz_tds"] == 3


def test_snaps_that_are_neither_pass_nor_rush_reach_no_team_total(pbp):
    dead_ball = pbp.filter(
        (pl.col("posteam") == "NYG")
        & (pl.col("yardline_100") <= RED_ZONE_YARDLINE)
        & ~pl.col("play_type").is_in(["pass", "run"])
    )
    nyg = _team_row(compute_team_rz(pbp), "NYG")

    assert dead_ball.height == 2
    assert nyg["pass_pct"] + nyg["rush_pct"] == pytest.approx(100.0)


# ── Team totals ──────────────────────────────────────────────────────────────


def test_trips_count_unique_drives_rather_than_plays(pbp):
    nyg_plays = _red_zone_plays(pbp).filter(pl.col("posteam") == "NYG")

    assert nyg_plays.height == 8
    assert _team_row(compute_team_rz(pbp), "NYG")["rz_trips"] == 3


def test_conversion_rate_is_touchdowns_over_trips(pbp):
    dal = _team_row(compute_team_rz(pbp), "DAL")

    assert dal["rz_tds"] == 1
    assert dal["rz_trips"] == 4
    assert dal["conv_pct"] == pytest.approx(25.0)


def test_conversion_rate_is_zero_for_a_team_that_scores_no_red_zone_touchdown(pbp):
    was = _team_row(compute_team_rz(pbp), "WAS")

    assert was["rz_tds"] == 0
    assert was["conv_pct"] == pytest.approx(0.0)


def test_pass_and_rush_shares_split_the_red_zone_plays(pbp):
    dal = _team_row(compute_team_rz(pbp), "DAL")

    assert dal["pass_pct"] == pytest.approx(75.0)
    assert dal["rush_pct"] == pytest.approx(25.0)


def test_red_zone_epa_averages_the_red_zone_plays(pbp):
    assert _team_row(compute_team_rz(pbp), "NYG")["rz_epa"] == pytest.approx(0.5)


def test_every_team_taking_a_red_zone_snap_gets_one_row(pbp):
    teams = compute_team_rz(pbp)

    assert sorted(teams["team"].to_list()) == sorted(LEAGUE_TEAMS)


def test_team_totals_are_kept_apart_by_season(pbp):
    season = pbp["season"][0]
    teams = compute_team_rz(_replayed_a_season_later(pbp))

    assert teams.height == 2 * len(LEAGUE_TEAMS)
    assert _team_row(teams.filter(pl.col("season") == season), "NYG")["rz_trips"] == 3


# ── Player totals ────────────────────────────────────────────────────────────


def test_target_share_divides_a_players_red_zone_targets_by_the_teams(pbp, rosters):
    players = compute_player_rz(pbp, rosters)
    receiver = _player_row(players, "NYG", "WR")
    tight_end = _player_row(players, "NYG", "TE")

    assert receiver["rz_targets"] == 3
    assert tight_end["rz_targets"] == 1
    assert receiver["rz_tgt_share"] == pytest.approx(75.0)


def test_target_shares_of_a_team_sum_to_one_hundred(pbp, rosters):
    shares = (
        compute_player_rz(pbp, rosters)
        .group_by("team")
        .agg(pl.col("rz_tgt_share").sum().alias("total"))
    )

    assert shares.height == len(LEAGUE_TEAMS)
    for row in shares.to_dicts():
        assert row["total"] == pytest.approx(100.0)


def test_target_share_divides_by_the_teams_targets_in_that_season(pbp, rosters):
    later = pbp["season"][0] + 1
    players = compute_player_rz(_replayed_a_season_later(pbp), rosters)

    row = _player_row(players.filter(pl.col("season") == later), "NYG", "WR")
    assert row["rz_targets"] == 3
    assert row["rz_tgt_share"] == pytest.approx(75.0)


def test_carries_count_only_red_zone_rushes(pbp, rosters):
    assert _player_row(compute_player_rz(pbp, rosters), "NYG", "RB")["rz_carries"] == 4


def test_touches_add_targets_to_carries(pbp, rosters):
    players = compute_player_rz(pbp, rosters)

    assert players.height > 0
    for row in players.to_dicts():
        assert row["rz_touches"] == row["rz_targets"] + row["rz_carries"]


def test_touchdown_rate_is_touchdowns_over_touches(pbp, rosters):
    back = _player_row(compute_player_rz(pbp, rosters), "NYG", "RB")

    assert back["rz_tds"] == 1
    assert back["rz_touches"] == 4
    assert back["td_pct"] == pytest.approx(25.0)


def test_touchdown_rate_is_zero_for_a_player_who_scores_no_red_zone_touchdown(pbp, rosters):
    tight_end = _player_row(compute_player_rz(pbp, rosters), "DAL", "TE")

    assert tight_end["rz_touches"] == 3
    assert tight_end["rz_tds"] == 0
    assert tight_end["td_pct"] == pytest.approx(0.0)


def test_touchdown_rate_is_never_null(pbp, rosters):
    assert compute_player_rz(pbp, rosters)["td_pct"].null_count() == 0


def test_player_taking_no_red_zone_target_reports_a_zero_target_share(pbp, rosters):
    back = _player_row(compute_player_rz(pbp, rosters), "NYG", "RB")

    assert back["rz_targets"] == 0
    assert back["rz_tgt_share"] == pytest.approx(0.0)


def test_player_who_both_catches_and_runs_in_the_red_zone_gets_one_row(pbp, rosters):
    carried_by_the_receiver = _hand_off_to(pbp, "NYG", "WR")
    players = compute_player_rz(carried_by_the_receiver, rosters)
    receiver = _player_row(players, "NYG", "WR")

    assert receiver["rz_targets"] == 3
    assert receiver["rz_carries"] == 4
    assert receiver["rz_touches"] == 7


def test_position_comes_from_the_roster(pbp, rosters):
    players = compute_player_rz(pbp, rosters)

    for row in players.to_dicts():
        team, _, _ = row["player"].partition(" ")
        assert row["position"] in LEAGUE_POSITIONS
        assert row["player"] == league_player_name(team, row["position"])


# ── One player, one row ──────────────────────────────────────────────────────


def test_player_spelled_two_ways_across_the_name_columns_gets_one_row(pbp, rosters):
    two_spellings = _respell_rusher(
        _hand_off_to(pbp, "NYG", "WR"),
        league_gsis_id("NYG", "WR"),
        ALTERNATE_SPELLING,
    )
    receiver = _rows_for(compute_player_rz(two_spellings, rosters), "NYG", "WR")

    assert len(receiver) == 1
    assert receiver[0]["rz_targets"] == 3
    assert receiver[0]["rz_carries"] == 4
    assert receiver[0]["rz_touches"] == 7
    assert receiver[0]["rz_tds"] == 2
    assert receiver[0]["td_pct"] == pytest.approx(200.0 / 7)


def test_player_spelled_two_ways_renders_under_the_receiving_spelling(pbp, rosters):
    two_spellings = _respell_rusher(
        _hand_off_to(pbp, "NYG", "WR"),
        league_gsis_id("NYG", "WR"),
        ALTERNATE_SPELLING,
    )
    receiver = _rows_for(compute_player_rz(two_spellings, rosters), "NYG", "WR")

    assert len(receiver) == 1
    assert receiver[0]["player"] == league_player_name("NYG", "WR")


def test_player_who_only_runs_in_the_red_zone_renders_under_the_rushing_spelling(pbp, rosters):
    back = _rows_for(compute_player_rz(pbp, rosters), "NYG", "RB")

    assert len(back) == 1
    assert back[0]["rz_targets"] == 0
    assert back[0]["player"] == league_player_name("NYG", "RB")


def test_every_player_row_carries_a_display_name(pbp, rosters):
    assert compute_player_rz(pbp, rosters)["player"].null_count() == 0


# ── Touchdown rate is defined on every row ───────────────────────────────────


def test_every_player_row_carries_at_least_one_red_zone_touch(pbp, rosters):
    touches = compute_player_rz(pbp, rosters)["rz_touches"]

    assert touches.len() > 0
    assert touches.min() >= 1


def test_touchdown_rate_is_the_touchdown_ratio_on_every_row(pbp, rosters):
    rows = compute_player_rz(pbp, rosters).to_dicts()

    assert rows
    for row in rows:
        assert row["td_pct"] == pytest.approx(row["rz_tds"] / row["rz_touches"] * 100)


# ── Row order ────────────────────────────────────────────────────────────────


def test_team_rows_land_in_one_order_whatever_order_the_plays_arrive_in(pbp):
    assert compute_team_rz(pbp).equals(compute_team_rz(_reordered(pbp)))


def test_team_rows_hold_that_order_across_two_seasons_of_the_same_plays(pbp):
    both_seasons = _replayed_a_season_later(pbp)

    assert compute_team_rz(both_seasons).equals(compute_team_rz(_reordered(both_seasons)))


def test_teams_tying_on_conversion_rate_are_ordered_by_team(pbp):
    perfect = compute_team_rz(pbp).filter(pl.col("conv_pct") == 100.0)

    assert perfect["team"].to_list() == ["NYG", "PHI"]


def test_player_rows_land_in_one_order_whatever_order_the_plays_arrive_in(pbp, rosters):
    assert compute_player_rz(pbp, rosters).equals(compute_player_rz(_reordered(pbp), rosters))


def test_players_tying_on_touchdowns_are_ordered_by_touches_then_name(pbp, rosters):
    scorers = compute_player_rz(pbp, rosters).filter(pl.col("rz_tds") == 1)

    assert scorers["player"].to_list() == [
        league_player_name("NYG", "RB"),
        league_player_name("DAL", "WR"),
        league_player_name("NYG", "WR"),
        league_player_name("NYG", "TE"),
        league_player_name("PHI", "RB"),
    ]
