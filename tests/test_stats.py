"""Tests for weekly stats derived from play-by-play.

The play builders below carry every column ``compute_weekly_stats_from_pbp`` reads
and nothing else, so a frame built here is the function's whole input contract.
Every expected point total is the PPR arithmetic spelled out against the plays that
produced it, so a failure names the scoring term that moved.
"""

from typing import NamedTuple

import polars as pl
import pytest
from polars.testing import assert_frame_equal

from ffb.data.stats import compute_weekly_stats_from_pbp
from ffb.start_sit.engine import OUTPUT_SCHEMA as START_SIT_SCHEMA
from ffb.start_sit.engine import compute_start_sit
from ffb.trade_value.engine import OUTPUT_SCHEMA as TRADE_VALUE_SCHEMA
from ffb.trade_value.engine import compute_trade_values
from tests.conftest import (
    LEAGUE_POSITIONS,
    LEAGUE_TEAMS,
    POSTSEASON_WEEK,
    league_gsis_id,
    league_opponent,
)

# ── Input contract ───────────────────────────────────────────────────────────

#: Every play-by-play column the function reads, typed as nflverse publishes it:
#: indicators and yardage as floats, identifiers and names as strings.
PBP_SCHEMA = {
    "play_type": pl.String,
    "posteam": pl.String,
    "defteam": pl.String,
    "season": pl.Int64,
    "week": pl.Int64,
    "season_type": pl.String,
    "pass_attempt": pl.Float64,
    "rush_attempt": pl.Float64,
    "complete_pass": pl.Float64,
    "pass_touchdown": pl.Float64,
    "rush_touchdown": pl.Float64,
    "interception": pl.Float64,
    "fumble_lost": pl.Float64,
    "passing_yards": pl.Float64,
    "receiving_yards": pl.Float64,
    "rushing_yards": pl.Float64,
    "passer_player_id": pl.String,
    "passer_player_name": pl.String,
    "receiver_player_id": pl.String,
    "receiver_player_name": pl.String,
    "rusher_player_id": pl.String,
    "rusher_player_name": pl.String,
    "fumbled_1_player_id": pl.String,
    "fumbled_1_player_name": pl.String,
}

#: Columns that count events or yardage, and so read zero rather than null on a
#: player who took no play of that kind.
STAT_COLUMNS = (
    "targets",
    "receptions",
    "receiving_yards",
    "receiving_tds",
    "carries",
    "rushing_yards",
    "rushing_tds",
    "passing_yards",
    "passing_tds",
    "interceptions",
    "fumbles_lost",
)

_INDICATOR_COLUMNS = (
    "pass_attempt",
    "rush_attempt",
    "complete_pass",
    "pass_touchdown",
    "rush_touchdown",
    "interception",
    "fumble_lost",
    "passing_yards",
    "receiving_yards",
    "rushing_yards",
)

HOME = "HOM"
AWAY = "AWY"
SEASON = 2025
WEEK = 1

#: The week the start/sit and trade value engines act on. Three weeks of results
#: precede it, the least history either engine takes.
PROJECTED_WEEK = 4


class Player(NamedTuple):
    """A play-by-play identity: the id a stat line keys on and the name it carries."""

    player_id: str
    name: str


PASSER = Player("00-0011", "Ada Everett")
CATCHER = Player("00-0012", "Bo Lindqvist")
CARRIER = Player("00-0013", "Cy Marchetti")
ALL_PURPOSE = Player("00-0014", "Dee Halvorsen")


def play(**fields: object) -> dict[str, object]:
    """One snap by `HOME` against `AWAY` in week `WEEK`, with every indicator at zero."""
    row: dict[str, object] = dict.fromkeys(PBP_SCHEMA)
    row.update(dict.fromkeys(_INDICATOR_COLUMNS, 0.0))
    row.update(
        {
            "posteam": HOME,
            "defteam": AWAY,
            "season": SEASON,
            "week": WEEK,
            "season_type": "REG",
        }
    )
    row.update(fields)
    return row


def throw(
    passer: Player,
    receiver: Player,
    *,
    complete: float = 1.0,
    yards: float = 0.0,
    td: float = 0.0,
    intercepted: float = 0.0,
    offense: str = HOME,
    defense: str = AWAY,
    week: int = WEEK,
    season_type: str = "REG",
) -> dict[str, object]:
    """One pass attempt from `passer` to `receiver`."""
    return play(
        play_type="pass",
        pass_attempt=1.0,
        complete_pass=complete,
        passing_yards=yards,
        receiving_yards=yards,
        pass_touchdown=td,
        interception=intercepted,
        passer_player_id=passer.player_id,
        passer_player_name=passer.name,
        receiver_player_id=receiver.player_id,
        receiver_player_name=receiver.name,
        posteam=offense,
        defteam=defense,
        week=week,
        season_type=season_type,
    )


def carry(
    rusher: Player,
    *,
    yards: float = 0.0,
    td: float = 0.0,
    fumbler: Player | None = None,
    offense: str = HOME,
    defense: str = AWAY,
    week: int = WEEK,
    season_type: str = "REG",
) -> dict[str, object]:
    """One rush by `rusher`, lost on a fumble by `fumbler` when one is given."""
    fields: dict[str, object] = {
        "play_type": "run",
        "rush_attempt": 1.0,
        "rushing_yards": yards,
        "rush_touchdown": td,
        "rusher_player_id": rusher.player_id,
        "rusher_player_name": rusher.name,
        "posteam": offense,
        "defteam": defense,
        "week": week,
        "season_type": season_type,
    }
    if fumbler is not None:
        fields["fumble_lost"] = 1.0
        fields["fumbled_1_player_id"] = fumbler.player_id
        fields["fumbled_1_player_name"] = fumbler.name
    return play(**fields)


def frame(plays: list[dict[str, object]]) -> pl.DataFrame:
    """Play-by-play frame over `plays`, typed as nflverse publishes it."""
    return pl.DataFrame(plays, schema=PBP_SCHEMA)


def rows_for(stats: pl.DataFrame, player: Player) -> pl.DataFrame:
    """Every weekly line `stats` carries for `player`."""
    return stats.filter(pl.col("player_id") == player.player_id)


def points_for(stats: pl.DataFrame, player: Player) -> float:
    """`player`'s PPR points in `stats`, requiring exactly one line for them."""
    return float(rows_for(stats, player)["fantasy_points_ppr"].item())


#: A week in which one player throws, catches, runs and fumbles. Their line is
#: 250 passing yards and 2 passing touchdowns against 1 interception, 5 catches for
#: 100 yards and a touchdown, and 3 carries for 50 yards, a touchdown and a fumble.
EVERY_ROLE_WEEK = [
    throw(ALL_PURPOSE, CATCHER, yards=100.0, td=1.0),
    throw(ALL_PURPOSE, CATCHER, yards=100.0, td=1.0),
    throw(ALL_PURPOSE, CATCHER, yards=50.0),
    throw(ALL_PURPOSE, CATCHER, complete=0.0, intercepted=1.0),
    throw(PASSER, ALL_PURPOSE, yards=40.0),
    throw(PASSER, ALL_PURPOSE, yards=30.0),
    throw(PASSER, ALL_PURPOSE, yards=20.0),
    throw(PASSER, ALL_PURPOSE, yards=10.0),
    throw(PASSER, ALL_PURPOSE, td=1.0),
    carry(ALL_PURPOSE, yards=30.0),
    carry(ALL_PURPOSE, yards=20.0, td=1.0),
    carry(ALL_PURPOSE, fumbler=ALL_PURPOSE),
]


# ── PPR scoring ──────────────────────────────────────────────────────────────


@pytest.mark.parametrize(
    ("plays", "scorer", "expected"),
    [
        pytest.param(
            [throw(PASSER, CATCHER)],
            CATCHER,
            1.0,
            id="a_reception_scores_one",
        ),
        pytest.param(
            [throw(PASSER, CATCHER, complete=0.0)],
            CATCHER,
            0.0,
            id="an_incomplete_target_scores_nothing",
        ),
        pytest.param(
            [throw(PASSER, CATCHER, yards=10.0)],
            CATCHER,
            2.0,
            id="ten_receiving_yards_score_one_over_the_reception",
        ),
        pytest.param(
            [throw(PASSER, CATCHER, td=1.0)],
            CATCHER,
            7.0,
            id="a_receiving_touchdown_scores_six_over_the_reception",
        ),
        pytest.param(
            [carry(CARRIER, yards=10.0)],
            CARRIER,
            1.0,
            id="ten_rushing_yards_score_one",
        ),
        pytest.param(
            [carry(CARRIER, td=1.0)],
            CARRIER,
            6.0,
            id="a_rushing_touchdown_scores_six",
        ),
        pytest.param(
            [throw(PASSER, CATCHER, yards=25.0)],
            PASSER,
            1.0,
            id="twenty_five_passing_yards_score_one",
        ),
        pytest.param(
            [throw(PASSER, CATCHER, td=1.0)],
            PASSER,
            4.0,
            id="a_passing_touchdown_scores_four",
        ),
        pytest.param(
            [throw(PASSER, CATCHER, complete=0.0, intercepted=1.0)],
            PASSER,
            -2.0,
            id="an_interception_scores_minus_two",
        ),
        pytest.param(
            [carry(CARRIER, fumbler=CARRIER)],
            CARRIER,
            -2.0,
            id="a_lost_fumble_scores_minus_two",
        ),
    ],
)
def test_ppr_scoring_term(plays: list[dict[str, object]], scorer: Player, expected: float) -> None:
    stats = compute_weekly_stats_from_pbp(frame(plays))
    assert points_for(stats, scorer) == pytest.approx(expected)


def test_ppr_points_sum_every_scoring_term() -> None:
    stats = compute_weekly_stats_from_pbp(frame(EVERY_ROLE_WEEK))
    passing = 250 / 25 + 2 * 4 - 1 * 2
    receiving = 100 / 10 + 5 * 1 + 1 * 6
    rushing = 50 / 10 + 1 * 6 - 1 * 2
    assert points_for(stats, ALL_PURPOSE) == pytest.approx(passing + receiving + rushing)


# ── Merging the four stat lines ──────────────────────────────────────────────


def test_a_multi_role_week_yields_one_line_per_player() -> None:
    stats = compute_weekly_stats_from_pbp(frame(EVERY_ROLE_WEEK))
    assert rows_for(stats, ALL_PURPOSE).height == 1


def test_a_merged_line_carries_the_totals_of_every_role() -> None:
    stats = compute_weekly_stats_from_pbp(frame(EVERY_ROLE_WEEK))
    line = rows_for(stats, ALL_PURPOSE).select(STAT_COLUMNS).row(0, named=True)
    assert line == {
        "targets": 5.0,
        "receptions": 5.0,
        "receiving_yards": 100.0,
        "receiving_tds": 1.0,
        "carries": 3.0,
        "rushing_yards": 50.0,
        "rushing_tds": 1.0,
        "passing_yards": 250.0,
        "passing_tds": 2.0,
        "interceptions": 1.0,
        "fumbles_lost": 1.0,
    }


def test_one_line_per_player_season_week_and_season_type(pbp: pl.DataFrame) -> None:
    stats = compute_weekly_stats_from_pbp(pbp)
    keys = stats.select("player_id", "season", "week", "season_type")
    assert keys.n_unique() == stats.height


def test_a_pass_with_no_receiver_credits_the_passer_alone() -> None:
    stats = compute_weekly_stats_from_pbp(
        frame(
            [
                play(
                    play_type="pass",
                    pass_attempt=1.0,
                    passer_player_id=PASSER.player_id,
                    passer_player_name=PASSER.name,
                )
            ]
        )
    )
    assert stats["player_id"].to_list() == [PASSER.player_id]


def test_snaps_that_are_neither_pass_nor_run_produce_no_lines() -> None:
    stats = compute_weekly_stats_from_pbp(
        frame(
            [
                play(play_type="field_goal"),
                play(play_type="punt"),
                play(play_type="no_play"),
            ]
        )
    )
    assert stats.height == 0


# ── Absent roles read zero, not null ─────────────────────────────────────────


def test_a_player_with_no_rushing_plays_carries_zero_rushing_stats() -> None:
    stats = compute_weekly_stats_from_pbp(frame([throw(PASSER, CATCHER, yards=10.0)]))
    line = rows_for(stats, CATCHER).select("carries", "rushing_yards", "rushing_tds")
    assert line.row(0, named=True) == {"carries": 0.0, "rushing_yards": 0.0, "rushing_tds": 0.0}


def test_a_player_with_no_passing_plays_carries_zero_passing_stats() -> None:
    stats = compute_weekly_stats_from_pbp(frame([carry(CARRIER, yards=10.0)]))
    line = rows_for(stats, CARRIER).select("passing_yards", "passing_tds", "interceptions")
    assert line.row(0, named=True) == {
        "passing_yards": 0.0,
        "passing_tds": 0.0,
        "interceptions": 0.0,
    }


def test_no_stat_column_carries_a_null(pbp: pl.DataFrame) -> None:
    stats = compute_weekly_stats_from_pbp(pbp)
    counted = (*STAT_COLUMNS, "fantasy_points_ppr")
    assert stats.select(counted).null_count().row(0) == (0,) * len(counted)


# ── Opponent ─────────────────────────────────────────────────────────────────


def test_opponent_team_follows_the_possessing_team() -> None:
    stats = compute_weekly_stats_from_pbp(
        frame(
            [
                throw(PASSER, CATCHER, yards=10.0),
                carry(CARRIER, yards=10.0, offense=AWAY, defense=HOME),
            ]
        )
    )
    assert dict(
        zip(stats["player_id"].to_list(), stats["opponent_team"].to_list(), strict=True)
    ) == {
        PASSER.player_id: AWAY,
        CATCHER.player_id: AWAY,
        CARRIER.player_id: HOME,
    }


def test_opponent_team_tracks_the_week(pbp: pl.DataFrame) -> None:
    stats = compute_weekly_stats_from_pbp(pbp).filter(pl.col("season_type") == "REG")
    assert stats["opponent_team"].to_list() == [
        league_opponent(row["season"], row["week"], row["recent_team"])
        for row in stats.iter_rows(named=True)
    ]


# ── Season type ──────────────────────────────────────────────────────────────


def test_postseason_plays_score_under_their_own_season_type(pbp: pl.DataFrame) -> None:
    post = compute_weekly_stats_from_pbp(pbp).filter(pl.col("season_type") == "POST")
    assert dict(
        zip(post["player_id"].to_list(), post["fantasy_points_ppr"].to_list(), strict=True)
    ) == pytest.approx(
        {
            league_gsis_id("NYG", "QB"): 5 / 25 + 4,
            league_gsis_id("NYG", "WR"): 5 / 10 + 1 + 6,
        }
    )


def test_a_player_scores_separately_in_each_week_they_play(pbp: pl.DataFrame) -> None:
    receiver = compute_weekly_stats_from_pbp(pbp).filter(
        pl.col("player_id") == league_gsis_id("NYG", "WR")
    )
    assert dict(
        zip(receiver["week"].to_list(), receiver["fantasy_points_ppr"].to_list(), strict=True)
    ) == pytest.approx(
        {
            1: 30 / 10 + 3,
            2: 20 / 10 + 1 + 6,
            POSTSEASON_WEEK: 5 / 10 + 1 + 6,
        }
    )


def test_a_week_of_play_by_play_scores_every_role(pbp: pl.DataFrame) -> None:
    week_one = compute_weekly_stats_from_pbp(pbp).filter(
        (pl.col("recent_team") == "NYG") & (pl.col("week") == 1)
    )
    assert dict(
        zip(
            week_one["player_id"].to_list(),
            week_one["fantasy_points_ppr"].to_list(),
            strict=True,
        )
    ) == pytest.approx(
        {
            league_gsis_id("NYG", "QB"): 50 / 25 + 4 - 2,
            league_gsis_id("NYG", "RB"): 30 / 10 + 6 - 2,
            league_gsis_id("NYG", "WR"): 30 / 10 + 3,
            league_gsis_id("NYG", "TE"): 20 / 10 + 2 + 6,
        }
    )


# ── Position ─────────────────────────────────────────────────────────────────


def test_position_comes_from_the_roster(pbp: pl.DataFrame, rosters: pl.DataFrame) -> None:
    stats = compute_weekly_stats_from_pbp(pbp, rosters)
    listed = {
        league_gsis_id(team, position): position
        for team in LEAGUE_TEAMS
        for position in LEAGUE_POSITIONS
    }
    assert stats["position"].to_list() == [listed[player_id] for player_id in stats["player_id"]]


def test_omitting_rosters_nulls_the_position_column_and_leaves_the_rest(
    pbp: pl.DataFrame, rosters: pl.DataFrame
) -> None:
    with_rosters = compute_weekly_stats_from_pbp(pbp, rosters)
    without_rosters = compute_weekly_stats_from_pbp(pbp, None)
    assert without_rosters.columns == with_rosters.columns
    assert without_rosters.height > 0
    assert without_rosters["position"].null_count() == without_rosters.height
    assert_frame_equal(
        without_rosters.drop("position"),
        with_rosters.drop("position"),
        check_row_order=False,
    )


# ── Fumbles away from scrimmage ──────────────────────────────────────────────


def return_fumble(fumbler: Player, *, play_type: str) -> dict[str, object]:
    """One `play_type` snap that `fumbler` loses on a fumble."""
    return play(
        play_type=play_type,
        fumble_lost=1.0,
        fumbled_1_player_id=fumbler.player_id,
        fumbled_1_player_name=fumbler.name,
    )


@pytest.mark.parametrize("play_type", ["kickoff", "punt"])
def test_a_fumble_lost_away_from_scrimmage_scores_minus_two(play_type: str) -> None:
    stats = compute_weekly_stats_from_pbp(frame([return_fumble(CARRIER, play_type=play_type)]))
    assert rows_for(stats, CARRIER)["fumbles_lost"].item() == 1.0
    assert points_for(stats, CARRIER) == pytest.approx(-2.0)


def test_a_week_charges_fumbles_from_scrimmage_and_from_returns_alike() -> None:
    stats = compute_weekly_stats_from_pbp(
        frame(
            [
                carry(CARRIER, yards=10.0, fumbler=CARRIER),
                return_fumble(CARRIER, play_type="punt"),
            ]
        )
    )
    line = rows_for(stats, CARRIER)
    assert line.height == 1
    assert line["fumbles_lost"].item() == 2.0
    assert points_for(stats, CARRIER) == pytest.approx(10 / 10 - 2 * 2)


# ── Identity across the role name columns ────────────────────────────────────

#: One player id under a different spelling in each role's name column, as nflverse
#: fills the four columns from separate source fields.
ROLE_SPELLINGS = {
    "receiving": "Dee Halvorsen",
    "rushing": "D.Halvorsen",
    "passing": "Dee Halvorsen Jr.",
    "fumbling": "DEE HALVORSEN",
}


def spelled(role: str) -> Player:
    """`ALL_PURPOSE`, under the spelling their `role` name column carries."""
    return Player(ALL_PURPOSE.player_id, ROLE_SPELLINGS[role])


#: A week `ALL_PURPOSE` plays in all four roles, each name column spelling them
#: differently: 25 passing yards, a catch for 10, and a carry for 10 lost on a fumble.
DISAGREEING_NAMES_WEEK = [
    throw(spelled("passing"), CATCHER, yards=25.0),
    throw(PASSER, spelled("receiving"), yards=10.0),
    carry(spelled("rushing"), yards=10.0, fumbler=spelled("fumbling")),
]


def test_a_player_spelled_differently_across_roles_yields_one_line() -> None:
    stats = compute_weekly_stats_from_pbp(frame(DISAGREEING_NAMES_WEEK))
    assert rows_for(stats, ALL_PURPOSE).height == 1


def test_a_line_merged_across_spellings_carries_the_totals_of_every_role() -> None:
    stats = compute_weekly_stats_from_pbp(frame(DISAGREEING_NAMES_WEEK))
    line = rows_for(stats, ALL_PURPOSE).select(STAT_COLUMNS).row(0, named=True)
    assert line == {
        "targets": 1.0,
        "receptions": 1.0,
        "receiving_yards": 10.0,
        "receiving_tds": 0.0,
        "carries": 1.0,
        "rushing_yards": 10.0,
        "rushing_tds": 0.0,
        "passing_yards": 25.0,
        "passing_tds": 0.0,
        "interceptions": 0.0,
        "fumbles_lost": 1.0,
    }


def test_a_line_merged_across_spellings_renders_one_of_them() -> None:
    stats = compute_weekly_stats_from_pbp(frame(DISAGREEING_NAMES_WEEK))
    rendered = rows_for(stats, ALL_PURPOSE)["player_display_name"].item()
    assert rendered in set(ROLE_SPELLINGS.values())


# ── Position, and the engines that read it ───────────────────────────────────


def test_the_position_column_carries_the_roster_dtype_without_rosters(
    pbp: pl.DataFrame, rosters: pl.DataFrame
) -> None:
    with_rosters = compute_weekly_stats_from_pbp(pbp, rosters)
    without_rosters = compute_weekly_stats_from_pbp(pbp, None)
    assert without_rosters.schema["position"] == with_rosters.schema["position"]
    assert without_rosters["position"].null_count() == without_rosters.height


def test_start_sit_reads_a_frame_derived_without_rosters(
    pbp: pl.DataFrame, full_season_schedules: pl.DataFrame
) -> None:
    """A null position matches no offensive position, so the projection is empty."""
    stats = compute_weekly_stats_from_pbp(pbp, None)
    result = compute_start_sit(stats, full_season_schedules, SEASON, PROJECTED_WEEK)
    assert result.schema == START_SIT_SCHEMA
    assert result.height == 0


def test_trade_value_reads_a_frame_derived_without_rosters(
    pbp: pl.DataFrame,
    league_snap_counts: pl.DataFrame,
    full_season_schedules: pl.DataFrame,
    injuries: pl.DataFrame,
    player_ids: pl.DataFrame,
) -> None:
    """A null position matches no offensive position, so the valuation is empty."""
    stats = compute_weekly_stats_from_pbp(pbp, None)
    result = compute_trade_values(
        stats,
        league_snap_counts,
        full_season_schedules,
        injuries,
        player_ids,
        SEASON,
        PROJECTED_WEEK,
    )
    assert result.schema == TRADE_VALUE_SCHEMA
    assert result.height == 0
