"""Tests for the trade value engine.

Expected numbers are arithmetic on the league constants in ``tests.conftest``. The
league plays a three-week pairing cycle, so a window covering whole cycles gives each
team one meeting with each of the other three, and a mean over the defenses a player
faces is a mean over ``POINTS_ALLOWED``.
"""

import polars as pl
import pytest

from ffb.trade_value.engine import (
    OUTPUT_SCHEMA,
    REGULAR_SEASON_WEEKS,
    _defensive_strength,
    _health_discount,
    _remaining_schedule_factor,
    _team_bye_weeks,
    compute_trade_values,
)
from tests.conftest import (
    ABSENCE_BONUS,
    ABSENCE_SEASON,
    ABSENCE_TEAM,
    ABSENCE_WEEKS,
    LEAGUE_POSITIONS,
    LEAGUE_SEASONS,
    LEAGUE_TEAMS,
    PAIRINGS,
    POINTS_ALLOWED,
    POSTSEASON_POINTS,
    POSTSEASON_WEEK,
    SEASON_BYES,
    SNAP_SHARE,
    league_gsis_id,
    league_opponent,
    league_player_name,
    ppr_points,
)

#: The league season carrying a bye, and the two teams that take it.
SEASON = 2025
BYE_WEEK, BYE_TEAMS = SEASON_BYES[SEASON]

#: Weeks in one pass through the pairing cycle.
CYCLE_WEEKS = len(PAIRINGS)

#: A week two full cycles into the season, ahead of the bye. Every team has played
#: each opponent twice.
BEFORE_BYE = 2 * CYCLE_WEEKS

#: A week five full cycles into the season, past the bye. One cycle is left to play,
#: so every team has one meeting with each opponent still ahead.
LAST_FULL_CYCLE = 5 * CYCLE_WEEKS

#: The season's earliest and latest weeks that yield values, and the weeks either side.
FIRST_SCORED_WEEK = 3
LAST_SCORED_WEEK = REGULAR_SEASON_WEEKS - 1

#: Every column ``ppr_points`` scores, at zero.
BLANK_LINE = dict.fromkeys(
    (
        "receiving_yards",
        "receiving_tds",
        "receptions",
        "rushing_yards",
        "rushing_tds",
        "passing_yards",
        "passing_tds",
        "interceptions",
        "fumbles_lost",
    ),
    0.0,
)

#: Points the league's one absence adds to the back's weekly line.
ABSENCE_BONUS_POINTS = ppr_points(BLANK_LINE | ABSENCE_BONUS["RB"])


def _opponents(team: str) -> tuple[str, ...]:
    """Every other team in the league."""
    return tuple(other for other in LEAGUE_TEAMS if other != team)


def _mean(values: list[float]) -> float:
    """Arithmetic mean, as polars takes it."""
    return sum(values) / len(values)


def _allowed(position: str, defenses: tuple[str, ...]) -> list[float]:
    """Points each of `defenses` concedes to a starter at `position`."""
    return [POINTS_ALLOWED[position][defense] for defense in defenses]


def _league_average(position: str) -> float:
    """Points the average league defense concedes at `position`."""
    return _mean(list(POINTS_ALLOWED[position].values()))


def _row(frame: pl.DataFrame, player: str) -> dict:
    """The one row `player` holds in a trade value result."""
    matches = frame.filter(pl.col("player") == player)
    assert matches.shape[0] == 1
    return matches.row(0, named=True)


def _injury_reports(rows: list[dict]) -> pl.DataFrame:
    """Injury report frame carrying the columns the health discount reads."""
    return pl.DataFrame(
        rows,
        schema={
            "gsis_id": pl.String,
            "season": pl.Int64,
            "season_type": pl.String,
            "report_status": pl.String,
        },
    )


def _report(gsis_id: str, status: str, season_type: str = "REG", season: int = SEASON) -> dict:
    """One injury report, in the regular season of `SEASON` unless told otherwise."""
    return {
        "gsis_id": gsis_id,
        "season": season,
        "season_type": season_type,
        "report_status": status,
    }


@pytest.fixture()
def league_values(
    league_weekly_stats: pl.DataFrame,
    league_snap_counts: pl.DataFrame,
    full_season_schedules: pl.DataFrame,
    injuries: pl.DataFrame,
    player_ids: pl.DataFrame,
):
    """Callable computing league trade values through a given week of `SEASON`.

    Keyword arguments substitute a modified weekly stats, snap counts or schedule
    frame; the rest of the league inputs stay as the shared fixtures build them.
    """

    def compute(
        current_week: int,
        *,
        weekly_stats: pl.DataFrame | None = None,
        snap_counts: pl.DataFrame | None = None,
        schedules: pl.DataFrame | None = None,
    ) -> pl.DataFrame:
        return compute_trade_values(
            league_weekly_stats if weekly_stats is None else weekly_stats,
            league_snap_counts if snap_counts is None else snap_counts,
            full_season_schedules if schedules is None else schedules,
            injuries,
            player_ids,
            SEASON,
            current_week,
        )

    return compute


# ── Bye weeks ────────────────────────────────────────────────────────────────


def test_bye_week_is_the_week_a_team_sits_out(full_season_schedules: pl.DataFrame) -> None:
    """A team missing one week of the regular season carries that week as its bye."""
    byes = _team_bye_weeks(full_season_schedules, SEASON)
    assert dict(zip(byes["team"], byes["bye_week"], strict=True)) == {
        team: BYE_WEEK for team in BYE_TEAMS
    }


def test_a_team_playing_every_week_carries_no_bye(full_season_schedules: pl.DataFrame) -> None:
    """Teams with a game in all eighteen weeks are absent from the bye table."""
    byes = _team_bye_weeks(full_season_schedules, SEASON)
    playing_every_week = set(LEAGUE_TEAMS) - set(BYE_TEAMS)
    assert playing_every_week.isdisjoint(byes["team"].to_list())


def test_a_season_without_byes_yields_a_typed_empty_bye_table(
    full_season_schedules: pl.DataFrame,
) -> None:
    """A season where every team plays every week yields zero rows, still typed."""
    byes = _team_bye_weeks(full_season_schedules, ABSENCE_SEASON)
    assert byes.shape[0] == 0
    assert byes.schema == pl.Schema({"team": pl.String(), "bye_week": pl.Int64()})


def test_the_earliest_missing_week_is_the_bye_week() -> None:
    """A team missing several weeks carries the first of them as its bye."""
    idle = (5, 9)
    schedules = pl.DataFrame(
        [
            {
                "game_id": f"{SEASON}_{week:02d}_BBB_AAA",
                "season": SEASON,
                "week": week,
                "game_type": "REG",
                "home_team": "AAA",
                "away_team": "BBB",
            }
            for week in range(1, REGULAR_SEASON_WEEKS + 1)
            if week not in idle
        ]
    )
    byes = _team_bye_weeks(schedules, SEASON)
    assert dict(zip(byes["team"], byes["bye_week"], strict=True)) == {
        "AAA": min(idle),
        "BBB": min(idle),
    }


def test_a_postseason_game_does_not_fill_a_bye_week() -> None:
    """Only regular-season games count as a week played."""
    idle = 5
    rows = [
        {
            "game_id": f"{SEASON}_{week:02d}_BBB_AAA",
            "season": SEASON,
            "week": week,
            "game_type": "REG",
            "home_team": "AAA",
            "away_team": "BBB",
        }
        for week in range(1, REGULAR_SEASON_WEEKS + 1)
        if week != idle
    ]
    rows.append(
        {
            "game_id": f"{SEASON}_{idle:02d}_BBB_AAA",
            "season": SEASON,
            "week": idle,
            "game_type": "WC",
            "home_team": "AAA",
            "away_team": "BBB",
        }
    )
    byes = _team_bye_weeks(pl.DataFrame(rows), SEASON)
    assert byes["bye_week"].to_list() == [idle, idle]


# ── Defensive strength ───────────────────────────────────────────────────────


def _row_value(strength: pl.DataFrame, defense: str, position: str) -> float:
    """Points `defense` concedes at `position` in a defensive strength frame."""
    matches = strength.filter(
        (pl.col("opponent_team") == defense) & (pl.col("position") == position)
    )
    assert matches.shape[0] == 1
    return float(matches["fpts_allowed"][0])


def test_defensive_strength_is_the_points_a_defense_concedes_per_game(
    league_weekly_stats: pl.DataFrame,
) -> None:
    """Each defense's average allowed at a position equals what it concedes per game."""
    strength = _defensive_strength(league_weekly_stats, SEASON, BEFORE_BYE)
    allowed = {
        (row["opponent_team"], row["position"]): row["fpts_allowed"]
        for row in strength.iter_rows(named=True)
    }
    assert allowed == {
        (defense, position): POINTS_ALLOWED[position][defense]
        for position in LEAGUE_POSITIONS
        for defense in LEAGUE_TEAMS
    }


def test_defensive_strength_counts_the_cutoff_week(league_weekly_stats: pl.DataFrame) -> None:
    """The window ends on the cutoff week and includes it.

    The first of ``ABSENCE_WEEKS`` is the first week the back on the absent team
    carries the absence bonus, so the defense across from them concedes more at RB in
    that week and in no earlier one.
    """
    bonus_week = min(ABSENCE_WEEKS)
    defense = league_opponent(ABSENCE_SEASON, bonus_week, ABSENCE_TEAM)
    baseline = POINTS_ALLOWED["RB"][defense]

    through_the_bonus = _row_value(
        _defensive_strength(league_weekly_stats, ABSENCE_SEASON, bonus_week), defense, "RB"
    )
    before_the_bonus = _row_value(
        _defensive_strength(league_weekly_stats, ABSENCE_SEASON, bonus_week - 1), defense, "RB"
    )

    assert before_the_bonus == baseline
    assert through_the_bonus == _mean(
        [baseline] * (bonus_week - 1) + [baseline + ABSENCE_BONUS_POINTS]
    )


def test_defensive_strength_excludes_postseason_games(
    league_weekly_stats: pl.DataFrame,
) -> None:
    """Postseason scoring leaves the regular-season average untouched."""
    strength = _defensive_strength(league_weekly_stats, SEASON, POSTSEASON_WEEK)
    assert _row_value(strength, "DAL", "WR") == POINTS_ALLOWED["WR"]["DAL"]


# ── Remaining schedule ───────────────────────────────────────────────────────


def _schedule_factor(
    schedules: pl.DataFrame,
    weekly_stats: pl.DataFrame,
    current_week: int,
    team: str,
    position: str,
) -> float:
    """Remaining schedule factor `team` carries at `position` after `current_week`."""
    strength = _defensive_strength(weekly_stats, SEASON, current_week)
    factors = _remaining_schedule_factor(schedules, strength, SEASON, current_week)
    matches = factors.filter((pl.col("team") == team) & (pl.col("position") == position))
    assert matches.shape[0] == 1
    return float(matches["sched_factor"][0])


def test_remaining_schedule_factor_averages_the_defenses_still_to_face(
    full_season_schedules: pl.DataFrame,
    league_weekly_stats: pl.DataFrame,
) -> None:
    """One cycle of games left gives the mean over the other three defenses."""
    factors = {
        team: _schedule_factor(
            full_season_schedules, league_weekly_stats, LAST_FULL_CYCLE, team, "WR"
        )
        for team in LEAGUE_TEAMS
    }
    assert factors == {team: _mean(_allowed("WR", _opponents(team))) for team in LEAGUE_TEAMS}


def test_remaining_schedule_factor_starts_after_the_current_week(
    full_season_schedules: pl.DataFrame,
    league_weekly_stats: pl.DataFrame,
) -> None:
    """A week counts as remaining while it sits ahead of the current week.

    A full cycle follows week ``LAST_FULL_CYCLE``, so the week before it leaves that
    cycle plus one more meeting — the game week ``LAST_FULL_CYCLE`` itself holds.
    """
    extra = league_opponent(SEASON, LAST_FULL_CYCLE, "NYG")
    factor = _schedule_factor(
        full_season_schedules, league_weekly_stats, LAST_FULL_CYCLE - 1, "NYG", "WR"
    )
    assert factor == _mean(_allowed("WR", (*_opponents("NYG"), extra)))


def test_remaining_schedule_factor_omits_a_bye_from_the_remaining_games(
    full_season_schedules: pl.DataFrame,
    league_weekly_stats: pl.DataFrame,
) -> None:
    """A team meets its bye partner one fewer time over the rest of the season.

    Four cycles follow week ``BEFORE_BYE``, so each pair meets four times, except the
    two teams whose meeting is the bye.
    """
    cycles_left = (REGULAR_SEASON_WEEKS - BEFORE_BYE) // CYCLE_WEEKS
    team, partner = BYE_TEAMS
    meetings = dict.fromkeys(_opponents(team), cycles_left)
    meetings[partner] -= 1
    faced = [POINTS_ALLOWED["WR"][opponent] for opponent, n in meetings.items() for _ in range(n)]

    factor = _schedule_factor(full_season_schedules, league_weekly_stats, BEFORE_BYE, team, "WR")
    assert factor == pytest.approx(_mean(faced))


# ── Health ───────────────────────────────────────────────────────────────────


def _health(frame: pl.DataFrame, gsis_id: str) -> float:
    """Health a player carries in a health discount frame."""
    matches = frame.filter(pl.col("gsis_id") == gsis_id)
    assert matches.shape[0] == 1
    return float(matches["health"][0])


def _weeks_out(injuries: pl.DataFrame, gsis_id: str) -> int:
    """Regular-season weeks a player is reported Out."""
    return injuries.filter(
        (pl.col("gsis_id") == gsis_id)
        & (pl.col("report_status") == "Out")
        & (pl.col("season_type") == "REG")
    ).shape[0]


def test_health_is_the_share_of_regular_season_weeks_available(
    injuries: pl.DataFrame,
) -> None:
    """Weeks reported Out come off a full slate for every season the frame carries."""
    receiver = league_gsis_id(ABSENCE_TEAM, "WR")
    span = len(LEAGUE_SEASONS) * REGULAR_SEASON_WEEKS
    assert _weeks_out(injuries, receiver) == len(ABSENCE_WEEKS)

    health = _health_discount(injuries)
    assert _health(health, receiver) == pytest.approx(1.0 - len(ABSENCE_WEEKS) / span)


def test_health_measures_every_player_against_the_seasons_the_frame_covers(
    injuries: pl.DataFrame,
) -> None:
    """The slate spans every regular season reported, not those one player reports in.

    The WAS quarterback reports inside one season of the two the frame carries, and
    the weeks out still come off both seasons' weeks.
    """
    quarterback = league_gsis_id("WAS", "QB")
    reported = injuries.filter(
        (pl.col("gsis_id") == quarterback) & (pl.col("season_type") == "REG")
    )["season"].n_unique()
    assert reported < len(LEAGUE_SEASONS)

    span = len(LEAGUE_SEASONS) * REGULAR_SEASON_WEEKS
    health = _health_discount(injuries)
    assert _health(health, quarterback) == pytest.approx(
        1.0 - _weeks_out(injuries, quarterback) / span
    )


def test_absences_score_alike_wherever_the_reports_fall() -> None:
    """Two players missing as many weeks as each other carry the same health.

    One player's Out weeks spread across both seasons the frame covers and the
    other's sit inside one of them.
    """
    weeks_out = len(LEAGUE_SEASONS)
    spread = [_report("00-0001", "Out", season=season) for season in LEAGUE_SEASONS]
    concentrated = [_report("00-0002", "Out") for _ in range(weeks_out)]
    assert len(spread) == len(concentrated) == weeks_out

    health = _health_discount(_injury_reports(spread + concentrated))
    assert _health(health, "00-0001") == _health(health, "00-0002")


def test_health_floors_at_half_when_most_of_the_slate_is_missed() -> None:
    """Missing more than half the weeks discounts no further than half."""
    weeks_out = REGULAR_SEASON_WEEKS // 2 + 1
    rows = [_report("00-0001", "Out") for _ in range(weeks_out)]

    health = _health_discount(_injury_reports(rows))
    assert _health(health, "00-0001") == 0.5


def test_health_reaches_the_floor_at_exactly_half_the_weeks_missed(
    injuries: pl.DataFrame,
) -> None:
    """Missing exactly half the weeks lands on the floor rather than past it."""
    tight_end = league_gsis_id("PHI", "TE")
    span = len(LEAGUE_SEASONS) * REGULAR_SEASON_WEEKS
    assert _weeks_out(injuries, tight_end) * 2 == span

    health = _health_discount(injuries)
    assert _health(health, tight_end) == 0.5


def test_a_player_reported_but_never_out_is_absent_from_the_health_table(
    injuries: pl.DataFrame,
) -> None:
    """Reports short of Out carry no discount at all."""
    health = _health_discount(injuries)
    assert league_gsis_id("DAL", "RB") not in health["gsis_id"].to_list()


def test_health_ignores_reports_outside_the_regular_season() -> None:
    """Postseason Out reports leave the discount where the regular season put it."""
    rows = [_report("00-0001", "Out")]
    rows += [_report("00-0001", "Out", season_type="POST") for _ in range(5)]
    health = _health_discount(_injury_reports(rows))
    assert _health(health, "00-0001") == pytest.approx(1.0 - 1 / REGULAR_SEASON_WEEKS)


def test_health_counts_only_weeks_reported_out() -> None:
    """A week reported anything short of Out is a week available."""
    weeks_out = 2
    rows = [_report("00-0001", "Out") for _ in range(weeks_out)]
    rows += [_report("00-0001", "Questionable") for _ in range(6)]
    health = _health_discount(_injury_reports(rows))
    assert _health(health, "00-0001") == pytest.approx(1.0 - weeks_out / REGULAR_SEASON_WEEKS)


def test_a_season_reported_only_in_the_postseason_widens_no_slate() -> None:
    """A season carrying no regular-season report adds no weeks to the slate."""
    weeks_out = 2
    rows = [_report("00-0001", "Out") for _ in range(weeks_out)]
    rows.append(_report("00-0002", "Out", season_type="POST", season=SEASON - 1))

    health = _health_discount(_injury_reports(rows))
    assert _health(health, "00-0001") == pytest.approx(1.0 - weeks_out / REGULAR_SEASON_WEEKS)


def test_a_frame_without_regular_season_reports_discounts_nobody() -> None:
    """Postseason reports alone put no player in the discount table."""
    rows = [_report("00-0001", "Out", season_type="POST")]

    health = _health_discount(_injury_reports(rows))
    assert health.is_empty()
    assert health.columns == ["gsis_id", "health"]


# ── Guard paths ──────────────────────────────────────────────────────────────


def test_a_season_with_no_weeks_left_returns_the_declared_schema(league_values) -> None:
    """The last week of the regular season leaves nothing to trade for."""
    result = league_values(REGULAR_SEASON_WEEKS)
    assert result.shape[0] == 0
    assert result.schema == OUTPUT_SCHEMA


def test_the_last_week_holding_a_game_still_scores_players(league_values) -> None:
    """One week left to play is enough to carry a value."""
    result = league_values(LAST_SCORED_WEEK)
    assert result.shape[0] == len(LEAGUE_TEAMS) * len(LEAGUE_POSITIONS)


def test_a_week_below_the_minimum_returns_the_declared_schema(league_values) -> None:
    """Two weeks of results are too few to score from."""
    result = league_values(FIRST_SCORED_WEEK - 1)
    assert result.shape[0] == 0
    assert result.schema == OUTPUT_SCHEMA


def test_the_minimum_week_scores_players(league_values) -> None:
    """Three weeks of results are enough to score from."""
    result = league_values(FIRST_SCORED_WEEK)
    assert result.shape[0] == len(LEAGUE_TEAMS) * len(LEAGUE_POSITIONS)


def test_an_empty_result_filters_by_position(league_values) -> None:
    """A result with no rows answers a position filter with no rows."""
    filtered = league_values(REGULAR_SEASON_WEEKS).filter(pl.col("position") == "WR")
    assert filtered.shape[0] == 0
    assert filtered.schema == OUTPUT_SCHEMA


def test_a_season_without_results_returns_the_declared_schema(
    league_values, league_weekly_stats: pl.DataFrame
) -> None:
    """A season carrying no weekly results scores nobody."""
    elsewhere = league_weekly_stats.filter(pl.col("season") != SEASON)
    result = league_values(LAST_FULL_CYCLE, weekly_stats=elsewhere)
    assert result.shape[0] == 0
    assert result.schema == OUTPUT_SCHEMA


def test_a_scored_result_carries_the_declared_schema(league_values) -> None:
    """Column order and dtypes match the declaration a caller indexes by."""
    assert league_values(LAST_FULL_CYCLE).schema == OUTPUT_SCHEMA


# ── Production and byes ──────────────────────────────────────────────────────


def test_a_bye_still_ahead_shortens_production_for_an_equal_scorer(league_values) -> None:
    """Two players scoring the same per game part on the games they have left.

    Through week ``BEFORE_BYE`` the DAL receiver and the WAS quarterback average the
    same points, and of the two teams only WAS has a bye ahead of it.
    """
    assert BYE_WEEK > BEFORE_BYE
    assert "WAS" in BYE_TEAMS
    assert "DAL" not in BYE_TEAMS

    ppg = _mean(_allowed("WR", _opponents("DAL")))
    assert ppg == _mean(_allowed("QB", _opponents("WAS")))

    result = league_values(BEFORE_BYE)
    weeks_left = REGULAR_SEASON_WEEKS - BEFORE_BYE
    playing_through = _row(result, league_player_name("DAL", "WR"))
    taking_a_bye = _row(result, league_player_name("WAS", "QB"))

    assert playing_through["ppg"] == ppg
    assert taking_a_bye["ppg"] == ppg
    assert playing_through["production_raw"] == ppg * weeks_left
    assert taking_a_bye["production_raw"] == ppg * (weeks_left - 1)


def test_a_bye_in_the_current_week_leaves_the_games_left_whole(league_values) -> None:
    """A bye is subtracted only while it is still ahead of the current week."""
    result = league_values(BYE_WEEK)
    weeks_left = REGULAR_SEASON_WEEKS - BYE_WEEK
    assert result["production_raw"].to_list() == pytest.approx(
        (result["ppg"] * weeks_left).to_list()
    )


def test_the_bye_week_of_a_players_team_is_published(league_values) -> None:
    """Teams holding a bye carry its week; teams without one carry none."""
    result = league_values(BEFORE_BYE)
    published = {(row["team"], row["bye_week"]) for row in result.iter_rows(named=True)}
    assert published == {(team, BYE_WEEK if team in BYE_TEAMS else None) for team in LEAGUE_TEAMS}


# ── Composite ────────────────────────────────────────────────────────────────


def test_trade_value_reconstructs_from_the_published_components(league_values) -> None:
    """Production weighs 0.50, schedule 0.20, usage 0.20 and health 0.10.

    The four published components determine the value on their own: production
    normalized by the field maximum, the schedule multiplier spread across its own
    range, snap share as a fraction of a hundred snaps, and health as it stands. The
    composite is rescaled so the leader reads 100, and every value is published to one
    decimal, so the reconstruction holds to half of that digit.
    """
    result = league_values(LAST_FULL_CYCLE)

    production = result["production_raw"] / result["production_raw"].max()
    floor = result["sched_mult"].min()
    schedule = (result["sched_mult"] - floor) / (result["sched_mult"].max() - floor + 1e-9)
    usage = result["avg_snap_pct"] / 100
    composite = production * 0.50 + schedule * 0.20 + usage * 0.20 + result["health"] * 0.10
    expected = composite / composite.max() * 100

    assert result["trade_value"].to_list() == pytest.approx(expected.to_list(), abs=0.05)


def test_the_most_valuable_player_scores_one_hundred(league_values) -> None:
    """The scale tops out at 100."""
    assert league_values(LAST_FULL_CYCLE)["trade_value"].max() == 100.0


def test_values_descend_down_the_result(league_values) -> None:
    """Rows arrive ordered by trade value, most valuable first."""
    values = league_values(LAST_FULL_CYCLE)["trade_value"].to_list()
    assert values == sorted(values, reverse=True)


def test_the_schedule_multiplier_measures_the_remaining_schedule_against_the_league(
    league_values,
) -> None:
    """A multiplier above one marks a remaining schedule easier than the league's."""
    result = league_values(LAST_FULL_CYCLE).filter(pl.col("position") == "WR").sort("team")
    assert result["team"].to_list() == sorted(LEAGUE_TEAMS)
    assert result["sched_mult"].to_list() == pytest.approx(
        [
            _mean(_allowed("WR", _opponents(team))) / _league_average("WR")
            for team in sorted(LEAGUE_TEAMS)
        ]
    )


def test_a_schedule_without_games_leaves_the_multiplier_neutral(
    league_values, full_season_schedules: pl.DataFrame
) -> None:
    """A team with no opponent to measure carries a multiplier of one."""
    result = league_values(LAST_FULL_CYCLE, schedules=full_season_schedules.head(0))
    assert result["sched_mult"].to_list() == [1.0] * result.shape[0]


def test_usage_is_the_average_snap_share_in_points(league_values) -> None:
    """Snap share published as a fraction reaches the result on a hundred-point scale."""
    result = league_values(LAST_FULL_CYCLE).sort("position", "team")
    assert result["avg_snap_pct"].to_list() == pytest.approx(
        [SNAP_SHARE[position] * 100 for position in sorted(LEAGUE_POSITIONS) for _ in LEAGUE_TEAMS]
    )


def test_snap_share_published_in_points_scores_the_same(
    league_values, league_snap_counts: pl.DataFrame
) -> None:
    """A snap share of 0.8 and one of 80 describe the same usage."""
    in_points = league_snap_counts.with_columns(pl.col("offense_pct") * 100)
    assert league_values(LAST_FULL_CYCLE, snap_counts=in_points).equals(
        league_values(LAST_FULL_CYCLE)
    )


def test_a_player_without_snap_rows_takes_the_default_share(
    league_values, league_snap_counts: pl.DataFrame
) -> None:
    """A player absent from the snap counts is scored at half the offense's snaps."""
    without_receiver = league_snap_counts.filter(
        ~((pl.col("team") == "NYG") & (pl.col("position") == "WR"))
    )
    result = league_values(LAST_FULL_CYCLE, snap_counts=without_receiver)
    assert _row(result, league_player_name("NYG", "WR"))["avg_snap_pct"] == 50.0


def test_a_player_without_injury_reports_is_fully_healthy(league_values) -> None:
    """A player carrying no injury report is scored at full health."""
    result = league_values(LAST_FULL_CYCLE)
    assert _row(result, league_player_name("NYG", "QB"))["health"] == 1.0


def test_three_games_played_is_the_fewest_that_scores(
    league_values, league_weekly_stats: pl.DataFrame
) -> None:
    """Three games earn a value and two do not."""
    played_twice = (pl.col("recent_team") == "NYG") & (pl.col("week") >= FIRST_SCORED_WEEK)
    played_three_times = (pl.col("recent_team") == "DAL") & (pl.col("week") > FIRST_SCORED_WEEK)
    thinned = league_weekly_stats.filter(
        ~(
            (pl.col("season") == SEASON)
            & (pl.col("week") <= BEFORE_BYE)
            & (pl.col("position") == "WR")
            & (played_twice | played_three_times)
        )
    )
    scored = thinned.filter(
        (pl.col("season") == SEASON) & (pl.col("week") <= BEFORE_BYE) & (pl.col("position") == "WR")
    )
    counted = scored.group_by("recent_team").len()
    games = dict(zip(counted["recent_team"], counted["len"], strict=True))
    assert games["NYG"] == FIRST_SCORED_WEEK - 1
    assert games["DAL"] == FIRST_SCORED_WEEK

    result = league_values(BEFORE_BYE, weekly_stats=thinned)
    receivers = result.filter(pl.col("position") == "WR")["player"].to_list()
    assert league_player_name("NYG", "WR") not in receivers
    assert _row(result, league_player_name("DAL", "WR"))["games_played"] == FIRST_SCORED_WEEK


def test_postseason_scoring_stays_out_of_the_player_baseline(
    league_values, league_weekly_stats: pl.DataFrame
) -> None:
    """A postseason line neither raises a player's average nor counts as a game."""
    postseason = league_weekly_stats.filter(
        (pl.col("season") == SEASON)
        & (pl.col("week") == BEFORE_BYE - 1)
        & (pl.col("recent_team") == "NYG")
        & (pl.col("position") == "WR")
    ).with_columns(
        pl.lit("POST").alias("season_type"),
        pl.lit(POSTSEASON_POINTS).alias("fantasy_points_ppr"),
    )

    result = league_values(BEFORE_BYE, weekly_stats=pl.concat([league_weekly_stats, postseason]))
    receiver = _row(result, league_player_name("NYG", "WR"))
    assert receiver["ppg"] == _mean(_allowed("WR", _opponents("NYG")))
    assert receiver["games_played"] == BEFORE_BYE


def test_only_offensive_positions_are_scored(
    league_values, league_weekly_stats: pl.DataFrame
) -> None:
    """A player off the offensive skill positions earns no value."""
    kicker = league_weekly_stats.filter(
        (pl.col("season") == SEASON)
        & (pl.col("recent_team") == "NYG")
        & (pl.col("position") == "WR")
    ).with_columns(
        pl.lit("K").alias("position"),
        pl.lit("NYG Kicker").alias("player_display_name"),
        pl.lit("00-0105").alias("player_id"),
    )

    result = league_values(BEFORE_BYE, weekly_stats=pl.concat([league_weekly_stats, kicker]))
    assert set(result["position"].to_list()) == set(LEAGUE_POSITIONS)
