"""Tests for the start/sit matchup projection engine.

Expected values are arithmetic on the fixture constants rather than literals. In the
league fixtures a starter scores exactly what the defence across from them concedes,
so a defence's points-allowed average is its ``POINTS_ALLOWED`` entry, the league
average at a position is the mean of that position's four entries, and a player's
baseline is the mean over the defences their schedule put them opposite.

The confidence thresholds need multipliers the league fixtures cannot reach, so
``boundary_weekly_stats`` supplies ten defences averaging ``BOUNDARY_LEAGUE_AVG``
whose points allowed land on each threshold and one point under it.
"""

import polars as pl
import pytest

from ffb.start_sit.engine import (
    MIN_BASELINE_GAMES,
    MIN_DEFENSIVE_GAMES,
    OUTPUT_SCHEMA,
    _defensive_rankings,
    _league_avg_by_position,
    _matchups_for_week,
    _player_baselines,
    compute_start_sit,
)
from tests.conftest import (
    LEAGUE_POSITIONS,
    LEAGUE_TEAMS,
    POINTS_ALLOWED,
    POSTSEASON_WEEK,
    REGULAR_SEASON_WEEKS,
    league_opponent,
    league_player_name,
)

#: Season of the league fixtures that carries no injury absence, so every average
#: reads straight off ``POINTS_ALLOWED``.
SEASON = 2025

#: First week the engine projects: three weeks of history precede it, and both league
#: pairings play it.
FIRST_PROJECTED_WEEK = MIN_DEFENSIVE_GAMES + 1

#: The 2025 bye week, when only NYG and DAL take the field.
BYE_WEEK = 7


# ── Boundary league ──────────────────────────────────────────────────────────

#: Points a defence concedes per game to the one opposing receiver, over ten teams
#: averaging exactly ``BOUNDARY_LEAGUE_AVG``. A multiplier is therefore an entry
#: divided by 100, and the entries pair each confidence threshold with the value one
#: point below it.
BOUNDARY_ALLOWED = {
    "BUF": 120.0,
    "MIA": 119.0,
    "NYJ": 108.0,
    "NE": 107.0,
    "CIN": 92.0,
    "BAL": 91.0,
    "CLE": 80.0,
    "PIT": 79.0,
    "HOU": 102.0,
    "IND": 102.0,
}

BOUNDARY_LEAGUE_AVG = 100.0
BOUNDARY_SEASON = 2025
BOUNDARY_WEEK = MIN_DEFENSIVE_GAMES + 1
BOUNDARY_POSITION = "WR"

#: Who plays whom in ``BOUNDARY_WEEK``, home side first. Each pair puts a team on a
#: threshold defence opposite a team one point under it.
BOUNDARY_PAIRS = (
    ("BUF", "MIA"),
    ("NYJ", "NE"),
    ("CIN", "BAL"),
    ("CLE", "PIT"),
    ("HOU", "IND"),
)


def _boundary_opponent(team: str, week: int) -> str:
    """Defence `team` faces in a baseline week of ``boundary_weekly_stats``."""
    teams = list(BOUNDARY_ALLOWED)
    return teams[(teams.index(team) + week) % len(teams)]


def _boundary_baseline(team: str) -> float:
    """Per-game average of `team`'s receiver across the baseline weeks."""
    faced = [
        BOUNDARY_ALLOWED[_boundary_opponent(team, week)]
        for week in range(1, MIN_DEFENSIVE_GAMES + 1)
    ]
    return sum(faced) / len(faced)


@pytest.fixture()
def boundary_weekly_stats() -> pl.DataFrame:
    """Receiver box scores for the ten ``BOUNDARY_ALLOWED`` teams.

    Shape: one row per team per baseline week, season ``BOUNDARY_SEASON``,
    season_type REG, position ``BOUNDARY_POSITION``. Carries the columns the engine
    reads: player_id, player_display_name, position, recent_team, opponent_team,
    season, week, season_type, fantasy_points_ppr.

    Property: each defence is faced once a week by exactly one receiver scoring its
    ``BOUNDARY_ALLOWED`` entry, so every defence clears ``MIN_DEFENSIVE_GAMES``, every
    points-allowed average equals its entry, and the league average is exactly
    ``BOUNDARY_LEAGUE_AVG``.
    """
    rows = []
    for team in BOUNDARY_ALLOWED:
        for week in range(1, MIN_DEFENSIVE_GAMES + 1):
            opponent = _boundary_opponent(team, week)
            rows.append(
                {
                    "player_id": f"00-{team}",
                    "player_display_name": f"{team} Receiver",
                    "position": BOUNDARY_POSITION,
                    "recent_team": team,
                    "opponent_team": opponent,
                    "season": BOUNDARY_SEASON,
                    "week": week,
                    "season_type": "REG",
                    "fantasy_points_ppr": BOUNDARY_ALLOWED[opponent],
                }
            )
    return pl.DataFrame(rows)


@pytest.fixture()
def boundary_schedules() -> pl.DataFrame:
    """The ``BOUNDARY_PAIRS`` games, all in ``BOUNDARY_WEEK``.

    Shape: one row per pair, season ``BOUNDARY_SEASON``, game_type REG.

    Property: every team in ``BOUNDARY_ALLOWED`` plays once, so each receiver in
    ``boundary_weekly_stats`` draws exactly one projection.
    """
    return pl.DataFrame(
        [
            {
                "game_id": f"{BOUNDARY_SEASON}_{BOUNDARY_WEEK:02d}_{away}_{home}",
                "season": BOUNDARY_SEASON,
                "week": BOUNDARY_WEEK,
                "game_type": "REG",
                "home_team": home,
                "away_team": away,
            }
            for home, away in BOUNDARY_PAIRS
        ]
    )


# ── Three-team league ────────────────────────────────────────────────────────

#: Points each receiver of the three-team fixture scores every week. LAR's defence
#: faces both of the others, so it concedes the sum of the two; SEA's faces only LAR;
#: ARI's faces nobody.
TRIO_RECEIVER_POINTS = {"LAR": 10.0, "SEA": 12.0, "ARI": 8.0}

TRIO_LAR_ALLOWED = TRIO_RECEIVER_POINTS["SEA"] + TRIO_RECEIVER_POINTS["ARI"]
TRIO_SEA_ALLOWED = TRIO_RECEIVER_POINTS["LAR"]
TRIO_SEASON = 2025
TRIO_WEEK = MIN_DEFENSIVE_GAMES + 1


@pytest.fixture()
def trio_weekly_stats() -> pl.DataFrame:
    """Receiver box scores for three teams whose defences carry unequal samples.

    Shape: one row per team per baseline week, season ``TRIO_SEASON``, season_type
    REG, position WR.

    Property: SEA and ARI both face LAR every week and LAR faces SEA, so LAR's defence
    is charged ``TRIO_LAR_ALLOWED`` a week and SEA's ``TRIO_SEA_ALLOWED``, while ARI's
    defence faces nobody and earns no ranking at all.
    """
    schedule = {"LAR": "SEA", "SEA": "LAR", "ARI": "LAR"}
    rows = []
    for team, opponent in schedule.items():
        for week in range(1, MIN_DEFENSIVE_GAMES + 1):
            rows.append(
                {
                    "player_id": f"00-{team}",
                    "player_display_name": f"{team} Receiver",
                    "position": "WR",
                    "recent_team": team,
                    "opponent_team": opponent,
                    "season": TRIO_SEASON,
                    "week": week,
                    "season_type": "REG",
                    "fantasy_points_ppr": TRIO_RECEIVER_POINTS[team],
                }
            )
    return pl.DataFrame(rows)


@pytest.fixture()
def trio_schedules() -> pl.DataFrame:
    """One game in ``TRIO_WEEK``: SEA at ARI.

    Shape: 1 row, season ``TRIO_SEASON``, game_type REG.

    Property: SEA's receiver faces the unranked ARI defence and ARI's receiver faces
    the ranked SEA defence, so one side of the game is projectable and the other is
    not.
    """
    return pl.DataFrame(
        [
            {
                "game_id": f"{TRIO_SEASON}_{TRIO_WEEK:02d}_SEA_ARI",
                "season": TRIO_SEASON,
                "week": TRIO_WEEK,
                "game_type": "REG",
                "home_team": "ARI",
                "away_team": "SEA",
            }
        ]
    )


# ── Expected values from the league fixtures ─────────────────────────────────


def league_avg(position: str) -> float:
    """League-wide points allowed per game at `position`."""
    allowed = POINTS_ALLOWED[position]
    return sum(allowed.values()) / len(allowed)


def baseline(team: str, position: str, through_week: int) -> float:
    """Per-game average of `team`'s starter at `position` through `through_week`."""
    faced = [league_opponent(SEASON, week, team) for week in range(1, through_week + 1)]
    scored = [POINTS_ALLOWED[position][opponent] for opponent in faced if opponent]
    return sum(scored) / len(scored)


def row_for(result: pl.DataFrame, team: str, position: str) -> dict[str, object]:
    """The one projection `result` carries for `team`'s starter at `position`."""
    match = result.filter((pl.col("team") == team) & (pl.col("position") == position))
    assert match.height == 1
    return match.row(0, named=True)


@pytest.fixture()
def projections(
    league_weekly_stats: pl.DataFrame, full_season_schedules: pl.DataFrame
) -> pl.DataFrame:
    """League projections for ``FIRST_PROJECTED_WEEK`` of ``SEASON``."""
    return compute_start_sit(
        league_weekly_stats, full_season_schedules, SEASON, FIRST_PROJECTED_WEEK
    )


# ── Empty results ────────────────────────────────────────────────────────────


@pytest.mark.parametrize(
    "week",
    [MIN_DEFENSIVE_GAMES, POSTSEASON_WEEK],
    ids=["short-history", "no-scheduled-game"],
)
def test_empty_result_carries_the_full_output_schema(
    league_weekly_stats: pl.DataFrame, full_season_schedules: pl.DataFrame, week: int
) -> None:
    """A week the engine declines to project yields a zero-row OUTPUT_SCHEMA frame."""
    result = compute_start_sit(league_weekly_stats, full_season_schedules, SEASON, week)
    assert result.height == 0
    assert result.schema == OUTPUT_SCHEMA


@pytest.mark.parametrize(
    "week",
    [MIN_DEFENSIVE_GAMES, POSTSEASON_WEEK],
    ids=["short-history", "no-scheduled-game"],
)
def test_empty_result_narrows_by_position(
    league_weekly_stats: pl.DataFrame, full_season_schedules: pl.DataFrame, week: int
) -> None:
    """An empty result answers a position filter instead of raising."""
    result = compute_start_sit(league_weekly_stats, full_season_schedules, SEASON, week)
    assert result.filter(pl.col("position") == "WR").height == 0


def test_a_week_short_of_the_baseline_floor_projects_nobody(
    league_weekly_stats: pl.DataFrame, full_season_schedules: pl.DataFrame
) -> None:
    """No week entered with fewer than MIN_BASELINE_GAMES weeks behind it projects."""
    for week in range(1, MIN_BASELINE_GAMES + 1):
        result = compute_start_sit(league_weekly_stats, full_season_schedules, SEASON, week)
        assert result.height == 0
        assert result.schema == OUTPUT_SCHEMA


def test_first_week_with_enough_history_projects(projections: pl.DataFrame) -> None:
    """Three preceding weeks are enough history to project a week."""
    assert projections.height == len(LEAGUE_TEAMS) * len(LEAGUE_POSITIONS)


# ── Multiplier and projection ────────────────────────────────────────────────


def test_defence_allowing_the_league_average_yields_a_unit_multiplier(
    projections: pl.DataFrame,
) -> None:
    """A defence conceding exactly the positional league average scales by 1.0."""
    assert POINTS_ALLOWED["QB"]["DAL"] == league_avg("QB")
    assert row_for(projections, "NYG", "QB")["matchup_mult"] == 1.0


def test_unit_multiplier_projects_the_baseline(projections: pl.DataFrame) -> None:
    """A 1.0 multiplier leaves the projection equal to the player's baseline."""
    row = row_for(projections, "NYG", "QB")
    expected = baseline("NYG", "QB", FIRST_PROJECTED_WEEK - 1)
    assert row["baseline_fpts"] == pytest.approx(expected)
    assert row["projected_fpts"] == pytest.approx(expected)


def test_weak_defence_projects_above_the_baseline(projections: pl.DataFrame) -> None:
    """A defence conceding more than the league average lifts the projection."""
    row = row_for(projections, "PHI", "WR")
    multiplier = POINTS_ALLOWED["WR"]["WAS"] / league_avg("WR")
    expected = baseline("PHI", "WR", FIRST_PROJECTED_WEEK - 1) * multiplier
    assert multiplier > 1.0
    assert row["projected_fpts"] == pytest.approx(expected)
    assert row["projected_fpts"] > row["baseline_fpts"]


def test_strong_defence_projects_below_the_baseline(projections: pl.DataFrame) -> None:
    """A defence conceding less than the league average cuts the projection."""
    row = row_for(projections, "DAL", "WR")
    multiplier = POINTS_ALLOWED["WR"]["NYG"] / league_avg("WR")
    expected = baseline("DAL", "WR", FIRST_PROJECTED_WEEK - 1) * multiplier
    assert multiplier < 1.0
    assert row["projected_fpts"] == pytest.approx(expected)
    assert row["projected_fpts"] < row["baseline_fpts"]


def test_points_allowed_is_the_opponent_defensive_average(
    projections: pl.DataFrame,
) -> None:
    """Every row carries the points its opponent concedes at that position."""
    expected = [
        POINTS_ALLOWED[row["position"]][row["opponent"]]
        for row in projections.iter_rows(named=True)
    ]
    assert projections["fpts_allowed"].to_list() == expected


def test_league_average_is_the_mean_over_every_defence(
    projections: pl.DataFrame,
) -> None:
    """Every row carries the mean points allowed across the whole league."""
    expected = [league_avg(row["position"]) for row in projections.iter_rows(named=True)]
    assert projections["league_avg"].to_list() == pytest.approx(expected)


def test_multiplier_is_points_allowed_over_the_league_average(
    projections: pl.DataFrame,
) -> None:
    """The multiplier divides opponent points allowed by the positional average."""
    expected = [
        POINTS_ALLOWED[row["position"]][row["opponent"]] / league_avg(row["position"])
        for row in projections.iter_rows(named=True)
    ]
    assert projections["matchup_mult"].to_list() == pytest.approx(expected)


def test_projection_is_the_baseline_scaled_by_the_multiplier(
    projections: pl.DataFrame,
) -> None:
    """Every projection is the player's baseline times their matchup multiplier."""
    expected = [
        baseline(row["team"], row["position"], FIRST_PROJECTED_WEEK - 1)
        * POINTS_ALLOWED[row["position"]][row["opponent"]]
        / league_avg(row["position"])
        for row in projections.iter_rows(named=True)
    ]
    assert projections["projected_fpts"].to_list() == pytest.approx(expected)


def test_projections_are_ordered_by_projected_points(
    projections: pl.DataFrame,
) -> None:
    """Rows descend by projected points."""
    values = projections["projected_fpts"].to_list()
    assert values == sorted(values, reverse=True)


# ── Confidence tiers ─────────────────────────────────────────────────────────


@pytest.mark.parametrize(
    ("team", "threshold", "tier"),
    [
        ("MIA", 1.20, "Strong Start"),
        ("NE", 1.08, "Lean Start"),
        ("BAL", 0.92, "Neutral"),
        ("PIT", 0.80, "Lean Sit"),
    ],
)
def test_confidence_tier_at_its_threshold(
    boundary_weekly_stats: pl.DataFrame,
    boundary_schedules: pl.DataFrame,
    team: str,
    threshold: float,
    tier: str,
) -> None:
    """A multiplier landing exactly on a threshold takes the tier above it."""
    result = compute_start_sit(
        boundary_weekly_stats, boundary_schedules, BOUNDARY_SEASON, BOUNDARY_WEEK
    )
    row = row_for(result, team, BOUNDARY_POSITION)
    assert row["matchup_mult"] == threshold
    assert row["confidence"] == tier


@pytest.mark.parametrize(
    ("team", "threshold", "tier"),
    [
        ("BUF", 1.20, "Lean Start"),
        ("NYJ", 1.08, "Neutral"),
        ("CIN", 0.92, "Lean Sit"),
        ("CLE", 0.80, "Strong Sit"),
    ],
)
def test_confidence_tier_below_its_threshold(
    boundary_weekly_stats: pl.DataFrame,
    boundary_schedules: pl.DataFrame,
    team: str,
    threshold: float,
    tier: str,
) -> None:
    """A multiplier one point under a threshold takes the tier below it."""
    result = compute_start_sit(
        boundary_weekly_stats, boundary_schedules, BOUNDARY_SEASON, BOUNDARY_WEEK
    )
    row = row_for(result, team, BOUNDARY_POSITION)
    assert row["matchup_mult"] < threshold
    assert row["confidence"] == tier


# ── Matchups ─────────────────────────────────────────────────────────────────


def test_matchups_for_week_faces_each_team_with_its_opponent(
    full_season_schedules: pl.DataFrame,
) -> None:
    """Each game yields two rows, one per side."""
    matchups = _matchups_for_week(full_season_schedules, SEASON, FIRST_PROJECTED_WEEK)
    expected = {
        (team, league_opponent(SEASON, FIRST_PROJECTED_WEEK, team)) for team in LEAGUE_TEAMS
    }
    assert matchups.height == len(LEAGUE_TEAMS)
    assert set(zip(matchups["team"], matchups["opponent"], strict=True)) == expected


def test_matchups_for_week_omits_a_team_on_its_bye(
    full_season_schedules: pl.DataFrame,
) -> None:
    """A team without a game in the week has no matchup row."""
    matchups = _matchups_for_week(full_season_schedules, SEASON, BYE_WEEK)
    playing = {team for team in LEAGUE_TEAMS if league_opponent(SEASON, BYE_WEEK, team)}
    assert set(matchups["team"]) == playing
    assert matchups.height == len(playing)


def test_matchups_for_week_omits_postseason_games(
    full_season_schedules: pl.DataFrame,
) -> None:
    """A week holding only a postseason game yields no matchups."""
    assert _matchups_for_week(full_season_schedules, SEASON, POSTSEASON_WEEK).is_empty()


def test_teams_on_a_bye_receive_no_projection(
    league_weekly_stats: pl.DataFrame, full_season_schedules: pl.DataFrame
) -> None:
    """Only the starters of teams playing that week are projected."""
    result = compute_start_sit(league_weekly_stats, full_season_schedules, SEASON, BYE_WEEK)
    playing = {team for team in LEAGUE_TEAMS if league_opponent(SEASON, BYE_WEEK, team)}
    assert set(result["team"]) == playing
    assert result.height == len(playing) * len(LEAGUE_POSITIONS)


def test_each_starter_is_projected_once(projections: pl.DataFrame) -> None:
    """One row per starter, so no join multiplies a player across matchups."""
    expected = {
        league_player_name(team, position) for team in LEAGUE_TEAMS for position in LEAGUE_POSITIONS
    }
    assert sorted(projections["player"]) == sorted(expected)


# ── Baselines ────────────────────────────────────────────────────────────────


def test_player_baselines_require_three_games(
    league_weekly_stats: pl.DataFrame,
) -> None:
    """Two games of history yield no baseline; three yield one per starter."""
    short = _player_baselines(league_weekly_stats, SEASON, MIN_DEFENSIVE_GAMES - 1)
    full = _player_baselines(league_weekly_stats, SEASON, MIN_DEFENSIVE_GAMES)
    assert short.height == 0
    assert full.height == len(LEAGUE_TEAMS) * len(LEAGUE_POSITIONS)


@pytest.mark.parametrize("through_week", [3, 4, REGULAR_SEASON_WEEKS])
def test_player_baseline_averages_the_games_inside_the_window(
    league_weekly_stats: pl.DataFrame, through_week: int
) -> None:
    """A baseline is the mean over the games played up to and including the week."""
    baselines = _player_baselines(league_weekly_stats, SEASON, through_week)
    expected = [
        baseline(row["team"], row["position"], through_week)
        for row in baselines.iter_rows(named=True)
    ]
    assert baselines["baseline_fpts"].to_list() == pytest.approx(expected)


def test_player_baselines_exclude_postseason_scoring(
    league_weekly_stats: pl.DataFrame,
) -> None:
    """A postseason line carries no weight in a regular-season baseline."""
    baselines = _player_baselines(league_weekly_stats, SEASON, POSTSEASON_WEEK)
    row = baselines.filter((pl.col("team") == "NYG") & (pl.col("position") == "QB")).row(
        0, named=True
    )
    assert row["baseline_fpts"] == pytest.approx(baseline("NYG", "QB", REGULAR_SEASON_WEEKS))


# ── Defensive rankings ───────────────────────────────────────────────────────


def test_defensive_rankings_require_three_games(
    league_weekly_stats: pl.DataFrame,
) -> None:
    """Two games of history rank no defence; three rank every position of each."""
    short = _defensive_rankings(league_weekly_stats, SEASON, MIN_DEFENSIVE_GAMES - 1)
    full = _defensive_rankings(league_weekly_stats, SEASON, MIN_DEFENSIVE_GAMES)
    assert short.height == 0
    assert full.height == len(LEAGUE_TEAMS) * len(LEAGUE_POSITIONS)


def test_defensive_ranking_is_the_points_conceded_per_game(
    league_weekly_stats: pl.DataFrame,
) -> None:
    """Each ranking is the average the defence concedes at that position."""
    ranks = _defensive_rankings(league_weekly_stats, SEASON, MIN_DEFENSIVE_GAMES)
    expected = [
        POINTS_ALLOWED[row["position"]][row["opponent_team"]] for row in ranks.iter_rows(named=True)
    ]
    assert ranks["fpts_allowed"].to_list() == pytest.approx(expected)


def test_defensive_rankings_exclude_postseason_scoring(
    league_weekly_stats: pl.DataFrame,
) -> None:
    """A postseason line carries no weight in a defensive ranking."""
    ranks = _defensive_rankings(league_weekly_stats, SEASON, POSTSEASON_WEEK)
    row = ranks.filter((pl.col("opponent_team") == "DAL") & (pl.col("position") == "QB")).row(
        0, named=True
    )
    assert row["fpts_allowed"] == pytest.approx(POINTS_ALLOWED["QB"]["DAL"])


def test_defence_is_charged_every_player_it_faces(
    trio_weekly_stats: pl.DataFrame,
) -> None:
    """A defence facing two receivers in a week concedes the sum of the two."""
    ranks = _defensive_rankings(trio_weekly_stats, TRIO_SEASON, MIN_DEFENSIVE_GAMES)
    row = ranks.filter(pl.col("opponent_team") == "LAR").row(0, named=True)
    assert row["fpts_allowed"] == pytest.approx(TRIO_LAR_ALLOWED)


def test_league_average_is_the_mean_of_the_ranked_defences(
    league_weekly_stats: pl.DataFrame,
) -> None:
    """The positional average is the mean over every ranked defence."""
    ranks = _defensive_rankings(league_weekly_stats, SEASON, MIN_DEFENSIVE_GAMES)
    averages = _league_avg_by_position(ranks)
    expected = [league_avg(row["position"]) for row in averages.iter_rows(named=True)]
    assert averages.height == len(LEAGUE_POSITIONS)
    assert averages["league_avg"].to_list() == pytest.approx(expected)


def test_unranked_defence_leaves_its_opponent_unprojected(
    trio_weekly_stats: pl.DataFrame, trio_schedules: pl.DataFrame
) -> None:
    """A player facing a defence with no ranking is dropped, not carried as a null."""
    result = compute_start_sit(trio_weekly_stats, trio_schedules, TRIO_SEASON, TRIO_WEEK)
    assert result["player"].to_list() == ["ARI Receiver"]
    expected = TRIO_RECEIVER_POINTS["ARI"] * (
        TRIO_SEA_ALLOWED / ((TRIO_LAR_ALLOWED + TRIO_SEA_ALLOWED) / 2)
    )
    assert result["projected_fpts"][0] == pytest.approx(expected)
