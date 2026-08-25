"""Shared test fixtures — small synthetic DataFrames for engine tests.

Three groups of fixtures live here.

**The pair fixtures** — ``snap_counts``, ``weekly_stats``, ``schedules`` — describe
two players on one team over six weeks. They are the smallest input the snap-share
and waiver-wire engines accept.

**The league fixtures** — ``league_snap_counts``, ``league_weekly_stats``,
``full_season_schedules``, ``pbp``, ``rosters``, ``injuries`` — describe a four-team
league, ``LEAGUE_TEAMS``, over the ``LEAGUE_SEASONS`` regular seasons, with one
starter per team at each of QB, RB, WR and TE.

**The focused series** — ``colliding_name_snaps``, ``colliding_name_weekly_stats``,
``breakout_snaps``, ``breakout_snaps_points_scale`` — are single-property inputs for
the windowing engines.

``player_ids`` is the one crosswalk covering every player in this file, so it pairs
with any snap fixture here.

Every number in the league fixtures is derived from a module constant, so an expected
value is arithmetic on a constant rather than a literal copied out of a run:

* ``PAIRINGS`` fixes the schedule. Week ``w`` plays ``PAIRINGS[(w - 1) % 3]``, so
  across any three consecutive weeks holding no bye, each team faces each of the
  other three exactly once.
* ``POINTS_ALLOWED[position][defense]`` is what a defense concedes, per game, to the
  one opposing starter at that position. A player scores exactly what the defense
  across from them allows, so a defense's points-allowed average equals its entry and
  a player's per-game average is the mean of the entries for the defenses they faced.
* ``SEASON_BYES`` sits one pairing out of one week. Season 2025 byes PHI and WAS in
  week 7 and plays NYG and DAL all 18; season 2024 byes nobody.
* ``SNAP_SHARE[position]`` is the offensive snap share every starter at that position
  takes, as a 0–1 fraction.
* ``ABSENCE_SEASON`` carries the league's one injury absence. Season 2025 carries
  none, so pass ``season=2025`` to any engine taking a season and every average reads
  straight off ``POINTS_ALLOWED``.

Identifiers are derived too: ``league_gsis_id``, ``league_pfr_id`` and
``league_player_name`` map a (team, position) pair to the ids and display name the
fixtures carry, and ``league_opponent`` and ``league_game_id`` answer who a team
played in a given week and under which game id.
"""

import polars as pl
import pytest

# ── The synthetic league ─────────────────────────────────────────────────────

LEAGUE_TEAMS = ("NYG", "DAL", "PHI", "WAS")
LEAGUE_POSITIONS = ("QB", "RB", "WR", "TE")
LEAGUE_SEASONS = (2024, 2025)

#: Weeks in a regular season, matching ``ffb.trade_value.engine.REGULAR_SEASON_WEEKS``.
REGULAR_SEASON_WEEKS = 18

#: Weekly pairings, cycled across the season: week ``w`` plays ``PAIRINGS[(w - 1) % 3]``.
#: Three consecutive weeks give every team one game against every other team.
PAIRINGS = (
    (("NYG", "DAL"), ("PHI", "WAS")),
    (("NYG", "PHI"), ("DAL", "WAS")),
    (("NYG", "WAS"), ("DAL", "PHI")),
)

#: Season → (bye week, the pairing that sits it out). A season absent from this map
#: plays every team in every week and so has no bye at all.
SEASON_BYES: dict[int, tuple[int, tuple[str, str]]] = {2025: (7, ("PHI", "WAS"))}

#: Fantasy points a defense concedes per game to the one opposing starter at a
#: position. Each position's four values are multiples of 3, so every mean over
#: three defenses is a whole number, and each set sums to an even number, so the
#: four-defense league average lands on a whole number or a half.
POINTS_ALLOWED = {
    "QB": {"NYG": 9.0, "DAL": 18.0, "PHI": 21.0, "WAS": 24.0},
    "RB": {"NYG": 12.0, "DAL": 9.0, "PHI": 6.0, "WAS": 3.0},
    "WR": {"NYG": 6.0, "DAL": 12.0, "PHI": 18.0, "WAS": 24.0},
    "TE": {"NYG": 9.0, "DAL": 12.0, "PHI": 15.0, "WAS": 18.0},
}

#: Offensive snap share each starter takes, as the 0–1 fraction nflverse publishes.
SNAP_SHARE = {"QB": 1.00, "RB": 0.60, "WR": 0.80, "TE": 0.40}

#: Points every postseason row is worth. Above every ``POINTS_ALLOWED`` value, so a
#: computation that drops the season-type filter reads visibly wrong.
POSTSEASON_POINTS = 30.0

#: The postseason week ``league_weekly_stats``, ``league_snap_counts`` and
#: ``full_season_schedules`` carry beyond the regular season.
POSTSEASON_WEEK = 19

# ── The league's one absence ─────────────────────────────────────────────────

ABSENCE_SEASON = 2024
ABSENCE_TEAM = "NYG"
ABSENCE_POSITION = "WR"

#: Weeks the absent starter sits out. Nine weeks, and the weeks played and the weeks
#: missed each face DAL, PHI and WAS three times, so both sides of an active/inactive
#: split average the same opponents and a teammate's delta is exactly their bonus.
ABSENCE_WEEKS = frozenset(range(4, 13))

#: What each remaining starter adds to their box score while the receiver is out.
#: Fantasy points follow from the box score, so the deltas are +6.0 for the back,
#: +3.0 for the tight end and -1.0 for the quarterback.
ABSENCE_BONUS = {
    "QB": {"passing_yards": -25.0},
    "RB": {"targets": 4.0, "carries": 5.0, "rushing_yards": 60.0},
    "TE": {"targets": 2.0, "receptions": 1.0, "receiving_yards": 20.0},
}

# ── Colliding display names ──────────────────────────────────────────────────

#: One display name worn by two players, on different teams and at different
#: positions, whose snap shares mirror each other week for week.
COLLIDING_NAME = "Casey Rivers"

COLLIDING_RISING_PFR_ID = "RIVE01"
COLLIDING_RISING_GSIS_ID = "00-0501"
COLLIDING_RISING_TEAM = "NYG"
COLLIDING_RISING_POSITION = "WR"
COLLIDING_RISING_PCT = (0.30, 0.40, 0.50, 0.60, 0.70, 0.80)

COLLIDING_FALLING_PFR_ID = "RIVE02"
COLLIDING_FALLING_GSIS_ID = "00-0502"
COLLIDING_FALLING_TEAM = "DAL"
COLLIDING_FALLING_POSITION = "RB"
COLLIDING_FALLING_PCT = (0.80, 0.70, 0.60, 0.50, 0.40, 0.30)

# ── Breakout series ──────────────────────────────────────────────────────────

BREAKOUT_PFR_ID = "BRKO01"
BREAKOUT_GSIS_ID = "00-0601"
BREAKOUT_NAME = "Devon Ascent"
BREAKOUT_TEAM = "PHI"
BREAKOUT_POSITION = "WR"

#: Eight weeks of offensive snap share that carry a three-week window through every
#: trend class: a rise, a crossing from under 50% to over 60%, a flat stretch,
#: another rise, and a two-week decline.
BREAKOUT_PCT = (0.40, 0.45, 0.48, 0.70, 0.55, 0.75, 0.50, 0.40)

# ── Identities ───────────────────────────────────────────────────────────────

_TEAM_SLOT = {"NYG": "1", "DAL": "2", "PHI": "3", "WAS": "4"}
_POSITION_SLOT = {"QB": "1", "RB": "2", "WR": "3", "TE": "4"}
_POSITION_NOUN = {"QB": "Quarterback", "RB": "Runner", "WR": "Receiver", "TE": "Tight End"}


def league_gsis_id(team: str, position: str) -> str:
    """GSIS id of the starter `team` fields at `position`."""
    return f"00-0{_TEAM_SLOT[team]}0{_POSITION_SLOT[position]}"


def league_pfr_id(team: str, position: str) -> str:
    """Pro-Football-Reference id of the starter `team` fields at `position`."""
    return f"{team}{_POSITION_SLOT[position]}"


def league_player_name(team: str, position: str) -> str:
    """Display name of the starter `team` fields at `position`."""
    return f"{team} {_POSITION_NOUN[position]}"


# ── Schedule ─────────────────────────────────────────────────────────────────


def _league_games(season: int) -> list[tuple[int, str, str]]:
    """Every regular-season game of `season` as (week, home team, away team).

    The home side alternates with the parity of the week, so both the home and away
    columns carry each team.
    """
    bye = SEASON_BYES.get(season)
    games = []
    for week in range(1, REGULAR_SEASON_WEEKS + 1):
        for pair in PAIRINGS[(week - 1) % len(PAIRINGS)]:
            if bye is not None and week == bye[0] and pair == bye[1]:
                continue
            first, second = pair
            home, away = (first, second) if week % 2 else (second, first)
            games.append((week, home, away))
    return games


def league_opponent(season: int, week: int, team: str) -> str | None:
    """Team `team` faces in `week`, or None when `week` is their bye."""
    for game_week, home, away in _league_games(season):
        if game_week != week:
            continue
        if team == home:
            return away
        if team == away:
            return home
    return None


def league_game_id(season: int, week: int, team: str) -> str:
    """nflverse game id of the game `team` played in `week`."""
    for game_week, home, away in _league_games(season):
        if game_week == week and team in (home, away):
            return f"{season}_{week:02d}_{away}_{home}"
    raise KeyError(f"{team} played no game in {season} week {week}")


# ── Box scores ───────────────────────────────────────────────────────────────

_STAT_COLUMNS = (
    "targets",
    "carries",
    "receptions",
    "receiving_yards",
    "receiving_tds",
    "rushing_yards",
    "rushing_tds",
    "passing_yards",
    "passing_tds",
    "interceptions",
    "fumbles_lost",
)


def _stat_line(position: str, points: float) -> dict[str, float]:
    """Box score for one game at `position` worth exactly `points` PPR points."""
    line = dict.fromkeys(_STAT_COLUMNS, 0.0)
    if position == "QB":
        line["passing_tds"] = 1.0
        line["passing_yards"] = (points - 4.0) * 25.0
    elif position == "RB":
        line["targets"] = 2.0
        line["carries"] = 20.0
        line["receptions"] = 1.0
        line["rushing_yards"] = (points - 1.0) * 10.0
    elif position == "WR":
        line["targets"] = 10.0
        line["receptions"] = 5.0
        line["receiving_yards"] = (points - 5.0) * 10.0
    else:
        line["targets"] = 6.0
        line["receptions"] = 4.0
        line["receiving_yards"] = (points - 4.0) * 10.0
    return line


def ppr_points(line: dict[str, float]) -> float:
    """PPR fantasy points of a box score, scored as `ffb.data.stats` scores one."""
    return (
        line["receiving_yards"] / 10
        + line["receiving_tds"] * 6
        + line["receptions"]
        + line["rushing_yards"] / 10
        + line["rushing_tds"] * 6
        + line["passing_yards"] / 25
        + line["passing_tds"] * 4
        - line["interceptions"] * 2
        - line["fumbles_lost"] * 2
    )


# ── Pair fixtures ────────────────────────────────────────────────────────────


@pytest.fixture()
def snap_counts() -> pl.DataFrame:
    """Minimal snap counts: 2 players, 6 weeks, 1 team.

    Shape: 12 rows, one per player per week, season 2025, game_type REG,
    ``offense_pct`` as a 0–1 fraction.

    Property: Alpha Player's share climbs five points every week and Beta Player's
    holds flat, so a rolling window separates a rising series from a stable one.
    """
    rows = []
    for week in range(1, 7):
        rows.append(
            {
                "pfr_player_id": "AAA01",
                "player": "Alpha Player",
                "position": "WR",
                "team": "NYG",
                "opponent": "DAL",
                "season": 2025,
                "week": week,
                "game_type": "REG",
                "offense_snaps": 50.0 + week * 5,
                "offense_pct": 0.50 + week * 0.05,
                "defense_snaps": 0.0,
                "defense_pct": 0.0,
                "st_snaps": 0.0,
                "st_pct": 0.0,
                "game_id": f"2025_{week:02d}_NYG_DAL",
                "pfr_game_id": f"202509{week:02d}dal",
            }
        )
        rows.append(
            {
                "pfr_player_id": "BBB02",
                "player": "Beta Player",
                "position": "RB",
                "team": "NYG",
                "opponent": "DAL",
                "season": 2025,
                "week": week,
                "game_type": "REG",
                "offense_snaps": 40.0,
                "offense_pct": 0.40,
                "defense_snaps": 0.0,
                "defense_pct": 0.0,
                "st_snaps": 0.0,
                "st_pct": 0.0,
                "game_id": f"2025_{week:02d}_NYG_DAL",
                "pfr_game_id": f"202509{week:02d}dal",
            }
        )
    return pl.DataFrame(rows)


@pytest.fixture()
def weekly_stats() -> pl.DataFrame:
    """Minimal weekly stats matching snap_counts players.

    Shape: 12 rows, one per player per week, season 2025, season_type REG, every
    week against DAL.

    Property: Alpha Player's points and targets climb every week while Beta Player's
    hold flat, so the two carry the same trends the snap shares do.
    """
    rows = []
    for week in range(1, 7):
        rows.append(
            {
                "player_id": "00-001",
                "player_display_name": "Alpha Player",
                "position": "WR",
                "recent_team": "NYG",
                "opponent_team": "DAL",
                "season": 2025,
                "week": week,
                "season_type": "REG",
                "fantasy_points_ppr": 12.0 + week * 2.0,
                "targets": 6.0 + week,
                "carries": 0.0,
                "receptions": 4.0,
                "receiving_yards": 60.0,
                "receiving_tds": 0.0,
                "rushing_yards": 0.0,
                "rushing_tds": 0.0,
                "passing_yards": 0.0,
                "passing_tds": 0.0,
                "interceptions": 0.0,
                "fumbles_lost": 0.0,
            }
        )
        rows.append(
            {
                "player_id": "00-002",
                "player_display_name": "Beta Player",
                "position": "RB",
                "recent_team": "NYG",
                "opponent_team": "DAL",
                "season": 2025,
                "week": week,
                "season_type": "REG",
                "fantasy_points_ppr": 10.0,
                "targets": 2.0,
                "carries": 15.0,
                "receptions": 1.0,
                "receiving_yards": 10.0,
                "receiving_tds": 0.0,
                "rushing_yards": 70.0,
                "rushing_tds": 0.0,
                "passing_yards": 0.0,
                "passing_tds": 0.0,
                "interceptions": 0.0,
                "fumbles_lost": 0.0,
            }
        )
    return pl.DataFrame(rows)


@pytest.fixture()
def schedules() -> pl.DataFrame:
    """Minimal schedule: NYG vs DAL for 6 weeks.

    Shape: 6 rows, season 2025, game_type REG, NYG away at DAL every week.

    Property: every week of the pair fixtures has a game, so a week-keyed matchup
    lookup resolves for any of the six.
    """
    rows = []
    for week in range(1, 7):
        rows.append(
            {
                "game_id": f"2025_{week:02d}_NYG_DAL",
                "season": 2025,
                "week": week,
                "game_type": "REG",
                "home_team": "DAL",
                "away_team": "NYG",
            }
        )
    return pl.DataFrame(rows)


# ── Crosswalk ────────────────────────────────────────────────────────────────


@pytest.fixture()
def player_ids() -> pl.DataFrame:
    """Crosswalk mapping pfr_id → gsis_id for every player in this file.

    Shape: one row per player across the pair fixtures, the league fixtures, the
    colliding-name pair and the breakout series, plus two rows a crosswalk build has
    to reduce — a null pfr_id, and one pfr_id claimed twice where the later row wins.

    Property: any snap fixture here joins against this one frame, and the two players
    sharing ``COLLIDING_NAME`` map to distinct GSIS ids.
    """
    rows: list[dict[str, str | None]] = [
        {"pfr_id": "AAA01", "gsis_id": "00-001", "name": "Alpha Player", "position": "WR"},
        {"pfr_id": "BBB02", "gsis_id": "00-002", "name": "Beta Player", "position": "RB"},
    ]
    for team in LEAGUE_TEAMS:
        for position in LEAGUE_POSITIONS:
            rows.append(
                {
                    "pfr_id": league_pfr_id(team, position),
                    "gsis_id": league_gsis_id(team, position),
                    "name": league_player_name(team, position),
                    "position": position,
                }
            )
    rows.extend(
        [
            {
                "pfr_id": COLLIDING_RISING_PFR_ID,
                "gsis_id": COLLIDING_RISING_GSIS_ID,
                "name": COLLIDING_NAME,
                "position": COLLIDING_RISING_POSITION,
            },
            {
                "pfr_id": COLLIDING_FALLING_PFR_ID,
                "gsis_id": COLLIDING_FALLING_GSIS_ID,
                "name": COLLIDING_NAME,
                "position": COLLIDING_FALLING_POSITION,
            },
            {
                "pfr_id": BREAKOUT_PFR_ID,
                "gsis_id": BREAKOUT_GSIS_ID,
                "name": BREAKOUT_NAME,
                "position": BREAKOUT_POSITION,
            },
            {"pfr_id": None, "gsis_id": "00-0801", "name": "Unlinked Player", "position": "TE"},
            {"pfr_id": "DUPE1", "gsis_id": "00-0802", "name": "Twice Listed", "position": "TE"},
            {"pfr_id": "DUPE1", "gsis_id": "00-0803", "name": "Twice Listed", "position": "TE"},
        ]
    )
    return pl.DataFrame(rows)


# ── League fixtures ──────────────────────────────────────────────────────────


def _absence_applies(season: int, team: str, week: int) -> bool:
    """Whether the league's one absence covers this team-week."""
    return season == ABSENCE_SEASON and team == ABSENCE_TEAM and week in ABSENCE_WEEKS


@pytest.fixture()
def full_season_schedules() -> pl.DataFrame:
    """Every game of both league seasons, plus one postseason game.

    Shape: 36 regular-season rows for 2024, 35 for 2025, one WC row for 2025 in week
    ``POSTSEASON_WEEK``. Columns: game_id, season, week, game_type, home_team,
    away_team.

    Property: PHI and WAS sit out week 7 of 2025 and NYG and DAL play all 18, so one
    season yields a bye for half the league; 2024 yields none at all. The postseason
    row is the only non-REG game_type, so it is what a regular-season filter drops.
    """
    rows = []
    for season in LEAGUE_SEASONS:
        for week, home, away in _league_games(season):
            rows.append(
                {
                    "game_id": f"{season}_{week:02d}_{away}_{home}",
                    "season": season,
                    "week": week,
                    "game_type": "REG",
                    "home_team": home,
                    "away_team": away,
                }
            )
    rows.append(
        {
            "game_id": f"2025_{POSTSEASON_WEEK}_NYG_DAL",
            "season": 2025,
            "week": POSTSEASON_WEEK,
            "game_type": "WC",
            "home_team": "DAL",
            "away_team": "NYG",
        }
    )
    return pl.DataFrame(rows)


@pytest.fixture()
def league_weekly_stats() -> pl.DataFrame:
    """Weekly box scores for every league starter across both seasons.

    Shape: one row per starter per game played, season_type REG, plus eight
    season_type POST rows for the NYG and DAL starters in week ``POSTSEASON_WEEK`` of
    2025. ``opponent_team`` carries the defense faced. Columns match the pair
    fixture ``weekly_stats``.

    Property: a regular-season row's ``fantasy_points_ppr`` is
    ``POINTS_ALLOWED[position][opponent]`` and its box score adds up to exactly that,
    so a defense's points-allowed average equals its ``POINTS_ALLOWED`` entry and a
    player's per-game average is the mean over the defenses they faced. Season 2025
    holds to that without exception; season ``ABSENCE_SEASON`` adds ``ABSENCE_BONUS``
    to the remaining NYG starters during ``ABSENCE_WEEKS`` and drops the absent
    receiver's rows altogether. Every POST row is worth ``POSTSEASON_POINTS``, which
    no regular-season row reaches.
    """
    rows = []
    for season in LEAGUE_SEASONS:
        for week, home, away in _league_games(season):
            for team, opponent in ((home, away), (away, home)):
                for position in LEAGUE_POSITIONS:
                    absent = _absence_applies(season, team, week)
                    if absent and position == ABSENCE_POSITION:
                        continue
                    line = _stat_line(position, POINTS_ALLOWED[position][opponent])
                    if absent:
                        for column, bonus in ABSENCE_BONUS[position].items():
                            line[column] += bonus
                    rows.append(
                        {
                            "player_id": league_gsis_id(team, position),
                            "player_display_name": league_player_name(team, position),
                            "position": position,
                            "recent_team": team,
                            "opponent_team": opponent,
                            "season": season,
                            "week": week,
                            "season_type": "REG",
                            "fantasy_points_ppr": ppr_points(line),
                            **line,
                        }
                    )
    for team, opponent in (("NYG", "DAL"), ("DAL", "NYG")):
        for position in LEAGUE_POSITIONS:
            line = _stat_line(position, POSTSEASON_POINTS)
            rows.append(
                {
                    "player_id": league_gsis_id(team, position),
                    "player_display_name": league_player_name(team, position),
                    "position": position,
                    "recent_team": team,
                    "opponent_team": opponent,
                    "season": 2025,
                    "week": POSTSEASON_WEEK,
                    "season_type": "POST",
                    "fantasy_points_ppr": ppr_points(line),
                    **line,
                }
            )
    return pl.DataFrame(rows)


@pytest.fixture()
def league_snap_counts() -> pl.DataFrame:
    """Snap counts for every league starter across both seasons.

    Shape: one row per starter per game played, game_type REG, plus eight game_type
    POST rows for the NYG and DAL starters in week ``POSTSEASON_WEEK`` of 2025.
    Columns match the pair fixture ``snap_counts``; ``offense_pct`` is a 0–1 fraction.

    Property: a starter takes ``SNAP_SHARE[position]`` of the offensive snaps every
    week, so a season average equals that entry. The absent receiver keeps a row in
    each of ``ABSENCE_WEEKS`` carrying zero snaps, which is what marks those weeks
    inactive rather than the row being missing.
    """
    rows = []

    def snap_row(
        season: int, week: int, team: str, opponent: str, position: str, game_type: str
    ) -> dict:
        played = not (_absence_applies(season, team, week) and position == ABSENCE_POSITION)
        pct = SNAP_SHARE[position] if played else 0.0
        return {
            "pfr_player_id": league_pfr_id(team, position),
            "player": league_player_name(team, position),
            "position": position,
            "team": team,
            "opponent": opponent,
            "season": season,
            "week": week,
            "game_type": game_type,
            "offense_snaps": round(70 * pct, 1),
            "offense_pct": pct,
            "defense_snaps": 0.0,
            "defense_pct": 0.0,
            "st_snaps": 0.0,
            "st_pct": 0.0,
            "game_id": f"{season}_{week:02d}_{opponent}_{team}",
            "pfr_game_id": f"{season}{week:02d}{team.lower()}",
        }

    for season in LEAGUE_SEASONS:
        for week, home, away in _league_games(season):
            for team, opponent in ((home, away), (away, home)):
                for position in LEAGUE_POSITIONS:
                    rows.append(snap_row(season, week, team, opponent, position, "REG"))
    for team, opponent in (("NYG", "DAL"), ("DAL", "NYG")):
        for position in LEAGUE_POSITIONS:
            rows.append(snap_row(2025, POSTSEASON_WEEK, team, opponent, position, "POST"))
    return pl.DataFrame(rows)


@pytest.fixture()
def rosters() -> pl.DataFrame:
    """Seasonal rosters for both league seasons.

    Shape: one row per league starter per season, plus a player listed at RB in 2024
    and WR in 2025, plus a player carrying a null position. Columns: season, team,
    position, player_name, player_id, status, years_exp.

    Property: a position lookup keyed on player_id resolves every league starter,
    drops the null-position row, and resolves the reclassified player to WR — the
    position on their later row.
    """
    rows = []
    for season in LEAGUE_SEASONS:
        for team in LEAGUE_TEAMS:
            for position in LEAGUE_POSITIONS:
                rows.append(
                    {
                        "season": season,
                        "team": team,
                        "position": position,
                        "player_name": league_player_name(team, position),
                        "player_id": league_gsis_id(team, position),
                        "status": "ACT",
                        "years_exp": 4,
                    }
                )
    rows.extend(
        [
            {
                "season": 2024,
                "team": "PHI",
                "position": "RB",
                "player_name": "Rey Flex",
                "player_id": "00-0901",
                "status": "ACT",
                "years_exp": 1,
            },
            {
                "season": 2025,
                "team": "PHI",
                "position": "WR",
                "player_name": "Rey Flex",
                "player_id": "00-0901",
                "status": "ACT",
                "years_exp": 2,
            },
            {
                "season": 2025,
                "team": "WAS",
                "position": None,
                "player_name": "Unlisted Position",
                "player_id": "00-0902",
                "status": "ACT",
                "years_exp": 0,
            },
        ]
    )
    return pl.DataFrame(rows)


@pytest.fixture()
def injuries() -> pl.DataFrame:
    """Injury reports for four league starters across both seasons.

    Shape: one row per player per reported week. Columns: season, season_type, team,
    week, gsis_id, position, full_name, report_primary_injury, report_status,
    practice_status.

    Property: each player's health discount is exact. Every player is measured
    against the same slate, both seasons' regular-season weeks, so the denominator
    is thirty-six throughout. The NYG receiver reports Out for the nine
    ``ABSENCE_WEEKS``, nine of thirty-six, and lands on 0.75. The PHI tight end
    reports Out for nine weeks of each season, eighteen of thirty-six, meeting the
    half-the-slate cap on missed weeks exactly and landing on the 0.5 floor. The WAS
    quarterback reports inside one of the two seasons, twelve of the same thirty-six,
    and lands on two thirds. The DAL back reports only Questionable, so no discount
    is computed for them at all. Three postseason Out rows sit outside the
    regular-season filter.
    """
    rows = []

    def report(
        season: int, week: int, team: str, position: str, status: str, season_type: str = "REG"
    ) -> dict:
        return {
            "season": season,
            "season_type": season_type,
            "team": team,
            "week": week,
            "gsis_id": league_gsis_id(team, position),
            "position": position,
            "full_name": league_player_name(team, position),
            "report_primary_injury": "Hamstring",
            "report_status": status,
            "practice_status": "Did Not Participate" if status == "Out" else "Limited",
        }

    for week in sorted(ABSENCE_WEEKS):
        rows.append(report(ABSENCE_SEASON, week, "NYG", "WR", "Out"))
    for week in (1, 2, 3):
        rows.append(report(ABSENCE_SEASON, week, "NYG", "WR", "Questionable"))
    for week in (1, 2):
        rows.append(report(2025, week, "NYG", "WR", "Questionable"))
    for week in (POSTSEASON_WEEK, POSTSEASON_WEEK + 1, POSTSEASON_WEEK + 2):
        rows.append(report(2025, week, "NYG", "WR", "Out", season_type="POST"))

    for season in LEAGUE_SEASONS:
        for week in range(1, 10):
            rows.append(report(season, week, "PHI", "TE", "Out"))

    for week in range(1, 13):
        rows.append(report(2025, week, "WAS", "QB", "Out"))

    for week in range(1, 7):
        rows.append(report(2025, week, "DAL", "RB", "Questionable"))

    return pl.DataFrame(rows)


# ── Colliding display names ──────────────────────────────────────────────────


def _series_snap_rows(
    pfr_id: str, name: str, team: str, position: str, pcts: tuple[float, ...], opponent: str
) -> list[dict]:
    """Snap rows for one player, one per entry of `pcts` starting at week 1."""
    return [
        {
            "pfr_player_id": pfr_id,
            "player": name,
            "position": position,
            "team": team,
            "opponent": opponent,
            "season": 2025,
            "week": week,
            "game_type": "REG",
            "offense_snaps": round(70 * pct, 1),
            "offense_pct": pct,
            "defense_snaps": 0.0,
            "defense_pct": 0.0,
            "st_snaps": 0.0,
            "st_pct": 0.0,
            "game_id": f"2025_{week:02d}_{opponent}_{team}",
            "pfr_game_id": f"2025{week:02d}{team.lower()}",
        }
        for week, pct in enumerate(pcts, start=1)
    ]


@pytest.fixture()
def colliding_name_snaps() -> pl.DataFrame:
    """Two players wearing ``COLLIDING_NAME`` on different teams.

    Shape: 12 rows, six weeks each, season 2025, game_type REG.

    Property: the two carry distinct ``pfr_player_id`` and distinct GSIS ids, and
    their shares mirror — ``COLLIDING_RISING_PCT`` climbs ten points a week while
    ``COLLIDING_FALLING_PCT`` drops ten, and the pair sums to 1.10 in every week. A
    window keyed on the display name therefore reads flat, and only a window keyed on
    the player id separates the rise from the fall.
    """
    rows = _series_snap_rows(
        COLLIDING_RISING_PFR_ID,
        COLLIDING_NAME,
        COLLIDING_RISING_TEAM,
        COLLIDING_RISING_POSITION,
        COLLIDING_RISING_PCT,
        "PHI",
    )
    rows += _series_snap_rows(
        COLLIDING_FALLING_PFR_ID,
        COLLIDING_NAME,
        COLLIDING_FALLING_TEAM,
        COLLIDING_FALLING_POSITION,
        COLLIDING_FALLING_PCT,
        "WAS",
    )
    return pl.DataFrame(rows)


@pytest.fixture()
def colliding_name_weekly_stats() -> pl.DataFrame:
    """Weekly stats for the two players wearing ``COLLIDING_NAME``.

    Shape: 12 rows, six weeks each, season 2025, season_type REG. Columns match the
    pair fixture ``weekly_stats``.

    Property: each is the only player on their team, so target share and touch share
    hold constant across the six weeks and every usage trend comes from the snap
    share alone. The rising player takes 5 targets and no carries, giving both
    shares 100. The falling player takes 10 carries and no targets, so their team's
    weekly target total is zero, which is the branch a share takes on a zero
    denominator.
    """
    rows = []
    for week in range(1, 7):
        rising = _stat_line("WR", 10.0)
        rising["targets"] = 5.0
        rows.append(
            {
                "player_id": COLLIDING_RISING_GSIS_ID,
                "player_display_name": COLLIDING_NAME,
                "position": COLLIDING_RISING_POSITION,
                "recent_team": COLLIDING_RISING_TEAM,
                "opponent_team": "PHI",
                "season": 2025,
                "week": week,
                "season_type": "REG",
                "fantasy_points_ppr": ppr_points(rising),
                **rising,
            }
        )
        falling = _stat_line("RB", 11.0)
        falling["targets"] = 0.0
        falling["receptions"] = 0.0
        falling["carries"] = 10.0
        rows.append(
            {
                "player_id": COLLIDING_FALLING_GSIS_ID,
                "player_display_name": COLLIDING_NAME,
                "position": COLLIDING_FALLING_POSITION,
                "recent_team": COLLIDING_FALLING_TEAM,
                "opponent_team": "WAS",
                "season": 2025,
                "week": week,
                "season_type": "REG",
                "fantasy_points_ppr": ppr_points(falling),
                **falling,
            }
        )
    return pl.DataFrame(rows)


# ── Breakout series ──────────────────────────────────────────────────────────


@pytest.fixture()
def breakout_snaps() -> pl.DataFrame:
    """One player whose eight weeks of snap share cover every trend class.

    Shape: 8 rows, season 2025, game_type REG, ``offense_pct`` as a 0–1 fraction
    taken from ``BREAKOUT_PCT``.

    Property: over a three-week window the series rises, crosses from under 50% to
    over 60% in week 4, flattens, rises again, then falls for two weeks — so one
    input classifies as breakout, rising, stable and falling.
    """
    rows = _series_snap_rows(
        BREAKOUT_PFR_ID,
        BREAKOUT_NAME,
        BREAKOUT_TEAM,
        BREAKOUT_POSITION,
        BREAKOUT_PCT,
        "WAS",
    )
    return pl.DataFrame(rows)


@pytest.fixture()
def breakout_snaps_points_scale(breakout_snaps: pl.DataFrame) -> pl.DataFrame:
    """``breakout_snaps`` with ``offense_pct`` on a 0–100 scale.

    Shape: identical to ``breakout_snaps`` with every ``offense_pct`` multiplied by
    100.

    Property: nflverse publishes snap share as a fraction and this frame publishes
    the same shares as points, so a trend computed from either yields the same
    ``snap_pct`` — a fraction is scaled once and points are left alone.
    """
    return breakout_snaps.with_columns(pl.col("offense_pct") * 100)


# ── Play-by-play ─────────────────────────────────────────────────────────────

_PBP_SCHEMA = {
    "game_id": pl.String,
    "season": pl.Int64,
    "week": pl.Int64,
    "season_type": pl.String,
    "posteam": pl.String,
    "defteam": pl.String,
    "fixed_drive": pl.Int64,
    "yardline_100": pl.Float64,
    "play_type": pl.String,
    "pass_attempt": pl.Float64,
    "rush_attempt": pl.Float64,
    "complete_pass": pl.Float64,
    "touchdown": pl.Float64,
    "pass_touchdown": pl.Float64,
    "rush_touchdown": pl.Float64,
    "interception": pl.Float64,
    "fumble_lost": pl.Float64,
    "passing_yards": pl.Float64,
    "receiving_yards": pl.Float64,
    "rushing_yards": pl.Float64,
    "epa": pl.Float64,
    "passer_player_id": pl.String,
    "passer_player_name": pl.String,
    "receiver_player_id": pl.String,
    "receiver_player_name": pl.String,
    "rusher_player_id": pl.String,
    "rusher_player_name": pl.String,
    "fumbled_1_player_id": pl.String,
    "fumbled_1_player_name": pl.String,
}

_PBP_ZERO = {
    "pass_attempt": 0.0,
    "rush_attempt": 0.0,
    "complete_pass": 0.0,
    "touchdown": 0.0,
    "pass_touchdown": 0.0,
    "rush_touchdown": 0.0,
    "interception": 0.0,
    "fumble_lost": 0.0,
    "passing_yards": 0.0,
    "receiving_yards": 0.0,
    "rushing_yards": 0.0,
}


def _throw(
    team: str,
    position: str,
    *,
    complete: float = 1.0,
    yards: float = 0.0,
    td: float = 0.0,
    intercepted: float = 0.0,
) -> dict:
    """One pass attempt from `team`'s quarterback to its starter at `position`."""
    return {
        "play_type": "pass",
        "pass_attempt": 1.0,
        "complete_pass": complete,
        "passing_yards": yards,
        "receiving_yards": yards,
        "pass_touchdown": td,
        "touchdown": td,
        "interception": intercepted,
        "passer_player_id": league_gsis_id(team, "QB"),
        "passer_player_name": league_player_name(team, "QB"),
        "receiver_player_id": league_gsis_id(team, position),
        "receiver_player_name": league_player_name(team, position),
    }


def _carry(team: str, *, yards: float = 0.0, td: float = 0.0, fumbled: float = 0.0) -> dict:
    """One rush by `team`'s starting back, optionally lost on a fumble."""
    fields = {
        "play_type": "run",
        "rush_attempt": 1.0,
        "rushing_yards": yards,
        "rush_touchdown": td,
        "touchdown": td,
        "rusher_player_id": league_gsis_id(team, "RB"),
        "rusher_player_name": league_player_name(team, "RB"),
    }
    if fumbled:
        fields["fumble_lost"] = fumbled
        fields["fumbled_1_player_id"] = league_gsis_id(team, "RB")
        fields["fumbled_1_player_name"] = league_player_name(team, "RB")
    return fields


def _dead_ball(play_type: str) -> dict:
    """One snap that is neither a pass nor a rush."""
    return {"play_type": play_type}


@pytest.fixture()
def pbp() -> pl.DataFrame:
    """Play-by-play for four league games plus one postseason game, season 2025.

    Shape: one row per play. NYG and DAL take snaps in weeks 1 and 2 of 2025, PHI in
    week 1, WAS in week 2, and NYG again in week ``POSTSEASON_WEEK``. Carries the
    identifier, indicator, yardage and EPA columns the red zone engine and
    ``ffb.data.stats`` read; indicator and yardage columns are Float64, as nflverse
    publishes them.

    Property: every red zone number is a small whole count. NYG takes 3 red zone
    trips over 8 plays, splitting them 4 passes to 4 rushes, and scores on all 3 —
    its receiver draws 3 of the 4 red zone targets and its tight end the fourth.
    DAL takes 4 trips over 8 plays, splits them 6 passes to 2 rushes and scores once.
    PHI takes 1 trip and scores; WAS takes 1 and does not. Plays that must not reach
    a red zone total sit inside the 20 to prove the filters: a field goal and a
    no-play on NYG drives, and a receiving touchdown on a season_type POST row.
    Away from the red zone the plays make each weekly line whole: NYG's week 1 gives
    the quarterback 50 passing yards with one touchdown and one interception, the
    receiver 5 targets for 3 catches and 30 yards, the tight end 2 catches for 20
    yards and a score, and the back 5 carries for 30 yards, a score and a lost fumble.
    """
    rows: list[dict] = []

    def play(
        week: int, posteam: str, drive: int, yardline: float, fields: dict, epa: float
    ) -> None:
        rows.append(
            {
                **dict.fromkeys(_PBP_SCHEMA, None),
                **_PBP_ZERO,
                "game_id": league_game_id(2025, week, posteam),
                "season": 2025,
                "week": week,
                "season_type": "REG",
                "posteam": posteam,
                "defteam": league_opponent(2025, week, posteam),
                "fixed_drive": drive,
                "yardline_100": yardline,
                "epa": epa,
                **fields,
            }
        )

    # NYG, week 1 — red zone
    play(1, "NYG", 3, 15.0, _throw("NYG", "WR", yards=5.0), 0.5)
    play(1, "NYG", 3, 10.0, _carry("NYG", yards=5.0), 0.5)
    play(1, "NYG", 3, 5.0, _throw("NYG", "TE", yards=5.0, td=1.0), 2.0)
    play(1, "NYG", 7, 8.0, _carry("NYG", yards=3.0), 0.0)
    play(1, "NYG", 7, 5.0, _carry("NYG", yards=5.0, td=1.0), 1.0)
    # NYG, week 1 — inside the 20, but neither a pass nor a rush
    play(1, "NYG", 3, 12.0, _dead_ball("no_play"), 0.0)
    play(1, "NYG", 5, 15.0, _dead_ball("field_goal"), 0.5)
    # NYG, week 1 — outside the 20
    play(1, "NYG", 1, 75.0, _throw("NYG", "WR", yards=10.0), 1.0)
    play(1, "NYG", 1, 60.0, _throw("NYG", "WR", yards=15.0), 1.0)
    play(1, "NYG", 1, 45.0, _throw("NYG", "WR", complete=0.0), -0.5)
    play(1, "NYG", 2, 55.0, _throw("NYG", "TE", yards=15.0), 1.0)
    play(1, "NYG", 2, 40.0, _throw("NYG", "WR", complete=0.0, intercepted=1.0), -3.0)
    play(1, "NYG", 4, 70.0, _carry("NYG", yards=10.0), 0.5)
    play(1, "NYG", 4, 60.0, _carry("NYG", yards=7.0, fumbled=1.0), -2.0)

    # NYG, week 2
    play(2, "NYG", 2, 20.0, _carry("NYG"), -1.0)
    play(2, "NYG", 2, 20.0, _throw("NYG", "WR", complete=0.0), -1.0)
    play(2, "NYG", 2, 20.0, _throw("NYG", "WR", yards=20.0, td=1.0), 2.0)
    play(2, "NYG", 4, 50.0, _throw("NYG", "TE", yards=30.0), 1.5)

    # DAL, weeks 1 and 2
    play(1, "DAL", 4, 18.0, _throw("DAL", "WR", yards=6.0), -0.5)
    play(1, "DAL", 4, 12.0, _throw("DAL", "TE", complete=0.0), -0.5)
    play(1, "DAL", 8, 15.0, _carry("DAL", yards=2.0), -0.5)
    play(1, "DAL", 8, 13.0, _throw("DAL", "WR", complete=0.0), -0.5)
    play(2, "DAL", 5, 19.0, _throw("DAL", "TE", yards=9.0), 0.5)
    play(2, "DAL", 5, 10.0, _throw("DAL", "WR", yards=10.0, td=1.0), 1.5)
    play(2, "DAL", 9, 20.0, _carry("DAL", yards=1.0), 0.0)
    play(2, "DAL", 9, 19.0, _throw("DAL", "TE", complete=0.0), 0.0)

    # PHI and WAS
    play(1, "PHI", 6, 10.0, _throw("PHI", "WR", yards=5.0), 0.0)
    play(1, "PHI", 6, 5.0, _carry("PHI", yards=5.0, td=1.0), 2.0)
    play(2, "WAS", 3, 20.0, _throw("WAS", "WR", complete=0.0), -1.0)
    play(2, "WAS", 3, 20.0, _throw("WAS", "TE", complete=0.0), -1.0)

    # Postseason
    rows.append(
        {
            **dict.fromkeys(_PBP_SCHEMA, None),
            **_PBP_ZERO,
            "game_id": f"2025_{POSTSEASON_WEEK}_NYG_DAL",
            "season": 2025,
            "week": POSTSEASON_WEEK,
            "season_type": "POST",
            "posteam": "NYG",
            "defteam": "DAL",
            "fixed_drive": 1,
            "yardline_100": 5.0,
            "epa": 5.0,
            **_throw("NYG", "WR", yards=5.0, td=1.0),
        }
    )

    return pl.DataFrame(rows, schema=_PBP_SCHEMA)
