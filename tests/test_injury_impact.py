"""Property tests for the injury impact engine.

The league fixtures carry one absence: the ``ABSENCE_TEAM`` starter at
``ABSENCE_POSITION`` sits out ``ABSENCE_WEEKS`` of ``ABSENCE_SEASON``. The weeks
played and the weeks missed face each of the other three defenses the same number of
times, so a teammate's split average is the mean of ``POINTS_ALLOWED`` over those
defenses on the played side and that mean plus ``ABSENCE_BONUS`` on the missed side.
Every expected number below is that arithmetic, not a value copied out of a run.
"""

import warnings
from collections.abc import Collection
from statistics import fmean

import polars as pl
import pytest

from ffb.injury_impact.engine import compute_injury_impact, get_searchable_players
from tests.conftest import (
    ABSENCE_BONUS,
    ABSENCE_POSITION,
    ABSENCE_SEASON,
    ABSENCE_TEAM,
    ABSENCE_WEEKS,
    LEAGUE_POSITIONS,
    LEAGUE_SEASONS,
    LEAGUE_TEAMS,
    POINTS_ALLOWED,
    REGULAR_SEASON_WEEKS,
    league_pfr_id,
    league_player_name,
    ppr_points,
)

#: The player whose absence the league fixtures carry.
ABSENT_PLAYER = league_player_name(ABSENCE_TEAM, ABSENCE_POSITION)

#: Teammates of the absent player: every league position but theirs.
TEAMMATE_POSITIONS = tuple(p for p in LEAGUE_POSITIONS if p != ABSENCE_POSITION)

#: The season carrying no absence at all, so a receiver sat down there splits a
#: teammate's weeks without any box-score bonus muddying the two sides.
CLEAN_SEASON = next(s for s in LEAGUE_SEASONS if s != ABSENCE_SEASON)

#: Default of ``compute_injury_impact(min_games_missed=...)``.
DEFAULT_MIN_GAMES_MISSED = 2

#: A scoreless box score, so ``ppr_points`` can price a bonus on its own.
_SCORELESS = {
    "receptions": 0.0,
    "receiving_yards": 0.0,
    "receiving_tds": 0.0,
    "rushing_yards": 0.0,
    "rushing_tds": 0.0,
    "passing_yards": 0.0,
    "passing_tds": 0.0,
    "interceptions": 0.0,
    "fumbles_lost": 0.0,
}


def _baseline(position: str, team: str) -> float:
    """Points `team`'s starter at `position` averages against the rest of the league."""
    return fmean(POINTS_ALLOWED[position][d] for d in LEAGUE_TEAMS if d != team)


def _bonus_points(position: str) -> float:
    """Points ``ABSENCE_BONUS`` adds to a `position` box score."""
    return ppr_points({**_SCORELESS, **ABSENCE_BONUS[position]})


def _sit_out(
    snaps: pl.DataFrame,
    weeks: Collection[int],
    *,
    season: int = CLEAN_SEASON,
    team: str = ABSENCE_TEAM,
    position: str = ABSENCE_POSITION,
) -> pl.DataFrame:
    """`snaps` with one starter taking no offensive snap in the named weeks."""
    return snaps.with_columns(
        pl.when(
            (pl.col("season") == season)
            & (pl.col("team") == team)
            & (pl.col("position") == position)
            & pl.col("week").is_in(list(weeks))
        )
        .then(pl.lit(0.0))
        .otherwise(pl.col("offense_snaps"))
        .alias("offense_snaps")
    )


def _extra_player(
    snaps: pl.DataFrame, *, player: str, position: str, game_type: str
) -> pl.DataFrame:
    """`snaps` plus one row for a player carrying the given labels."""
    extra = snaps.head(1).with_columns(
        pl.lit("ZZZ99").alias("pfr_player_id"),
        pl.lit(player).alias("player"),
        pl.lit(position).alias("position"),
        pl.lit(game_type).alias("game_type"),
    )
    return pl.concat([snaps, extra])


#: Seeds the order-independence tests present the engine with.
SHUFFLE_SEEDS = range(16)


def _shuffled(df: pl.DataFrame, seed: int) -> pl.DataFrame:
    """`df` with its rows reordered, reproducibly from `seed`."""
    return df.sample(fraction=1.0, shuffle=True, seed=seed)


def _teammate_row(teammates: pl.DataFrame, position: str) -> dict:
    """The single row `teammates` holds for the absent player's teammate at `position`."""
    name = league_player_name(ABSENCE_TEAM, position)
    rows = teammates.filter(pl.col("teammate") == name).to_dicts()
    assert len(rows) == 1
    return rows[0]


@pytest.fixture()
def clean_snaps(league_snap_counts: pl.DataFrame) -> pl.DataFrame:
    """League snap counts narrowed to the season carrying no absence."""
    return league_snap_counts.filter(pl.col("season") == CLEAN_SEASON)


@pytest.fixture()
def clean_stats(league_weekly_stats: pl.DataFrame) -> pl.DataFrame:
    """League box scores narrowed to the season carrying no absence."""
    return league_weekly_stats.filter(pl.col("season") == CLEAN_SEASON)


# ── Empty results ────────────────────────────────────────────────────────────


def test_unknown_player_name_returns_the_empty_result(
    league_snap_counts, league_weekly_stats, player_ids
):
    info, teammates = compute_injury_impact(
        "Nobody Rostered", league_snap_counts, league_weekly_stats, player_ids
    )

    assert info is None
    assert teammates.is_empty()


def test_player_who_never_sits_returns_the_empty_result(clean_snaps, clean_stats, player_ids):
    info, teammates = compute_injury_impact(ABSENT_PLAYER, clean_snaps, clean_stats, player_ids)

    assert info is None
    assert teammates.is_empty()


@pytest.mark.parametrize(
    ("minimum", "missed"),
    [(DEFAULT_MIN_GAMES_MISSED, DEFAULT_MIN_GAMES_MISSED - 1), (5, 4)],
)
def test_absence_shorter_than_the_minimum_returns_the_empty_result(
    clean_snaps, clean_stats, player_ids, minimum, missed
):
    info, teammates = compute_injury_impact(
        ABSENT_PLAYER,
        _sit_out(clean_snaps, range(1, missed + 1)),
        clean_stats,
        player_ids,
        min_games_missed=minimum,
    )

    assert info is None
    assert teammates.is_empty()


@pytest.mark.parametrize(
    ("minimum", "missed"),
    [(DEFAULT_MIN_GAMES_MISSED, DEFAULT_MIN_GAMES_MISSED), (5, 5)],
)
def test_absence_reaching_the_minimum_returns_a_result(
    clean_snaps, clean_stats, player_ids, minimum, missed
):
    info, teammates = compute_injury_impact(
        ABSENT_PLAYER,
        _sit_out(clean_snaps, range(1, missed + 1)),
        clean_stats,
        player_ids,
        min_games_missed=minimum,
    )

    assert info is not None
    assert info["games_missed"] == missed
    assert teammates.height == len(TEAMMATE_POSITIONS)


# ── Player description ───────────────────────────────────────────────────────


def test_player_info_names_the_team_season_the_absence_falls_in(
    league_snap_counts, league_weekly_stats, player_ids
):
    info, _ = compute_injury_impact(
        ABSENT_PLAYER, league_snap_counts, league_weekly_stats, player_ids
    )

    assert info == {
        "name": ABSENT_PLAYER,
        "position": ABSENCE_POSITION,
        "team": ABSENCE_TEAM,
        "season": ABSENCE_SEASON,
        "games_missed": len(ABSENCE_WEEKS),
    }


def test_player_info_names_the_most_recent_qualifying_team_season(
    league_snap_counts, league_weekly_stats, player_ids
):
    both_seasons = _sit_out(league_snap_counts, range(1, 4))

    info, _ = compute_injury_impact(ABSENT_PLAYER, both_seasons, league_weekly_stats, player_ids)

    assert info is not None
    assert info["season"] == max(LEAGUE_SEASONS)


def test_seeded_shuffles_present_distinct_row_orders(league_snap_counts):
    orders = {
        tuple(_shuffled(league_snap_counts, seed)["week"].to_list()) for seed in SHUFFLE_SEEDS
    }

    assert len(orders) == len(SHUFFLE_SEEDS)


def test_player_info_is_identical_across_input_row_orders(
    league_snap_counts, league_weekly_stats, player_ids
):
    both_seasons = _sit_out(league_snap_counts, range(1, 4))

    descriptions = {
        tuple(
            sorted(
                compute_injury_impact(
                    ABSENT_PLAYER,
                    _shuffled(both_seasons, seed),
                    _shuffled(league_weekly_stats, seed),
                    _shuffled(player_ids, seed),
                )[0].items()
            )
        )
        for seed in SHUFFLE_SEEDS
    }

    assert len(descriptions) == 1


def test_teammate_splits_are_identical_across_input_row_orders(
    league_snap_counts, league_weekly_stats, player_ids
):
    both_seasons = _sit_out(league_snap_counts, range(1, 4))

    splits = {
        tuple(
            sorted(
                compute_injury_impact(
                    ABSENT_PLAYER,
                    _shuffled(both_seasons, seed),
                    _shuffled(league_weekly_stats, seed),
                    _shuffled(player_ids, seed),
                )[1].rows()
            )
        )
        for seed in SHUFFLE_SEEDS
    }

    assert len(splits) == 1


# ── The with / without split ─────────────────────────────────────────────────


def test_teammate_gains_exactly_the_production_the_absence_frees(
    league_snap_counts, league_weekly_stats, player_ids
):
    _, teammates = compute_injury_impact(
        ABSENT_PLAYER, league_snap_counts, league_weekly_stats, player_ids
    )
    back = _teammate_row(teammates, "RB")
    with_player = _baseline("RB", ABSENCE_TEAM)

    assert back["fpts_with"] == pytest.approx(with_player)
    assert back["fpts_without"] == pytest.approx(with_player + _bonus_points("RB"))
    assert back["delta_fpts"] == pytest.approx(_bonus_points("RB"))


def test_teammate_losing_production_carries_a_negative_delta(
    league_snap_counts, league_weekly_stats, player_ids
):
    _, teammates = compute_injury_impact(
        ABSENT_PLAYER, league_snap_counts, league_weekly_stats, player_ids
    )
    quarterback = _teammate_row(teammates, "QB")

    assert _bonus_points("QB") < 0
    assert quarterback["delta_fpts"] == pytest.approx(_bonus_points("QB"))


def test_target_split_reflects_the_targets_the_absence_frees(
    league_snap_counts, league_weekly_stats, player_ids
):
    _, teammates = compute_injury_impact(
        ABSENT_PLAYER, league_snap_counts, league_weekly_stats, player_ids
    )
    back = _teammate_row(teammates, "RB")

    assert back["tgt_without"] - back["tgt_with"] == pytest.approx(ABSENCE_BONUS["RB"]["targets"])


def test_touch_split_counts_targets_alongside_carries(
    league_snap_counts, league_weekly_stats, player_ids
):
    _, teammates = compute_injury_impact(
        ABSENT_PLAYER, league_snap_counts, league_weekly_stats, player_ids
    )
    back = _teammate_row(teammates, "RB")
    freed = ABSENCE_BONUS["RB"]["targets"] + ABSENCE_BONUS["RB"]["carries"]

    assert back["touches_without"] - back["touches_with"] == pytest.approx(freed)


def test_games_split_accounts_for_every_week_of_the_season(
    league_snap_counts, league_weekly_stats, player_ids
):
    _, teammates = compute_injury_impact(
        ABSENT_PLAYER, league_snap_counts, league_weekly_stats, player_ids
    )
    back = _teammate_row(teammates, "RB")

    assert back["games_without"] == len(ABSENCE_WEEKS)
    assert back["games_with"] == REGULAR_SEASON_WEEKS - len(ABSENCE_WEEKS)


def test_missed_weeks_are_the_weeks_carrying_no_offensive_snap(
    clean_snaps, clean_stats, player_ids
):
    missed = (2, 5, 11, 17)

    info, teammates = compute_injury_impact(
        ABSENT_PLAYER, _sit_out(clean_snaps, missed), clean_stats, player_ids
    )
    back = _teammate_row(teammates, "RB")

    assert info is not None
    assert info["games_missed"] == len(missed)
    assert back["games_without"] == len(missed)
    assert back["games_with"] == REGULAR_SEASON_WEEKS - len(missed)


def test_postseason_games_reach_neither_side_of_the_split(clean_snaps, clean_stats, player_ids):
    missed = (1, 2)

    _, teammates = compute_injury_impact(
        ABSENT_PLAYER, _sit_out(clean_snaps, missed), clean_stats, player_ids
    )
    back = _teammate_row(teammates, "RB")

    assert back["games_with"] == REGULAR_SEASON_WEEKS - len(missed)


def test_every_teammate_of_the_absent_player_gets_one_row(
    league_snap_counts, league_weekly_stats, player_ids
):
    _, teammates = compute_injury_impact(
        ABSENT_PLAYER, league_snap_counts, league_weekly_stats, player_ids
    )

    assert sorted(teammates["teammate"].to_list()) == sorted(
        league_player_name(ABSENCE_TEAM, p) for p in TEAMMATE_POSITIONS
    )


def test_rows_are_ordered_by_absolute_fantasy_delta(
    league_snap_counts, league_weekly_stats, player_ids
):
    _, teammates = compute_injury_impact(
        ABSENT_PLAYER, league_snap_counts, league_weekly_stats, player_ids
    )

    magnitudes = [abs(d) for d in teammates["delta_fpts"].to_list()]
    assert magnitudes == sorted(magnitudes, reverse=True)


# ── Teammate labelling ───────────────────────────────────────────────────────


#: The teammate the reclassification tests list at two positions, named — as league
#: starters are throughout this file — by the position they hold. The snap source
#: carries them at ``RECLASSIFIED_POSITION`` over ``RECLASSIFIED_WEEKS`` and at their
#: own position in every earlier week.
RECLASSIFIED_TEAMMATE = "RB"
RECLASSIFIED_POSITION = "TE"
RECLASSIFIED_WEEKS = tuple(range(10, REGULAR_SEASON_WEEKS + 1))

#: A second listing for that teammate inside their last week. It sorts after
#: ``RECLASSIFIED_POSITION``, so a reduction that takes the label sorting first and
#: one that takes whichever row lands last give different answers.
LATE_TIE_POSITION = "WR"


def _relist(
    snaps: pl.DataFrame,
    *,
    teammate: str = RECLASSIFIED_TEAMMATE,
    position: str = RECLASSIFIED_POSITION,
    weeks: Collection[int] = RECLASSIFIED_WEEKS,
) -> pl.DataFrame:
    """`snaps` with the absent player's `teammate` listed at `position` in `weeks`."""
    return snaps.with_columns(
        pl.when(
            (pl.col("pfr_player_id") == league_pfr_id(ABSENCE_TEAM, teammate))
            & pl.col("week").is_in(list(weeks))
        )
        .then(pl.lit(position))
        .otherwise(pl.col("position"))
        .alias("position")
    )


def _second_listing(
    snaps: pl.DataFrame,
    *,
    teammate: str = RECLASSIFIED_TEAMMATE,
    position: str = LATE_TIE_POSITION,
) -> pl.DataFrame:
    """`snaps` plus a row listing `teammate` at `position` in their last league week."""
    duplicate = snaps.filter(
        (pl.col("pfr_player_id") == league_pfr_id(ABSENCE_TEAM, teammate))
        & (pl.col("season") == ABSENCE_SEASON)
        & (pl.col("game_type") == "REG")
        & (pl.col("week") == REGULAR_SEASON_WEEKS)
    ).with_columns(pl.lit(position).alias("position"))
    assert duplicate.height == 1
    return pl.concat([snaps, duplicate])


def _reclassified_position(
    snaps: pl.DataFrame, weekly_stats: pl.DataFrame, player_ids: pl.DataFrame
) -> str:
    """Position the engine reports for the teammate carrying two listings."""
    _, teammates = compute_injury_impact(ABSENT_PLAYER, snaps, weekly_stats, player_ids)
    return _teammate_row(teammates, RECLASSIFIED_TEAMMATE)["position"]


def test_teammate_position_comes_from_their_last_listed_week(
    league_snap_counts, league_weekly_stats, player_ids
):
    position = _reclassified_position(_relist(league_snap_counts), league_weekly_stats, player_ids)

    assert position == RECLASSIFIED_POSITION


def test_teammate_position_is_identical_across_input_row_orders(
    league_snap_counts, league_weekly_stats, player_ids
):
    reclassified = _relist(league_snap_counts)

    positions = {
        _reclassified_position(
            _shuffled(reclassified, seed),
            _shuffled(league_weekly_stats, seed),
            _shuffled(player_ids, seed),
        )
        for seed in SHUFFLE_SEEDS
    }

    assert positions == {RECLASSIFIED_POSITION}


def test_splits_are_identical_when_a_teammate_carries_two_positions(
    league_snap_counts, league_weekly_stats, player_ids
):
    reclassified = _relist(league_snap_counts)

    splits = {
        tuple(
            sorted(
                compute_injury_impact(
                    ABSENT_PLAYER,
                    _shuffled(reclassified, seed),
                    _shuffled(league_weekly_stats, seed),
                    _shuffled(player_ids, seed),
                )[1].rows()
            )
        )
        for seed in SHUFFLE_SEEDS
    }

    assert len(splits) == 1


def test_two_listings_inside_the_last_week_resolve_on_the_position_label(
    league_snap_counts, league_weekly_stats, player_ids
):
    tied = _second_listing(_relist(league_snap_counts))

    positions = {
        _reclassified_position(
            _shuffled(tied, seed),
            _shuffled(league_weekly_stats, seed),
            _shuffled(player_ids, seed),
        )
        for seed in SHUFFLE_SEEDS
    }

    assert positions == {min(RECLASSIFIED_POSITION, LATE_TIE_POSITION)}


# ── Confidence ───────────────────────────────────────────────────────────────


@pytest.mark.parametrize(
    ("missed", "confidence"),
    [(2, "Low"), (3, "Low"), (4, "Med"), (6, "Med"), (7, "High"), (8, "High")],
)
def test_confidence_tier_follows_the_games_without_the_player(
    clean_snaps, clean_stats, player_ids, missed, confidence
):
    _, teammates = compute_injury_impact(
        ABSENT_PLAYER, _sit_out(clean_snaps, range(1, missed + 1)), clean_stats, player_ids
    )

    assert teammates["games_without"].to_list() == [missed] * teammates.height
    assert teammates["confidence"].to_list() == [confidence] * teammates.height


# ── Search ───────────────────────────────────────────────────────────────────


def test_searchable_players_are_the_sorted_unique_offensive_names(league_snap_counts):
    assert get_searchable_players(league_snap_counts) == sorted(
        league_player_name(t, p) for t in LEAGUE_TEAMS for p in LEAGUE_POSITIONS
    )


def test_searchable_players_exclude_non_offensive_positions(league_snap_counts):
    kicker = "NYG Kicker"

    players = get_searchable_players(
        _extra_player(league_snap_counts, player=kicker, position="K", game_type="REG")
    )

    assert kicker not in players


def test_searchable_players_exclude_a_player_seen_only_in_the_postseason(
    league_snap_counts,
):
    reserve = "NYG Reserve"

    players = get_searchable_players(
        _extra_player(league_snap_counts, player=reserve, position="WR", game_type="POST")
    )

    assert reserve not in players


# ── Polars call sites ────────────────────────────────────────────────────────


def _deprecations(raised: list[warnings.WarningMessage]) -> list[str]:
    """Deprecation texts among `raised`.

    A filter set to ``error`` does not stop a polars deprecation: the call returns its
    result and the text lands on stderr. Recording the warnings is what puts one in
    reach of an assertion.
    """
    return [str(w.message) for w in raised if issubclass(w.category, DeprecationWarning)]


def test_the_engine_makes_no_deprecated_polars_call(
    league_snap_counts, league_weekly_stats, player_ids
):
    with warnings.catch_warnings(record=True) as raised:
        warnings.simplefilter("always")
        players = get_searchable_players(league_snap_counts)
        info, teammates = compute_injury_impact(
            ABSENT_PLAYER, league_snap_counts, league_weekly_stats, player_ids
        )

    assert ABSENT_PLAYER in players
    assert info is not None
    assert teammates.height == len(TEAMMATE_POSITIONS)
    assert _deprecations(raised) == []
