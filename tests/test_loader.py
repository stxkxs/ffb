"""Tests for the caching, download and fallback behaviour around the loaders.

No test reaches the network: `FakeNflreadpy` stands in for the nflreadpy module the
loaders read through, and the download tests pass their own callable to
`loader._download`.
"""

import ast
import json
import pathlib
import threading
from collections.abc import Callable
from typing import Any

import polars as pl
import pytest

from ffb.data import cache, loader
from tests.conftest import league_gsis_id


class FakeNflreadpy:
    """Stand-in for the nflreadpy module that records every loader call it is given.

    Each keyword names an nflreadpy loader and gives what that loader yields: a frame
    to return, or a callable taking the arguments the caller passes. A loader with no
    keyword raises `AttributeError`, so a fetch for data the test did not describe
    fails as loudly as it would against a module that has no such loader.

    Standing in for the module rather than for the function that reads through it
    leaves `_nflverse_season` under test: the exception mapping, the empty-frame rule
    and the renames each loader applies all run.
    """

    def __init__(self, **loaders: Any) -> None:
        self.loaders = loaders
        self.calls: list[tuple[str, tuple[Any, ...]]] = []

    def __getattr__(self, name: str) -> Callable[..., pl.DataFrame]:
        loaders = self.__dict__["loaders"]
        if name not in loaders:
            raise AttributeError(f"the test described no {name!r} loader")

        def call(*args: Any) -> pl.DataFrame:
            self.calls.append((name, args))
            handler = loaders[name]
            return handler(*args) if callable(handler) else handler

        return call

    @property
    def names(self) -> list[str]:
        """The loaders called, in call order."""
        return [name for name, _ in self.calls]


def marked(marker: int) -> pl.DataFrame:
    """A one-row frame whose `marker` column identifies which fetch produced it."""
    return pl.DataFrame({"marker": [marker]})


_RELEASE_URL = (
    "https://github.com/nflverse/nflverse-data/releases/download"
    "/stats_player/stats_player_week_2099.parquet"
)


def unavailable(*args: Any) -> pl.DataFrame:
    """A loader whose release asset is not published.

    nflreadpy wraps the HTTP failure in `ConnectionError`, which derives from
    `OSError`, and raises the same exception for a transfer that fails part way. The
    fallback tests are pinned to that exception, so they hold against both.
    """
    raise ConnectionError(f"Failed to download {_RELEASE_URL}: 404 Client Error: Not Found")


def published_weekly(season: int, points: float) -> pl.DataFrame:
    """One row of weekly stats in the shape nflverse publishes them.

    The asset spells the player's team `team`, and carries both a `position` naming the
    listing and a `position_group` naming the side of the ball. The loader renames the
    first and keeps the group.
    """
    return pl.DataFrame(
        {
            "player_id": ["00-0999"],
            "player_display_name": ["Published Starter"],
            "team": ["NYG"],
            "position": ["FB"],
            "position_group": ["RB"],
            "season": [season],
            "week": [1],
            "season_type": ["REG"],
            "fantasy_points_ppr": [points],
        }
    )


@pytest.fixture(autouse=True)
def cache_dir(tmp_path, monkeypatch):
    """Point the cache at a directory of this test's own."""
    monkeypatch.setattr(cache, "CACHE_DIR", tmp_path)
    return tmp_path


@pytest.fixture()
def nflverse(monkeypatch):
    """Install a `FakeNflreadpy` in place of the module the loaders read through."""

    def install(**loaders: Any) -> FakeNflreadpy:
        fake = FakeNflreadpy(**loaders)
        monkeypatch.setattr(loader, "_nflreadpy", lambda: fake)
        return fake

    return install


@pytest.fixture()
def stall():
    """A fetch that blocks until the test ends, standing in for a stalled transfer."""
    released = threading.Event()

    def fetch(*args: Any) -> str:
        released.wait()
        return "released"

    yield fetch
    released.set()


# ── Cache reads and writes ───────────────────────────────────────────────────


def test_a_miss_fetches_through_the_loader_it_names(nflverse):
    fake = nflverse(load_snap_counts=marked(1))
    loader.load_snap_counts([2025])
    assert fake.calls == [("load_snap_counts", ([2025],))]


def test_a_fresh_entry_is_served_without_a_second_fetch(nflverse):
    fake = nflverse(load_snap_counts=marked(1))
    loader.load_snap_counts([2025])
    loader.load_snap_counts([2025])
    assert fake.names == ["load_snap_counts"]


def test_a_served_entry_carries_the_values_that_were_fetched(nflverse):
    nflverse(load_snap_counts=marked(7))
    loader.load_snap_counts([2025])
    assert loader.load_snap_counts([2025])["marker"].to_list() == [7]


def test_force_refresh_fetches_past_a_fresh_entry(nflverse):
    markers = iter([1, 2])
    fake = nflverse(load_snap_counts=lambda seasons: marked(next(markers)))
    loader.load_snap_counts([2025])
    loader.load_snap_counts([2025], force_refresh=True)
    assert fake.names == ["load_snap_counts", "load_snap_counts"]


def test_force_refresh_returns_the_frame_it_fetched(nflverse):
    markers = iter([1, 2])
    nflverse(load_snap_counts=lambda seasons: marked(next(markers)))
    loader.load_snap_counts([2025])
    assert loader.load_snap_counts([2025], force_refresh=True)["marker"].to_list() == [2]


def test_force_refresh_stores_the_frame_it_fetched(nflverse):
    markers = iter([1, 2])
    nflverse(load_snap_counts=lambda seasons: marked(next(markers)))
    loader.load_snap_counts([2025])
    loader.load_snap_counts([2025], force_refresh=True)
    # A third fetch would exhaust `markers`, so this frame comes from the cache.
    assert loader.load_snap_counts([2025])["marker"].to_list() == [2]


# ── Cache keys ───────────────────────────────────────────────────────────────


def test_a_key_names_the_dataset_and_the_one_season_it_holds(nflverse, cache_dir):
    nflverse(load_pbp=marked(1))
    loader.load_pbp([2025, 2024])
    assert sorted(path.name for path in cache_dir.glob("*.parquet")) == [
        "pbp_2024.parquet",
        "pbp_2025.parquet",
    ]


def test_a_season_asked_for_in_a_second_ordering_is_served_from_the_cache(nflverse):
    fake = nflverse(load_snap_counts=marked(1))
    loader.load_snap_counts([2024, 2025])
    loader.load_snap_counts([2025, 2024])
    # The two fetches are the first request's two seasons; the second request adds none.
    assert fake.names == ["load_snap_counts", "load_snap_counts"]


def test_datasets_sharing_a_season_hold_separate_entries(nflverse):
    nflverse(load_snap_counts=marked(1), load_pbp=marked(2))
    loader.load_snap_counts([2025])
    assert loader.load_pbp([2025])["marker"].to_list() == [2]


def test_distinct_seasons_of_one_dataset_hold_separate_entries(nflverse):
    nflverse(load_snap_counts=lambda seasons: marked(sum(seasons)))
    loader.load_snap_counts([2024])
    loader.load_snap_counts([2025])
    assert loader.load_snap_counts([2024])["marker"].to_list() == [2024]


def test_the_player_crosswalk_is_fetched_without_seasons(nflverse):
    fake = nflverse(load_ff_playerids=marked(1))
    loader.load_player_ids()
    assert fake.calls == [("load_ff_playerids", ())]


def test_the_player_crosswalk_is_stored_under_a_key_naming_no_seasons(nflverse, cache_dir):
    nflverse(load_ff_playerids=marked(1))
    loader.load_player_ids()
    assert [path.name for path in cache_dir.glob("*.parquet")] == ["player_ids.parquet"]


# ── Download supervision ─────────────────────────────────────────────────────


def test_a_download_returns_what_the_fetch_produced():
    assert loader._download(lambda: "frame") == "frame"


def test_a_download_forwards_its_positional_arguments():
    received: list[Any] = []
    loader._download(lambda *args: received.append(args), "load_pbp", [2025])
    assert received == [("load_pbp", [2025])]


def test_a_failing_fetch_raises_on_the_calling_thread():
    def refuse() -> None:
        raise OSError("HTTP Error 404: Not Found")

    with pytest.raises(OSError, match="404"):
        loader._download(refuse)


def test_a_stalled_download_times_out_naming_its_timeout(stall):
    with pytest.raises(TimeoutError, match="timed out after 1s"):
        loader._download(stall, timeout=1)


def test_a_download_after_a_timeout_returns_within_its_own_budget(stall):
    with pytest.raises(TimeoutError):
        loader._download(stall, timeout=1)
    assert loader._download(lambda: "second", timeout=1) == "second"


def test_a_download_beside_a_stalled_one_returns_without_waiting_for_it(stall):
    stalled = threading.Thread(target=lambda: loader._download(stall, timeout=60), daemon=True)
    stalled.start()
    assert loader._download(lambda: "beside", timeout=1) == "beside"


# ── Weekly stats and the play-by-play fallback ───────────────────────────────


def nyg_receiver_week_one(weekly: pl.DataFrame) -> pl.DataFrame:
    """The row for the NYG receiver's opening week of 2025."""
    return weekly.filter(
        (pl.col("player_id") == league_gsis_id("NYG", "WR"))
        & (pl.col("season") == 2025)
        & (pl.col("week") == 1)
    )


def test_a_published_season_passes_through_unchanged(nflverse):
    nflverse(load_player_stats=lambda seasons: published_weekly(seasons[0], 12.5))
    weekly = loader.load_weekly_stats([2024])
    assert weekly["fantasy_points_ppr"].to_list() == [12.5]


def test_a_published_season_carries_the_team_under_the_name_its_joins_use(nflverse):
    """The asset spells it `team`; the frames it is joined to spell it `recent_team`."""
    nflverse(load_player_stats=lambda seasons: published_weekly(seasons[0], 12.5))
    weekly = loader.load_weekly_stats([2024])
    assert weekly["recent_team"].to_list() == ["NYG"]
    assert "team" not in weekly.columns


def test_a_published_position_is_the_side_of_the_ball_not_the_listing(nflverse):
    """The asset lists a fullback FB, which `OFFENSIVE_POSITIONS` does not hold.

    Carrying the listing forward would drop that player from every tool filtering on
    position, so `position_group`, which resolves a fullback to RB, is what the frame
    carries.
    """
    nflverse(load_player_stats=lambda seasons: published_weekly(seasons[0], 12.5))
    weekly = loader.load_weekly_stats([2024])
    assert weekly["position"].to_list() == ["RB"]
    assert "position_group" not in weekly.columns


def test_a_published_season_reads_nothing_beside_the_asset(nflverse):
    """Position travels in the weekly frame, so the published path fetches one thing.

    Reading the rosters here would make a roster failure cost a season the weekly asset
    covers in full.
    """
    fake = nflverse(load_player_stats=lambda seasons: published_weekly(seasons[0], 12.5))
    loader.load_weekly_stats([2024])
    assert fake.names == ["load_player_stats"]


def test_a_derived_season_the_rosters_do_not_cover_carries_a_null_position(nflverse, pbp):
    """Play-by-play names no position, so the derivation has nowhere else to read one."""
    nflverse(
        load_player_stats=unavailable,
        load_pbp=pbp,
        load_rosters=unavailable,
    )
    assert loader.load_weekly_stats([2025])["position"].unique().to_list() == [None]


def test_an_unpublished_season_is_derived_from_play_by_play(nflverse, pbp, published_rosters):
    nflverse(
        load_player_stats=unavailable,
        load_pbp=pbp,
        load_rosters=published_rosters,
    )
    weekly = loader.load_weekly_stats([2025])
    assert nyg_receiver_week_one(weekly)["fantasy_points_ppr"].to_list() == [6.0]


def test_derived_rows_count_the_targets_the_play_by_play_holds(nflverse, pbp, published_rosters):
    nflverse(
        load_player_stats=unavailable,
        load_pbp=pbp,
        load_rosters=published_rosters,
    )
    weekly = loader.load_weekly_stats([2025])
    assert nyg_receiver_week_one(weekly)["targets"].to_list() == [5.0]


def test_derived_rows_carry_the_position_the_rosters_list(nflverse, pbp, published_rosters):
    nflverse(
        load_player_stats=unavailable,
        load_pbp=pbp,
        load_rosters=published_rosters,
    )
    weekly = loader.load_weekly_stats([2025])
    assert nyg_receiver_week_one(weekly)["position"].to_list() == ["WR"]


def test_only_the_unpublished_seasons_reach_the_fallback(nflverse, pbp, published_rosters):
    fake = nflverse(
        load_player_stats=lambda seasons: (
            published_weekly(2024, 12.5) if seasons == [2024] else unavailable()
        ),
        load_pbp=pbp,
        load_rosters=published_rosters,
    )
    loader.load_weekly_stats([2024, 2025])
    assert [call for call in fake.calls if call[0] == "load_pbp"] == [("load_pbp", ([2025],))]


def test_a_published_row_survives_beside_derived_rows(nflverse, pbp, published_rosters):
    nflverse(
        load_player_stats=lambda seasons: (
            published_weekly(2024, 12.5) if seasons == [2024] else unavailable()
        ),
        load_pbp=pbp,
        load_rosters=published_rosters,
    )
    weekly = loader.load_weekly_stats([2024, 2025])
    published = weekly.filter(pl.col("season") == 2024)
    assert published["fantasy_points_ppr"].to_list() == [12.5]


def test_derivation_proceeds_when_the_rosters_are_unpublished(nflverse, pbp):
    nflverse(
        load_player_stats=unavailable,
        load_pbp=pbp,
        load_rosters=unavailable,
    )
    weekly = loader.load_weekly_stats([2025])
    assert nyg_receiver_week_one(weekly)["fantasy_points_ppr"].to_list() == [6.0]


def test_a_stalled_roster_download_propagates(nflverse, pbp):
    def stalled(*args: Any) -> pl.DataFrame:
        raise TimeoutError("Download timed out after 120s.")

    nflverse(
        load_player_stats=unavailable,
        load_pbp=pbp,
        load_rosters=stalled,
    )
    with pytest.raises(TimeoutError):
        loader.load_weekly_stats([2025])


@pytest.mark.parametrize("failure", [KeyError, RuntimeError])
def test_a_roster_failure_that_is_not_unavailability_propagates(nflverse, pbp, failure):
    def refuse(seasons: list[int]) -> pl.DataFrame:
        raise failure("upstream refused the request")

    nflverse(
        load_player_stats=unavailable,
        load_pbp=pbp,
        load_rosters=refuse,
    )
    with pytest.raises(failure):
        loader.load_weekly_stats([2025])


def test_a_roster_failure_that_is_not_unavailability_stores_nothing(nflverse, pbp, cache_dir):
    def refuse(seasons: list[int]) -> pl.DataFrame:
        raise RuntimeError("upstream refused the request")

    nflverse(
        load_player_stats=unavailable,
        load_pbp=pbp,
        load_rosters=refuse,
    )
    with pytest.raises(RuntimeError):
        loader.load_weekly_stats([2025])
    assert list(cache_dir.glob("*.parquet")) == []


@pytest.mark.parametrize("failure", [KeyError, RuntimeError, TimeoutError])
def test_a_weekly_failure_that_is_not_unavailability_propagates(nflverse, failure):
    def refuse(seasons: list[int]) -> pl.DataFrame:
        raise failure("upstream refused the request")

    nflverse(load_player_stats=refuse, load_rosters=unavailable)
    with pytest.raises(failure):
        loader.load_weekly_stats([2025])


@pytest.mark.parametrize("failure", [KeyError, RuntimeError, TimeoutError])
def test_a_weekly_failure_that_is_not_unavailability_skips_the_fallback(nflverse, failure):
    def refuse(seasons: list[int]) -> pl.DataFrame:
        raise failure("upstream refused the request")

    fake = nflverse(load_player_stats=refuse)
    with pytest.raises(failure):
        loader.load_weekly_stats([2025])
    assert fake.names == ["load_player_stats"]


def test_a_weekly_failure_that_is_not_unavailability_stores_nothing(nflverse, cache_dir):
    def refuse(seasons: list[int]) -> pl.DataFrame:
        raise RuntimeError("upstream refused the request")

    nflverse(load_player_stats=refuse, load_rosters=unavailable)
    with pytest.raises(RuntimeError):
        loader.load_weekly_stats([2025])
    assert list(cache_dir.glob("*.parquet")) == []


@pytest.mark.parametrize("published", [(True, True), (True, False), (False, True), (False, False)])
def test_a_request_naming_seasons_yields_rows_however_they_are_sourced(
    nflverse, pbp, published_rosters, published
):
    """Every named season lands in the published set or the fallback, never neither."""
    sourced = dict(zip([2024, 2025], published, strict=True))
    nflverse(
        load_player_stats=lambda seasons: (
            published_weekly(seasons[0], 12.5) if sourced[seasons[0]] else unavailable()
        ),
        load_pbp=pbp,
        load_rosters=published_rosters,
    )
    assert loader.load_weekly_stats([2024, 2025]).height > 0


def test_a_request_for_no_seasons_yields_no_weekly_stats(nflverse):
    nflverse()
    with pytest.raises(RuntimeError, match="No weekly stats available"):
        loader.load_weekly_stats([])


def test_a_request_for_no_seasons_is_the_condition_the_message_names(nflverse):
    nflverse()
    with pytest.raises(RuntimeError, match="no seasons requested"):
        loader.load_weekly_stats([])


def test_a_request_for_no_seasons_reaches_no_loader(nflverse):
    fake = nflverse()
    with pytest.raises(RuntimeError):
        loader.load_weekly_stats([])
    assert fake.names == []


def test_derived_weekly_stats_are_cached_under_their_season_key(
    nflverse, pbp, published_rosters, cache_dir
):
    nflverse(
        load_player_stats=unavailable,
        load_pbp=pbp,
        load_rosters=published_rosters,
    )
    loader.load_weekly_stats([2025])
    assert (cache_dir / "weekly_stats_2025.parquet").exists()


def test_a_derived_season_is_fetched_once_and_then_served_from_the_cache(
    nflverse, pbp, published_rosters
):
    fake = nflverse(
        load_player_stats=unavailable,
        load_pbp=pbp,
        load_rosters=published_rosters,
    )
    loader.load_weekly_stats([2025])
    loader.load_weekly_stats([2025])
    assert fake.names == [
        "load_player_stats",
        "load_pbp",
        "load_rosters",
    ]


def test_a_completed_load_leaves_no_temporary_file(nflverse, cache_dir):
    nflverse(load_snap_counts=marked(1))
    loader.load_snap_counts([2025])
    assert sorted(path.name for path in cache_dir.iterdir()) == [
        "_meta.json",
        "snap_counts_2025.parquet",
    ]


def test_a_stored_entry_is_recorded_under_the_key_it_was_stored_with(nflverse, cache_dir):
    nflverse(load_injuries=marked(1))
    loader.load_injuries([2024, 2025])
    meta = json.loads((cache_dir / "_meta.json").read_text())
    assert sorted(meta) == ["injuries_2024", "injuries_2025"]


# ── The record of what a load dropped ────────────────────────────────────────


def test_a_dropped_season_reaches_an_open_record(nflverse):
    """A returned frame names the seasons it holds, and nothing names the ones it does not."""
    nflverse(load_snap_counts=source(2025))
    with loader.collect_drops() as dropped:
        loader.load_snap_counts([2025, 2026])
    assert dropped == [("snap counts", 2026)]


def test_a_request_that_drops_nothing_records_nothing(nflverse):
    nflverse(load_snap_counts=source(2025, 2026))
    with loader.collect_drops() as dropped:
        loader.load_snap_counts([2025, 2026])
    assert dropped == []


def test_a_record_names_every_dataset_its_block_dropped(nflverse):
    """One load reaches several loaders, and each drops its seasons on its own."""
    nflverse(load_snap_counts=source(), load_injuries=source(2025))
    with loader.collect_drops() as dropped:
        with pytest.raises(RuntimeError):
            loader.load_snap_counts([2026])
        loader.load_injuries([2025, 2026])
    assert dropped == [("snap counts", 2026), ("injuries", 2026)]


def test_a_loader_outside_a_record_drops_its_season_all_the_same(nflverse):
    """The bookkeeping is the caller's to ask for; the tolerance is not conditional."""
    nflverse(load_snap_counts=source(2025))
    assert loader.load_snap_counts([2025, 2026])["season"].to_list() == [2025]


def test_a_record_hands_the_thread_back_to_the_one_around_it(nflverse):
    """Each block takes the drops inside it and leaves the block outside its own."""
    nflverse(load_snap_counts=source(2025), load_injuries=source(2025))
    with loader.collect_drops() as outer:
        with loader.collect_drops() as inner:
            loader.load_snap_counts([2025, 2026])
        loader.load_injuries([2025, 2026])
    assert inner == [("snap counts", 2026)]
    assert outer == [("injuries", 2026)]


# ── Seasons the source has not published ─────────────────────────────────────


def season_rows(season: int) -> pl.DataFrame:
    """A one-row frame carrying the season that produced it."""
    return pl.DataFrame({"season": [season]})


def out_of_range(*args: Any) -> pl.DataFrame:
    """A loader refusing a season before it reaches the network.

    Every nflreadpy loader range-checks the season against the earliest one its release
    holds and against the season it computes as current, and raises `ValueError`. The
    ceiling moves with the calendar, so a season this repository resolves can sit above
    it for the opening days of a September.
    """
    raise ValueError("Season must be between 1999 and 2026")


def empty(*args: Any) -> pl.DataFrame:
    """A loader filtering an all-seasons asset down to no rows.

    A loader whose release covers every season in one file answers a season it does not
    hold with an empty frame rather than by failing.
    """
    return pl.DataFrame({"season": []}, schema={"season": pl.Int64})


def source(
    *published: int,
    rows: Callable[[int], pl.DataFrame] = season_rows,
    absent: Callable[..., pl.DataFrame] = unavailable,
) -> Callable[[list[int]], pl.DataFrame]:
    """A loader holding `published` and no other season.

    An nflreadpy loader reads one release asset per season a request names and
    concatenates them only once every read has returned, so a request naming one season
    with no asset yields none of the seasons beside it. `absent` is the shape that
    failure takes.
    """

    def load(seasons: list[int]) -> pl.DataFrame:
        for season in seasons:
            if season not in published:
                absent()
        return pl.concat([rows(season) for season in seasons])

    return load


def roster_rows(season: int) -> pl.DataFrame:
    """A one-row roster frame in the shape the nflverse asset publishes."""
    return pl.DataFrame({"season": [season], "gsis_id": ["00-0999"], "position": ["WR"]})


def weekly_row(season: int) -> pl.DataFrame:
    """One published weekly row for `season`."""
    return published_weekly(season, 12.5)


#: ffb loader → the nflreadpy loader it reads a season list through, and the row shape
#: that loader yields. Rosters differ: the asset carries the id the loader renames.
SEASON_LOADERS = {
    "load_injuries": ("load_injuries", season_rows),
    "load_pbp": ("load_pbp", season_rows),
    "load_rosters": ("load_rosters", roster_rows),
    "load_schedules": ("load_schedules", season_rows),
    "load_snap_counts": ("load_snap_counts", season_rows),
}


@pytest.mark.parametrize(("name", "reads"), sorted(SEASON_LOADERS.items()))
def test_a_loader_taking_a_season_list_returns_the_seasons_the_source_holds(nflverse, name, reads):
    nflreadpy_loader, rows = reads
    nflverse(**{nflreadpy_loader: source(2025, rows=rows)})
    assert getattr(loader, name)([2025, 2026])["season"].to_list() == [2025]


@pytest.mark.parametrize(("name", "reads"), sorted(SEASON_LOADERS.items()))
def test_a_loader_taking_a_season_list_raises_when_the_source_holds_none_of_them(
    nflverse, name, reads
):
    nflreadpy_loader, rows = reads
    nflverse(**{nflreadpy_loader: source(rows=rows)})
    with pytest.raises(RuntimeError, match="obtained for"):
        getattr(loader, name)([2026])


def test_a_published_season_is_returned_beside_an_unpublished_one(nflverse):
    nflverse(load_snap_counts=source(2025))
    assert loader.load_snap_counts([2025, 2026])["season"].to_list() == [2025]


def test_each_season_is_asked_for_in_its_own_request(nflverse):
    fake = nflverse(load_snap_counts=source(2025, 2026))
    loader.load_snap_counts([2026, 2025])
    assert fake.calls == [
        ("load_snap_counts", ([2025],)),
        ("load_snap_counts", ([2026],)),
    ]


def _loaders_reached_for() -> set[str]:
    """Every nflreadpy loader `loader` reaches for, read off its own source.

    Reading the names rather than listing them here keeps this from being a second
    place a rename has to land, and therefore a second place it can be wrong.
    """
    tree = ast.parse(pathlib.Path(loader.__file__).read_text())
    names = {
        node.value
        for node in ast.walk(tree)
        if isinstance(node, ast.Constant)
        and isinstance(node.value, str)
        and node.value.startswith("load_")
    }
    names |= {
        node.attr
        for node in ast.walk(tree)
        if isinstance(node, ast.Attribute)
        and isinstance(node.value, ast.Call)
        and isinstance(node.value.func, ast.Name)
        and node.value.func.id == "_nflreadpy"
    }
    return names


def test_every_loader_reached_for_exists_on_the_module() -> None:
    """A loader is named by a string, which nothing but the module itself can check.

    `FakeNflreadpy` answers to whatever name a test describes, so a name wrong in both
    files passes every other test here and fails on the first live fetch.
    """
    import nflreadpy

    reached = _loaders_reached_for()
    assert reached, "no loader names were found in the source"
    assert sorted(name for name in reached if not hasattr(nflreadpy, name)) == []


def test_the_module_is_configured_before_anything_reads_through_it() -> None:
    """Settings taken from the environment would make a fetch depend on the shell.

    This reaches the real module because the configuration is what is under test: a
    stand-in would assert only that the test set what the test set.
    """
    from nflreadpy.config import get_config

    loader._nflreadpy.cache_clear()
    try:
        loader._nflreadpy()
        config = get_config()
        assert config.cache_mode.value == "memory"
        assert config.timeout == loader._DOWNLOAD_TIMEOUT
    finally:
        loader._nflreadpy.cache_clear()


def test_a_season_the_loader_refuses_by_range_contributes_no_rows(nflverse):
    """A refusal raised before the network still costs one season and no more.

    `ValueError` is not an `OSError`, so a catch written for the download alone would
    let this abort the whole request.
    """
    nflverse(load_pbp=source(2025, absent=out_of_range))
    assert loader.load_pbp([2025, 2026])["season"].to_list() == [2025]


def test_a_season_the_loader_answers_with_no_rows_contributes_none(nflverse, cache_dir):
    """An all-seasons asset filters rather than fails, and an empty answer is not data.

    The rows it contributes are none either way, so what pins the rule is the key it
    does not write: a stored emptiness would be served until the entry went stale,
    where a season left unwritten is asked for again.
    """
    nflverse(load_schedules=lambda seasons: season_rows(2025) if seasons == [2025] else empty())
    assert loader.load_schedules([2025, 2026])["season"].to_list() == [2025]
    assert sorted(path.name for path in cache_dir.glob("*.parquet")) == ["schedules_2025.parquet"]


def test_a_season_answered_with_no_rows_is_asked_for_again(nflverse):
    """Nothing negative is stored, so a season joins the next request that obtains it."""
    published: set[int] = {2025}
    fake = nflverse(
        load_schedules=lambda seasons: (
            season_rows(seasons[0]) if seasons[0] in published else empty()
        )
    )
    loader.load_schedules([2025, 2026])
    published.add(2026)
    assert loader.load_schedules([2025, 2026])["season"].to_list() == [2025, 2026]
    assert fake.names.count("load_schedules") == 3


def test_a_season_answered_with_no_rows_stores_nothing(nflverse, cache_dir):
    """Caching an empty answer would serve it until the entry went stale."""
    nflverse(load_schedules=empty)
    with pytest.raises(RuntimeError, match="obtained for"):
        loader.load_schedules([2026])
    assert list(cache_dir.glob("*.parquet")) == []


def test_a_request_holding_no_published_season_raises(nflverse):
    nflverse(load_snap_counts=source())
    with pytest.raises(RuntimeError):
        loader.load_snap_counts([2025, 2026])


def test_a_request_holding_no_published_season_names_the_seasons(nflverse):
    nflverse(load_snap_counts=source())
    with pytest.raises(RuntimeError, match="2025, 2026"):
        loader.load_snap_counts([2025, 2026])


def test_a_request_holding_no_published_season_blames_no_cause(nflverse):
    """A dropped season names what was not obtained, not why.

    A season drops on a missing release asset and on a failed transfer alike, and the
    loader cannot tell the two apart. Naming nflverse in the message would send a
    reader to wait on a publication that may already have happened.
    """
    nflverse(load_snap_counts=source())
    with pytest.raises(RuntimeError, match="^No snap counts obtained for seasons 2025, 2026$"):
        loader.load_snap_counts([2025, 2026])


def test_a_request_holding_no_published_season_names_the_dataset(nflverse):
    nflverse(load_injuries=source())
    with pytest.raises(RuntimeError, match="injur"):
        loader.load_injuries([2026])


def test_a_request_holding_no_published_season_stores_nothing(nflverse, cache_dir):
    nflverse(load_snap_counts=source())
    with pytest.raises(RuntimeError):
        loader.load_snap_counts([2025, 2026])
    assert list(cache_dir.glob("*.parquet")) == []


@pytest.mark.parametrize("failure", [KeyError, RuntimeError, TimeoutError])
def test_a_failure_that_is_not_unavailability_propagates(nflverse, failure):
    """A stalled transfer carries no information about what nflverse holds.

    `TimeoutError` derives from `OSError`, the exception a 404 arrives as, so a
    handler reading a missing season off `OSError` alone drops a season the source
    does publish.
    """

    def refuse(seasons: list[int]) -> pl.DataFrame:
        raise failure("upstream refused the request")

    nflverse(load_snap_counts=refuse)
    with pytest.raises(failure):
        loader.load_snap_counts([2025, 2026])


@pytest.mark.parametrize("failure", [KeyError, RuntimeError, TimeoutError])
def test_a_failure_that_is_not_unavailability_stores_nothing(nflverse, cache_dir, failure):
    def refuse(seasons: list[int]) -> pl.DataFrame:
        raise failure("upstream refused the request")

    nflverse(load_snap_counts=refuse)
    with pytest.raises(failure):
        loader.load_snap_counts([2025, 2026])
    assert list(cache_dir.glob("*.parquet")) == []


# ── Caching a request whose seasons resolve apart ────────────────────────────


def test_only_the_seasons_that_resolved_are_stored(nflverse, cache_dir):
    nflverse(load_snap_counts=source(2025))
    loader.load_snap_counts([2025, 2026])
    assert [path.name for path in cache_dir.glob("*.parquet")] == ["snap_counts_2025.parquet"]


def test_a_season_published_after_a_partial_load_reaches_the_result(nflverse):
    fake = nflverse(load_snap_counts=source(2025))
    loader.load_snap_counts([2025, 2026])
    fake.loaders["load_snap_counts"] = source(2025, 2026)
    assert loader.load_snap_counts([2025, 2026])["season"].to_list() == [2025, 2026]


def test_a_season_stored_by_a_partial_load_is_served_from_the_cache(nflverse):
    fake = nflverse(load_snap_counts=source(2025))
    loader.load_snap_counts([2025, 2026])
    loader.load_snap_counts([2025, 2026])
    assert fake.calls == [
        ("load_snap_counts", ([2025],)),
        ("load_snap_counts", ([2026],)),
        ("load_snap_counts", ([2026],)),
    ]


# ── Weekly stats for a season neither asset covers ───────────────────────────


def test_a_season_covered_by_neither_weekly_asset_nor_play_by_play_is_skipped(nflverse):
    nflverse(
        load_player_stats=source(2025, rows=weekly_row),
        load_pbp=source(absent=out_of_range),
        load_rosters=source(),
    )
    assert loader.load_weekly_stats([2025, 2026])["season"].to_list() == [2025]


def test_a_derived_season_survives_beside_a_season_neither_asset_covers(
    nflverse, pbp, published_rosters
):
    nflverse(
        load_player_stats=source(),
        load_pbp=lambda seasons: pbp if seasons == [2025] else unavailable(),
        load_rosters=lambda seasons: published_rosters if seasons == [2025] else unavailable(),
    )
    weekly = loader.load_weekly_stats([2025, 2026])
    assert weekly["season"].unique().to_list() == [2025]


def test_weekly_stats_covered_by_neither_asset_in_any_season_raise(nflverse):
    nflverse(
        load_player_stats=source(),
        load_pbp=source(absent=out_of_range),
        load_rosters=source(),
    )
    with pytest.raises(RuntimeError, match="obtained for"):
        loader.load_weekly_stats([2025, 2026])


def test_a_season_covered_by_neither_asset_stores_nothing(nflverse, cache_dir):
    nflverse(
        load_player_stats=source(2025, rows=weekly_row),
        load_pbp=source(absent=out_of_range),
        load_rosters=source(),
    )
    loader.load_weekly_stats([2025, 2026])
    assert [path.name for path in cache_dir.glob("*.parquet")] == ["weekly_stats_2025.parquet"]
