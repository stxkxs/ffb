"""Tests for the caching, download and fallback behaviour around the loaders.

No test reaches the network: `FakeNflverse` replaces `loader._nflverse`, the single
seam every loader fetches through, and the download tests pass their own callable to
`loader._download`.
"""

import email.message
import json
import threading
import urllib.error
from collections.abc import Callable
from typing import Any

import polars as pl
import pytest

from ffb.data import cache, loader
from tests.conftest import league_gsis_id


class FakeNflverse:
    """Stand-in for `loader._nflverse` that records every call it is given.

    Each keyword names an nfl_data_py importer and gives what that importer yields:
    a frame to return, or a callable taking the arguments the loader passes. An
    importer with no keyword raises `KeyError`, so a loader reaching for data the
    test did not describe fails loudly.
    """

    def __init__(self, **importers: Any) -> None:
        self.importers = importers
        self.calls: list[tuple[str, tuple[Any, ...]]] = []

    def __call__(self, importer: str, *args: Any) -> pl.DataFrame:
        self.calls.append((importer, args))
        handler = self.importers[importer]
        return handler(*args) if callable(handler) else handler

    @property
    def names(self) -> list[str]:
        """The importers called, in call order."""
        return [name for name, _ in self.calls]


def marked(marker: int) -> pl.DataFrame:
    """A one-row frame whose `marker` column identifies which fetch produced it."""
    return pl.DataFrame({"marker": [marker]})


_RELEASE_URL = (
    "https://github.com/nflverse/nflverse-data/releases/download"
    "/player_stats/player_stats_2099.parquet"
)


def unavailable(*args: Any) -> pl.DataFrame:
    """An importer whose release asset is not published.

    nfl_data_py reads a season straight from its GitHub release asset URL, so a
    season with no asset surfaces as the 404 urllib raises: `HTTPError`, which
    derives from `OSError`. The fallback tests are pinned to that exception, so
    they hold against what nflverse hands the loader.
    """
    raise urllib.error.HTTPError(
        url=_RELEASE_URL,
        code=404,
        msg="Not Found",
        hdrs=email.message.Message(),
        fp=None,
    )


def published_weekly(season: int, points: float) -> pl.DataFrame:
    """One row of weekly stats in the shape nflverse publishes them."""
    return pl.DataFrame(
        {
            "player_id": ["00-0999"],
            "player_display_name": ["Published Starter"],
            "recent_team": ["NYG"],
            "position": ["WR"],
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
    """Install a `FakeNflverse` over the loader's fetch seam."""

    def install(**importers: Any) -> FakeNflverse:
        fake = FakeNflverse(**importers)
        monkeypatch.setattr(loader, "_nflverse", fake)
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


def test_a_miss_fetches_from_the_importer_the_loader_names(nflverse):
    fake = nflverse(import_snap_counts=marked(1))
    loader.load_snap_counts([2025])
    assert fake.calls == [("import_snap_counts", ([2025],))]


def test_a_fresh_entry_is_served_without_a_second_fetch(nflverse):
    fake = nflverse(import_snap_counts=marked(1))
    loader.load_snap_counts([2025])
    loader.load_snap_counts([2025])
    assert fake.names == ["import_snap_counts"]


def test_a_served_entry_carries_the_values_that_were_fetched(nflverse):
    nflverse(import_snap_counts=marked(7))
    loader.load_snap_counts([2025])
    assert loader.load_snap_counts([2025])["marker"].to_list() == [7]


def test_force_refresh_fetches_past_a_fresh_entry(nflverse):
    markers = iter([1, 2])
    fake = nflverse(import_snap_counts=lambda seasons: marked(next(markers)))
    loader.load_snap_counts([2025])
    loader.load_snap_counts([2025], force_refresh=True)
    assert fake.names == ["import_snap_counts", "import_snap_counts"]


def test_force_refresh_returns_the_frame_it_fetched(nflverse):
    markers = iter([1, 2])
    nflverse(import_snap_counts=lambda seasons: marked(next(markers)))
    loader.load_snap_counts([2025])
    assert loader.load_snap_counts([2025], force_refresh=True)["marker"].to_list() == [2]


def test_force_refresh_stores_the_frame_it_fetched(nflverse):
    markers = iter([1, 2])
    nflverse(import_snap_counts=lambda seasons: marked(next(markers)))
    loader.load_snap_counts([2025])
    loader.load_snap_counts([2025], force_refresh=True)
    # A third fetch would exhaust `markers`, so this frame comes from the cache.
    assert loader.load_snap_counts([2025])["marker"].to_list() == [2]


# ── Cache keys ───────────────────────────────────────────────────────────────


def test_a_key_names_the_dataset_and_the_one_season_it_holds(nflverse, cache_dir):
    nflverse(import_pbp_data=marked(1))
    loader.load_pbp([2025, 2024])
    assert sorted(path.name for path in cache_dir.glob("*.parquet")) == [
        "pbp_2024.parquet",
        "pbp_2025.parquet",
    ]


def test_a_season_asked_for_in_a_second_ordering_is_served_from_the_cache(nflverse):
    fake = nflverse(import_snap_counts=marked(1))
    loader.load_snap_counts([2024, 2025])
    loader.load_snap_counts([2025, 2024])
    # The two fetches are the first request's two seasons; the second request adds none.
    assert fake.names == ["import_snap_counts", "import_snap_counts"]


def test_datasets_sharing_a_season_hold_separate_entries(nflverse):
    nflverse(import_snap_counts=marked(1), import_pbp_data=marked(2))
    loader.load_snap_counts([2025])
    assert loader.load_pbp([2025])["marker"].to_list() == [2]


def test_distinct_seasons_of_one_dataset_hold_separate_entries(nflverse):
    nflverse(import_snap_counts=lambda seasons: marked(sum(seasons)))
    loader.load_snap_counts([2024])
    loader.load_snap_counts([2025])
    assert loader.load_snap_counts([2024])["marker"].to_list() == [2024]


def test_the_player_crosswalk_is_fetched_without_seasons(nflverse):
    fake = nflverse(import_ids=marked(1))
    loader.load_player_ids()
    assert fake.calls == [("import_ids", ())]


def test_the_player_crosswalk_is_stored_under_a_key_naming_no_seasons(nflverse, cache_dir):
    nflverse(import_ids=marked(1))
    loader.load_player_ids()
    assert [path.name for path in cache_dir.glob("*.parquet")] == ["player_ids.parquet"]


# ── Download supervision ─────────────────────────────────────────────────────


def test_a_download_returns_what_the_fetch_produced():
    assert loader._download(lambda: "frame") == "frame"


def test_a_download_forwards_its_positional_arguments():
    received: list[Any] = []
    loader._download(lambda *args: received.append(args), "import_pbp_data", [2025])
    assert received == [("import_pbp_data", [2025])]


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
    nflverse(import_weekly_data=lambda seasons: published_weekly(seasons[0], 12.5))
    weekly = loader.load_weekly_stats([2024])
    assert weekly["fantasy_points_ppr"].to_list() == [12.5]


def test_an_unpublished_season_is_derived_from_play_by_play(nflverse, pbp, rosters):
    nflverse(
        import_weekly_data=unavailable,
        import_pbp_data=pbp,
        import_seasonal_rosters=rosters,
    )
    weekly = loader.load_weekly_stats([2025])
    assert nyg_receiver_week_one(weekly)["fantasy_points_ppr"].to_list() == [6.0]


def test_derived_rows_count_the_targets_the_play_by_play_holds(nflverse, pbp, rosters):
    nflverse(
        import_weekly_data=unavailable,
        import_pbp_data=pbp,
        import_seasonal_rosters=rosters,
    )
    weekly = loader.load_weekly_stats([2025])
    assert nyg_receiver_week_one(weekly)["targets"].to_list() == [5.0]


def test_derived_rows_carry_the_position_the_rosters_list(nflverse, pbp, rosters):
    nflverse(
        import_weekly_data=unavailable,
        import_pbp_data=pbp,
        import_seasonal_rosters=rosters,
    )
    weekly = loader.load_weekly_stats([2025])
    assert nyg_receiver_week_one(weekly)["position"].to_list() == ["WR"]


def test_only_the_unpublished_seasons_reach_the_fallback(nflverse, pbp, rosters):
    fake = nflverse(
        import_weekly_data=lambda seasons: (
            published_weekly(2024, 12.5) if seasons == [2024] else unavailable()
        ),
        import_pbp_data=pbp,
        import_seasonal_rosters=rosters,
    )
    loader.load_weekly_stats([2024, 2025])
    assert [call for call in fake.calls if call[0] == "import_pbp_data"] == [
        ("import_pbp_data", ([2025],))
    ]


def test_a_published_row_survives_beside_derived_rows(nflverse, pbp, rosters):
    nflverse(
        import_weekly_data=lambda seasons: (
            published_weekly(2024, 12.5) if seasons == [2024] else unavailable()
        ),
        import_pbp_data=pbp,
        import_seasonal_rosters=rosters,
    )
    weekly = loader.load_weekly_stats([2024, 2025])
    published = weekly.filter(pl.col("season") == 2024)
    assert published["fantasy_points_ppr"].to_list() == [12.5]


def test_derivation_proceeds_when_the_rosters_are_unpublished(nflverse, pbp):
    nflverse(
        import_weekly_data=unavailable,
        import_pbp_data=pbp,
        import_seasonal_rosters=unavailable,
    )
    weekly = loader.load_weekly_stats([2025])
    assert nyg_receiver_week_one(weekly)["fantasy_points_ppr"].to_list() == [6.0]


def test_a_stalled_roster_download_propagates(nflverse, pbp):
    def stalled(*args: Any) -> pl.DataFrame:
        raise TimeoutError("Download timed out after 120s.")

    nflverse(
        import_weekly_data=unavailable,
        import_pbp_data=pbp,
        import_seasonal_rosters=stalled,
    )
    with pytest.raises(TimeoutError):
        loader.load_weekly_stats([2025])


@pytest.mark.parametrize("failure", [ValueError, AttributeError])
def test_a_roster_failure_that_is_not_unavailability_propagates(nflverse, pbp, failure):
    def refuse(seasons: list[int]) -> pl.DataFrame:
        raise failure("upstream refused the request")

    nflverse(
        import_weekly_data=unavailable,
        import_pbp_data=pbp,
        import_seasonal_rosters=refuse,
    )
    with pytest.raises(failure):
        loader.load_weekly_stats([2025])


def test_a_roster_failure_that_is_not_unavailability_stores_nothing(nflverse, pbp, cache_dir):
    def refuse(seasons: list[int]) -> pl.DataFrame:
        raise ValueError("upstream refused the request")

    nflverse(
        import_weekly_data=unavailable,
        import_pbp_data=pbp,
        import_seasonal_rosters=refuse,
    )
    with pytest.raises(ValueError):
        loader.load_weekly_stats([2025])
    assert list(cache_dir.glob("*.parquet")) == []


@pytest.mark.parametrize("failure", [ValueError, AttributeError, TimeoutError])
def test_a_weekly_failure_that_is_not_unavailability_propagates(nflverse, failure):
    def refuse(seasons: list[int]) -> pl.DataFrame:
        raise failure("upstream refused the request")

    nflverse(import_weekly_data=refuse)
    with pytest.raises(failure):
        loader.load_weekly_stats([2025])


@pytest.mark.parametrize("failure", [ValueError, AttributeError, TimeoutError])
def test_a_weekly_failure_that_is_not_unavailability_skips_the_fallback(nflverse, failure):
    def refuse(seasons: list[int]) -> pl.DataFrame:
        raise failure("upstream refused the request")

    fake = nflverse(import_weekly_data=refuse)
    with pytest.raises(failure):
        loader.load_weekly_stats([2025])
    assert fake.names == ["import_weekly_data"]


def test_a_weekly_failure_that_is_not_unavailability_stores_nothing(nflverse, cache_dir):
    def refuse(seasons: list[int]) -> pl.DataFrame:
        raise ValueError("upstream refused the request")

    nflverse(import_weekly_data=refuse)
    with pytest.raises(ValueError):
        loader.load_weekly_stats([2025])
    assert list(cache_dir.glob("*.parquet")) == []


@pytest.mark.parametrize("published", [(True, True), (True, False), (False, True), (False, False)])
def test_a_request_naming_seasons_yields_rows_however_they_are_sourced(
    nflverse, pbp, rosters, published
):
    """Every named season lands in the published set or the fallback, never neither."""
    sourced = dict(zip([2024, 2025], published, strict=True))
    nflverse(
        import_weekly_data=lambda seasons: (
            published_weekly(seasons[0], 12.5) if sourced[seasons[0]] else unavailable()
        ),
        import_pbp_data=pbp,
        import_seasonal_rosters=rosters,
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


def test_a_request_for_no_seasons_reaches_no_importer(nflverse):
    fake = nflverse()
    with pytest.raises(RuntimeError):
        loader.load_weekly_stats([])
    assert fake.names == []


def test_derived_weekly_stats_are_cached_under_their_season_key(nflverse, pbp, rosters, cache_dir):
    nflverse(
        import_weekly_data=unavailable,
        import_pbp_data=pbp,
        import_seasonal_rosters=rosters,
    )
    loader.load_weekly_stats([2025])
    assert (cache_dir / "weekly_stats_2025.parquet").exists()


def test_a_derived_season_is_fetched_once_and_then_served_from_the_cache(nflverse, pbp, rosters):
    fake = nflverse(
        import_weekly_data=unavailable,
        import_pbp_data=pbp,
        import_seasonal_rosters=rosters,
    )
    loader.load_weekly_stats([2025])
    loader.load_weekly_stats([2025])
    assert fake.names == [
        "import_weekly_data",
        "import_pbp_data",
        "import_seasonal_rosters",
    ]


def test_a_completed_load_leaves_no_temporary_file(nflverse, cache_dir):
    nflverse(import_snap_counts=marked(1))
    loader.load_snap_counts([2025])
    assert sorted(path.name for path in cache_dir.iterdir()) == [
        "_meta.json",
        "snap_counts_2025.parquet",
    ]


def test_a_stored_entry_is_recorded_under_the_key_it_was_stored_with(nflverse, cache_dir):
    nflverse(import_injuries=marked(1))
    loader.load_injuries([2024, 2025])
    meta = json.loads((cache_dir / "_meta.json").read_text())
    assert sorted(meta) == ["injuries_2024", "injuries_2025"]


# ── Seasons the source has not published ─────────────────────────────────────


def season_rows(season: int) -> pl.DataFrame:
    """A one-row frame carrying the season that produced it."""
    return pl.DataFrame({"season": [season]})


def undefined_name(*args: Any) -> pl.DataFrame:
    """An importer that cannot report the download it failed.

    `nfl_data_py.import_pbp_data` handles a failed download with `except Error as e`,
    and `Error` is bound nowhere in that module, so reaching the handler raises
    NameError over the failure that reached it. A season that importer holds no asset
    for arrives in this shape rather than as the 404 underneath it.
    """
    raise NameError("name 'Error' is not defined")


def source(
    *published: int,
    rows: Callable[[int], pl.DataFrame] = season_rows,
    absent: Callable[..., pl.DataFrame] = unavailable,
) -> Callable[[list[int]], pl.DataFrame]:
    """An importer holding `published` and no other season.

    nfl_data_py reads one release asset per season a request names and concatenates
    them only once every read has returned, so a request naming one season with no
    asset yields none of the seasons beside it. `absent` is the shape that failure
    takes.
    """

    def importer(seasons: list[int]) -> pl.DataFrame:
        for season in seasons:
            if season not in published:
                absent()
        return pl.concat([rows(season) for season in seasons])

    return importer


def weekly_row(season: int) -> pl.DataFrame:
    """One published weekly row for `season`."""
    return published_weekly(season, 12.5)


#: Loader name → the nfl_data_py importer it reads a season list through.
SEASON_LOADERS = {
    "load_injuries": "import_injuries",
    "load_pbp": "import_pbp_data",
    "load_rosters": "import_seasonal_rosters",
    "load_schedules": "import_schedules",
    "load_snap_counts": "import_snap_counts",
}


@pytest.mark.parametrize(("name", "importer"), sorted(SEASON_LOADERS.items()))
def test_a_loader_taking_a_season_list_returns_the_seasons_the_source_holds(
    nflverse, name, importer
):
    nflverse(**{importer: source(2025)})
    assert getattr(loader, name)([2025, 2026])["season"].to_list() == [2025]


@pytest.mark.parametrize(("name", "importer"), sorted(SEASON_LOADERS.items()))
def test_a_loader_taking_a_season_list_raises_when_the_source_holds_none_of_them(
    nflverse, name, importer
):
    nflverse(**{importer: source()})
    with pytest.raises(RuntimeError, match="nflverse has not published"):
        getattr(loader, name)([2026])


def test_a_published_season_is_returned_beside_an_unpublished_one(nflverse):
    nflverse(import_snap_counts=source(2025))
    assert loader.load_snap_counts([2025, 2026])["season"].to_list() == [2025]


def test_each_season_is_asked_for_in_its_own_request(nflverse):
    fake = nflverse(import_snap_counts=source(2025, 2026))
    loader.load_snap_counts([2026, 2025])
    assert fake.calls == [
        ("import_snap_counts", ([2025],)),
        ("import_snap_counts", ([2026],)),
    ]


def test_an_unpublished_season_reported_as_a_name_error_contributes_no_rows(nflverse):
    nflverse(import_pbp_data=source(2025, absent=undefined_name))
    assert loader.load_pbp([2025, 2026])["season"].to_list() == [2025]


def test_a_request_holding_no_published_season_raises(nflverse):
    nflverse(import_snap_counts=source())
    with pytest.raises(RuntimeError):
        loader.load_snap_counts([2025, 2026])


def test_a_request_holding_no_published_season_names_the_seasons(nflverse):
    nflverse(import_snap_counts=source())
    with pytest.raises(RuntimeError, match="2025, 2026"):
        loader.load_snap_counts([2025, 2026])


def test_a_request_holding_no_published_season_names_the_source(nflverse):
    nflverse(import_snap_counts=source())
    with pytest.raises(RuntimeError, match="nflverse has not published"):
        loader.load_snap_counts([2025, 2026])


def test_a_request_holding_no_published_season_names_the_dataset(nflverse):
    nflverse(import_injuries=source())
    with pytest.raises(RuntimeError, match="injur"):
        loader.load_injuries([2026])


def test_a_request_holding_no_published_season_stores_nothing(nflverse, cache_dir):
    nflverse(import_snap_counts=source())
    with pytest.raises(RuntimeError):
        loader.load_snap_counts([2025, 2026])
    assert list(cache_dir.glob("*.parquet")) == []


@pytest.mark.parametrize("failure", [ValueError, AttributeError, TimeoutError])
def test_a_failure_that_is_not_unavailability_propagates(nflverse, failure):
    """A stalled transfer carries no information about what nflverse holds.

    `TimeoutError` derives from `OSError`, the exception a 404 arrives as, so a
    handler reading a missing season off `OSError` alone drops a season the source
    does publish.
    """

    def refuse(seasons: list[int]) -> pl.DataFrame:
        raise failure("upstream refused the request")

    nflverse(import_snap_counts=refuse)
    with pytest.raises(failure):
        loader.load_snap_counts([2025, 2026])


@pytest.mark.parametrize("failure", [ValueError, AttributeError, TimeoutError])
def test_a_failure_that_is_not_unavailability_stores_nothing(nflverse, cache_dir, failure):
    def refuse(seasons: list[int]) -> pl.DataFrame:
        raise failure("upstream refused the request")

    nflverse(import_snap_counts=refuse)
    with pytest.raises(failure):
        loader.load_snap_counts([2025, 2026])
    assert list(cache_dir.glob("*.parquet")) == []


# ── Caching a request whose seasons resolve apart ────────────────────────────


def test_only_the_seasons_that_resolved_are_stored(nflverse, cache_dir):
    nflverse(import_snap_counts=source(2025))
    loader.load_snap_counts([2025, 2026])
    assert [path.name for path in cache_dir.glob("*.parquet")] == ["snap_counts_2025.parquet"]


def test_a_season_published_after_a_partial_load_reaches_the_result(nflverse):
    fake = nflverse(import_snap_counts=source(2025))
    loader.load_snap_counts([2025, 2026])
    fake.importers["import_snap_counts"] = source(2025, 2026)
    assert loader.load_snap_counts([2025, 2026])["season"].to_list() == [2025, 2026]


def test_a_season_stored_by_a_partial_load_is_served_from_the_cache(nflverse):
    fake = nflverse(import_snap_counts=source(2025))
    loader.load_snap_counts([2025, 2026])
    loader.load_snap_counts([2025, 2026])
    assert fake.calls == [
        ("import_snap_counts", ([2025],)),
        ("import_snap_counts", ([2026],)),
        ("import_snap_counts", ([2026],)),
    ]


# ── Weekly stats for a season neither asset covers ───────────────────────────


def test_a_season_covered_by_neither_weekly_asset_nor_play_by_play_is_skipped(nflverse):
    nflverse(
        import_weekly_data=source(2025, rows=weekly_row),
        import_pbp_data=source(absent=undefined_name),
        import_seasonal_rosters=source(),
    )
    assert loader.load_weekly_stats([2025, 2026])["season"].to_list() == [2025]


def test_a_derived_season_survives_beside_a_season_neither_asset_covers(nflverse, pbp, rosters):
    nflverse(
        import_weekly_data=source(),
        import_pbp_data=lambda seasons: pbp if seasons == [2025] else unavailable(),
        import_seasonal_rosters=lambda seasons: rosters if seasons == [2025] else unavailable(),
    )
    weekly = loader.load_weekly_stats([2025, 2026])
    assert weekly["season"].unique().to_list() == [2025]


def test_weekly_stats_covered_by_neither_asset_in_any_season_raise(nflverse):
    nflverse(
        import_weekly_data=source(),
        import_pbp_data=source(absent=undefined_name),
        import_seasonal_rosters=source(),
    )
    with pytest.raises(RuntimeError, match="nflverse has not published"):
        loader.load_weekly_stats([2025, 2026])


def test_a_season_covered_by_neither_asset_stores_nothing(nflverse, cache_dir):
    nflverse(
        import_weekly_data=source(2025, rows=weekly_row),
        import_pbp_data=source(absent=undefined_name),
        import_seasonal_rosters=source(),
    )
    loader.load_weekly_stats([2025, 2026])
    assert [path.name for path in cache_dir.glob("*.parquet")] == ["weekly_stats_2025.parquet"]
