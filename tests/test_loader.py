"""Tests for the caching, download and fallback behaviour around the loaders.

No test reaches the network: `FakeNflverse` replaces `loader._nflverse`, the single
seam every loader fetches through, and the download tests pass their own callable to
`loader._download`.
"""

import email.message
import json
import threading
import urllib.error
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
    loader.load_snap_counts([2024, 2025])
    assert fake.calls == [("import_snap_counts", ([2024, 2025],))]


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


def test_a_season_key_names_the_dataset_and_its_seasons_ascending(nflverse, cache_dir):
    nflverse(import_pbp_data=marked(1))
    loader.load_pbp([2025, 2024])
    assert (cache_dir / "pbp_2024_2025.parquet").exists()


def test_a_season_key_ignores_the_order_the_seasons_are_asked_for(nflverse):
    fake = nflverse(import_snap_counts=marked(1))
    loader.load_snap_counts([2024, 2025])
    loader.load_snap_counts([2025, 2024])
    assert fake.names == ["import_snap_counts"]


def test_datasets_sharing_a_season_hold_separate_entries(nflverse):
    nflverse(import_snap_counts=marked(1), import_pbp_data=marked(2))
    loader.load_snap_counts([2025])
    assert loader.load_pbp([2025])["marker"].to_list() == [2]


def test_distinct_season_lists_hold_separate_entries(nflverse):
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
    assert list(meta) == ["injuries_2024_2025"]
