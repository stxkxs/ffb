"""Data loaders wrapping nfl_data_py with local caching."""

import logging
import threading
from collections.abc import Callable
from typing import Any

import polars as pl

from ffb.data import cache

log = logging.getLogger(__name__)

_DOWNLOAD_TIMEOUT = 120  # seconds


def _download(
    fetch: Callable[..., Any],
    *args: Any,
    timeout: int = _DOWNLOAD_TIMEOUT,
) -> Any:
    """Run a blocking download in a worker thread, giving up after `timeout` seconds.

    Each call owns its worker, so one download spends only its own timeout budget:
    a caller waiting on a stalled transfer neither delays a concurrent download nor
    eats into the deadline of the next one. Giving up cannot stop a socket read that
    is already under way, so the abandoned worker runs to completion with nothing
    left to hand its result to; it is a daemon, which keeps a stalled transfer from
    holding the interpreter open at exit.
    """
    outcome: dict[str, Any] = {}

    def run() -> None:
        # The caller sees whatever the fetch raised: holding it here and re-raising it
        # on the calling thread keeps the traceback out of the thread excepthook.
        try:
            outcome["value"] = fetch(*args)
        except BaseException as e:
            outcome["error"] = e

    worker = threading.Thread(target=run, name="ffb-download", daemon=True)
    worker.start()
    worker.join(timeout)

    if worker.is_alive():
        raise TimeoutError(f"Download timed out after {timeout}s. Check your network connection.")
    if "error" in outcome:
        raise outcome["error"]
    return outcome["value"]


def _nflverse(importer: str, *args: Any) -> pl.DataFrame:
    """Fetch a frame from the named nfl_data_py importer, under the download timeout.

    nfl_data_py is imported here rather than at module scope because it pulls in
    pandas and numpy, and the TUI reaches its first frame without paying that cost.
    """
    import nfl_data_py as nfl  # type: ignore[import-untyped]

    pdf = _download(getattr(nfl, importer), *args)
    return pl.from_pandas(pdf)


def _season_key(prefix: str, seasons: list[int]) -> str:
    """Cache key for a per-season dataset, identical for any ordering of `seasons`."""
    return f"{prefix}_{'_'.join(str(s) for s in sorted(seasons))}"


def _cached(
    key: str,
    fetch: Callable[..., pl.DataFrame],
    *args: Any,
    force_refresh: bool,
) -> pl.DataFrame:
    """Return the frame stored under `key`, fetching and storing it on a miss.

    `force_refresh` skips the cache read, not the write: the fetched frame replaces
    whatever the key held.
    """
    if not force_refresh:
        cached = cache.get(key)
        if cached is not None:
            return cached

    df = fetch(*args)
    cache.put(key, df)
    return df


def load_snap_counts(
    seasons: list[int],
    force_refresh: bool = False,
) -> pl.DataFrame:
    """Load snap count data for given seasons, with caching."""
    return _cached(
        _season_key("snap_counts", seasons),
        _nflverse,
        "import_snap_counts",
        seasons,
        force_refresh=force_refresh,
    )


def load_pbp(
    seasons: list[int],
    force_refresh: bool = False,
) -> pl.DataFrame:
    """Load play-by-play data for given seasons, with caching."""
    return _cached(
        _season_key("pbp", seasons),
        _nflverse,
        "import_pbp_data",
        seasons,
        force_refresh=force_refresh,
    )


def load_rosters(
    seasons: list[int],
    force_refresh: bool = False,
) -> pl.DataFrame:
    """Load seasonal roster data for given seasons, with caching."""
    return _cached(
        _season_key("rosters", seasons),
        _nflverse,
        "import_seasonal_rosters",
        seasons,
        force_refresh=force_refresh,
    )


def _fetch_weekly_stats(seasons: list[int]) -> pl.DataFrame:
    """Weekly player stats per season, derived from play-by-play where nflverse has none.

    nflverse publishes player_stats as one release asset per season, so a season in
    progress can have no asset at all while its play-by-play updates every week.
    Play-by-play carries what the weekly columns are built from, so a season missing
    the asset is reconstructed rather than dropped.
    """
    from ffb.data.stats import compute_weekly_stats_from_pbp

    frames: list[pl.DataFrame] = []
    pbp_fallback_seasons: list[int] = []

    for season in seasons:
        try:
            frames.append(_nflverse("import_weekly_data", [season]))
        except TimeoutError:
            # A transfer that stalls carries no information about what nflverse holds.
            raise
        except OSError as e:
            # An unpublished season has no release asset, so the request 404s.
            log.info("weekly data unavailable for %d, deriving from PBP: %s", season, e)
            pbp_fallback_seasons.append(season)

    if pbp_fallback_seasons:
        log.info("deriving weekly stats from PBP for seasons %s", pbp_fallback_seasons)
        pbp_df = _nflverse("import_pbp_data", pbp_fallback_seasons)
        roster_df: pl.DataFrame | None
        try:
            roster_df = _nflverse("import_seasonal_rosters", pbp_fallback_seasons)
        except TimeoutError:
            # A stalled transfer is not a missing roster asset.
            raise
        except OSError as e:
            log.warning(
                "roster data unavailable for seasons %s, positions will be null: %s",
                pbp_fallback_seasons,
                e,
            )
            roster_df = None
        frames.append(compute_weekly_stats_from_pbp(pbp_df, roster_df))

    if not frames:
        # Every requested season lands in either the published set or the PBP
        # fallback, so an empty frame list means the request named no seasons.
        raise RuntimeError(f"No weekly stats available: no seasons requested (got {seasons})")

    return pl.concat(frames, how="diagonal_relaxed")


def load_weekly_stats(
    seasons: list[int],
    force_refresh: bool = False,
) -> pl.DataFrame:
    """Load weekly player stats for given seasons, with caching."""
    return _cached(
        _season_key("weekly_stats", seasons),
        _fetch_weekly_stats,
        seasons,
        force_refresh=force_refresh,
    )


def load_player_ids(
    force_refresh: bool = False,
) -> pl.DataFrame:
    """Load player ID crosswalk table (pfr_id <-> gsis_id), with caching."""
    return _cached(
        "player_ids",
        _nflverse,
        "import_ids",
        force_refresh=force_refresh,
    )


def load_schedules(
    seasons: list[int],
    force_refresh: bool = False,
) -> pl.DataFrame:
    """Load schedule data for given seasons, with caching."""
    return _cached(
        _season_key("schedules", seasons),
        _nflverse,
        "import_schedules",
        seasons,
        force_refresh=force_refresh,
    )


def load_injuries(
    seasons: list[int],
    force_refresh: bool = False,
) -> pl.DataFrame:
    """Load injury report data for given seasons, with caching."""
    return _cached(
        _season_key("injuries", seasons),
        _nflverse,
        "import_injuries",
        seasons,
        force_refresh=force_refresh,
    )
