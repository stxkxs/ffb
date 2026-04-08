"""Data loaders wrapping nfl_data_py with local caching."""

import logging
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import TimeoutError as FuturesTimeout

import polars as pl

from ffb.data import cache

log = logging.getLogger(__name__)

_DOWNLOAD_TIMEOUT = 120  # seconds
_executor = ThreadPoolExecutor(max_workers=1)


def _fetch_with_timeout(fn, *args, timeout: int = _DOWNLOAD_TIMEOUT):
    """Run a blocking download with a timeout."""
    future = _executor.submit(fn, *args)
    try:
        return future.result(timeout=timeout)
    except FuturesTimeout:
        raise TimeoutError(
            f"Download timed out after {timeout}s. Check your network connection."
        ) from None


def load_snap_counts(
    seasons: list[int],
    force_refresh: bool = False,
) -> pl.DataFrame:
    """Load snap count data for given seasons, with caching."""
    key = f"snap_counts_{'_'.join(str(s) for s in sorted(seasons))}"

    if not force_refresh:
        cached = cache.get(key)
        if cached is not None:
            return cached

    import nfl_data_py as nfl

    pdf = _fetch_with_timeout(nfl.import_snap_counts, seasons)
    df = pl.from_pandas(pdf)

    cache.put(key, df)
    return df


def load_pbp(
    seasons: list[int],
    force_refresh: bool = False,
) -> pl.DataFrame:
    """Load play-by-play data for given seasons, with caching."""
    key = f"pbp_{'_'.join(str(s) for s in sorted(seasons))}"

    if not force_refresh:
        cached = cache.get(key)
        if cached is not None:
            return cached

    import nfl_data_py as nfl

    pdf = _fetch_with_timeout(nfl.import_pbp_data, seasons)
    df = pl.from_pandas(pdf)

    cache.put(key, df)
    return df


def load_rosters(
    seasons: list[int],
    force_refresh: bool = False,
) -> pl.DataFrame:
    """Load seasonal roster data for given seasons, with caching."""
    key = f"rosters_{'_'.join(str(s) for s in sorted(seasons))}"

    if not force_refresh:
        cached = cache.get(key)
        if cached is not None:
            return cached

    import nfl_data_py as nfl

    pdf = _fetch_with_timeout(nfl.import_seasonal_rosters, seasons)
    df = pl.from_pandas(pdf)

    cache.put(key, df)
    return df


def load_weekly_stats(
    seasons: list[int],
    force_refresh: bool = False,
) -> pl.DataFrame:
    """Load weekly player stats for given seasons, with caching."""
    key = f"weekly_stats_{'_'.join(str(s) for s in sorted(seasons))}"

    if not force_refresh:
        cached = cache.get(key)
        if cached is not None:
            return cached

    import nfl_data_py as nfl

    from ffb.data.stats import compute_weekly_stats_from_pbp

    # Load each season: try nflverse weekly data first, fall back to PBP derivation
    frames: list[pl.DataFrame] = []
    pbp_fallback_seasons: list[int] = []

    for season in seasons:
        try:
            pdf = _fetch_with_timeout(nfl.import_weekly_data, [season])
            frames.append(pl.from_pandas(pdf))
        except Exception:
            log.info("weekly data unavailable for %d, will derive from PBP", season)
            pbp_fallback_seasons.append(season)

    # Derive stats from PBP for seasons missing weekly data
    if pbp_fallback_seasons:
        log.info("deriving weekly stats from PBP for seasons %s", pbp_fallback_seasons)
        pbp_pdf = _fetch_with_timeout(nfl.import_pbp_data, pbp_fallback_seasons)
        pbp_df = pl.from_pandas(pbp_pdf)
        try:
            roster_pdf = _fetch_with_timeout(nfl.import_seasonal_rosters, pbp_fallback_seasons)
            roster_df = pl.from_pandas(roster_pdf)
        except Exception:
            log.warning("roster data unavailable for PBP fallback seasons, positions will be null")
            roster_df = None
        frames.append(compute_weekly_stats_from_pbp(pbp_df, roster_df))

    if not frames:
        raise RuntimeError(
            f"No weekly stats available for seasons {seasons} "
            "(nflverse and PBP fallback both failed)"
        )

    df = pl.concat(frames, how="diagonal_relaxed")

    cache.put(key, df)
    return df


def load_player_ids(
    force_refresh: bool = False,
) -> pl.DataFrame:
    """Load player ID crosswalk table (pfr_id <-> gsis_id), with caching."""
    key = "player_ids"

    if not force_refresh:
        cached = cache.get(key)
        if cached is not None:
            return cached

    import nfl_data_py as nfl

    pdf = _fetch_with_timeout(nfl.import_ids)
    df = pl.from_pandas(pdf)

    cache.put(key, df)
    return df


def load_schedules(
    seasons: list[int],
    force_refresh: bool = False,
) -> pl.DataFrame:
    """Load schedule data for given seasons, with caching."""
    key = f"schedules_{'_'.join(str(s) for s in sorted(seasons))}"

    if not force_refresh:
        cached = cache.get(key)
        if cached is not None:
            return cached

    import nfl_data_py as nfl

    pdf = _fetch_with_timeout(nfl.import_schedules, seasons)
    df = pl.from_pandas(pdf)

    cache.put(key, df)
    return df


def load_injuries(
    seasons: list[int],
    force_refresh: bool = False,
) -> pl.DataFrame:
    """Load injury report data for given seasons, with caching."""
    key = f"injuries_{'_'.join(str(s) for s in sorted(seasons))}"

    if not force_refresh:
        cached = cache.get(key)
        if cached is not None:
            return cached

    import nfl_data_py as nfl

    pdf = _fetch_with_timeout(nfl.import_injuries, seasons)
    df = pl.from_pandas(pdf)

    cache.put(key, df)
    return df
