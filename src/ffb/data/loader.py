"""Data loaders wrapping nfl_data_py with local caching.

nflverse publishes a season as its own set of release assets, and the per-game assets
— play-by-play, snap counts, weekly stats, injuries — appear only once games have been
played. A season the calendar names can therefore have no asset to read.

Every loader taking a season list resolves it one season at a time, so a season with no
asset contributes no rows while the seasons beside it render. A request whose every
season is unpublished raises rather than yielding an empty frame: a frame with no
columns has no `season` column for a screen to derive its filter options from, and
raises `ColumnNotFoundError` the moment an engine filters it.
"""

import logging
import threading
from collections.abc import Callable
from functools import partial
from typing import Any

import polars as pl

from ffb.data import cache

log = logging.getLogger(__name__)

_DOWNLOAD_TIMEOUT = 120  # seconds


class _SeasonUnavailable(Exception):
    """Signals one season the source holds no asset for."""


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


def _nflverse_season(importer: str, season: int) -> pl.DataFrame:
    """Fetch one season from the named importer, or raise `_SeasonUnavailable`.

    A season with no release asset takes one of two shapes. nfl_data_py reads the asset
    URL directly, so a missing asset arrives as the 404 urllib raises: `HTTPError`,
    which derives from `OSError`.

    `import_pbp_data` cannot report a failed download at all. Its download handler
    reads `except Error as e`, and `Error` is bound nowhere in that module, so reaching
    the handler raises `NameError` in place of the failure that reached it — run
    `inspect.getsource(nfl.import_pbp_data)` to read the handler. `NameError` is
    therefore the only signal that importer emits for a season it cannot download.

    `TimeoutError` derives from `OSError` too, and a stalled transfer says nothing
    about what nflverse holds, so it passes through as itself.

    Narrowing the catch to this one call into nfl_data_py keeps a `NameError` raised
    anywhere in this repository a `NameError`.
    """
    try:
        return _nflverse(importer, [season])
    except TimeoutError:
        raise
    except (OSError, NameError) as e:
        raise _SeasonUnavailable(f"{importer} holds no {season} data: {e}") from e


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


def _seasons_phrase(seasons: list[int]) -> str:
    """`seasons` as a phrase naming each one: "season 2026", "seasons 2025, 2026"."""
    noun = "season" if len(seasons) == 1 else "seasons"
    return f"{noun} {', '.join(str(season) for season in seasons)}"


def _by_season(
    label: str,
    prefix: str,
    fetch: Callable[[int], pl.DataFrame],
    seasons: list[int],
    force_refresh: bool,
) -> pl.DataFrame:
    """Concatenate one cached frame per season, dropping the seasons the source has none of.

    A season is the unit of both the request and the cache entry. Asking for one season
    at a time is what makes tolerance possible at all: nfl_data_py returns a multi-season
    request only once every season in it has been read, so one unpublished season in a
    request costs every published season beside it. Storing one season per key follows
    from the same split — a request that resolves some of its seasons writes only the
    seasons it holds, so no key promises a season list it never obtained, and a season
    the source publishes between two requests is fetched by the second rather than
    served from a partial the first stored.

    Raising on a request that resolves nothing keeps an empty frame out of the engines.
    The message names the seasons and the source, so the error a screen reports says
    which season to wait on.
    """
    if not seasons:
        raise RuntimeError(f"No {label} available: no seasons requested (got {seasons})")

    frames: list[pl.DataFrame] = []
    unpublished: list[int] = []
    for season in sorted(set(seasons)):
        try:
            frames.append(_cached(f"{prefix}_{season}", fetch, season, force_refresh=force_refresh))
        except _SeasonUnavailable as e:
            log.warning("skipping %d, nflverse has published no %s for it: %s", season, label, e)
            unpublished.append(season)

    if not frames:
        raise RuntimeError(f"nflverse has not published {label} for {_seasons_phrase(unpublished)}")

    return pl.concat(frames, how="diagonal_relaxed")


def load_snap_counts(
    seasons: list[int],
    force_refresh: bool = False,
) -> pl.DataFrame:
    """Load snap count data for given seasons, with caching."""
    return _by_season(
        "snap counts",
        "snap_counts",
        partial(_nflverse_season, "import_snap_counts"),
        seasons,
        force_refresh=force_refresh,
    )


def load_pbp(
    seasons: list[int],
    force_refresh: bool = False,
) -> pl.DataFrame:
    """Load play-by-play data for given seasons, with caching."""
    return _by_season(
        "play-by-play",
        "pbp",
        partial(_nflverse_season, "import_pbp_data"),
        seasons,
        force_refresh=force_refresh,
    )


def load_rosters(
    seasons: list[int],
    force_refresh: bool = False,
) -> pl.DataFrame:
    """Load seasonal roster data for given seasons, with caching."""
    return _by_season(
        "rosters",
        "rosters",
        partial(_nflverse_season, "import_seasonal_rosters"),
        seasons,
        force_refresh=force_refresh,
    )


def _fetch_weekly_stats(season: int) -> pl.DataFrame:
    """One season of weekly player stats, derived from play-by-play where nflverse has none.

    nflverse publishes player_stats as one release asset per season, so a season in
    progress can have no asset at all while its play-by-play updates every week.
    Play-by-play carries what the weekly columns are built from, so a season missing
    the asset is reconstructed rather than dropped. A season neither asset covers
    raises `_SeasonUnavailable`, which drops that season from the request.
    """
    from ffb.data.stats import compute_weekly_stats_from_pbp

    try:
        return _nflverse_season("import_weekly_data", season)
    except _SeasonUnavailable as e:
        log.info("weekly data unavailable for %d, deriving from PBP: %s", season, e)

    pbp_df = _nflverse_season("import_pbp_data", season)
    roster_df: pl.DataFrame | None
    try:
        roster_df = _nflverse_season("import_seasonal_rosters", season)
    except _SeasonUnavailable as e:
        log.warning("roster data unavailable for %d, positions will be null: %s", season, e)
        roster_df = None
    return compute_weekly_stats_from_pbp(pbp_df, roster_df)


def load_weekly_stats(
    seasons: list[int],
    force_refresh: bool = False,
) -> pl.DataFrame:
    """Load weekly player stats for given seasons, with caching."""
    return _by_season(
        "weekly stats",
        "weekly_stats",
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
    return _by_season(
        "schedules",
        "schedules",
        partial(_nflverse_season, "import_schedules"),
        seasons,
        force_refresh=force_refresh,
    )


def load_injuries(
    seasons: list[int],
    force_refresh: bool = False,
) -> pl.DataFrame:
    """Load injury report data for given seasons, with caching."""
    return _by_season(
        "injuries",
        "injuries",
        partial(_nflverse_season, "import_injuries"),
        seasons,
        force_refresh=force_refresh,
    )
