"""Data loaders wrapping nflreadpy with local caching.

nflverse publishes a season as its own set of release assets, and the per-game assets
— play-by-play, snap counts, weekly stats, injuries — appear only once games have been
played. A season the calendar names can therefore have no asset to read.

Every loader taking a season list resolves it one season at a time, so a season the
request does not obtain contributes no rows while the seasons beside it render. A
request that obtains none of its seasons raises rather than yielding an empty frame: a
frame with no columns has no `season` column for a screen to derive its filter options
from, and raises `ColumnNotFoundError` the moment an engine filters it.

What a request does not obtain, it cannot explain. A missing asset and a failed
transfer reach these loaders as the same exception, so nothing below names a cause.

nflreadpy spells a column as the nflverse release spells it. Where a release spells one
differently from the frames it is joined to, the loader returning it renames it here,
so no engine has to know two spellings for one thing.
"""

import logging
import threading
from collections.abc import Callable
from functools import lru_cache, partial
from typing import Any

import polars as pl

from ffb.data import cache

log = logging.getLogger(__name__)

#: Deadline for one loader call, which may cover several requests.
_DOWNLOAD_TIMEOUT = 120  # seconds

#: The message nflreadpy raises `ValueError` with for a season outside a loader's range.
#: Its downloader raises `ValueError` for anything that goes wrong after a response
#: arrives as well, and the message is what separates a season it declined from a
#: payload it could not read.
_OUT_OF_RANGE = "Season must be between"


class _SeasonUnavailable(Exception):
    """Signals one season a request did not obtain."""


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


@lru_cache(maxsize=1)
def _nflreadpy() -> Any:
    """The nflreadpy module, configured before anything reads through it.

    nflreadpy takes its settings from the environment and from a `.env` file in the
    working directory. Writing them here instead leaves what the process happens to
    inherit unable to change how a season is fetched.

    Its cache holds what it downloaded in memory, keyed by URL. That is worth keeping
    beside `cache`, which stores by season: a release that covers every season in one
    asset is downloaded once for a request naming several, where a per-season store has
    no way to know the two seasons came from one file. Nothing is written to disk
    twice, because only the memory mode is on.

    Its timeout governs one request, and setting it to the deadline `_download` already
    holds leaves that the only one that fires — a transfer that stalls is reported as a
    stall rather than as a season the request could not obtain.

    The import is deferred to first use, and the TUI reaches its first frame without
    paying for it.
    """
    import nflreadpy  # type: ignore[import-untyped]
    from nflreadpy.config import CacheMode, update_config  # type: ignore[import-untyped]

    update_config(cache_mode=CacheMode.MEMORY, timeout=_DOWNLOAD_TIMEOUT)
    return nflreadpy


def _nflverse_season(loader: str, season: int) -> pl.DataFrame:
    """Fetch one season through the named nflreadpy loader, or raise `_SeasonUnavailable`.

    A read that yields no season takes one of three shapes.

    An asset nflverse does not publish and a transfer that fails both arrive as the
    `ConnectionError` nflreadpy raises around the HTTP failure. It derives from
    `OSError`, and it is the same exception either way, which is what makes the cause
    unknowable at this layer.

    Some loaders range-check the season before requesting anything and raise
    `ValueError` for one their release does not reach. The ceiling moves with the
    calendar and is not the cut `seasons.season_for` makes, so a season this repository
    resolves can sit above it. `load_player_stats` and `load_schedules` do not check,
    and answer the same season with a 404 and an empty frame instead — which is why
    every shape below drops one season rather than the request.

    nflreadpy's downloader also raises `ValueError` for anything that goes wrong once a
    response has arrived, a payload it cannot parse among it. That is a failure to read
    what was fetched rather than a season the source will not give, so only the range
    check is caught here and the rest carries to the caller: a season silently missing
    is worse than a load that stops.

    A loader whose release covers every season in one asset filters that asset rather
    than failing, so a season it does not hold returns a frame with no rows. A season
    that yields no rows is a season not obtained, and raising rather than returning it
    keeps an empty frame out of the cache — the next request asks the source again
    instead of being served a stored emptiness for as long as the entry stays fresh.

    `TimeoutError` derives from `OSError` too, and the caller set that deadline, so it
    passes through as itself.
    """
    fetch = getattr(_nflreadpy(), loader)
    try:
        df: pl.DataFrame = _download(fetch, [season])
    except TimeoutError:
        raise
    except OSError as e:
        raise _SeasonUnavailable(f"{loader} returned no {season} data: {e}") from e
    except ValueError as e:
        if _OUT_OF_RANGE not in str(e):
            raise
        raise _SeasonUnavailable(f"{loader} does not reach {season}: {e}") from e

    if df.height == 0:
        raise _SeasonUnavailable(f"{loader} returned no {season} rows")
    return df


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
    at a time is what makes tolerance possible at all: an nflreadpy loader given a list
    reads the seasons in it one after another and raises on the first it cannot read, so
    one absent season in a request costs every season beside it. Storing one season per
    key follows from the same split — a request that resolves some of its seasons writes
    only the seasons it holds, so no key promises a season list it never obtained, and a
    season the source publishes between two requests is fetched by the second rather
    than served from a partial the first stored.

    Raising on a request that resolves nothing keeps an empty frame out of the engines.
    The message names the seasons it obtained nothing for. What it does not name is a
    cause: a season drops on a missing asset and on a failed transfer alike, and the
    two are indistinguishable at this layer.
    """
    if not seasons:
        raise RuntimeError(f"No {label} available: no seasons requested (got {seasons})")

    frames: list[pl.DataFrame] = []
    dropped: list[int] = []
    for season in sorted(set(seasons)):
        try:
            frames.append(_cached(f"{prefix}_{season}", fetch, season, force_refresh=force_refresh))
        except _SeasonUnavailable as e:
            log.warning("skipping %d, no %s obtained for it: %s", season, label, e)
            dropped.append(season)

    if not frames:
        raise RuntimeError(f"No {label} obtained for {_seasons_phrase(dropped)}")

    return pl.concat(frames, how="diagonal_relaxed")


def load_snap_counts(
    seasons: list[int],
    force_refresh: bool = False,
) -> pl.DataFrame:
    """Load snap count data for given seasons, with caching."""
    return _by_season(
        "snap counts",
        "snap_counts",
        partial(_nflverse_season, "load_snap_counts"),
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
        partial(_nflverse_season, "load_pbp"),
        seasons,
        force_refresh=force_refresh,
    )


def _seasonal_rosters(season: int) -> pl.DataFrame:
    """One season of rosters, carrying the player id under the name its joins use.

    The roster asset spells the player's GSIS id `gsis_id`. Play-by-play, weekly stats
    and every engine that joins them spell it `player_id`.
    """
    return _nflverse_season("load_rosters", season).rename({"gsis_id": "player_id"})


def load_rosters(
    seasons: list[int],
    force_refresh: bool = False,
) -> pl.DataFrame:
    """Load seasonal roster data for given seasons, with caching."""
    return _by_season(
        "rosters",
        "rosters",
        _seasonal_rosters,
        seasons,
        force_refresh=force_refresh,
    )


def _fetch_weekly_stats(season: int) -> pl.DataFrame:
    """One season of weekly player stats, derived from play-by-play where nflverse has none.

    The weekly asset updates through a season rather than landing complete at the end
    of one, so it covers a season being played. A season it does not carry at all is
    reconstructed from play-by-play, which holds the counting stats but names no
    position — the rosters are read for that path alone, and a season neither path
    covers raises `_SeasonUnavailable`, dropping it from the request.

    Position is read from `position_group` rather than from the asset's `position`,
    which labels a fullback FB and a two-way player by the side of the ball they are
    listed on; `OFFENSIVE_POSITIONS` holds neither. `position_group` resolves both to
    the side they play and travels in the frame that carries them, so the published
    path needs nothing fetched beside it.
    """
    from ffb.data.stats import compute_weekly_stats_from_pbp

    try:
        weekly = _nflverse_season("load_player_stats", season)
    except _SeasonUnavailable as e:
        log.info("no weekly stats asset for %d, deriving from play-by-play: %s", season, e)
    else:
        return weekly.drop("position").rename({"team": "recent_team", "position_group": "position"})

    pbp = _nflverse_season("load_pbp", season)
    rosters: pl.DataFrame | None
    try:
        rosters = _seasonal_rosters(season)
    except _SeasonUnavailable as e:
        log.warning("no rosters for %d, positions will be null: %s", season, e)
        rosters = None
    return compute_weekly_stats_from_pbp(pbp, rosters)


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


def _player_crosswalk() -> pl.DataFrame:
    """The player id table carrying the pfr_id and gsis_id spellings of one player.

    The table covers every season at once, so it is fetched whole rather than by season.
    """
    df: pl.DataFrame = _download(_nflreadpy().load_ff_playerids)
    return df


def load_player_ids(
    force_refresh: bool = False,
) -> pl.DataFrame:
    """Load player ID crosswalk table (pfr_id <-> gsis_id), with caching."""
    return _cached(
        "player_ids",
        _player_crosswalk,
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
        partial(_nflverse_season, "load_schedules"),
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
        partial(_nflverse_season, "load_injuries"),
        seasons,
        force_refresh=force_refresh,
    )
