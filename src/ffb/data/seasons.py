"""Season arithmetic for the nflverse season label.

nflverse labels a season by the calendar year it opens in: season 2025 runs from
September 2025 through the playoffs in February 2026. A date therefore names a
season only once the month is known, which is what this module resolves.
"""

from datetime import date

# Week 1 falls in September, so a date earlier in the year belongs to the season
# labelled with the previous year.
SEASON_START_MONTH = 9


def current_season(today: date | None = None) -> int:
    """Return the label of the most recent season to have played games.

    Parameters
    ----------
    today : date, optional
        Date to resolve against. Defaults to the system date.
    """
    day = date.today() if today is None else today
    return day.year if day.month >= SEASON_START_MONTH else day.year - 1


def recent_seasons(count: int = 2, today: date | None = None) -> list[int]:
    """Return the `count` most recent season labels, oldest first.

    The list ends at `current_season`, so a loader given these seasons asks nflverse
    only for data it has begun publishing.

    Parameters
    ----------
    count : int
        How many seasons to return. Must be at least 1.
    today : date, optional
        Date to resolve against. Defaults to the system date.
    """
    if count < 1:
        raise ValueError(f"count must be at least 1, got {count}")
    latest = current_season(today)
    return list(range(latest - count + 1, latest + 1))
