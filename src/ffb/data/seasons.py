"""Season arithmetic for the nflverse season label.

nflverse labels a season by the calendar year it opens in: season 2025 runs from its
opening week in September 2025 to the Super Bowl in February 2026. A date therefore
names a season only once the month is known.

What this module resolves is the calendar and only the calendar: which season a date
belongs to, never whether that season has kicked off or nflverse has published a row
for it.
"""

from datetime import date

# An NFL season opens in September of the year that labels it and finishes by February
# of the year after, so cutting the calendar at 1 September gives every date exactly one
# label. March through August falls between one season's last game and the next season's
# first; those dates resolve to the season that finished.
SEASON_START_MONTH = 9


def season_for(day: date) -> int:
    """Return the nflverse season label that `day` falls in.

    The label is a calendar fact, not evidence of play. A date in the opening days of
    September resolves to a season whose first kickoff is still ahead of it, and
    nflverse publishes a season's schedule and rosters months before that opener but
    adds the per-game assets — play-by-play, snap counts, weekly stats, injuries — only
    once games have been played. The label this returns can therefore name a season
    whose per-game assets answer HTTP 404. Establishing what nflverse holds is the
    caller's job; this function resolves the calendar.

    Parameters
    ----------
    day : date
        Date to resolve.
    """
    return day.year if day.month >= SEASON_START_MONTH else day.year - 1


def recent_seasons(count: int = 2, day: date | None = None) -> list[int]:
    """Return the `count` most recent season labels, oldest first.

    The list ends at the season `day` falls in, so between 1 September and that
    season's opening kickoff the last entry names a season for which nflverse has
    published no per-game rows.

    Parameters
    ----------
    count : int
        How many seasons to return. Must be at least 1.
    day : date, optional
        Date to resolve against. Defaults to the system date.
    """
    if count < 1:
        raise ValueError(f"count must be at least 1, got {count}")
    latest = season_for(date.today() if day is None else day)
    return list(range(latest - count + 1, latest + 1))
