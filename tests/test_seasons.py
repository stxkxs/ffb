"""Tests for nflverse season arithmetic.

Every case fixes the date it resolves against, so a run on one day agrees with a run on
any other.
"""

from datetime import date

import pytest

from ffb.data import seasons
from ffb.data.seasons import recent_seasons, season_for


@pytest.mark.parametrize("month", [1, 2, 3, 4, 5, 6, 7, 8])
def test_a_month_before_september_belongs_to_the_prior_season(month):
    assert season_for(date(2025, month, 15)) == 2024


@pytest.mark.parametrize("month", [9, 10, 11, 12])
def test_september_onward_belongs_to_the_season_the_year_labels(month):
    assert season_for(date(2025, month, 15)) == 2025


def test_the_last_day_of_august_belongs_to_the_prior_season():
    assert season_for(date(2025, 8, 31)) == 2024


def test_the_first_day_of_september_opens_the_season_the_year_labels():
    assert season_for(date(2025, 9, 1)) == 2025


def test_a_september_date_ahead_of_the_opener_resolves_to_the_season_it_opens():
    """The label is the calendar's answer, not a claim that a game has been played.

    The 2026 season opens on 9 September 2026, so the first days of that month sit
    inside a season with no completed games.
    """
    assert season_for(date(2026, 9, 1)) == 2026
    assert season_for(date(2026, 9, 8)) == 2026


def test_an_opening_day_early_in_september_resolves_to_the_season_it_opens():
    """The 2000 season opened on 3 September, two days into the boundary month."""
    assert season_for(date(2000, 9, 3)) == 2000


def test_a_month_between_seasons_belongs_to_the_season_that_finished():
    assert season_for(date(2026, 3, 1)) == 2025
    assert season_for(date(2026, 6, 15)) == 2025


def test_january_belongs_to_the_season_that_opened_the_prior_autumn():
    assert season_for(date(2026, 1, 1)) == 2025


def test_february_belongs_to_the_season_that_opened_the_prior_autumn():
    assert season_for(date(2026, 2, 28)) == 2025


def test_a_mid_february_super_bowl_belongs_to_the_season_that_opened_the_prior_autumn():
    """The 2021 season closed on 13 February 2022, deep into the calendar year after it opened."""
    assert season_for(date(2022, 2, 13)) == 2021


def test_a_leap_day_belongs_to_the_season_that_opened_the_prior_autumn():
    assert season_for(date(2024, 2, 29)) == 2023


def test_the_last_day_of_december_belongs_to_the_season_the_year_labels():
    assert season_for(date(2025, 12, 31)) == 2025


def test_the_label_advances_once_a_year_between_august_and_september():
    labels = [season_for(date(2025, month, 1)) for month in range(1, 13)]
    assert labels == [2024] * 8 + [2025] * 4


def test_recent_seasons_lists_consecutive_labels_oldest_first():
    assert recent_seasons(4, date(2026, 1, 5)) == [2022, 2023, 2024, 2025]


@pytest.mark.parametrize("count", [1, 2, 3, 10])
def test_recent_seasons_returns_the_requested_count(count):
    assert len(recent_seasons(count, date(2025, 10, 1))) == count


@pytest.mark.parametrize(
    "day",
    [date(2025, 8, 31), date(2025, 9, 1), date(2026, 2, 1), date(2025, 12, 31)],
)
def test_recent_seasons_ends_at_the_season_the_date_falls_in(day):
    assert recent_seasons(3, day)[-1] == season_for(day)


def test_recent_seasons_of_one_holds_the_resolved_season_alone():
    assert recent_seasons(1, date(2025, 9, 1)) == [2025]


def test_recent_seasons_defaults_to_a_pair():
    assert recent_seasons(day=date(2025, 9, 1)) == [2024, 2025]


def test_recent_seasons_crosses_the_september_boundary_with_the_resolved_season():
    assert recent_seasons(2, date(2025, 8, 31)) == [2023, 2024]


def test_recent_seasons_spans_the_opener_with_a_season_holding_no_per_game_rows():
    """A window taken before the opener carries the season about to start.

    A caller filters on the seasons its loaded rows carry, so a season contributing
    none drops out of the filter rather than emptying the view.
    """
    assert recent_seasons(2, date(2026, 9, 1)) == [2025, 2026]


def test_an_omitted_date_resolves_against_the_system_date(monkeypatch):
    class FixedDate(date):
        @classmethod
        def today(cls):
            return cls(2026, 9, 1)

    monkeypatch.setattr(seasons, "date", FixedDate)
    assert seasons.recent_seasons(2) == [2025, 2026]


@pytest.mark.parametrize("count", [0, -1, -10])
def test_a_count_below_one_is_rejected(count):
    with pytest.raises(ValueError):
        recent_seasons(count, date(2025, 9, 1))


def test_the_rejection_names_the_count_it_rejected():
    with pytest.raises(ValueError, match="got 0"):
        recent_seasons(0, date(2025, 9, 1))
