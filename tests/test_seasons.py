"""Tests for nflverse season arithmetic.

Every case injects the date it resolves against, so a run on one day agrees with a
run on any other.
"""

from datetime import date

import pytest

from ffb.data.seasons import current_season, recent_seasons


@pytest.mark.parametrize("month", [1, 2, 3, 4, 5, 6, 7, 8])
def test_a_month_before_september_belongs_to_the_prior_season(month):
    assert current_season(date(2025, month, 15)) == 2024


@pytest.mark.parametrize("month", [9, 10, 11, 12])
def test_september_onward_belongs_to_the_season_the_year_labels(month):
    assert current_season(date(2025, month, 15)) == 2025


def test_the_last_day_of_august_belongs_to_the_prior_season():
    assert current_season(date(2025, 8, 31)) == 2024


def test_the_first_day_of_september_opens_the_season_the_year_labels():
    assert current_season(date(2025, 9, 1)) == 2025


def test_january_belongs_to_the_season_that_opened_the_prior_autumn():
    assert current_season(date(2026, 1, 1)) == 2025


def test_february_belongs_to_the_season_that_opened_the_prior_autumn():
    assert current_season(date(2026, 2, 28)) == 2025


def test_a_leap_day_belongs_to_the_season_that_opened_the_prior_autumn():
    assert current_season(date(2024, 2, 29)) == 2023


def test_the_last_day_of_december_belongs_to_the_season_the_year_labels():
    assert current_season(date(2025, 12, 31)) == 2025


def test_recent_seasons_lists_consecutive_labels_oldest_first():
    assert recent_seasons(4, date(2026, 1, 5)) == [2022, 2023, 2024, 2025]


@pytest.mark.parametrize("count", [1, 2, 3, 10])
def test_recent_seasons_returns_the_requested_count(count):
    assert len(recent_seasons(count, date(2025, 10, 1))) == count


@pytest.mark.parametrize(
    "today",
    [date(2025, 8, 31), date(2025, 9, 1), date(2026, 2, 1), date(2025, 12, 31)],
)
def test_recent_seasons_ends_at_the_current_season(today):
    assert recent_seasons(3, today)[-1] == current_season(today)


def test_recent_seasons_of_one_holds_the_current_season_alone():
    assert recent_seasons(1, date(2025, 9, 1)) == [2025]


def test_recent_seasons_defaults_to_a_pair():
    assert recent_seasons(today=date(2025, 9, 1)) == [2024, 2025]


def test_recent_seasons_crosses_the_september_boundary_with_current_season():
    assert recent_seasons(2, date(2025, 8, 31)) == [2023, 2024]


@pytest.mark.parametrize("count", [0, -1, -10])
def test_a_count_below_one_is_rejected(count):
    with pytest.raises(ValueError):
        recent_seasons(count, date(2025, 9, 1))


def test_the_rejection_names_the_count_it_rejected():
    with pytest.raises(ValueError, match="got 0"):
        recent_seasons(0, date(2025, 9, 1))
