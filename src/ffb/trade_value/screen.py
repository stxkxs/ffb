"""Trade value chart — Textual TUI view."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import polars as pl
from textual.app import ComposeResult
from textual.widgets import DataTable, Select

from ffb.data.loader import (
    load_injuries,
    load_player_ids,
    load_schedules,
    load_snap_counts,
    load_weekly_stats,
)
from ffb.data.seasons import recent_seasons
from ffb.trade_value.engine import REGULAR_SEASON_WEEKS, compute_trade_values
from ffb.ui.base import ALL, ToolView

COLUMNS = (
    "Rank",
    "Player",
    "Pos",
    "Team",
    "PPG",
    "GP",
    "Sched",
    "Snap%",
    "Health",
    "Bye",
    "Value",
)

#: Seasons the load asks for. Production, schedule and usage read the one season
#: selected; the health component reads injury weeks across every season loaded, so a
#: second season buys a longer availability history. The season filter offers the
#: loaded seasons a ranking can run on, which is narrower: a season nflverse has
#: published a schedule for carries no results until its games have been played.
SEASON_WINDOW = 2

#: Weeks of results a ranking rests on. The engine ranks nobody below this, so the week
#: filter offers no week below it either.
MIN_WEEKS_PLAYED = 3

#: Stands in for the table when no loaded season carries enough played weeks to rank.
NO_RANKABLE_WEEKS = (
    f"No season has the {MIN_WEEKS_PLAYED} completed weeks a ranking rests on. "
    "nflverse publishes a season's weekly stats once its games have been played."
)


@dataclass(frozen=True, slots=True)
class TradeValueData:
    """The frames one load hands to the view."""

    weekly: pl.DataFrame
    snaps: pl.DataFrame
    schedules: pl.DataFrame
    injuries: pl.DataFrame
    ids: pl.DataFrame


def _rankable_weeks(weekly: pl.DataFrame, season: int) -> list[int]:
    """Regular-season weeks of `season` a ranking can run through, in order.

    A trade value is worth what a player brings over the weeks left, so the last week
    of the regular season is out along with the weeks too early to rank. The weeks come
    off the results rather than off the schedule: a schedule carries every week of a
    season from the moment nflverse publishes it, months before the first is played.
    """
    played = (
        weekly.filter((pl.col("season") == season) & (pl.col("season_type") == "REG"))["week"]
        .unique()
        .drop_nulls()
        .to_list()
    )
    return sorted(week for week in played if MIN_WEEKS_PLAYED <= week < REGULAR_SEASON_WEEKS)


def _value_row(row: dict[str, Any]) -> tuple[str, ...]:
    """Map one ranked player to the cells of `COLUMNS`."""
    bye = row["bye_week"]
    return (
        str(row["rank"]),
        row["player"],
        row["position"],
        row["team"],
        f"{row['ppg']:.1f}",
        str(row["games_played"]),
        f"{row['sched_mult']:.2f}x",
        f"{row['avg_snap_pct']:.0f}%",
        f"{row['health']:.0%}",
        str(int(bye)) if bye is not None else "—",
        f"{row['trade_value']:.1f}",
    )


class TradeValueView(ToolView):
    """Rest-of-season trade value chart."""

    ID_PREFIX = "tv"
    LOAD_LABEL = "Loading weekly stats, snap counts, schedules and injuries"

    def __init__(self, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self._data: TradeValueData | None = None
        self._values: pl.DataFrame | None = None

    def compose_content(self) -> ComposeResult:
        yield from self.compose_filter_bar(
            self.position_select(),
            Select[str]([], prompt="Team", id="tv-filter-team"),
            Select[int]([], prompt="Season", id="tv-filter-season"),
            Select[int]([], prompt="As of Week", id="tv-filter-week"),
        )
        yield DataTable(id="tv-table")

    def on_mount(self) -> None:
        table = self.query_one("#tv-table", DataTable)
        table.add_columns(*COLUMNS)
        table.cursor_type = "row"

    # ── load ─────────────────────────────────────────────────

    def fetch(self, force_refresh: bool) -> TradeValueData:
        seasons = recent_seasons(SEASON_WINDOW)
        return TradeValueData(
            weekly=load_weekly_stats(seasons, force_refresh=force_refresh),
            snaps=load_snap_counts(seasons, force_refresh=force_refresh),
            schedules=load_schedules(seasons, force_refresh=force_refresh),
            injuries=load_injuries(seasons, force_refresh=force_refresh),
            ids=load_player_ids(force_refresh=force_refresh),
        )

    def apply(self, payload: TradeValueData) -> None:
        self._data = payload
        self._populate_filters()
        self._compute_values()

    # ── filters ──────────────────────────────────────────────

    def _populate_filters(self) -> None:
        data = self._data
        if data is None:
            return

        reg = data.schedules.filter(pl.col("game_type") == "REG")

        seasons = sorted(
            season
            for season in reg["season"].unique().drop_nulls().to_list()
            if _rankable_weeks(data.weekly, season)
        )
        season_select = self.query_one("#tv-filter-season", Select)
        season_select.set_options([(str(s), s) for s in seasons])
        if seasons:
            season_select.value = seasons[-1]

        self._update_week_and_team_options()

    def _update_week_and_team_options(self) -> None:
        data = self._data
        if data is None:
            return
        season = self.selected("tv-filter-season")
        if season is None:
            return

        weeks = _rankable_weeks(data.weekly, season)
        week_select = self.query_one("#tv-filter-week", Select)
        week_select.set_options([(f"Week {w}", w) for w in weeks])
        if weeks:
            week_select.value = weeks[-1]

        reg = data.schedules.filter((pl.col("season") == season) & (pl.col("game_type") == "REG"))
        teams = sorted(
            set(reg["home_team"].unique().drop_nulls().to_list())
            | set(reg["away_team"].unique().drop_nulls().to_list())
        )
        team_select = self.query_one("#tv-filter-team", Select)
        team_select.set_options(self.all_options(teams))
        team_select.value = ALL

    def _compute_values(self) -> None:
        """Rank the selected season through the selected week."""
        data = self._data
        if data is None:
            return

        season = self.selected("tv-filter-season")
        week = self.selected("tv-filter-week")
        # Both filters hold no selection until a load fills their options, and a load
        # that reached no rankable week fills neither.
        if not isinstance(season, int) or not isinstance(week, int):
            self._values = None
            self.fill_table("tv-table", pl.DataFrame(), _value_row, NO_RANKABLE_WEEKS)
            return

        self._values = compute_trade_values(
            data.weekly,
            data.snaps,
            data.schedules,
            data.injuries,
            data.ids,
            season=season,
            current_week=week,
        )
        self._apply_filters()

    def _apply_filters(self) -> None:
        values = self._values
        if values is None:
            return

        season = self.selected("tv-filter-season")
        week = self.selected("tv-filter-week")

        # Emptiness is settled before any column is named: a row-less result owes the
        # caller no columns, and naming one on such a frame raises.
        if values.height == 0:
            self.fill_table("tv-table", values, _value_row, _unranked_message(season, week))
            return

        pos = self.narrowing("tv-filter-position")
        team = self.narrowing("tv-filter-team")

        df = values
        if pos is not None:
            df = df.filter(pl.col("position") == pos)
        if team is not None:
            df = df.filter(pl.col("team") == team)

        self.fill_table(
            "tv-table",
            df.with_row_index("rank", offset=1),
            _value_row,
            _no_match_message(pos, team, season, week),
        )

    # ── events ───────────────────────────────────────────────

    def filter_changed(self, filter_id: str | None) -> None:
        """Rank again for a season or week change; re-filter the ranking for the rest.

        A season change rebuilds the week and team options first, so the ranking is
        computed for a week the selected season played.
        """
        if filter_id in ("tv-filter-season", "tv-filter-week"):
            if filter_id == "tv-filter-season":
                self._update_week_and_team_options()
            self._compute_values()
        else:
            self._apply_filters()


def _unranked_message(season: Any, week: Any) -> str:
    """Say why a chosen season and week rank nobody."""
    return (
        f"No trade values for {season} week {week}. A ranking needs "
        f"{MIN_WEEKS_PLAYED} weeks of results behind it and a week still to play."
    )


def _no_match_message(pos: Any, team: Any, season: Any, week: Any) -> str:
    """Name the filters that narrowed a populated ranking down to nothing.

    `pos` and `team` are what those filters narrow to, so None names neither.
    """
    if pos is not None and team is not None:
        who = f"{pos} on {team}"
    elif pos is not None:
        who = f"{pos} players"
    elif team is not None:
        who = f"{team} players"
    else:
        who = "players"
    return f"No {who} in the {season} week {week} rankings."
