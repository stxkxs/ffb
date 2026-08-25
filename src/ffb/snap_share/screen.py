"""Snap share trend tracker — Textual TUI view."""

from __future__ import annotations

from typing import Any

import polars as pl
from textual.app import ComposeResult
from textual.containers import Horizontal, Vertical
from textual.widgets import DataTable, Select, Static, TabbedContent, TabPane

from ffb.data.loader import load_snap_counts
from ffb.data.seasons import recent_seasons
from ffb.snap_share.engine import compute_trends
from ffb.ui.base import ALL, ToolView

#: "vs Avg" holds the week's snap share minus the rolling average of the weeks before
#: it, and "Vel" the per-week slope across the window.
COLUMNS = ("Player", "Pos", "Team", "Wk", "Snap%", "Avg", "vs Avg", "Vel", "Trend")

#: Seasons the filter offers, counting back from the most recent. Rolling windows group
#: by season, so each season is trended from its own week 1.
SEASON_COUNT = 2

#: Trends that count as an upward alert.
RISING_TRENDS = ("rising", "breakout")

#: Tables that share the trend column layout.
TABLE_IDS = ("sn-rising-table", "sn-falling-table", "sn-all-table")


def _trend_row(row: dict[str, Any]) -> tuple[str, ...]:
    """Format one trend row as table cells, rendering an absent measure as an em dash."""
    snap = row["snap_pct"]
    avg = row["rolling_avg"]
    delta = row["delta"]
    velocity = row["velocity"]
    return (
        row["player"],
        row["position"],
        row["team"],
        str(row["week"]),
        f"{snap:.1f}%" if snap is not None else "—",
        f"{avg:.1f}%" if avg is not None else "—",
        f"{delta:+.1f}" if delta is not None else "—",
        f"{velocity:+.1f}" if velocity is not None else "—",
        row["trend"],
    )


class SnapShareView(ToolView):
    """Snap count trend tracker with alerts and full player table."""

    ID_PREFIX = "sn"
    LOAD_LABEL = "Downloading snap counts"

    def __init__(self, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self._trends: pl.DataFrame | None = None

    def compose_content(self) -> ComposeResult:
        yield from self.compose_filter_bar(
            self.position_select(),
            Select[str]([], prompt="Team", id="sn-filter-team"),
            Select[int]([], prompt="Season", id="sn-filter-season"),
            Select[int]([], prompt="Week", id="sn-filter-week"),
        )
        with TabbedContent():
            with TabPane("Alerts", id="sn-tab-alerts"):
                with Horizontal(classes="alert-panels"):
                    with Vertical(classes="alert-panel"):
                        yield Static("▲ Rising", classes="panel-header-rising")
                        yield DataTable(id="sn-rising-table")
                    with Vertical(classes="alert-panel"):
                        yield Static("▼ Falling", classes="panel-header-falling")
                        yield DataTable(id="sn-falling-table")
            with TabPane("All Players", id="sn-tab-all"):
                yield DataTable(id="sn-all-table")

    def on_mount(self) -> None:
        for table_id in TABLE_IDS:
            table = self.query_one(f"#{table_id}", DataTable)
            table.add_columns(*COLUMNS)
            table.cursor_type = "row"

    # ── data ─────────────────────────────────────────────────

    def fetch(self, force_refresh: bool) -> pl.DataFrame:
        """Download snap counts and trend every player's share of them."""
        snaps = load_snap_counts(recent_seasons(SEASON_COUNT), force_refresh=force_refresh)
        return compute_trends(snaps)

    def apply(self, trends: pl.DataFrame) -> None:
        self._trends = trends
        self._populate_filters()
        self._apply_filters()

    # ── filters ──────────────────────────────────────────────

    def _populate_filters(self) -> None:
        if self._trends is None:
            return

        teams = sorted(self._trends["team"].unique().drop_nulls().to_list())
        team_select = self.query_one("#sn-filter-team", Select)
        team_select.set_options(self.all_options(teams))
        team_select.value = ALL

        seasons = sorted(self._trends["season"].unique().drop_nulls().to_list())
        season_select = self.query_one("#sn-filter-season", Select)
        season_select.set_options([(str(season), season) for season in seasons])
        if seasons:
            season_select.value = seasons[-1]

        self._update_week_options()

    def _update_week_options(self) -> None:
        """Offer the weeks the selected season carries, defaulting to the latest.

        Rebuilding the option list on every call keeps the filter from advertising a
        week the selected season has no rows for.
        """
        if self._trends is None:
            return
        season = self.selected("sn-filter-season")
        weeks: list[int] = (
            []
            if season is None
            else sorted(
                self._trends.filter(pl.col("season") == season)["week"]
                .unique()
                .drop_nulls()
                .to_list()
            )
        )
        week_select = self.query_one("#sn-filter-week", Select)
        week_select.set_options([(f"Week {week}", week) for week in weeks])
        if weeks:
            week_select.value = weeks[-1]

    def _apply_filters(self) -> None:
        if self._trends is None:
            return

        df = self._trends

        pos = self.narrowing("sn-filter-position")
        if pos is not None:
            df = df.filter(pl.col("position") == pos)

        team = self.narrowing("sn-filter-team")
        if team is not None:
            df = df.filter(pl.col("team") == team)

        season = self.selected("sn-filter-season")
        if season is not None:
            df = df.filter(pl.col("season") == season)

        week = self.selected("sn-filter-week")
        if week is not None:
            df = df.filter(pl.col("week") == week)

        self._update_tables(df)

    # ── tables ───────────────────────────────────────────────

    def _update_tables(self, df: pl.DataFrame) -> None:
        rising = df.filter(pl.col("trend").is_in(RISING_TRENDS)).sort("velocity", descending=True)
        self.fill_table(
            "sn-rising-table",
            rising,
            _trend_row,
            "No rising or breakout players for the selected season, week, position and team.",
        )

        falling = df.filter(pl.col("trend") == "falling").sort("velocity")
        self.fill_table(
            "sn-falling-table",
            falling,
            _trend_row,
            "No falling players for the selected season, week, position and team.",
        )

        self.fill_table(
            "sn-all-table",
            df.sort("delta", descending=True),
            _trend_row,
            "No snap counts for the selected season, week, position and team.",
        )

    # ── events ───────────────────────────────────────────────

    def filter_changed(self, filter_id: str | None) -> None:
        """Re-filter, rebuilding the week options first where the season moved."""
        if filter_id == "sn-filter-season":
            self._update_week_options()
        self._apply_filters()
