"""Waiver wire trend scanner — Textual TUI view."""

from __future__ import annotations

from typing import Any

import polars as pl
from textual.app import ComposeResult
from textual.containers import Horizontal, Vertical
from textual.widgets import DataTable, Select, Static, TabbedContent, TabPane

from ffb.data.loader import load_player_ids, load_snap_counts, load_weekly_stats
from ffb.data.seasons import recent_seasons
from ffb.ui.base import ALL, ToolView
from ffb.waiver_wire.engine import compute_usage_trends

COLUMNS = (
    "Player",
    "Pos",
    "Team",
    "Wk",
    "Snap%",
    "Tgt%",
    "Tch%",
    "Usage",
    "Avg",
    "Δ",
    "Vel",
    "Trend",
)

#: How many seasons of weekly data the season filter offers.
SEASON_COUNT = 2

#: Stands in for a measurement a player's week does not carry.
MISSING = "—"


def _fmt(value: float | None, spec: str, suffix: str = "") -> str:
    """Render `value` under `spec` with `suffix`, or as an em dash where it is absent.

    A player can reach a week with no snap or touch data — an inactive week, or one
    nflverse has not published — and a blank cell reads as a zero.
    """
    return f"{value:{spec}}{suffix}" if value is not None else MISSING


def _cells(row: dict[str, Any]) -> tuple[str, ...]:
    """Map one trend row to the twelve cells of `COLUMNS`."""
    return (
        row["player"],
        row["position"],
        row["team"],
        str(row["week"]),
        _fmt(row["snap_pct"], ".1f", "%"),
        _fmt(row["tgt_share"], ".1f", "%"),
        _fmt(row["touch_share"], ".1f", "%"),
        _fmt(row["usage_score"], ".1f"),
        _fmt(row["rolling_avg"], ".1f"),
        _fmt(row["delta"], "+.1f"),
        _fmt(row["velocity"], "+.1f"),
        row["trend"],
    )


class WaiverWireView(ToolView):
    """Free agent usage trend scanner."""

    ID_PREFIX = "ww"
    LOAD_LABEL = "Downloading snap counts, weekly stats and player ids"

    def __init__(self, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self._trends: pl.DataFrame | None = None

    def compose_content(self) -> ComposeResult:
        yield from self.compose_filter_bar(
            self.position_select(),
            Select[str]([], prompt="Team", id="ww-filter-team"),
            Select[int]([], prompt="Season", id="ww-filter-season"),
            Select[int]([], prompt="Week", id="ww-filter-week"),
        )
        with TabbedContent(initial="ww-tab-rising"):
            with TabPane("Rising", id="ww-tab-rising"):
                with Horizontal(classes="alert-panels"):
                    with Vertical(classes="alert-panel"):
                        yield Static("▲ Rising Usage", classes="panel-header-rising")
                        yield DataTable(id="ww-rising-table")
                    with Vertical(classes="alert-panel"):
                        yield Static("▼ Falling Usage", classes="panel-header-falling")
                        yield DataTable(id="ww-falling-table")
            with TabPane("All Players", id="ww-tab-all"):
                yield DataTable(id="ww-all-table")

    def on_mount(self) -> None:
        for tid in ("ww-rising-table", "ww-falling-table", "ww-all-table"):
            table = self.query_one(f"#{tid}", DataTable)
            table.add_columns(*COLUMNS)
            table.cursor_type = "row"

    # ── load ─────────────────────────────────────────────────

    def fetch(self, force_refresh: bool) -> pl.DataFrame:
        """Derive usage trends from snap counts, weekly stats and the id crosswalk.

        The season filter offers exactly the seasons loaded here.
        """
        seasons = recent_seasons(SEASON_COUNT)
        snaps = load_snap_counts(seasons, force_refresh=force_refresh)
        weekly = load_weekly_stats(seasons, force_refresh=force_refresh)
        ids = load_player_ids(force_refresh=force_refresh)
        return compute_usage_trends(snaps, weekly, ids)

    def apply(self, payload: pl.DataFrame) -> None:
        self._trends = payload
        self._populate_filters()
        self._apply_filters()

    # ── filters ──────────────────────────────────────────────

    def _populate_filters(self) -> None:
        if self._trends is None:
            return

        teams = sorted(self._trends["team"].unique().drop_nulls().to_list())
        team_select = self.query_one("#ww-filter-team", Select)
        team_select.set_options(self.all_options(teams))
        team_select.value = ALL

        seasons = sorted(self._trends["season"].unique().drop_nulls().to_list())
        if not seasons:
            return
        season_select = self.query_one("#ww-filter-season", Select)
        season_select.set_options([(str(s), s) for s in seasons])
        season_select.value = seasons[-1]

        self._update_week_options()

    def _update_week_options(self) -> None:
        """Offer the weeks the selected season carries, defaulted to its latest."""
        if self._trends is None:
            return
        season = self.selected("ww-filter-season")
        if season is None:
            return
        weeks = sorted(
            self._trends.filter(pl.col("season") == season)["week"].unique().drop_nulls().to_list()
        )
        if not weeks:
            return
        week_select = self.query_one("#ww-filter-week", Select)
        week_select.set_options([(f"Week {w}", w) for w in weeks])
        week_select.value = weeks[-1]

    def _apply_filters(self) -> None:
        if self._trends is None:
            return

        df = self._trends

        pos = self.narrowing("ww-filter-position")
        if pos is not None:
            df = df.filter(pl.col("position") == pos)

        team = self.narrowing("ww-filter-team")
        if team is not None:
            df = df.filter(pl.col("team") == team)

        season = self.selected("ww-filter-season")
        if season is not None:
            df = df.filter(pl.col("season") == season)

        week = self.selected("ww-filter-week")
        if week is not None:
            df = df.filter(pl.col("week") == week)

        self._update_tables(df)

    # ── tables ───────────────────────────────────────────────

    def _update_tables(self, df: pl.DataFrame) -> None:
        """Split the filtered frame into the two alert panels and the full listing.

        Rising sorts by steepest climb and falling by steepest drop, so the top row of
        each panel is the player whose usage moved the most.
        """
        rising = df.filter(pl.col("trend") == "rising").sort("velocity", descending=True)
        self.fill_table(
            "ww-rising-table",
            rising,
            _cells,
            "No rising usage among the players these filters match.",
        )

        falling = df.filter(pl.col("trend") == "falling").sort("velocity")
        self.fill_table(
            "ww-falling-table",
            falling,
            _cells,
            "No falling usage among the players these filters match.",
        )

        self.fill_table(
            "ww-all-table",
            df.sort("velocity", descending=True),
            _cells,
            "No players match these filters. Widen the position, team or week filter.",
        )

    # ── events ───────────────────────────────────────────────

    def filter_changed(self, filter_id: str | None) -> None:
        """Re-filter, rebuilding the week options first where the season moved."""
        if filter_id == "ww-filter-season":
            self._update_week_options()
        self._apply_filters()
