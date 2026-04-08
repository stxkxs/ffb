"""Snap share trend tracker — Textual TUI view."""

from __future__ import annotations

import polars as pl
from textual import work
from textual.app import ComposeResult
from textual.containers import Horizontal, Vertical
from textual.widget import Widget
from textual.widgets import (
    Button,
    DataTable,
    LoadingIndicator,
    Select,
    Static,
    TabbedContent,
    TabPane,
)

from ffb.data.loader import load_snap_counts
from ffb.snap_share.engine import compute_trends

COLUMNS = ("Player", "Pos", "Team", "Wk", "Snap%", "Avg", "Δ", "Vel", "Trend")


class SnapShareView(Widget):
    """Snap count trend tracker with alerts and full player table."""

    DEFAULT_CSS = """
    SnapShareView {
        height: 1fr;
        width: 1fr;
    }

    #sn-loading {
        height: 1fr;
    }

    #sn-content {
        height: 1fr;
    }

    .filter-bar {
        height: auto;
        max-height: 5;
        padding: 1;
        background: $surface-darken-1;
    }

    .filter-bar Select {
        width: 1fr;
        margin-right: 1;
    }

    .filter-bar #sn-btn-refresh {
        margin-left: 1;
    }

    .alert-panels {
        height: 1fr;
    }

    .alert-panel {
        width: 1fr;
        margin: 0 1;
    }

    .panel-header-rising {
        color: $success;
        text-style: bold;
        padding: 1 0 0 1;
    }

    .panel-header-falling {
        color: $error;
        text-style: bold;
        padding: 1 0 0 1;
    }
    """

    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)
        self._trends: pl.DataFrame | None = None
        self._activated = False
        self._initializing = True

    def compose(self) -> ComposeResult:
        yield LoadingIndicator(id="sn-loading")
        with Vertical(id="sn-content"):
            with Horizontal(classes="filter-bar"):
                yield Select[str](
                    [("All", "All"), ("QB", "QB"), ("RB", "RB"), ("WR", "WR"), ("TE", "TE")],
                    value="All",
                    id="sn-filter-position",
                )
                yield Select[str]([], prompt="Team", id="sn-filter-team")
                yield Select[int]([], prompt="Season", id="sn-filter-season")
                yield Select[int]([], prompt="Week", id="sn-filter-week")
                yield Button("Refresh", id="sn-btn-refresh", variant="primary")
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
        self.query_one("#sn-content").display = False
        for tid in ("sn-rising-table", "sn-falling-table", "sn-all-table"):
            table = self.query_one(f"#{tid}", DataTable)
            table.add_columns(*COLUMNS)
            table.cursor_type = "row"

    def activate(self) -> None:
        """Load data on first activation."""
        if not self._activated:
            self._activated = True
            self._fetch_data()

    @work(thread=True, exclusive=True)
    def _fetch_data(self, force_refresh: bool = False) -> None:
        try:
            snaps = load_snap_counts([2024, 2025], force_refresh=force_refresh)
            trends = compute_trends(snaps)
            self.app.call_from_thread(self._on_data_loaded, trends)
        except Exception as e:
            self.app.call_from_thread(self._on_data_error, str(e))

    def _on_data_loaded(self, trends: pl.DataFrame) -> None:
        self._trends = trends
        self._initializing = True
        self._populate_filters()
        self._initializing = False
        self._apply_filters()
        self.query_one("#sn-loading").display = False
        self.query_one("#sn-content").display = True

    def _on_data_error(self, error: str) -> None:
        self.query_one("#sn-loading").display = False
        self.notify(f"Failed to load data: {error}", severity="error", timeout=10)

    # ── filters ──────────────────────────────────────────────

    def _populate_filters(self) -> None:
        if self._trends is None:
            return

        teams = sorted(self._trends["team"].unique().drop_nulls().to_list())
        team_select = self.query_one("#sn-filter-team", Select)
        team_select.set_options([("All", "All")] + [(t, t) for t in teams])
        team_select.value = "All"

        seasons = sorted(self._trends["season"].unique().drop_nulls().to_list())
        season_select = self.query_one("#sn-filter-season", Select)
        season_select.set_options([(str(s), s) for s in seasons])
        season_select.value = seasons[-1]

        self._update_week_options()

    def _update_week_options(self) -> None:
        if self._trends is None:
            return
        season = self.query_one("#sn-filter-season", Select).value
        if season is Select.BLANK:
            return
        weeks = sorted(
            self._trends.filter(pl.col("season") == season)["week"]
            .unique()
            .drop_nulls()
            .to_list()
        )
        if not weeks:
            return
        week_select = self.query_one("#sn-filter-week", Select)
        week_select.set_options([(f"Week {w}", w) for w in weeks])
        week_select.value = weeks[-1]

    def _apply_filters(self) -> None:
        if self._trends is None:
            return

        df = self._trends

        pos = self.query_one("#sn-filter-position", Select).value
        if pos not in ("All", Select.BLANK):
            df = df.filter(pl.col("position") == pos)

        team = self.query_one("#sn-filter-team", Select).value
        if team not in ("All", Select.BLANK):
            df = df.filter(pl.col("team") == team)

        season = self.query_one("#sn-filter-season", Select).value
        if season is not Select.BLANK:
            df = df.filter(pl.col("season") == season)

        week = self.query_one("#sn-filter-week", Select).value
        if week is not Select.BLANK:
            df = df.filter(pl.col("week") == week)

        self._update_tables(df)

    # ── tables ───────────────────────────────────────────────

    def _update_tables(self, df: pl.DataFrame) -> None:
        rising = df.filter(
            pl.col("trend").is_in(["rising", "breakout"])
        ).sort("velocity", descending=True)
        self._fill_table("sn-rising-table", rising)

        falling = df.filter(pl.col("trend") == "falling").sort("velocity")
        self._fill_table("sn-falling-table", falling)

        self._fill_table("sn-all-table", df.sort("delta", descending=True))

    def _fill_table(self, table_id: str, df: pl.DataFrame) -> None:
        table = self.query_one(f"#{table_id}", DataTable)
        table.clear()
        for row in df.iter_rows(named=True):
            snap = row["snap_pct"]
            avg = row["rolling_avg"]
            delta = row["delta"]
            vel = row["velocity"]
            table.add_row(
                row["player"],
                row["position"],
                row["team"],
                str(row["week"]),
                f"{snap:.1f}%" if snap is not None else "—",
                f"{avg:.1f}%" if avg is not None else "—",
                f"{delta:+.1f}" if delta is not None else "—",
                f"{vel:+.1f}" if vel is not None else "—",
                row["trend"],
            )

    # ── events ───────────────────────────────────────────────

    def on_select_changed(self, event: Select.Changed) -> None:
        if self._initializing:
            return
        if event.select.id == "sn-filter-season":
            self._initializing = True
            self._update_week_options()
            self._initializing = False
        self._apply_filters()

    def on_button_pressed(self, event: Button.Pressed) -> None:
        if event.button.id == "sn-btn-refresh":
            self.query_one("#sn-content").display = False
            self.query_one("#sn-loading").display = True
            self._fetch_data(force_refresh=True)
