"""Waiver wire trend scanner — Textual TUI view."""

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

from ffb.data.loader import load_player_ids, load_snap_counts, load_weekly_stats
from ffb.waiver_wire.engine import compute_usage_trends

COLUMNS = (
    "Player", "Pos", "Team", "Wk",
    "Snap%", "Tgt%", "Tch%", "Usage",
    "Avg", "Δ", "Vel", "Trend",
)


class WaiverWireView(Widget):
    """Free agent usage trend scanner."""

    DEFAULT_CSS = """
    WaiverWireView {
        height: 1fr;
        width: 1fr;
    }

    #ww-loading {
        height: 1fr;
    }

    #ww-content {
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

    .filter-bar #ww-btn-refresh {
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
        yield LoadingIndicator(id="ww-loading")
        with Vertical(id="ww-content"):
            with Horizontal(classes="filter-bar"):
                yield Select[str](
                    [("All", "All"), ("QB", "QB"), ("RB", "RB"), ("WR", "WR"), ("TE", "TE")],
                    value="All",
                    id="ww-filter-position",
                )
                yield Select[str]([], prompt="Team", id="ww-filter-team")
                yield Select[int]([], prompt="Season", id="ww-filter-season")
                yield Select[int]([], prompt="Week", id="ww-filter-week")
                yield Button("Refresh", id="ww-btn-refresh", variant="primary")
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
        self.query_one("#ww-content").display = False
        for tid in ("ww-rising-table", "ww-falling-table", "ww-all-table"):
            table = self.query_one(f"#{tid}", DataTable)
            table.add_columns(*COLUMNS)
            table.cursor_type = "row"

    def activate(self) -> None:
        if not self._activated:
            self._activated = True
            self._fetch_data()

    @work(thread=True, exclusive=True)
    def _fetch_data(self, force_refresh: bool = False) -> None:
        try:
            snaps = load_snap_counts([2024, 2025], force_refresh=force_refresh)
            weekly = load_weekly_stats([2024, 2025], force_refresh=force_refresh)
            ids = load_player_ids(force_refresh=force_refresh)
            trends = compute_usage_trends(snaps, weekly, ids)
            self.app.call_from_thread(self._on_data_loaded, trends)
        except Exception as e:
            self.app.call_from_thread(self._on_data_error, str(e))

    def _on_data_loaded(self, trends: pl.DataFrame) -> None:
        self._trends = trends
        self._initializing = True
        self._populate_filters()
        self._initializing = False
        self._apply_filters()
        self.query_one("#ww-loading").display = False
        self.query_one("#ww-content").display = True

    def _on_data_error(self, error: str) -> None:
        self.query_one("#ww-loading").display = False
        self.notify(f"Failed to load data: {error}", severity="error", timeout=10)

    # ── filters ──────────────────────────────────────────────

    def _populate_filters(self) -> None:
        if self._trends is None:
            return

        teams = sorted(self._trends["team"].unique().drop_nulls().to_list())
        team_select = self.query_one("#ww-filter-team", Select)
        team_select.set_options([("All", "All")] + [(t, t) for t in teams])
        team_select.value = "All"

        seasons = sorted(self._trends["season"].unique().drop_nulls().to_list())
        season_select = self.query_one("#ww-filter-season", Select)
        season_select.set_options([(str(s), s) for s in seasons])
        season_select.value = seasons[-1]

        self._update_week_options()

    def _update_week_options(self) -> None:
        if self._trends is None:
            return
        season = self.query_one("#ww-filter-season", Select).value
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
        week_select = self.query_one("#ww-filter-week", Select)
        week_select.set_options([(f"Week {w}", w) for w in weeks])
        week_select.value = weeks[-1]

    def _apply_filters(self) -> None:
        if self._trends is None:
            return

        df = self._trends

        pos = self.query_one("#ww-filter-position", Select).value
        if pos not in ("All", Select.BLANK):
            df = df.filter(pl.col("position") == pos)

        team = self.query_one("#ww-filter-team", Select).value
        if team not in ("All", Select.BLANK):
            df = df.filter(pl.col("team") == team)

        season = self.query_one("#ww-filter-season", Select).value
        if season is not Select.BLANK:
            df = df.filter(pl.col("season") == season)

        week = self.query_one("#ww-filter-week", Select).value
        if week is not Select.BLANK:
            df = df.filter(pl.col("week") == week)

        self._update_tables(df)

    # ── tables ───────────────────────────────────────────────

    def _update_tables(self, df: pl.DataFrame) -> None:
        rising = df.filter(pl.col("trend") == "rising").sort("velocity", descending=True)
        self._fill_table("ww-rising-table", rising)

        falling = df.filter(pl.col("trend") == "falling").sort("velocity")
        self._fill_table("ww-falling-table", falling)

        self._fill_table("ww-all-table", df.sort("velocity", descending=True))

    def _fill_table(self, table_id: str, df: pl.DataFrame) -> None:
        table = self.query_one(f"#{table_id}", DataTable)
        table.clear()
        for row in df.iter_rows(named=True):
            table.add_row(
                row["player"],
                row["position"],
                row["team"],
                str(row["week"]),
                f"{row['snap_pct']:.1f}%" if row["snap_pct"] is not None else "—",
                f"{row['tgt_share']:.1f}%" if row["tgt_share"] is not None else "—",
                f"{row['touch_share']:.1f}%" if row["touch_share"] is not None else "—",
                f"{row['usage_score']:.1f}" if row["usage_score"] is not None else "—",
                f"{row['rolling_avg']:.1f}" if row["rolling_avg"] is not None else "—",
                f"{row['delta']:+.1f}" if row["delta"] is not None else "—",
                f"{row['velocity']:+.1f}" if row["velocity"] is not None else "—",
                row["trend"],
            )

    # ── events ───────────────────────────────────────────────

    def on_select_changed(self, event: Select.Changed) -> None:
        if self._initializing:
            return
        if event.select.id == "ww-filter-season":
            self._initializing = True
            self._update_week_options()
            self._initializing = False
        self._apply_filters()

    def on_button_pressed(self, event: Button.Pressed) -> None:
        if event.button.id == "ww-btn-refresh":
            self.query_one("#ww-content").display = False
            self.query_one("#ww-loading").display = True
            self._activated = False
            self.activate()
