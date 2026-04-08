"""Trade value chart — Textual TUI view."""

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
)

from ffb.data.loader import (
    load_injuries,
    load_player_ids,
    load_schedules,
    load_snap_counts,
    load_weekly_stats,
)
from ffb.trade_value.engine import compute_trade_values

COLUMNS = (
    "Rank", "Player", "Pos", "Team",
    "PPG", "GP", "Sched", "Snap%",
    "Health", "Bye", "Value",
)


class TradeValueView(Widget):
    """Rest-of-season trade value chart."""

    DEFAULT_CSS = """
    TradeValueView {
        height: 1fr;
        width: 1fr;
    }

    #tv-loading {
        height: 1fr;
    }

    #tv-content {
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

    .filter-bar #tv-btn-refresh {
        margin-left: 1;
    }
    """

    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)
        self._values: pl.DataFrame | None = None
        self._activated = False
        self._initializing = True

    def compose(self) -> ComposeResult:
        yield LoadingIndicator(id="tv-loading")
        with Vertical(id="tv-content"):
            with Horizontal(classes="filter-bar"):
                yield Select[str](
                    [("All", "All"), ("QB", "QB"), ("RB", "RB"), ("WR", "WR"), ("TE", "TE")],
                    value="All",
                    id="tv-filter-position",
                )
                yield Select[str]([], prompt="Team", id="tv-filter-team")
                yield Select[int]([], prompt="Season", id="tv-filter-season")
                yield Select[int]([], prompt="As of Week", id="tv-filter-week")
                yield Button("Refresh", id="tv-btn-refresh", variant="primary")
            yield DataTable(id="tv-table")

    def on_mount(self) -> None:
        self.query_one("#tv-content").display = False
        table = self.query_one("#tv-table", DataTable)
        table.add_columns(*COLUMNS)
        table.cursor_type = "row"

    def activate(self) -> None:
        if not self._activated:
            self._activated = True
            self._fetch_data()

    @work(thread=True, exclusive=True)
    def _fetch_data(self, force_refresh: bool = False) -> None:
        try:
            weekly = load_weekly_stats([2024, 2025], force_refresh=force_refresh)
            snaps = load_snap_counts([2024, 2025], force_refresh=force_refresh)
            schedules = load_schedules([2024, 2025], force_refresh=force_refresh)
            injuries = load_injuries([2024, 2025], force_refresh=force_refresh)
            ids = load_player_ids(force_refresh=force_refresh)
            self.app.call_from_thread(
                self._on_data_loaded, weekly, snaps, schedules, injuries, ids
            )
        except Exception as e:
            self.app.call_from_thread(self._on_data_error, str(e))

    def _on_data_loaded(
        self,
        weekly: pl.DataFrame,
        snaps: pl.DataFrame,
        schedules: pl.DataFrame,
        injuries: pl.DataFrame,
        ids: pl.DataFrame,
    ) -> None:
        self._weekly = weekly
        self._snaps = snaps
        self._schedules = schedules
        self._injuries = injuries
        self._ids = ids

        self._initializing = True
        self._populate_filters()
        self._initializing = False
        self._compute_values()
        self.query_one("#tv-loading").display = False
        self.query_one("#tv-content").display = True

    def _on_data_error(self, error: str) -> None:
        self.query_one("#tv-loading").display = False
        self.notify(f"Failed to load data: {error}", severity="error", timeout=10)

    # ── filters ──────────────────────────────────────────────

    def _populate_filters(self) -> None:
        if self._schedules is None:
            return

        reg = self._schedules.filter(pl.col("game_type") == "REG")

        seasons = sorted(reg["season"].unique().drop_nulls().to_list())
        season_select = self.query_one("#tv-filter-season", Select)
        season_select.set_options([(str(s), s) for s in seasons])
        season_select.value = seasons[-1]

        self._update_week_and_team_options()

    def _update_week_and_team_options(self) -> None:
        if self._weekly is None or self._schedules is None:
            return
        season = self.query_one("#tv-filter-season", Select).value
        if season is Select.BLANK:
            return

        # Weeks with data
        played = self._weekly.filter(
            (pl.col("season") == season) & (pl.col("season_type") == "REG")
        )["week"].unique().drop_nulls().to_list()
        # Only show weeks where games remain (exclude final week)
        weeks = sorted([w for w in played if 3 <= w < 18])
        week_select = self.query_one("#tv-filter-week", Select)
        week_select.set_options([(f"Week {w}", w) for w in weeks])
        week_select.value = weeks[-1] if weeks else Select.BLANK

        reg = self._schedules.filter(
            (pl.col("season") == season) & (pl.col("game_type") == "REG")
        )
        teams = sorted(
            set(reg["home_team"].unique().drop_nulls().to_list())
            | set(reg["away_team"].unique().drop_nulls().to_list())
        )
        team_select = self.query_one("#tv-filter-team", Select)
        team_select.set_options([("All", "All")] + [(t, t) for t in teams])
        team_select.value = "All"

    def _compute_values(self) -> None:
        season = self.query_one("#tv-filter-season", Select).value
        week = self.query_one("#tv-filter-week", Select).value
        if season is Select.BLANK or week is Select.BLANK:
            return

        self._values = compute_trade_values(
            self._weekly, self._snaps, self._schedules,
            self._injuries, self._ids,
            season=int(season), current_week=int(week),
        )
        self._apply_filters()

    def _apply_filters(self) -> None:
        if self._values is None:
            return

        df = self._values

        pos = self.query_one("#tv-filter-position", Select).value
        if pos not in ("All", Select.BLANK):
            df = df.filter(pl.col("position") == pos)

        team = self.query_one("#tv-filter-team", Select).value
        if team not in ("All", Select.BLANK):
            df = df.filter(pl.col("team") == team)

        self._fill_table(df)

    # ── table ────────────────────────────────────────────────

    def _fill_table(self, df: pl.DataFrame) -> None:
        table = self.query_one("#tv-table", DataTable)
        table.clear()
        for i, row in enumerate(df.iter_rows(named=True), 1):
            bye = row["bye_week"]
            table.add_row(
                str(i),
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

    # ── events ───────────────────────────────────────────────

    def on_select_changed(self, event: Select.Changed) -> None:
        if self._initializing:
            return
        if event.select.id in ("tv-filter-season", "tv-filter-week"):
            if event.select.id == "tv-filter-season":
                self._initializing = True
                self._update_week_and_team_options()
                self._initializing = False
            self._compute_values()
        else:
            self._apply_filters()

    def on_button_pressed(self, event: Button.Pressed) -> None:
        if event.button.id == "tv-btn-refresh":
            self.query_one("#tv-content").display = False
            self.query_one("#tv-loading").display = True
            self._activated = False
            self.activate()
