"""Start/sit matchup projections — Textual TUI view."""

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

from ffb.data.loader import load_schedules, load_weekly_stats
from ffb.start_sit.engine import compute_start_sit

COLUMNS = (
    "Player", "Pos", "Team", "Opp",
    "Baseline", "Opp Allows", "Lg Avg",
    "Mult", "Projected", "Verdict",
)


class StartSitView(Widget):
    """Matchup-based start/sit projection engine."""

    DEFAULT_CSS = """
    StartSitView {
        height: 1fr;
        width: 1fr;
    }

    #ss-loading {
        height: 1fr;
    }

    #ss-content {
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

    .filter-bar #ss-btn-refresh {
        margin-left: 1;
    }
    """

    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)
        self._weekly_stats: pl.DataFrame | None = None
        self._schedules: pl.DataFrame | None = None
        self._projections: pl.DataFrame | None = None
        self._activated = False
        self._initializing = True

    def compose(self) -> ComposeResult:
        yield LoadingIndicator(id="ss-loading")
        with Vertical(id="ss-content"):
            with Horizontal(classes="filter-bar"):
                yield Select[str](
                    [("All", "All"), ("QB", "QB"), ("RB", "RB"), ("WR", "WR"), ("TE", "TE")],
                    value="All",
                    id="ss-filter-position",
                )
                yield Select[str]([], prompt="Team", id="ss-filter-team")
                yield Select[int]([], prompt="Season", id="ss-filter-season")
                yield Select[int]([], prompt="Week", id="ss-filter-week")
                yield Button("Refresh", id="ss-btn-refresh", variant="primary")
            yield DataTable(id="ss-table")

    def on_mount(self) -> None:
        self.query_one("#ss-content").display = False
        table = self.query_one("#ss-table", DataTable)
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
            schedules = load_schedules([2024, 2025], force_refresh=force_refresh)
            self.app.call_from_thread(self._on_data_loaded, weekly, schedules)
        except Exception as e:
            self.app.call_from_thread(self._on_data_error, str(e))

    def _on_data_loaded(self, weekly: pl.DataFrame, schedules: pl.DataFrame) -> None:
        self._weekly_stats = weekly
        self._schedules = schedules
        self._initializing = True
        self._populate_filters()
        self._initializing = False
        self._compute_projections()
        self.query_one("#ss-loading").display = False
        self.query_one("#ss-content").display = True

    def _on_data_error(self, error: str) -> None:
        self.query_one("#ss-loading").display = False
        self.notify(f"Failed to load data: {error}", severity="error", timeout=10)

    # ── filters ──────────────────────────────────────────────

    def _populate_filters(self) -> None:
        if self._schedules is None:
            return

        reg = self._schedules.filter(pl.col("game_type") == "REG")

        seasons = sorted(reg["season"].unique().drop_nulls().to_list())
        season_select = self.query_one("#ss-filter-season", Select)
        season_select.set_options([(str(s), s) for s in seasons])
        season_select.value = seasons[-1]

        self._update_week_and_team_options()

    def _update_week_and_team_options(self) -> None:
        if self._schedules is None:
            return
        season = self.query_one("#ss-filter-season", Select).value
        if season is Select.BLANK:
            return

        reg = self._schedules.filter(
            (pl.col("season") == season) & (pl.col("game_type") == "REG")
        )

        weeks = sorted(reg["week"].unique().drop_nulls().to_list())
        week_select = self.query_one("#ss-filter-week", Select)
        week_select.set_options([(f"Week {w}", w) for w in weeks])
        # Default to latest week with games
        week_select.value = weeks[-1] if weeks else Select.BLANK

        teams = sorted(
            set(reg["home_team"].unique().drop_nulls().to_list())
            | set(reg["away_team"].unique().drop_nulls().to_list())
        )
        team_select = self.query_one("#ss-filter-team", Select)
        team_select.set_options([("All", "All")] + [(t, t) for t in teams])
        team_select.value = "All"

    def _compute_projections(self) -> None:
        if self._weekly_stats is None or self._schedules is None:
            return

        season = self.query_one("#ss-filter-season", Select).value
        week = self.query_one("#ss-filter-week", Select).value
        if season is Select.BLANK or week is Select.BLANK:
            return

        proj = compute_start_sit(self._weekly_stats, self._schedules, int(season), int(week))
        self._projections = proj
        self._apply_filters()

    def _apply_filters(self) -> None:
        if self._projections is None:
            return

        df = self._projections

        pos = self.query_one("#ss-filter-position", Select).value
        if pos not in ("All", Select.BLANK):
            df = df.filter(pl.col("position") == pos)

        team = self.query_one("#ss-filter-team", Select).value
        if team not in ("All", Select.BLANK):
            df = df.filter(pl.col("team") == team)

        self._fill_table(df)

    # ── table ────────────────────────────────────────────────

    def _fill_table(self, df: pl.DataFrame) -> None:
        table = self.query_one("#ss-table", DataTable)
        table.clear()
        for row in df.iter_rows(named=True):
            table.add_row(
                row["player"],
                row["position"],
                row["team"],
                row["opponent"],
                f"{row['baseline_fpts']:.1f}",
                f"{row['fpts_allowed']:.1f}" if row["fpts_allowed"] is not None else "—",
                f"{row['league_avg']:.1f}" if row["league_avg"] is not None else "—",
                f"{row['matchup_mult']:.2f}x" if row["matchup_mult"] is not None else "—",
                f"{row['projected_fpts']:.1f}",
                row["confidence"],
            )

    # ── events ───────────────────────────────────────────────

    def on_select_changed(self, event: Select.Changed) -> None:
        if self._initializing:
            return
        if event.select.id == "ss-filter-season":
            self._initializing = True
            self._update_week_and_team_options()
            self._initializing = False
            self._compute_projections()
        elif event.select.id == "ss-filter-week":
            self._compute_projections()
        else:
            self._apply_filters()

    def on_button_pressed(self, event: Button.Pressed) -> None:
        if event.button.id == "ss-btn-refresh":
            self.query_one("#ss-content").display = False
            self.query_one("#ss-loading").display = True
            self._activated = False
            self.activate()
