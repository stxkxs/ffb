"""Red zone efficiency — Textual TUI view."""

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
    TabbedContent,
    TabPane,
)

from ffb.data.loader import load_pbp, load_rosters
from ffb.red_zone.engine import compute_player_rz, compute_team_rz

TEAM_COLS = ("Team", "RZ Trips", "RZ TD", "Conv%", "Pass%", "Rush%", "EPA/Play")
PLAYER_COLS = ("Player", "Pos", "Team", "RZ Tgt", "Tgt%", "RZ Rush", "Touches", "RZ TD", "TD%")


class RedZoneView(Widget):
    """Red zone efficiency analyzer with team and player views."""

    DEFAULT_CSS = """
    RedZoneView {
        height: 1fr;
        width: 1fr;
    }

    #rz-loading {
        height: 1fr;
    }

    #rz-content {
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

    .filter-bar #rz-btn-refresh {
        margin-left: 1;
    }
    """

    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)
        self._team_data: pl.DataFrame | None = None
        self._player_data: pl.DataFrame | None = None
        self._initializing = True
        self._activated = False

    def compose(self) -> ComposeResult:
        yield LoadingIndicator(id="rz-loading")
        with Vertical(id="rz-content"):
            with Horizontal(classes="filter-bar"):
                yield Select[str](
                    [("All", "All"), ("QB", "QB"), ("RB", "RB"), ("WR", "WR"), ("TE", "TE")],
                    value="All",
                    id="rz-filter-position",
                )
                yield Select[str]([], prompt="Team", id="rz-filter-team")
                yield Select[int]([], prompt="Season", id="rz-filter-season")
                yield Button("Refresh", id="rz-btn-refresh", variant="primary")
            with TabbedContent(initial="rz-tab-players"):
                with TabPane("Players", id="rz-tab-players"):
                    yield DataTable(id="rz-player-table")
                with TabPane("Teams", id="rz-tab-teams"):
                    yield DataTable(id="rz-team-table")

    def on_mount(self) -> None:
        self.query_one("#rz-content").display = False
        team_table = self.query_one("#rz-team-table", DataTable)
        team_table.add_columns(*TEAM_COLS)
        team_table.cursor_type = "row"
        player_table = self.query_one("#rz-player-table", DataTable)
        player_table.add_columns(*PLAYER_COLS)
        player_table.cursor_type = "row"

    def activate(self) -> None:
        """Load data on first activation (called when view becomes visible)."""
        if not self._activated:
            self._activated = True
            self._fetch_data()

    @work(thread=True, exclusive=True)
    def _fetch_data(self, force_refresh: bool = False) -> None:
        try:
            pbp = load_pbp([2024, 2025], force_refresh=force_refresh)
            rosters = load_rosters([2024, 2025], force_refresh=force_refresh)
            team = compute_team_rz(pbp)
            player = compute_player_rz(pbp, rosters)
            self.app.call_from_thread(self._on_data_loaded, team, player)
        except Exception as e:
            self.app.call_from_thread(self._on_data_error, str(e))

    def _on_data_loaded(self, team: pl.DataFrame, player: pl.DataFrame) -> None:
        self._team_data = team
        self._player_data = player
        self._initializing = True
        self._populate_filters()
        self._initializing = False
        self._apply_filters()
        self.query_one("#rz-loading").display = False
        self.query_one("#rz-content").display = True

    def _on_data_error(self, error: str) -> None:
        self.query_one("#rz-loading").display = False
        self.notify(f"Failed to load data: {error}", severity="error", timeout=10)

    # ── filters ──────────────────────────────────────────────

    def _populate_filters(self) -> None:
        if self._team_data is None:
            return

        teams = sorted(self._team_data["team"].unique().drop_nulls().to_list())
        team_select = self.query_one("#rz-filter-team", Select)
        team_select.set_options([("All", "All")] + [(t, t) for t in teams])
        team_select.value = "All"

        seasons = sorted(self._team_data["season"].unique().drop_nulls().to_list())
        season_select = self.query_one("#rz-filter-season", Select)
        season_select.set_options([(str(s), s) for s in seasons])
        season_select.value = seasons[-1]

    def _apply_filters(self) -> None:
        if self._team_data is None or self._player_data is None:
            return

        team_df = self._team_data
        player_df = self._player_data

        team_val = self.query_one("#rz-filter-team", Select).value
        if team_val not in ("All", Select.BLANK):
            team_df = team_df.filter(pl.col("team") == team_val)
            player_df = player_df.filter(pl.col("team") == team_val)

        season_val = self.query_one("#rz-filter-season", Select).value
        if season_val is not Select.BLANK:
            team_df = team_df.filter(pl.col("season") == season_val)
            player_df = player_df.filter(pl.col("season") == season_val)

        pos_val = self.query_one("#rz-filter-position", Select).value
        if pos_val not in ("All", Select.BLANK):
            player_df = player_df.filter(pl.col("position") == pos_val)

        self._fill_team_table(team_df)
        self._fill_player_table(player_df)

    # ── tables ───────────────────────────────────────────────

    def _fill_team_table(self, df: pl.DataFrame) -> None:
        table = self.query_one("#rz-team-table", DataTable)
        table.clear()
        for row in df.sort("conv_pct", descending=True).iter_rows(named=True):
            table.add_row(
                row["team"],
                str(row["rz_trips"]),
                str(row["rz_tds"]),
                f"{row['conv_pct']:.1f}%",
                f"{row['pass_pct']:.1f}%",
                f"{row['rush_pct']:.1f}%",
                f"{row['rz_epa']:.3f}" if row["rz_epa"] is not None else "—",
            )

    def _fill_player_table(self, df: pl.DataFrame) -> None:
        table = self.query_one("#rz-player-table", DataTable)
        table.clear()
        for row in df.sort("rz_tds", descending=True).iter_rows(named=True):
            table.add_row(
                row["player"],
                row["position"] or "—",
                row["team"],
                str(row["rz_targets"]),
                f"{row['rz_tgt_share']:.1f}%",
                str(row["rz_carries"]),
                str(row["rz_touches"]),
                str(row["rz_tds"]),
                f"{row['td_pct']:.1f}%",
            )

    # ── events ───────────────────────────────────────────────

    def on_select_changed(self, event: Select.Changed) -> None:
        if self._initializing:
            return
        self._apply_filters()

    def on_button_pressed(self, event: Button.Pressed) -> None:
        if event.button.id == "rz-btn-refresh":
            self.query_one("#rz-content").display = False
            self.query_one("#rz-loading").display = True
            self._fetch_data(force_refresh=True)
