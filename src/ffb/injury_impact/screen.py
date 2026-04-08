"""Injury impact — Textual TUI view."""

from __future__ import annotations

import polars as pl
from textual import work
from textual.app import ComposeResult
from textual.containers import Horizontal, Vertical
from textual.suggester import SuggestFromList
from textual.widget import Widget
from textual.widgets import (
    Button,
    DataTable,
    Input,
    LoadingIndicator,
    Select,
    Static,
)

from ffb.data.loader import load_player_ids, load_snap_counts, load_weekly_stats
from ffb.injury_impact.engine import compute_injury_impact, get_searchable_players

COLUMNS = (
    "Teammate", "Pos", "Tm", "Szn",
    "Gm W/", "Gm W/O",
    "FPts W/", "FPts W/O", "Δ FPts",
    "Tgt W/", "Tgt W/O",
    "Tch W/", "Tch W/O",
    "Conf",
)


class InjuryImpactView(Widget):
    """Teammate usage shifts when stars miss games."""

    DEFAULT_CSS = """
    InjuryImpactView {
        height: 1fr;
        width: 1fr;
    }

    #ii-loading {
        height: 1fr;
    }

    #ii-content {
        height: 1fr;
    }

    .filter-bar {
        height: auto;
        max-height: 5;
        padding: 1;
        background: $surface-darken-1;
    }

    .filter-bar Input {
        width: 2fr;
        margin-right: 1;
    }

    .filter-bar Select {
        width: 1fr;
        margin-right: 1;
    }

    .filter-bar #ii-btn-refresh {
        margin-left: 1;
    }

    #ii-player-info {
        height: auto;
        padding: 0 1;
        color: $accent;
        text-style: bold;
    }
    """

    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)
        self._snaps: pl.DataFrame | None = None
        self._weekly_stats: pl.DataFrame | None = None
        self._player_ids: pl.DataFrame | None = None
        self._activated = False
        self._initializing = True

    def compose(self) -> ComposeResult:
        yield LoadingIndicator(id="ii-loading")
        with Vertical(id="ii-content"):
            with Horizontal(classes="filter-bar"):
                yield Input(
                    placeholder="Search player...",
                    id="ii-search",
                )
                yield Select[str]([], prompt="Season", id="ii-filter-season")
                yield Button("Refresh", id="ii-btn-refresh", variant="primary")
            yield Static("", id="ii-player-info")
            yield DataTable(id="ii-table")

    def on_mount(self) -> None:
        self.query_one("#ii-content").display = False
        table = self.query_one("#ii-table", DataTable)
        table.add_columns(*COLUMNS)
        table.cursor_type = "row"

    def activate(self) -> None:
        """Load base data on first activation."""
        if not self._activated:
            self._activated = True
            self._fetch_base_data()

    @work(thread=True, exclusive=True)
    def _fetch_base_data(self, force_refresh: bool = False) -> None:
        try:
            snaps = load_snap_counts([2023, 2024, 2025], force_refresh=force_refresh)
            weekly = load_weekly_stats([2023, 2024, 2025], force_refresh=force_refresh)
            ids = load_player_ids(force_refresh=force_refresh)
            players = get_searchable_players(snaps)
            self.app.call_from_thread(
                self._on_base_data_loaded, snaps, weekly, ids, players
            )
        except Exception as e:
            self.app.call_from_thread(self._on_data_error, str(e))

    def _on_base_data_loaded(
        self,
        snaps: pl.DataFrame,
        weekly: pl.DataFrame,
        ids: pl.DataFrame,
        players: list[str],
    ) -> None:
        self._snaps = snaps
        self._weekly_stats = weekly
        self._player_ids = ids

        # Set up search autocomplete
        search = self.query_one("#ii-search", Input)
        search.suggester = SuggestFromList(players, case_sensitive=False)

        # Populate season filter
        self._initializing = True
        reg_snaps = snaps.filter(snaps["game_type"] == "REG")
        seasons = sorted(reg_snaps["season"].unique().drop_nulls().to_list())
        season_select = self.query_one("#ii-filter-season", Select)
        season_select.set_options([("All", "All")] + [(str(s), str(s)) for s in seasons])
        season_select.value = "All"
        self._initializing = False

        self.query_one("#ii-loading").display = False
        self.query_one("#ii-content").display = True

    def _on_data_error(self, error: str) -> None:
        self.query_one("#ii-loading").display = False
        self.notify(f"Failed to load data: {error}", severity="error", timeout=10)

    # ── search ───────────────────────────────────────────────

    def on_input_submitted(self, event: Input.Submitted) -> None:
        if event.input.id == "ii-search" and event.value.strip():
            self._run_search(event.value.strip())

    @work(thread=True, exclusive=True)
    def _run_search(self, player_name: str) -> None:
        if self._snaps is None or self._weekly_stats is None or self._player_ids is None:
            return

        snaps = self._snaps
        weekly = self._weekly_stats

        # Apply season filter
        season_val = self.app.call_from_thread(self._get_season_filter)
        if season_val and season_val != "All":
            season_int = int(season_val)
            snaps = snaps.filter(pl.col("season") == season_int)
            weekly = weekly.filter(pl.col("season") == season_int)

        info, df = compute_injury_impact(
            player_name, snaps, weekly, self._player_ids
        )
        self.app.call_from_thread(self._on_search_complete, player_name, info, df)

    def _get_season_filter(self) -> str:
        val = self.query_one("#ii-filter-season", Select).value
        return str(val) if val is not Select.BLANK else "All"

    def _on_search_complete(
        self,
        player_name: str,
        info: dict | None,
        df: pl.DataFrame,
    ) -> None:
        info_widget = self.query_one("#ii-player-info", Static)
        table = self.query_one("#ii-table", DataTable)
        table.clear()

        if info is None:
            info_widget.update("")
            self.notify(
                f"No missed games found for {player_name}",
                severity="warning",
                timeout=5,
            )
            return

        info_widget.update(
            f"{info['name']}  |  {info['team']}  |  {info['position']}  |  "
            f"{info['games_missed']} games missed in {info['season']}"
        )

        for row in df.iter_rows(named=True):
            table.add_row(
                row["teammate"] or "—",
                row["position"] or "—",
                row["team"],
                str(row["season"]),
                str(row["games_with"]),
                str(row["games_without"]),
                f"{row['fpts_with']:.1f}" if row["fpts_with"] is not None else "—",
                f"{row['fpts_without']:.1f}" if row["fpts_without"] is not None else "—",
                f"{row['delta_fpts']:+.1f}" if row["delta_fpts"] is not None else "—",
                f"{row['tgt_with']:.1f}" if row["tgt_with"] is not None else "—",
                f"{row['tgt_without']:.1f}" if row["tgt_without"] is not None else "—",
                f"{row['touches_with']:.1f}" if row["touches_with"] is not None else "—",
                f"{row['touches_without']:.1f}" if row["touches_without"] is not None else "—",
                row["confidence"],
            )

    # ── events ───────────────────────────────────────────────

    def on_select_changed(self, event: Select.Changed) -> None:
        if self._initializing:
            return
        # Re-run search if one is active
        search = self.query_one("#ii-search", Input)
        if search.value.strip():
            self._run_search(search.value.strip())

    def on_button_pressed(self, event: Button.Pressed) -> None:
        if event.button.id == "ii-btn-refresh":
            self.query_one("#ii-content").display = False
            self.query_one("#ii-loading").display = True
            self._fetch_base_data(force_refresh=True)
