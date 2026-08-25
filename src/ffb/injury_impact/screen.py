"""Injury impact — Textual TUI view."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import polars as pl
from textual.app import ComposeResult
from textual.suggester import SuggestFromList
from textual.widgets import DataTable, Input, Select, Static

from ffb.data.loader import load_player_ids, load_snap_counts, load_weekly_stats
from ffb.data.seasons import recent_seasons
from ffb.injury_impact.engine import PlayerInfo, compute_injury_impact, get_searchable_players
from ffb.ui.base import ALL, ToolView

COLUMNS = (
    "Teammate",
    "Pos",
    "Tm",
    "Szn",
    "Gm W/",
    "Gm W/O",
    "FPts W/",
    "FPts W/O",
    "Δ FPts",
    "Tgt W/",
    "Tgt W/O",
    "Tch W/",
    "Tch W/O",
    "Conf",
)

#: How many seasons the splits draw on. A star misses games rarely, so a window
#: shorter than this leaves most searches with no absence to compare against.
SEASON_WINDOW = 3

#: Stands in for the table until a search runs.
PROMPT_MESSAGE = "Search a player to see how their teammates' usage shifted while they sat out."


@dataclass(frozen=True)
class BaseData:
    """The frames every search runs against, plus the names the search suggests."""

    snaps: pl.DataFrame
    weekly_stats: pl.DataFrame
    player_ids: pl.DataFrame
    players: list[str]


def _fixed(value: float | None, *, signed: bool = False) -> str:
    """Format a per-game average to one decimal, or mark it absent."""
    if value is None:
        return "—"
    return f"{value:+.1f}" if signed else f"{value:.1f}"


def _row_cells(row: dict[str, Any]) -> tuple[str, ...]:
    """Map one teammate split to its table cells, in `COLUMNS` order."""
    return (
        row["teammate"] or "—",
        row["position"] or "—",
        row["team"],
        str(row["season"]),
        str(row["games_with"]),
        str(row["games_without"]),
        _fixed(row["fpts_with"]),
        _fixed(row["fpts_without"]),
        _fixed(row["delta_fpts"], signed=True),
        _fixed(row["tgt_with"]),
        _fixed(row["tgt_without"]),
        _fixed(row["touches_with"]),
        _fixed(row["touches_without"]),
        row["confidence"],
    )


class InjuryImpactView(ToolView):
    """Teammate usage shifts when stars miss games."""

    ID_PREFIX = "ii"
    LOAD_LABEL = "Downloading snap counts, weekly stats and player IDs"

    DEFAULT_CSS = """
    #ii-player-info {
        height: auto;
        padding: 0 1;
        color: $accent;
        text-style: bold;
    }
    """

    def __init__(self, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self._data: BaseData | None = None

    def compose_content(self) -> ComposeResult:
        yield from self.compose_filter_bar(
            Input(placeholder="Search player...", id="ii-search"),
            Select[str]([], prompt="Season", id="ii-filter-season"),
        )
        yield Static("", id="ii-player-info")
        yield DataTable(id="ii-table")

    def on_mount(self) -> None:
        table = self.query_one("#ii-table", DataTable)
        table.add_columns(*COLUMNS)
        table.cursor_type = "row"

    # ── load ─────────────────────────────────────────────────

    def fetch(self, force_refresh: bool) -> BaseData:
        seasons = recent_seasons(SEASON_WINDOW)
        snaps = load_snap_counts(seasons, force_refresh=force_refresh)
        weekly = load_weekly_stats(seasons, force_refresh=force_refresh)
        ids = load_player_ids(force_refresh=force_refresh)
        return BaseData(snaps, weekly, ids, get_searchable_players(snaps))

    def apply(self, payload: BaseData) -> None:
        self._data = payload

        search = self.query_one("#ii-search", Input)
        search.suggester = SuggestFromList(payload.players, case_sensitive=False)

        reg_snaps = payload.snaps.filter(pl.col("game_type") == "REG")
        seasons = sorted(reg_snaps["season"].unique().drop_nulls().to_list())
        season_select = self.query_one("#ii-filter-season", Select)
        season_select.set_options(self.all_options(str(season) for season in seasons))
        season_select.value = ALL

        self.query_one("#ii-player-info", Static).update("")
        self.fill_table("ii-table", pl.DataFrame(), _row_cells, PROMPT_MESSAGE)

    # ── search ───────────────────────────────────────────────

    def on_input_submitted(self, event: Input.Submitted) -> None:
        if event.input.id != "ii-search":
            return
        player_name = event.value.strip()
        if player_name:
            self._search(player_name)

    def _search(self, player_name: str) -> None:
        """Start a search, or say why the data it needs is not there.

        The tool is reachable with no data after a cancelled or failed load, so the
        absence gets an answer rather than a search that does nothing.
        """
        data = self._data
        if data is None:
            self.notify(
                "No data loaded. Press Refresh to download snap counts, "
                "weekly stats and player IDs.",
                severity="warning",
                timeout=5,
            )
            return
        season = self.narrowing("ii-filter-season")
        # A search started while an earlier one runs renders its own result: the shared
        # worker drops the result of the search it supersedes, so the table answers the
        # term in the search box.
        self.run_off_thread(
            lambda: self._splits(player_name, data, season),
            lambda result: self._on_search_complete(player_name, *result),
            self._on_search_error,
            group="ii-search",
        )

    def _splits(
        self,
        player_name: str,
        data: BaseData,
        season: str | None,
    ) -> tuple[PlayerInfo | None, pl.DataFrame]:
        """Compute one player's teammate splits, off the UI thread.

        `season` narrows both frames to one season; None reads every season loaded.
        """
        snaps = data.snaps
        weekly = data.weekly_stats
        if season is not None:
            snaps = snaps.filter(pl.col("season") == int(season))
            weekly = weekly.filter(pl.col("season") == int(season))
        return compute_injury_impact(player_name, snaps, weekly, data.player_ids)

    def _on_search_complete(
        self,
        player_name: str,
        info: PlayerInfo | None,
        df: pl.DataFrame,
    ) -> None:
        info_widget = self.query_one("#ii-player-info", Static)

        if info is None:
            info_widget.update("")
            self.fill_table(
                "ii-table",
                pl.DataFrame(),
                _row_cells,
                f"No missed-game splits for {player_name}. Either no player of that "
                f"name took an offensive snap in the selected seasons, or they missed "
                f"too few games to compare.",
            )
            return

        info_widget.update(
            f"{info['name']}  |  {info['team']}  |  {info['position']}  |  "
            f"{info['games_missed']} games missed in {info['season']}"
        )
        self.fill_table(
            "ii-table",
            df,
            _row_cells,
            f"{player_name} missed games, but no teammate recorded stats both with "
            f"and without them.",
        )

    def _on_search_error(self, error: str) -> None:
        self.report_error(f"Search failed: {error}")

    # ── events ───────────────────────────────────────────────

    def filter_changed(self, filter_id: str | None) -> None:
        """Run the search box's term again against the season the filter reads."""
        search = self.query_one("#ii-search", Input).value.strip()
        if search:
            self._search(search)
