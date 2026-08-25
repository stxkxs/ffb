"""Red zone efficiency — Textual TUI view."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import polars as pl
from textual.app import ComposeResult
from textual.widgets import DataTable, Select, TabbedContent, TabPane

from ffb.data.loader import load_pbp, load_rosters
from ffb.data.seasons import recent_seasons
from ffb.red_zone.engine import compute_player_rz, compute_team_rz
from ffb.ui.base import ALL, ToolView

TEAM_COLS = ("Team", "RZ Trips", "RZ TD", "Conv%", "Pass%", "Rush%", "EPA/Play")
PLAYER_COLS = ("Player", "Pos", "Team", "RZ Tgt", "Tgt%", "RZ Rush", "Touches", "RZ TD", "TD%")

#: Seasons loaded together, so a red zone rate is read against the season before it.
SEASON_COUNT = 2


@dataclass(frozen=True)
class RedZoneData:
    """Both tables of one load, in the order they are rendered."""

    team: pl.DataFrame
    player: pl.DataFrame


def _team_row(row: dict[str, Any]) -> tuple[str, ...]:
    """Map a team red zone row to its table cells."""
    return (
        row["team"],
        str(row["rz_trips"]),
        str(row["rz_tds"]),
        f"{row['conv_pct']:.1f}%",
        f"{row['pass_pct']:.1f}%",
        f"{row['rush_pct']:.1f}%",
        f"{row['rz_epa']:.3f}" if row["rz_epa"] is not None else "—",
    )


def _player_row(row: dict[str, Any]) -> tuple[str, ...]:
    """Map a player red zone row to its table cells."""
    return (
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


class RedZoneView(ToolView):
    """Red zone efficiency analyzer with team and player views."""

    ID_PREFIX = "rz"
    LOAD_LABEL = f"Downloading {SEASON_COUNT} seasons of play-by-play and rosters"

    def __init__(self, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self._data: RedZoneData | None = None

    def compose_content(self) -> ComposeResult:
        yield from self.compose_filter_bar(
            self.position_select(),
            Select[str]([], prompt="Team", id="rz-filter-team"),
            Select[int]([], prompt="Season", id="rz-filter-season"),
        )
        with TabbedContent(initial="rz-tab-players"):
            with TabPane("Players", id="rz-tab-players"):
                yield DataTable(id="rz-player-table")
            with TabPane("Teams", id="rz-tab-teams"):
                yield DataTable(id="rz-team-table")

    def on_mount(self) -> None:
        for table_id, columns in (
            ("rz-team-table", TEAM_COLS),
            ("rz-player-table", PLAYER_COLS),
        ):
            table = self.query_one(f"#{table_id}", DataTable)
            table.add_columns(*columns)
            table.cursor_type = "row"

    # ── load ─────────────────────────────────────────────────

    def fetch(self, force_refresh: bool) -> RedZoneData:
        """Reduce play-by-play and rosters to the team and player red zone tables.

        Both frames come back in display order, so the filters narrow them without
        having to re-establish the ranking each table is read by.
        """
        seasons = recent_seasons(SEASON_COUNT)
        pbp = load_pbp(seasons, force_refresh=force_refresh)
        rosters = load_rosters(seasons, force_refresh=force_refresh)
        return RedZoneData(
            team=compute_team_rz(pbp).sort("conv_pct", descending=True),
            player=compute_player_rz(pbp, rosters).sort("rz_tds", descending=True),
        )

    def apply(self, payload: RedZoneData) -> None:
        self._data = payload
        self._populate_filters()
        self._apply_filters()

    # ── filters ──────────────────────────────────────────────

    def _populate_filters(self) -> None:
        if self._data is None:
            return

        teams = sorted(self._data.team["team"].unique().drop_nulls().to_list())
        team_select = self.query_one("#rz-filter-team", Select)
        team_select.set_options(self.all_options(teams))
        team_select.value = ALL

        seasons = sorted(self._data.team["season"].unique().drop_nulls().to_list())
        season_select = self.query_one("#rz-filter-season", Select)
        season_select.set_options([(str(s), s) for s in seasons])
        # A load that reached no played games leaves the season filter blank rather
        # than selecting a season the data does not carry.
        if seasons:
            season_select.value = seasons[-1]

    def _apply_filters(self) -> None:
        if self._data is None:
            return

        team_df = self._data.team
        player_df = self._data.player

        team = self.narrowing("rz-filter-team")
        if team is not None:
            team_df = team_df.filter(pl.col("team") == team)
            player_df = player_df.filter(pl.col("team") == team)

        season = self.selected("rz-filter-season")
        if season is not None:
            team_df = team_df.filter(pl.col("season") == season)
            player_df = player_df.filter(pl.col("season") == season)

        position = self.narrowing("rz-filter-position")
        if position is not None:
            player_df = player_df.filter(pl.col("position") == position)

        self.fill_table(
            "rz-team-table",
            team_df,
            _team_row,
            "No red zone trips for the selected team and season.",
        )
        self.fill_table(
            "rz-player-table",
            player_df,
            _player_row,
            "No red zone touches for the selected position, team and season.",
        )

    # ── events ───────────────────────────────────────────────

    def filter_changed(self, filter_id: str | None) -> None:
        """Narrow both tables to the position, team and season the filters read."""
        self._apply_filters()
