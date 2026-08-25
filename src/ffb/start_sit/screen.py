"""Start/sit matchup projections — Textual TUI view."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import polars as pl
from textual.app import ComposeResult
from textual.widgets import DataTable, Select

from ffb.data.loader import load_schedules, load_weekly_stats
from ffb.data.seasons import recent_seasons
from ffb.start_sit.engine import compute_start_sit
from ffb.ui.base import ALL, ToolView

#: Table headers, in the order `_projection_row` emits cells.
COLUMNS = (
    "Player",
    "Pos",
    "Team",
    "Opp",
    "Baseline",
    "Opp Allows",
    "Lg Avg",
    "Mult",
    "Projected",
    "Verdict",
)

#: Seasons loaded. The season filter offers one entry per loaded season, so this
#: window sets how far back a lineup decision can be reviewed.
SEASON_WINDOW = 2

#: Stands in for the table when the engine projects nobody for the selected week.
NO_PROJECTIONS = (
    "No projections for this week. A matchup multiplier needs three completed weeks "
    "of defensive results behind it, so week 4 is the earliest week that projects."
)

#: Stands in for the table when projections exist but the filters exclude all of them.
NO_MATCHES = (
    "No players match the position and team filters. "
    "Widen either filter to see this week's projections."
)


@dataclass(frozen=True)
class StartSitData:
    """The two frames every projection in this view is computed from."""

    weekly_stats: pl.DataFrame
    schedules: pl.DataFrame


def _projection_row(row: dict[str, Any]) -> tuple[str, ...]:
    """Format one projection as table cells, in COLUMNS order.

    An em dash stands in for an absent points-allowed average, league average or
    multiplier, so a missing defensive sample reads as missing rather than as zero.
    """
    return (
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


class StartSitView(ToolView):
    """Matchup-adjusted start/sit projections for one week."""

    ID_PREFIX = "ss"
    LOAD_LABEL = "Loading weekly stats and schedules"

    def __init__(self, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self._weekly_stats: pl.DataFrame | None = None
        self._schedules: pl.DataFrame | None = None
        self._projections: pl.DataFrame | None = None

    def compose_content(self) -> ComposeResult:
        yield from self.compose_filter_bar(
            self.position_select(),
            Select[str]([], prompt="Team", id="ss-filter-team"),
            Select[int]([], prompt="Season", id="ss-filter-season"),
            Select[int]([], prompt="Week", id="ss-filter-week"),
        )
        yield DataTable(id="ss-table")

    def on_mount(self) -> None:
        # Textual dispatches every on_mount along the MRO, so the base class hides the
        # panes and pairs the table with its empty-state label without a super() call.
        table = self.query_one("#ss-table", DataTable)
        table.add_columns(*COLUMNS)
        table.cursor_type = "row"

    # ── load ─────────────────────────────────────────────────

    def fetch(self, force_refresh: bool) -> StartSitData:
        seasons = recent_seasons(SEASON_WINDOW)
        return StartSitData(
            weekly_stats=load_weekly_stats(seasons, force_refresh=force_refresh),
            schedules=load_schedules(seasons, force_refresh=force_refresh),
        )

    def apply(self, payload: StartSitData) -> None:
        self._weekly_stats = payload.weekly_stats
        self._schedules = payload.schedules
        self._populate_filters()
        self._compute_projections()

    # ── filters ──────────────────────────────────────────────

    def _populate_filters(self) -> None:
        if self._schedules is None:
            return

        reg = self._schedules.filter(pl.col("game_type") == "REG")

        seasons = sorted(reg["season"].unique().drop_nulls().to_list())
        season_select = self.query_one("#ss-filter-season", Select)
        season_select.set_options([(str(s), s) for s in seasons])
        if seasons:
            season_select.value = seasons[-1]

        self._update_week_and_team_options()

    def _update_week_and_team_options(self) -> None:
        if self._schedules is None:
            return
        season = self.selected("ss-filter-season")
        if season is None:
            return

        reg = self._schedules.filter((pl.col("season") == season) & (pl.col("game_type") == "REG"))

        weeks = sorted(reg["week"].unique().drop_nulls().to_list())
        week_select = self.query_one("#ss-filter-week", Select)
        week_select.set_options([(f"Week {w}", w) for w in weeks])
        # The week with the most recent results is the one a lineup decision is about.
        if weeks:
            week_select.value = weeks[-1]

        teams = sorted(
            set(reg["home_team"].unique().drop_nulls().to_list())
            | set(reg["away_team"].unique().drop_nulls().to_list())
        )
        team_select = self.query_one("#ss-filter-team", Select)
        team_select.set_options(self.all_options(teams))
        team_select.value = ALL

    def _compute_projections(self) -> None:
        """Re-run the engine for the selected season and week.

        Projections are a function of the week, not of the position and team filters,
        so a season or week change recomputes rather than re-filtering.
        """
        if self._weekly_stats is None or self._schedules is None:
            return

        season = self.selected("ss-filter-season")
        week = self.selected("ss-filter-week")
        # Both selects carry the season and week integers taken from the schedule until
        # a load fills their options, and a filter holding no selection projects nothing.
        if not isinstance(season, int) or not isinstance(week, int):
            return

        self._projections = compute_start_sit(self._weekly_stats, self._schedules, season, week)
        self._apply_filters()

    def _apply_filters(self) -> None:
        df = self._projections
        if df is None:
            return

        # Filter expressions name columns, and a result with no rows is not required to
        # declare any. Routing an empty result straight to the table keeps this view
        # correct whatever shape the engine gives an empty frame, and filtering zero
        # rows could only ever yield zero rows.
        if df.height == 0:
            self.fill_table("ss-table", df, _projection_row, NO_PROJECTIONS)
            return

        pos = self.narrowing("ss-filter-position")
        if pos is not None:
            df = df.filter(pl.col("position") == pos)

        team = self.narrowing("ss-filter-team")
        if team is not None:
            df = df.filter(pl.col("team") == team)

        self.fill_table("ss-table", df, _projection_row, NO_MATCHES)

    # ── events ───────────────────────────────────────────────

    def filter_changed(self, filter_id: str | None) -> None:
        """Project again for a season or week change; re-filter for the rest.

        A season change rebuilds the week and team options, which reaches the engine
        through the week the rebuild selects.
        """
        if filter_id == "ss-filter-season":
            self._update_week_and_team_options()
            self._compute_projections()
        elif filter_id == "ss-filter-week":
            self._compute_projections()
        else:
            self._apply_filters()
