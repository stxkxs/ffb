"""Properties of the shared tool view: one load, one refresh, one filter pass.

Every test here drives a real Textual app through the test pilot. The loaders are
stubbed with the league fixtures, so a view under test reaches its engines with data
and reaches the network with nothing.
"""

from __future__ import annotations

import asyncio
import functools
import threading
from collections.abc import Awaitable, Callable
from types import ModuleType
from typing import Any, NamedTuple

import polars as pl
import pytest
from textual.app import App, ComposeResult
from textual.pilot import Pilot
from textual.widgets import Button, DataTable, Select, Static
from textual.widgets._select import InvalidSelectValueError

from ffb.injury_impact import screen as injury_impact_screen
from ffb.red_zone import screen as red_zone_screen
from ffb.snap_share import screen as snap_share_screen
from ffb.start_sit import screen as start_sit_screen
from ffb.trade_value import screen as trade_value_screen
from ffb.ui import base
from ffb.ui.base import ALL, POSITIONS, ToolView
from ffb.waiver_wire import screen as waiver_wire_screen
from tests.conftest import LEAGUE_POSITIONS, LEAGUE_SEASONS, LEAGUE_TEAMS, REGULAR_SEASON_WEEKS

#: How long a probe waits for the test to open its gate before giving up, in seconds.
#: A test that forgets to open one fails on its assertions rather than hanging.
GATE_TIMEOUT = 5.0

#: How many times a settle dispatches the message queue and drains the workers. A load
#: answers on a worker, renders on the UI thread and posts filter messages from there,
#: so the queue has to be drained more than once for the view to come to rest.
SETTLE_PASSES = 3

#: The season the league fixtures end at, which every view's season filter defaults to.
LATEST_SEASON = max(LEAGUE_SEASONS)

#: Starters the league fields in a week no team sits out.
LEAGUE_STARTERS = len(LEAGUE_TEAMS) * len(LEAGUE_POSITIONS)

#: Stands in for the probe's table when a result carries no rows.
EMPTY_MESSAGE = "No rows for these filters."

#: A season a schedule covers and no result reaches, as nflverse holds one between
#: publishing a season's schedule and its first game being played.
UNPLAYED_SEASON = LATEST_SEASON + 1

#: Weeks of results a season part-way through carries.
WEEKS_PLAYED = 5


# ── the six tools ────────────────────────────────────────────────────────────


class Tool(NamedTuple):
    """One tool: the view class, and the module its loader names are bound in."""

    name: str
    module: ModuleType
    view: type[ToolView]


TOOLS = (
    Tool("snap-share", snap_share_screen, snap_share_screen.SnapShareView),
    Tool("red-zone", red_zone_screen, red_zone_screen.RedZoneView),
    Tool("injury-impact", injury_impact_screen, injury_impact_screen.InjuryImpactView),
    Tool("waiver-wire", waiver_wire_screen, waiver_wire_screen.WaiverWireView),
    Tool("start-sit", start_sit_screen, start_sit_screen.StartSitView),
    Tool("trade-value", trade_value_screen, trade_value_screen.TradeValueView),
)

TOOL_IDS = [tool.name for tool in TOOLS]


class LoaderCall(NamedTuple):
    """One loader call a view made, and whether it asked for a fresh download."""

    view: str
    loader: str
    force_refresh: bool


def loader_frames(
    snaps: pl.DataFrame,
    weekly: pl.DataFrame,
    ids: pl.DataFrame,
    schedules: pl.DataFrame,
    injuries: pl.DataFrame,
    pbp: pl.DataFrame,
    rosters: pl.DataFrame,
) -> dict[str, pl.DataFrame]:
    """Map each loader name to the frame a stub of it answers with."""
    return {
        "load_snap_counts": snaps,
        "load_weekly_stats": weekly,
        "load_player_ids": ids,
        "load_schedules": schedules,
        "load_injuries": injuries,
        "load_pbp": pbp,
        "load_rosters": rosters,
    }


def _recorder(
    view: str,
    loader: str,
    frame: pl.DataFrame,
    calls: list[LoaderCall],
) -> Callable[..., pl.DataFrame]:
    """Build a stand-in for one loader that records its call and answers with `frame`."""

    def load(*_seasons: Any, force_refresh: bool = False) -> pl.DataFrame:
        calls.append(LoaderCall(view, loader, force_refresh))
        return frame

    return load


def stub_loaders(
    monkeypatch: pytest.MonkeyPatch,
    frames: dict[str, pl.DataFrame],
    calls: list[LoaderCall],
) -> None:
    """Point every screen module's loaders at `frames`, recording each call in `calls`.

    A loader is stubbed in the module that binds the name, so a view reaches the
    recorder through the same reference its `fetch` calls.
    """
    for tool in TOOLS:
        for loader, frame in frames.items():
            if not hasattr(tool.module, loader):
                continue
            monkeypatch.setattr(tool.module, loader, _recorder(tool.name, loader, frame, calls))


@pytest.fixture()
def loaders(
    monkeypatch: pytest.MonkeyPatch,
    league_snap_counts: pl.DataFrame,
    league_weekly_stats: pl.DataFrame,
    player_ids: pl.DataFrame,
    full_season_schedules: pl.DataFrame,
    injuries: pl.DataFrame,
    pbp: pl.DataFrame,
    rosters: pl.DataFrame,
) -> list[LoaderCall]:
    """Every loader call the views make, in order, against the league fixtures."""
    calls: list[LoaderCall] = []
    stub_loaders(
        monkeypatch,
        loader_frames(
            league_snap_counts,
            league_weekly_stats,
            player_ids,
            full_season_schedules,
            injuries,
            pbp,
            rosters,
        ),
        calls,
    )
    return calls


# ── harness ──────────────────────────────────────────────────────────────────


def piloted(test: Callable[..., Awaitable[None]]) -> Callable[..., None]:
    """Adapt an async test body to pytest, which collects synchronous functions.

    Textual drives a view from an asyncio loop, so every test that mounts one is a
    coroutine and owns the loop it runs on.
    """

    @functools.wraps(test)
    def run(*args: Any, **kwargs: Any) -> None:
        asyncio.run(test(*args, **kwargs))

    return run


class Host(App[None]):
    """An app whose only widget is the view under test."""

    def __init__(self, tool: ToolView) -> None:
        super().__init__()
        self.tool = tool

    def compose(self) -> ComposeResult:
        yield self.tool


async def settle(pilot: Pilot[None]) -> None:
    """Run the view to rest: every worker finished, every message it posted dispatched."""
    for _ in range(SETTLE_PASSES):
        await pilot.pause()
        await pilot.app.workers.wait_for_complete()
    await pilot.pause()


async def reached(event: threading.Event) -> None:
    """Wait, off the UI thread, until the probe's fetch reaches `event`."""
    assert await asyncio.to_thread(event.wait, GATE_TIMEOUT), "the probe's fetch stalled"


def spy(monkeypatch: pytest.MonkeyPatch, module: ModuleType, name: str) -> list[tuple]:
    """Record the arguments of every call to `module.name`, leaving its result alone."""
    calls: list[tuple] = []
    original = getattr(module, name)

    def record(*args: Any, **kwargs: Any) -> Any:
        calls.append((args, kwargs))
        return original(*args, **kwargs)

    monkeypatch.setattr(module, name, record)
    return calls


class Clock:
    """A monotonic clock the test advances by hand."""

    def __init__(self) -> None:
        self.seconds = 0.0

    def __call__(self) -> float:
        return self.seconds


class ProbeView(ToolView):
    """A tool view whose fetch, payload and failure the test owns.

    `gate` holds the fetch on its worker thread while it is clear, so a test can read
    the view mid-load; `entered` and `returned` mark the fetch reaching and leaving
    that gate.
    """

    ID_PREFIX = "pb"
    LOAD_LABEL = "Downloading probe data"

    def __init__(self, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.fetched: list[bool] = []
        self.applied: list[pl.DataFrame] = []
        self.filter_passes: list[str | None] = []
        self.payload = pl.DataFrame({"player": ["Alpha"]})
        self.failure: Exception | None = None
        self.gate = threading.Event()
        self.gate.set()
        self.entered = threading.Event()
        self.returned = threading.Event()

    def compose_content(self) -> ComposeResult:
        yield from self.compose_filter_bar(
            self.position_select(),
            Select[int]([("Zero", 0), ("One", 1)], id="pb-filter-count"),
        )
        yield DataTable(id="pb-table")

    def on_mount(self) -> None:
        self.query_one("#pb-table", DataTable).add_columns("Player")

    def fetch(self, force_refresh: bool) -> pl.DataFrame:
        self.fetched.append(force_refresh)
        self.entered.set()
        self.gate.wait(GATE_TIMEOUT)
        try:
            if self.failure is not None:
                raise self.failure
            return self.payload
        finally:
            self.returned.set()

    def apply(self, payload: pl.DataFrame) -> None:
        self.applied.append(payload)
        self.fill_table("pb-table", payload, lambda row: (row["player"],), EMPTY_MESSAGE)

    def filter_changed(self, filter_id: str | None) -> None:
        self.filter_passes.append(filter_id)


# ── the refresh contract ─────────────────────────────────────────────────────


@pytest.mark.parametrize("tool", TOOLS, ids=TOOL_IDS)
@piloted
async def test_refresh_reaches_every_loader_with_force_refresh(
    tool: Tool, loaders: list[LoaderCall]
) -> None:
    """Refresh promises a fresh download, so every loader the view calls is forced."""
    view = tool.view()
    async with Host(view).run_test() as pilot:
        view.activate()
        await settle(pilot)
        first = [call.loader for call in loaders]
        assert first, f"{tool.name} reached no loader on its first load"

        loaders.clear()
        view.query_one(f"#{view.ID_PREFIX}-btn-refresh", Button).press()
        await settle(pilot)

        assert loaders == [LoaderCall(tool.name, loader, True) for loader in first]


@pytest.mark.parametrize("tool", TOOLS, ids=TOOL_IDS)
@piloted
async def test_first_activation_reads_the_cache(tool: Tool, loaders: list[LoaderCall]) -> None:
    """Showing a tool asks for what is cached; only Refresh asks for a download."""
    view = tool.view()
    async with Host(view).run_test() as pilot:
        view.activate()
        await settle(pilot)

        assert loaders, f"{tool.name} reached no loader on its first load"
        assert [call.force_refresh for call in loaders] == [False] * len(loaders)


@piloted
async def test_a_second_activation_does_not_reload() -> None:
    """A tool shown again renders what it holds rather than downloading it again."""
    view = ProbeView()
    async with Host(view).run_test() as pilot:
        view.activate()
        await settle(pilot)
        view.activate()
        await settle(pilot)

        assert view.fetched == [False]


@piloted
async def test_refresh_reloads_a_view_that_already_holds_data() -> None:
    """The refresh button loads again whatever the view already rendered."""
    view = ProbeView()
    async with Host(view).run_test() as pilot:
        view.activate()
        await settle(pilot)
        view.query_one("#pb-btn-refresh", Button).press()
        await settle(pilot)

        assert view.fetched == [False, True]


# ── load lifecycle ───────────────────────────────────────────────────────────


@piloted
async def test_the_loading_pane_stands_in_for_the_content_during_a_load() -> None:
    view = ProbeView()
    async with Host(view).run_test() as pilot:
        view.gate.clear()
        try:
            view.activate()
            await pilot.pause()
            await reached(view.entered)
            panes = (
                view.query_one("#pb-loading").display,
                view.query_one("#pb-content").display,
            )
        finally:
            view.gate.set()
        await settle(pilot)

    assert panes == (True, False)


@piloted
async def test_a_finished_load_shows_the_content() -> None:
    view = ProbeView()
    async with Host(view).run_test() as pilot:
        view.activate()
        await settle(pilot)

        assert (
            view.query_one("#pb-loading").display,
            view.query_one("#pb-content").display,
        ) == (False, True)


@piloted
async def test_a_failing_fetch_reports_through_the_shared_error_path(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    reported: list[str] = []
    monkeypatch.setattr(ToolView, "report_error", lambda _self, message: reported.append(message))
    view = ProbeView()
    view.failure = RuntimeError("nflverse is unreachable")
    async with Host(view).run_test() as pilot:
        view.activate()
        await settle(pilot)

        assert reported == ["Failed to load data: nflverse is unreachable"]


@piloted
async def test_a_failing_fetch_renders_nothing() -> None:
    view = ProbeView()
    view.failure = RuntimeError("nflverse is unreachable")
    async with Host(view).run_test() as pilot:
        view.activate()
        await settle(pilot)

        assert view.applied == []


@piloted
async def test_a_failing_fetch_hides_the_loading_pane() -> None:
    """A failure returns the tool to its content, so a retry starts from the view."""
    view = ProbeView()
    view.failure = RuntimeError("nflverse is unreachable")
    async with Host(view).run_test() as pilot:
        view.activate()
        await settle(pilot)

        assert (
            view.query_one("#pb-loading").display,
            view.query_one("#pb-content").display,
        ) == (False, True)


@piloted
async def test_a_cancelled_load_never_renders_its_result() -> None:
    view = ProbeView()
    async with Host(view).run_test() as pilot:
        view.gate.clear()
        try:
            view.activate()
            await pilot.pause()
            await reached(view.entered)
            view.query_one("#pb-btn-cancel", Button).press()
            await pilot.pause()
        finally:
            view.gate.set()
        await reached(view.returned)
        await settle(pilot)

        assert view.applied == []


@piloted
async def test_a_cancelled_load_shows_the_content_again() -> None:
    view = ProbeView()
    async with Host(view).run_test() as pilot:
        view.gate.clear()
        try:
            view.activate()
            await pilot.pause()
            await reached(view.entered)
            view.query_one("#pb-btn-cancel", Button).press()
            await pilot.pause()
        finally:
            view.gate.set()
        await reached(view.returned)
        await settle(pilot)

        assert (
            view.query_one("#pb-loading").display,
            view.query_one("#pb-content").display,
        ) == (False, True)


@piloted
async def test_the_cancel_binding_is_offered_while_a_load_runs() -> None:
    view = ProbeView()
    async with Host(view).run_test() as pilot:
        view.gate.clear()
        try:
            view.activate()
            await pilot.pause()
            await reached(view.entered)
            offered = view.check_action("cancel_load", ())
        finally:
            view.gate.set()
        await settle(pilot)

    assert offered is True


@piloted
async def test_the_cancel_binding_is_withheld_once_the_load_finishes() -> None:
    view = ProbeView()
    async with Host(view).run_test() as pilot:
        view.activate()
        await settle(pilot)

        assert view.check_action("cancel_load", ()) is False


# ── the elapsed readout ──────────────────────────────────────────────────────


@pytest.mark.parametrize(
    ("elapsed", "readout"),
    [
        (0.0, "00:00"),
        (0.9, "00:00"),
        (1.0, "00:01"),
        (59.0, "00:59"),
        (60.0, "01:00"),
        (65.0, "01:05"),
        (3600.0, "60:00"),
    ],
)
@piloted
async def test_the_elapsed_readout_counts_the_time_since_the_load_started(
    monkeypatch: pytest.MonkeyPatch, elapsed: float, readout: str
) -> None:
    clock = Clock()
    monkeypatch.setattr(base, "monotonic", clock)
    view = ProbeView()
    async with Host(view).run_test() as pilot:
        view.gate.clear()
        try:
            view.activate()
            await pilot.pause()
            await reached(view.entered)
            clock.seconds = elapsed
            view._render_elapsed()
            status = str(view.query_one("#pb-load-status", Static).content)
        finally:
            view.gate.set()
        await settle(pilot)

    assert status == f"{ProbeView.LOAD_LABEL} — {readout}"


@piloted
async def test_the_elapsed_readout_carries_no_percentage(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The download reports no progress, so the readout claims none."""
    clock = Clock()
    monkeypatch.setattr(base, "monotonic", clock)
    view = ProbeView()
    async with Host(view).run_test() as pilot:
        view.gate.clear()
        try:
            view.activate()
            await pilot.pause()
            await reached(view.entered)
            clock.seconds = 42.0
            view._render_elapsed()
            status = str(view.query_one("#pb-load-status", Static).content)
        finally:
            view.gate.set()
        await settle(pilot)

    assert "%" not in status


@piloted
async def test_the_elapsed_timer_runs_only_while_a_load_is_in_flight() -> None:
    view = ProbeView()
    async with Host(view).run_test() as pilot:
        view.gate.clear()
        try:
            view.activate()
            await pilot.pause()
            await reached(view.entered)
            during = view._elapsed_timer
        finally:
            view.gate.set()
        await settle(pilot)

        assert during is not None
        assert view._elapsed_timer is None


# ── filter reads ─────────────────────────────────────────────────────────────


@piloted
async def test_an_unselected_filter_reads_as_none() -> None:
    """`Select.NULL` is the unselected sentinel, and the reader collapses it to None.

    `Select.BLANK` is the inherited `Widget.BLANK`, the bool False, so it equals no
    filter value and matches no sentinel.
    """
    view = ProbeView()
    async with Host(view).run_test() as pilot:
        await pilot.pause()

        assert view.query_one("#pb-filter-count", Select).value is Select.NULL
        assert view.selected("pb-filter-count") is None


@piloted
async def test_a_filter_reads_back_the_value_it_holds() -> None:
    view = ProbeView()
    async with Host(view).run_test() as pilot:
        await pilot.pause()
        view.query_one("#pb-filter-count", Select).value = 1

        assert view.selected("pb-filter-count") == 1


@piloted
async def test_a_filter_holding_zero_reads_as_zero() -> None:
    """Zero is a value a filter can hold, and only the sentinel reads as no selection."""
    view = ProbeView()
    async with Host(view).run_test() as pilot:
        await pilot.pause()
        view.query_one("#pb-filter-count", Select).value = 0

        assert view.selected("pb-filter-count") == 0


@piloted
async def test_narrowing_reads_the_all_option_as_narrowing_nothing() -> None:
    view = ProbeView()
    async with Host(view).run_test() as pilot:
        await pilot.pause()

        assert view.selected("pb-filter-position") == ALL
        assert view.narrowing("pb-filter-position") is None


@piloted
async def test_narrowing_reads_an_unselected_filter_as_narrowing_nothing() -> None:
    view = ProbeView()
    async with Host(view).run_test() as pilot:
        await pilot.pause()

        assert view.narrowing("pb-filter-count") is None


@piloted
async def test_narrowing_reads_back_the_value_a_filter_narrows_to() -> None:
    view = ProbeView()
    async with Host(view).run_test() as pilot:
        await pilot.pause()
        view.query_one("#pb-filter-position", Select).value = "QB"

        assert view.narrowing("pb-filter-position") == "QB"


@piloted
async def test_all_options_offers_all_ahead_of_the_values() -> None:
    view = ProbeView()
    async with Host(view).run_test() as pilot:
        await pilot.pause()

        assert view.all_options(("QB", "RB")) == [("All", "All"), ("QB", "QB"), ("RB", "RB")]


@pytest.mark.parametrize("position", POSITIONS)
@piloted
async def test_the_position_filter_offers_every_fantasy_position(position: str) -> None:
    """A `Select` refuses a value its options do not carry, so holding one proves it."""
    view = ProbeView()
    async with Host(view).run_test() as pilot:
        await pilot.pause()
        view.query_one("#pb-filter-position", Select).value = position

        assert view.selected("pb-filter-position") == position


# ── filter passes ────────────────────────────────────────────────────────────


@pytest.mark.parametrize("tool", TOOLS, ids=TOOL_IDS)
@piloted
async def test_a_load_dispatches_no_filter_pass(
    tool: Tool, loaders: list[LoaderCall], monkeypatch: pytest.MonkeyPatch
) -> None:
    """A load renders once, from `apply`; the filter messages it posts repeat nothing."""
    passes: list[str | None] = []
    original = tool.view.filter_changed

    def counted(self: ToolView, filter_id: str | None) -> None:
        passes.append(filter_id)
        original(self, filter_id)

    monkeypatch.setattr(tool.view, "filter_changed", counted)

    view = tool.view()
    async with Host(view).run_test() as pilot:
        view.activate()
        await settle(pilot)

        assert passes == []


@piloted
async def test_a_user_filter_change_dispatches_one_filter_pass() -> None:
    view = ProbeView()
    async with Host(view).run_test() as pilot:
        view.activate()
        await settle(pilot)
        view.filter_passes.clear()
        view.query_one("#pb-filter-position", Select).value = "QB"
        await settle(pilot)

        assert view.filter_passes == ["pb-filter-position"]


@piloted
async def test_a_filter_set_back_to_the_rendered_value_dispatches_no_pass() -> None:
    """The render behind the view already answers those values."""
    view = ProbeView()
    async with Host(view).run_test() as pilot:
        view.activate()
        await settle(pilot)
        view.filter_passes.clear()
        view.query_one("#pb-filter-position", Select).value = ALL
        await settle(pilot)

        assert view.filter_passes == []


@piloted
async def test_a_start_sit_load_runs_the_projection_once(
    loaders: list[LoaderCall], monkeypatch: pytest.MonkeyPatch
) -> None:
    """One engine run per load, for the latest week of the latest season loaded."""
    runs = spy(monkeypatch, start_sit_screen, "compute_start_sit")
    view = start_sit_screen.StartSitView()
    async with Host(view).run_test() as pilot:
        view.activate()
        await settle(pilot)

        assert [args[2:] for args, _ in runs] == [(LATEST_SEASON, REGULAR_SEASON_WEEKS)]


@piloted
async def test_a_trade_value_load_runs_the_ranking_once(
    loaders: list[LoaderCall], monkeypatch: pytest.MonkeyPatch
) -> None:
    """One engine run per load, for the last week the season still has a game after."""
    runs = spy(monkeypatch, trade_value_screen, "compute_trade_values")
    view = trade_value_screen.TradeValueView()
    async with Host(view).run_test() as pilot:
        view.activate()
        await settle(pilot)

        assert [(kwargs["season"], kwargs["current_week"]) for _, kwargs in runs] == [
            (LATEST_SEASON, REGULAR_SEASON_WEEKS - 1)
        ]


@piloted
async def test_a_position_filter_narrows_the_rendered_rows(
    loaders: list[LoaderCall],
) -> None:
    """Each team fields one starter at a position, so a narrowed table holds one per team."""
    view = snap_share_screen.SnapShareView()
    async with Host(view).run_test() as pilot:
        view.activate()
        await settle(pilot)
        assert view.query_one("#sn-all-table", DataTable).row_count == LEAGUE_STARTERS

        view.query_one("#sn-filter-position", Select).value = "QB"
        await settle(pilot)

        assert view.query_one("#sn-all-table", DataTable).row_count == len(LEAGUE_TEAMS)


# ── tables ───────────────────────────────────────────────────────────────────


@piloted
async def test_an_empty_result_renders_the_empty_message() -> None:
    view = ProbeView()
    view.payload = pl.DataFrame({"player": []})
    async with Host(view).run_test() as pilot:
        view.activate()
        await settle(pilot)
        label = view.query_one("#pb-table-empty", Static)

        assert (str(label.content), label.display) == (EMPTY_MESSAGE, True)


@piloted
async def test_an_empty_result_hides_the_table() -> None:
    """A header with no rows under it reads as a result; the message says there is none."""
    view = ProbeView()
    view.payload = pl.DataFrame({"player": []})
    async with Host(view).run_test() as pilot:
        view.activate()
        await settle(pilot)

        assert view.query_one("#pb-table", DataTable).display is False


@piloted
async def test_a_result_carrying_no_columns_renders_the_empty_message() -> None:
    """Emptiness is read from the frame's shape, which a column-less frame still has."""
    view = ProbeView()
    view.payload = pl.DataFrame()
    async with Host(view).run_test() as pilot:
        view.activate()
        await settle(pilot)
        label = view.query_one("#pb-table-empty", Static)

        assert (str(label.content), label.display) == (EMPTY_MESSAGE, True)


@piloted
async def test_a_single_row_result_hides_the_empty_message() -> None:
    view = ProbeView()
    view.payload = pl.DataFrame({"player": ["Alpha"]})
    async with Host(view).run_test() as pilot:
        view.activate()
        await settle(pilot)

        assert view.query_one("#pb-table-empty", Static).display is False
        assert view.query_one("#pb-table", DataTable).display is True


@piloted
async def test_every_row_of_a_result_reaches_the_table_in_order() -> None:
    view = ProbeView()
    view.payload = pl.DataFrame({"player": ["Alpha", "Bravo", "Charlie"]})
    async with Host(view).run_test() as pilot:
        view.activate()
        await settle(pilot)
        table = view.query_one("#pb-table", DataTable)

        assert [table.get_row_at(index) for index in range(table.row_count)] == [
            ["Alpha"],
            ["Bravo"],
            ["Charlie"],
        ]


@piloted
async def test_a_refilled_table_carries_only_the_latest_result() -> None:
    view = ProbeView()
    view.payload = pl.DataFrame({"player": ["Alpha", "Bravo", "Charlie"]})
    async with Host(view).run_test() as pilot:
        view.activate()
        await settle(pilot)
        view.payload = pl.DataFrame({"player": ["Delta"]})
        view.query_one("#pb-btn-refresh", Button).press()
        await settle(pilot)
        table = view.query_one("#pb-table", DataTable)

        assert [table.get_row_at(index) for index in range(table.row_count)] == [["Delta"]]


@pytest.mark.parametrize("tool", TOOLS, ids=TOOL_IDS)
@piloted
async def test_every_table_is_paired_with_an_empty_state_label(
    tool: Tool, loaders: list[LoaderCall]
) -> None:
    """`fill_table` reaches a label at `{table_id}-empty` for every table a tool renders."""
    view = tool.view()
    async with Host(view).run_test() as pilot:
        await pilot.pause()
        tables = [table.id for table in view.query(DataTable)]

        assert tables, f"{tool.name} renders no table"
        assert [tid for tid in tables if not view.query(f"#{tid}-empty")] == []


# ── season filters ───────────────────────────────────────────────────────────


def unplayed(schedules: pl.DataFrame) -> pl.DataFrame:
    """`schedules` with a copy of its latest season relabelled `UNPLAYED_SEASON`.

    nflverse publishes a season's schedule months before its opening kickoff, so a
    schedule covering a season no per-game asset carries is what the source holds
    between the calendar reaching a season and its first game being played.
    """
    latest = schedules.filter(pl.col("season") == LATEST_SEASON)
    return pl.concat(
        [
            schedules,
            latest.with_columns(
                pl.lit(UNPLAYED_SEASON).cast(schedules["season"].dtype).alias("season")
            ),
        ]
    )


def frames_for(
    monkeypatch: pytest.MonkeyPatch,
    weekly: pl.DataFrame,
    schedules: pl.DataFrame,
    fixtures: dict[str, pl.DataFrame],
) -> None:
    """Stub every loader with the league fixtures, `weekly` and `schedules` overriding."""
    stub_loaders(
        monkeypatch, {**fixtures, "load_weekly_stats": weekly, "load_schedules": schedules}, []
    )


@pytest.fixture()
def fixture_frames(
    league_snap_counts: pl.DataFrame,
    league_weekly_stats: pl.DataFrame,
    player_ids: pl.DataFrame,
    full_season_schedules: pl.DataFrame,
    injuries: pl.DataFrame,
    pbp: pl.DataFrame,
    rosters: pl.DataFrame,
) -> dict[str, pl.DataFrame]:
    """The frame each loader answers with, keyed by loader name."""
    return loader_frames(
        league_snap_counts,
        league_weekly_stats,
        player_ids,
        full_season_schedules,
        injuries,
        pbp,
        rosters,
    )


@piloted
async def test_start_sit_offers_no_season_the_weekly_stats_have_no_week_of(
    monkeypatch: pytest.MonkeyPatch,
    fixture_frames: dict[str, pl.DataFrame],
) -> None:
    """The season a schedule covers and no result reaches is absent from the filter."""
    frames_for(
        monkeypatch,
        fixture_frames["load_weekly_stats"],
        unplayed(fixture_frames["load_schedules"]),
        fixture_frames,
    )
    view = start_sit_screen.StartSitView()
    async with Host(view).run_test() as pilot:
        view.activate()
        await settle(pilot)
        season = view.query_one("#ss-filter-season", Select)

        assert season.value == LATEST_SEASON
        with pytest.raises(InvalidSelectValueError):
            season.value = UNPLAYED_SEASON


@piloted
async def test_trade_value_offers_no_season_the_weekly_stats_have_no_week_of(
    monkeypatch: pytest.MonkeyPatch,
    fixture_frames: dict[str, pl.DataFrame],
) -> None:
    """The season a schedule covers and no result reaches is absent from the filter."""
    frames_for(
        monkeypatch,
        fixture_frames["load_weekly_stats"],
        unplayed(fixture_frames["load_schedules"]),
        fixture_frames,
    )
    view = trade_value_screen.TradeValueView()
    async with Host(view).run_test() as pilot:
        view.activate()
        await settle(pilot)
        season = view.query_one("#tv-filter-season", Select)

        assert season.value == LATEST_SEASON
        with pytest.raises(InvalidSelectValueError):
            season.value = UNPLAYED_SEASON


@piloted
async def test_the_start_sit_week_filter_ends_at_the_last_week_of_results(
    monkeypatch: pytest.MonkeyPatch,
    fixture_frames: dict[str, pl.DataFrame],
) -> None:
    """A season part-way through offers its played weeks, not its scheduled ones."""
    frames_for(
        monkeypatch,
        fixture_frames["load_weekly_stats"].filter(pl.col("week") <= WEEKS_PLAYED),
        fixture_frames["load_schedules"],
        fixture_frames,
    )
    view = start_sit_screen.StartSitView()
    async with Host(view).run_test() as pilot:
        view.activate()
        await settle(pilot)
        week = view.query_one("#ss-filter-week", Select)

        assert week.value == WEEKS_PLAYED
        with pytest.raises(InvalidSelectValueError):
            week.value = WEEKS_PLAYED + 1


@piloted
async def test_start_sit_says_so_when_no_season_carries_a_completed_week(
    monkeypatch: pytest.MonkeyPatch,
    fixture_frames: dict[str, pl.DataFrame],
) -> None:
    frames_for(
        monkeypatch,
        fixture_frames["load_weekly_stats"].clear(),
        fixture_frames["load_schedules"],
        fixture_frames,
    )
    view = start_sit_screen.StartSitView()
    async with Host(view).run_test() as pilot:
        view.activate()
        await settle(pilot)

        assert view.query_one("#ss-table", DataTable).row_count == 0
        assert str(view.query_one("#ss-table-empty", Static).visual) == start_sit_screen.NO_RESULTS


@piloted
async def test_trade_value_says_so_when_no_season_reaches_the_week_a_ranking_rests_on(
    monkeypatch: pytest.MonkeyPatch,
    fixture_frames: dict[str, pl.DataFrame],
) -> None:
    """A season fewer weeks deep than the floor ranks nobody, and the table says why."""
    frames_for(
        monkeypatch,
        fixture_frames["load_weekly_stats"].filter(
            pl.col("week") < trade_value_screen.MIN_WEEKS_PLAYED
        ),
        fixture_frames["load_schedules"],
        fixture_frames,
    )
    view = trade_value_screen.TradeValueView()
    async with Host(view).run_test() as pilot:
        view.activate()
        await settle(pilot)

        assert view.query_one("#tv-table", DataTable).row_count == 0
        assert (
            str(view.query_one("#tv-table-empty", Static).visual)
            == trade_value_screen.NO_RANKABLE_WEEKS
        )
