"""Properties of the app shell: the tool registry, the sidebar and the switcher."""

from __future__ import annotations

import polars as pl
import pytest
from textual.pilot import Pilot
from textual.widgets import ContentSwitcher, Label, ListItem, ListView

from ffb.app import FFBApp
from ffb.ui.base import ToolView
from tests.test_ui_base import LoaderCall, loader_frames, piloted, settle, stub_loaders

#: Every tool the app registers, in sidebar order.
TOOL_IDS = [tool_id for tool_id, _ in FFBApp.TOOLS]

#: What the sidebar calls each tool.
TOOL_LABELS = dict(FFBApp.TOOLS)

#: The tool the shell opens on.
FIRST_TOOL = TOOL_IDS[0]

#: The tools reached by navigating to them.
LATER_TOOLS = TOOL_IDS[1:]


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
    """Every loader call the tools make, in order, against the league fixtures."""
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


async def select_tool(pilot: Pilot[None], tool_id: str) -> None:
    """Walk the sidebar cursor onto `tool_id` and select it, as a user does."""
    nav = pilot.app.query_one("#nav", ListView)
    nav.focus()
    nav.index = TOOL_IDS.index(tool_id)
    await pilot.press("enter")
    await settle(pilot)


# ── the registry ─────────────────────────────────────────────────────────────


def test_every_tool_is_registered() -> None:
    assert TOOL_IDS == [
        "snap-share",
        "red-zone",
        "injury-impact",
        "waiver-wire",
        "start-sit",
        "trade-value",
    ]


@piloted
async def test_the_sidebar_lists_one_entry_per_registered_tool(
    loaders: list[LoaderCall],
) -> None:
    async with FFBApp().run_test() as pilot:
        await settle(pilot)
        nav = pilot.app.query_one("#nav", ListView)

        assert [item.id for item in nav.query(ListItem)] == [
            f"nav-{tool_id}" for tool_id in TOOL_IDS
        ]


@pytest.mark.parametrize("tool_id", TOOL_IDS)
@piloted
async def test_each_sidebar_entry_carries_the_registered_label(
    tool_id: str, loaders: list[LoaderCall]
) -> None:
    async with FFBApp().run_test() as pilot:
        await settle(pilot)
        item = pilot.app.query_one(f"#nav-{tool_id}", ListItem)

        assert str(item.query_one(Label).content) == TOOL_LABELS[tool_id]


@piloted
async def test_the_switcher_carries_one_pane_per_registered_tool(
    loaders: list[LoaderCall],
) -> None:
    async with FFBApp().run_test() as pilot:
        await settle(pilot)
        switcher = pilot.app.query_one(ContentSwitcher)

        assert [pane.id for pane in switcher.children] == TOOL_IDS


@pytest.mark.parametrize("tool_id", TOOL_IDS)
@piloted
async def test_every_registered_tool_id_resolves_to_a_mounted_view(
    tool_id: str, loaders: list[LoaderCall]
) -> None:
    async with FFBApp().run_test() as pilot:
        await settle(pilot)
        view = pilot.app.query_one(f"#{tool_id}", ToolView)

        assert view.parent is pilot.app.query_one(ContentSwitcher)


# ── navigation ───────────────────────────────────────────────────────────────


@piloted
async def test_the_shell_opens_on_the_first_registered_tool(
    loaders: list[LoaderCall],
) -> None:
    async with FFBApp().run_test() as pilot:
        await settle(pilot)

        assert pilot.app.query_one(ContentSwitcher).current == FIRST_TOOL


@piloted
async def test_the_tool_the_shell_opens_on_is_the_only_one_that_loads(
    loaders: list[LoaderCall],
) -> None:
    """A tool downloads when it is shown, so opening the shell costs one tool's load."""
    async with FFBApp().run_test() as pilot:
        await settle(pilot)

        assert {call.view for call in loaders} == {FIRST_TOOL}


@pytest.mark.parametrize("tool_id", TOOL_IDS)
@piloted
async def test_selecting_a_tool_shows_it(tool_id: str, loaders: list[LoaderCall]) -> None:
    async with FFBApp().run_test() as pilot:
        await settle(pilot)
        await select_tool(pilot, tool_id)

        assert pilot.app.query_one(ContentSwitcher).current == tool_id


@pytest.mark.parametrize("tool_id", LATER_TOOLS)
@piloted
async def test_selecting_a_tool_loads_its_data(tool_id: str, loaders: list[LoaderCall]) -> None:
    async with FFBApp().run_test() as pilot:
        await settle(pilot)
        loaders.clear()
        await select_tool(pilot, tool_id)

        assert loaders, f"{tool_id} loaded nothing"
        assert {call.view for call in loaders} == {tool_id}


@pytest.mark.parametrize("tool_id", TOOL_IDS)
@piloted
async def test_reselecting_a_tool_does_not_reload_it(
    tool_id: str, loaders: list[LoaderCall]
) -> None:
    async with FFBApp().run_test() as pilot:
        await settle(pilot)
        await select_tool(pilot, tool_id)
        loaders.clear()
        await select_tool(pilot, tool_id)

        assert loaders == []
