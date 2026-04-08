"""FFB football analytics TUI."""

from textual.app import App, ComposeResult
from textual.containers import Horizontal, Vertical
from textual.widgets import ContentSwitcher, Footer, Header, Label, ListItem, ListView, Static

from ffb.injury_impact.screen import InjuryImpactView
from ffb.red_zone.screen import RedZoneView
from ffb.snap_share.screen import SnapShareView
from ffb.start_sit.screen import StartSitView
from ffb.trade_value.screen import TradeValueView
from ffb.waiver_wire.screen import WaiverWireView


class FFBApp(App):
    """Main application shell with sidebar navigation."""

    TITLE = "FFB"
    CSS = """
    #sidebar {
        width: 22;
        dock: left;
        background: $surface-darken-1;
        border-right: thick $primary-darken-2;
        padding: 1;
    }

    .sidebar-title {
        text-style: bold;
        color: $accent;
        text-align: center;
        padding: 0 0 1 0;
    }

    #nav {
        height: auto;
    }

    #nav > ListItem {
        padding: 0 1;
    }

    ContentSwitcher {
        height: 1fr;
        width: 1fr;
    }
    """

    TOOLS = [
        ("snap-share", "Snap Share"),
        ("red-zone", "Red Zone"),
        ("injury-impact", "Injury Impact"),
        ("waiver-wire", "Waiver Wire"),
        ("start-sit", "Start/Sit"),
        ("trade-value", "Trade Value"),
    ]

    def compose(self) -> ComposeResult:
        yield Header()
        with Horizontal():
            with Vertical(id="sidebar"):
                yield Static("FFB", classes="sidebar-title")
                yield ListView(
                    *[ListItem(Label(label), id=f"nav-{tid}") for tid, label in self.TOOLS],
                    id="nav",
                )
            with ContentSwitcher(initial="snap-share"):
                yield SnapShareView(id="snap-share")
                yield RedZoneView(id="red-zone")
                yield InjuryImpactView(id="injury-impact")
                yield WaiverWireView(id="waiver-wire")
                yield StartSitView(id="start-sit")
                yield TradeValueView(id="trade-value")
        yield Footer()

    def on_mount(self) -> None:
        self.query_one("#snap-share").activate()

    def on_list_view_selected(self, event: ListView.Selected) -> None:
        tool_id = event.item.id
        if tool_id and tool_id.startswith("nav-"):
            view_id = tool_id.removeprefix("nav-")
            self.query_one(ContentSwitcher).current = view_id
            view = self.query_one(f"#{view_id}")
            if hasattr(view, "activate"):
                view.activate()


def main() -> None:
    app = FFBApp()
    app.run()
