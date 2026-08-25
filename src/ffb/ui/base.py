"""Shared chrome and load lifecycle for the tool views."""

from __future__ import annotations

from collections.abc import Callable, Iterable, Sequence
from time import monotonic
from typing import Any, ClassVar, TypeVar

import polars as pl
from textual.app import ComposeResult
from textual.binding import BindingType
from textual.containers import Center, Horizontal, Vertical
from textual.timer import Timer
from textual.widget import Widget
from textual.widgets import Button, DataTable, LoadingIndicator, Select, Static
from textual.worker import Worker, get_current_worker

#: Fantasy-relevant positions in depth-chart order, as the position filter lists them.
POSITIONS: tuple[str, ...] = ("QB", "RB", "WR", "TE")

#: The option a filter carries for "every value qualifies". `all_options` puts it in
#: front of every list, and `narrowing` reads a filter holding it as narrowing nothing.
ALL = "All"

#: Maps one row of a frame, keyed by column name, to the cells of one table row.
RowFn = Callable[[dict[str, Any]], Sequence[Any]]

#: What one unit of off-thread work hands back to the UI thread.
Result = TypeVar("Result")

#: One filter's id paired with the value it holds.
FilterState = tuple[tuple[str | None, Any], ...]


class ToolView(Widget):
    """A tool: one download off the UI thread, one render on it, one refresh path.

    A subclass supplies `ID_PREFIX`, `LOAD_LABEL`, `compose_content`, `fetch`, `apply`
    and, where it carries filters, `filter_changed`. Everything else — the loading pane,
    the elapsed-time readout, cancel, refresh, filter reads, error reporting and
    empty-result rendering — is handled here so that all six tools answer those
    questions the same way.

    Widget ids are derived from `ID_PREFIX`: `{prefix}-loading`, `{prefix}-content`,
    `{prefix}-load-status`, `{prefix}-btn-cancel` and `{prefix}-btn-refresh`.
    """

    #: Id namespace for this tool's widgets.
    ID_PREFIX: ClassVar[str]

    #: What the load downloads, named on the loading pane next to the elapsed time.
    LOAD_LABEL: ClassVar[str]

    BINDINGS: ClassVar[list[BindingType]] = [("escape", "cancel_load", "Cancel")]

    DEFAULT_CSS = """
    ToolView {
        height: 1fr;
        width: 1fr;
    }

    .tool-loading {
        height: 1fr;
        width: 1fr;
        align: center middle;
    }

    .tool-loading LoadingIndicator {
        height: 3;
    }

    .load-status {
        width: 1fr;
        height: auto;
        padding: 1 0;
        color: $text-muted;
        text-align: center;
    }

    .load-cancel {
        width: auto;
    }

    .tool-content {
        height: 1fr;
        width: 1fr;
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

    .filter-bar Input {
        width: 2fr;
        margin-right: 1;
    }

    .filter-bar Button {
        margin-left: 1;
    }

    .alert-panels {
        height: 1fr;
    }

    .alert-panel {
        width: 1fr;
        margin: 0 1;
    }

    .panel-header-rising {
        color: $success;
        text-style: bold;
        padding: 1 0 0 1;
    }

    .panel-header-falling {
        color: $error;
        text-style: bold;
        padding: 1 0 0 1;
    }

    .empty-state {
        display: none;
        height: auto;
        width: 1fr;
        padding: 2 4;
        color: $text-muted;
        text-align: center;
    }
    """

    def __init__(self, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self._activated = False
        self._loading = False
        self._load_worker: Worker[None] | None = None
        self._load_started = 0.0
        self._elapsed_timer: Timer | None = None
        self._rendered_filters: FilterState = ()

    # ── composition ──────────────────────────────────────────

    def compose(self) -> ComposeResult:
        with Vertical(id=f"{self.ID_PREFIX}-loading", classes="tool-loading"):
            yield LoadingIndicator()
            yield Static("", id=f"{self.ID_PREFIX}-load-status", classes="load-status")
            with Center():
                yield Button("Cancel", id=f"{self.ID_PREFIX}-btn-cancel", classes="load-cancel")
        with Vertical(id=f"{self.ID_PREFIX}-content", classes="tool-content"):
            yield from self.compose_content()

    def compose_content(self) -> ComposeResult:
        """Yield the tool's own widgets: filter bar, tabs, tables.

        The result is wrapped in the content container, which stays hidden until a load
        succeeds. Subclasses implement this rather than `compose` so that every tool
        gets the same loading pane.
        """
        raise NotImplementedError(f"{type(self).__name__} must implement compose_content()")

    def compose_filter_bar(self, *controls: Widget) -> ComposeResult:
        """Yield the filter bar: `controls` followed by the refresh button."""
        with Horizontal(classes="filter-bar"):
            yield from controls
            yield Button("Refresh", id=f"{self.ID_PREFIX}-btn-refresh", variant="primary")

    def position_select(self) -> Select[str]:
        """Build the position filter, defaulted to `ALL`."""
        return Select[str](
            self.all_options(POSITIONS),
            value=ALL,
            id=f"{self.ID_PREFIX}-filter-position",
        )

    def on_mount(self) -> None:
        self.query_one(f"#{self.ID_PREFIX}-loading").display = False
        self.query_one(f"#{self.ID_PREFIX}-content").display = False
        for table in self.query(DataTable):
            self._pair_empty_state(table)
        # A filter built with a default value posts that value as a change once it is
        # mounted; recording the filters here leaves that message nothing to re-render.
        self._rendered_filters = self._filter_state()

    def _pair_empty_state(self, table: DataTable[Any]) -> None:
        """Give `table` the label that stands in for it when a result has no rows.

        Pairing every table here rather than in each tool's `compose` keeps the id
        convention `fill_table` relies on — `{table_id}-empty` — true by construction.
        """
        table_id = table.id
        parent = table.parent
        if table_id is None or not isinstance(parent, Widget):
            return
        if self.query(f"#{table_id}-empty"):
            return
        parent.mount(Static("", id=f"{table_id}-empty", classes="empty-state"), after=table)

    # ── lifecycle ────────────────────────────────────────────

    def activate(self) -> None:
        """Load this tool's data the first time it is shown."""
        if not self._activated:
            self._start_load(force_refresh=False)

    def fetch(self, force_refresh: bool) -> object:
        """Download and compute everything this tool renders, off the UI thread.

        Returns one payload object, handed straight to `apply`. Raising here routes the
        message to the error toast. Honour `force_refresh` by passing it to every
        loader call: the refresh button promises a fresh download, not a cache hit.
        """
        raise NotImplementedError(f"{type(self).__name__} must implement fetch()")

    def apply(self, payload: Any) -> None:
        """Store the payload, populate the filters and fill the tables, on the UI thread.

        Setting filter options and values here costs no extra render: `filter_changed`
        runs only for a filter state that differs from the one this method leaves
        behind. The parameter is typed `Any` so a subclass can narrow it to the type its
        `fetch` returns.
        """
        raise NotImplementedError(f"{type(self).__name__} must implement apply()")

    def _start_load(self, force_refresh: bool) -> None:
        self._activated = True
        self._loading = True
        self.query_one(f"#{self.ID_PREFIX}-content").display = False
        self.query_one(f"#{self.ID_PREFIX}-loading").display = True
        self._start_elapsed()
        self._load_worker = self.run_off_thread(
            lambda: self.fetch(force_refresh),
            self._on_data_loaded,
            self._on_data_error,
            group="tool-load",
        )
        # Escape reaches this view's bindings only from a focused descendant, and the
        # cancel button is the one focusable widget on show during a load.
        self.query_one(f"#{self.ID_PREFIX}-btn-cancel", Button).focus()
        self.refresh_bindings()

    def run_off_thread(
        self,
        work: Callable[[], Result],
        on_result: Callable[[Result], None],
        on_error: Callable[[str], None],
        *,
        group: str,
    ) -> Worker[None]:
        """Run `work` on a worker thread and answer it back on the UI thread.

        `on_result` takes what `work` returned, `on_error` the message it raised. A
        worker cancelled while `work` ran answers with neither: its result belongs to a
        request the view has moved past, and rendering it would overwrite the answer to
        the request that replaced it. Workers sharing a `group` are exclusive, so
        starting one supersedes the one before it.
        """

        def run() -> None:
            worker = get_current_worker()
            try:
                result = work()
            except Exception as error:
                if not worker.is_cancelled:
                    self.app.call_from_thread(on_error, str(error))
                return
            if not worker.is_cancelled:
                self.app.call_from_thread(on_result, result)

        return self.run_worker(run, thread=True, exclusive=True, group=group)

    def report_error(self, message: str) -> None:
        """Show `message` as the error toast every failure in this view reports through."""
        self.notify(message, severity="error", timeout=10)

    def _on_data_loaded(self, payload: object) -> None:
        self._loading = False
        self._stop_elapsed()
        try:
            self.apply(payload)
        finally:
            self._rendered_filters = self._filter_state()
        self._show_content()
        self.refresh_bindings()

    def _on_data_error(self, error: str) -> None:
        self._loading = False
        self._stop_elapsed()
        self._show_content()
        self.refresh_bindings()
        self.report_error(f"Failed to load data: {error}")

    def action_cancel_load(self) -> None:
        """Stop waiting on the running load and show the tool again."""
        self._cancel_load()

    def check_action(self, action: str, parameters: tuple[object, ...]) -> bool | None:
        """Offer the cancel binding only while a load is in flight.

        `False` disables the binding and drops it from the footer, so escape advertises
        cancelling exactly when there is something to cancel.
        """
        if action == "cancel_load":
            return self._loading
        return True

    def _cancel_load(self) -> None:
        """Discard the running load's result and restore the previous content.

        A thread worker cannot be interrupted, so the download runs to completion or to
        its timeout in the background; cancelling marks its result stale, and
        `run_off_thread` drops that result rather than rendering it.
        """
        if not self._loading:
            return
        self._loading = False
        if self._load_worker is not None:
            self._load_worker.cancel()
        self._stop_elapsed()
        self._show_content()
        self.refresh_bindings()
        self.notify("Load cancelled.", severity="warning", timeout=5)

    def _show_content(self) -> None:
        content = self.query_one(f"#{self.ID_PREFIX}-content")
        loading = self.query_one(f"#{self.ID_PREFIX}-loading")
        content.display = True
        cancel = self.query_one(f"#{self.ID_PREFIX}-btn-cancel", Button)
        if self.screen.focused is cancel:
            self.screen.focus_next()
        loading.display = False

    # ── elapsed time ─────────────────────────────────────────

    def _start_elapsed(self) -> None:
        self._load_started = monotonic()
        self._render_elapsed()
        if self._elapsed_timer is None:
            self._elapsed_timer = self.set_interval(1.0, self._render_elapsed)

    def _stop_elapsed(self) -> None:
        if self._elapsed_timer is not None:
            self._elapsed_timer.stop()
            self._elapsed_timer = None

    def _render_elapsed(self) -> None:
        """Report the load as a named target and the time spent on it.

        nfl_data_py downloads a release asset in one blocking call and reports no
        progress, so a fraction or a bar would be invented. Elapsed time is measured.
        """
        minutes, seconds = divmod(int(monotonic() - self._load_started), 60)
        status = self.query_one(f"#{self.ID_PREFIX}-load-status", Static)
        status.update(f"{self.LOAD_LABEL} — {minutes:02d}:{seconds:02d}")

    # ── filters ──────────────────────────────────────────────

    def all_options(self, values: Iterable[str]) -> list[tuple[str, str]]:
        """Build a filter's options: `values`, behind the `ALL` entry that narrows nothing."""
        return [(ALL, ALL), *((value, value) for value in values)]

    def selected(self, filter_id: str) -> Any | None:
        """Return what the named filter holds, or None where it holds no selection.

        A `Select` with nothing selected holds a sentinel object rather than a value,
        and polars builds no expression from one. Every filter read goes through here,
        so no caller has to know the sentinel exists.
        """
        return self.query_one(f"#{filter_id}", Select).selection

    def narrowing(self, filter_id: str) -> Any | None:
        """Return what the named filter narrows to, or None where it narrows nothing.

        `ALL` and no selection both leave every row in, so a caller filters on exactly
        the values this returns and skips the filter for None.
        """
        value = self.selected(filter_id)
        return None if value == ALL else value

    def filter_changed(self, filter_id: str | None) -> None:
        """Re-render for the filters as they read, `filter_id` naming the one that moved.

        Called once per change to a filter this view carries. Read the filters through
        `selected` and `narrowing` rather than from the id: a change to one filter can
        reset the options of another, and the render answers to all of them.
        """
        raise NotImplementedError(f"{type(self).__name__} must implement filter_changed()")

    def _filter_state(self) -> FilterState:
        """Every filter this view carries, paired with the value it holds."""
        return tuple((select.id, select.value) for select in self.query(Select))

    # ── tables ───────────────────────────────────────────────

    def fill_table(
        self,
        table_id: str,
        df: pl.DataFrame,
        row_fn: RowFn,
        empty_message: str,
    ) -> None:
        """Render `df` into a table, or put `empty_message` where the table would be.

        A result with no rows is otherwise indistinguishable from one still loading or
        one filtered down to nothing, so the message says which. `row_fn` maps a row to
        its cells and is called only for rows that exist.

        Emptiness is read from the frame's shape, never from a column: a frame carrying
        no columns at all answers `height`, and raises on any column reference.
        """
        table = self.query_one(f"#{table_id}", DataTable)
        label = self.query_one(f"#{table_id}-empty", Static)
        table.clear()
        if df.height == 0:
            label.update(empty_message)
            label.display = True
            table.display = False
            return
        label.display = False
        table.display = True
        for row in df.iter_rows(named=True):
            table.add_row(*row_fn(row))

    # ── events ───────────────────────────────────────────────

    def on_button_pressed(self, event: Button.Pressed) -> None:
        button_id = event.button.id
        if button_id == f"{self.ID_PREFIX}-btn-refresh":
            self._start_load(force_refresh=True)
        elif button_id == f"{self.ID_PREFIX}-btn-cancel":
            self._cancel_load()

    def on_select_changed(self, event: Select.Changed) -> None:
        """Hand a filter change to `filter_changed`, once per distinct set of values.

        Setting a `Select`'s options or its value posts `Changed` to the message queue
        rather than raising it inline, so the messages a load's filter population raises
        arrive after the render they would repeat, and so do the messages a
        `filter_changed` raises while rebuilding one filter's options from another.
        Reading the filters as they stand on delivery and comparing them against the
        values behind the last render tells an echo from a change a user made: one pass
        per change, none per echo.
        """
        if self._filter_state() == self._rendered_filters:
            return
        self.filter_changed(event.select.id)
        self._rendered_filters = self._filter_state()
