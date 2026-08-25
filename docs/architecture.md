# Architecture

`ffb` is a single-user terminal application over public nflverse data. Six analytics tools
share one shell, one download path and one disk cache; each tool otherwise stands alone.

## The shape

```
┌───────────────────────────────────────────────────────────────────────┐
│  app.py — FFBApp                                                      │
│  sidebar ListView ──▶ ContentSwitcher ──▶ activate() on first show    │
└─────────────────────────────────┬─────────────────────────────────────┘
                                  │ mounts one ToolView per tool
┌─────────────────────────────────▼─────────────────────────────────────┐
│  ui/base.py — ToolView                                                │
│  loading pane · elapsed clock · cancel · thread worker · error toast  │
│  compose_filter_bar · position_select · fill_table                    │
└─────────────────────────────────┬─────────────────────────────────────┘
                                  │ subclassed by
   snap_share/   red_zone/   injury_impact/   waiver_wire/   start_sit/   trade_value/
    screen.py     screen.py     screen.py       screen.py     screen.py    screen.py
        │             │             │               │             │            │
        │  apply() calls the sibling engine on frames already in memory        │
        │             ▼             ▼               ▼             ▼            ▼
        │        engine.py — pure polars: frames in, frames out, no I/O
        │
        │  fetch() runs off the UI thread
        ▼
┌───────────────────────────────────────────────────────────────────────┐
│  data/seasons.py — which seasons to ask for                           │
│  data/loader.py  — one _cached() call per dataset, 120 s per download │
│  data/cache.py   — TTL, atomic write, LRU ceiling, ~/.fantasy/cache   │
└───────────────────────────────────────────────────────────────────────┘
```

## A package per tool

Each tool is a Python package holding exactly two modules: `engine.py` and `screen.py`.
Nothing crosses between tools. Snap Share and Waiver Wire both window a usage series over
three weeks, and they do it twice, in their own code, because the two windows answer
different questions and are free to diverge — Snap Share windows raw snap share keyed on
`pfr_player_id`, Waiver Wire windows a weighted composite keyed on `gsis_id`. Extracting a
shared "trend" helper would couple the two tools' definitions of a trend to each other, and
the next tool that wants a fourth variant pays for the coupling.

The boundary is what makes the tools cheap to reason about: reading `trade_value/` tells you
everything trade value does. The shared code is deliberately narrow — the view chrome in
`ui/base.py`, the data layer in `data/`, and the two pure helpers in `data/__init__.py`
(`OFFENSIVE_POSITIONS` and `build_id_crosswalk`) that every engine would otherwise restate.

## Engines are pure

An engine module imports `polars` and, at most, `ffb.data`. It imports neither
`ffb.data.loader` nor `textual`. Its functions take DataFrames and return a DataFrame.

Three properties follow.

**Tests need no network and no terminal.** `tests/conftest.py` builds a synthetic league —
four teams, one player per offensive position, two seasons — plus the play-by-play, roster
and injury frames the same league implies, and the engine tests call the engine directly. A
suite that had to download two seasons of play-by-play would fail for reasons that have
nothing to do with the code under test.

**Filter changes cost no I/O.** A screen holds the frames one load produced. Changing the
position filter re-runs the engine, or just re-filters its output, against memory. Only the
Refresh button and the first activation reach the network.

**The data source is replaceable.** Every nflverse quirk — an unpublished weekly-stats
asset, `pfr_id` versus `gsis_id`, snap share arriving as a fraction in one season and as
points in another — is absorbed by the loader or normalized at the top of the engine, not
threaded through the computation.

An engine that can return nothing declares its result shape as a module-level `OUTPUT_SCHEMA`
and returns a zero-row frame carrying it. A caller filters and indexes the result by column
name; an empty frame with no columns raises on the first column reference, so the guard path
must be shaped like the success path.

## What ToolView owns, and what a screen owns

`ToolView` exists so that all six tools answer the same questions the same way. A user who
learns how to cancel a load in one tool has learned it in every tool.

| `ToolView` (`src/ffb/ui/base.py`) | The screen |
| --- | --- |
| The loading pane, and the display toggle between it and the content pane | `compose_content()` — the filter bar, tabs and tables |
| Running `fetch()` on a thread worker, so a download never blocks the UI | `fetch()` — which loaders to call, and threading `force_refresh` through every one |
| Reporting a raised exception as an error toast | `apply()` — populating filter options and filling tables |
| The elapsed clock, ticking once a second under the load label | `LOAD_LABEL` — what the clock says is downloading |
| Cancel: a button, an `escape` binding, and `check_action` offering the binding only mid-load | Filter events, row formatting, empty-state wording |
| The Refresh button, and forcing a fresh download when it is pressed | `ID_PREFIX` — the namespace every widget id derives from |
| `fill_table()` — rows, or an empty-state message where the table would be | The `DataTable` columns, added in `on_mount` |
| Pairing every `DataTable` with the `{table_id}-empty` label `fill_table` looks for | |

Two conventions hold this together.

**Widget ids derive from `ID_PREFIX`.** `ToolView` queries `#{prefix}-loading`,
`#{prefix}-content`, `#{prefix}-load-status`, `#{prefix}-btn-cancel` and
`#{prefix}-btn-refresh` without knowing anything else about the subclass.

**A screen's `on_mount` does not shadow the base's.** Textual dispatches an event handler
once per class in the MRO that defines it, subclass first. A screen adds its table columns
in `on_mount`; `ToolView.on_mount` then hides both panes and mounts an empty-state label
after each `DataTable` it finds. Neither needs to know about the other.

`fetch()` returns one payload object, usually a frozen dataclass naming the frames, and
`ToolView` hands that straight to `apply()`. One return value keeps the thread boundary a
single hop: everything crossing it is in that object.

## The data layer

Three modules sit under every `fetch()`.

`data/seasons.py` answers which seasons to ask for. nflverse labels a season by the calendar
year it opens in, so a date resolves to a season only once the month is known.
`current_season()` and `recent_seasons()` both accept an injected date, which is what makes
the arithmetic testable and keeps season lists out of the screens.

`data/loader.py` is seven loaders over one `_cached(key, fetch, *args, force_refresh)`
helper. `force_refresh` skips the cache read, not the write, which is precisely what the
Refresh button promises. Each loader owns one cache key; a per-season key sorts its seasons
so that any ordering names the same entry. `_download` runs each fetch on its own daemon
thread and gives up after the timeout, so one stalled transfer neither delays a concurrent
download nor eats the next one's budget. A thread cannot be interrupted, so the abandoned
worker runs to completion with nothing left to hand its result to; being a daemon keeps it
from holding the interpreter open at exit.

`data/cache.py` is a directory of Parquet files with a JSON metadata sidecar. Entries are
written to a temporary file and renamed into place, so a crash leaves one complete file —
the entry being replaced or its replacement — never a mix of the two. Reads past the TTL
count as misses. Writes reap records whose data file is gone and data files no record
claims, then evict least-recently-read entries until the directory fits the size ceiling.
Corruption is self-healing in both directions: unreadable metadata is reset, an unreadable
entry is invalidated and refetched.

The layering is one-way. Engines know nothing of the loader; the loader knows nothing of the
cache's eviction policy beyond `get` and `put`; the cache knows nothing about seasons.

## Adding a seventh tool

Follow the shape rather than a checklist — the shape is what makes the tool feel like the
other six.

1. **Create `src/ffb/<tool>/` with `__init__.py`, `engine.py` and `screen.py`.** The package
   name is the bounded context.

2. **Write the engine first, against frames.** Decide what columns the result carries and
   name them once. If any input can produce no rows, declare `OUTPUT_SCHEMA` at module level
   and return `pl.DataFrame(schema=OUTPUT_SCHEMA)` from the guard path. Import `polars` and,
   if the tool needs them, `OFFENSIVE_POSITIONS` and `build_id_crosswalk` from `ffb.data` —
   nothing else.

3. **Test the engine against synthetic frames.** `tests/conftest.py` fixtures snap counts,
   weekly stats, player ids, schedules, rosters, injuries and play-by-play, each at the
   scale its engines need; extend it rather than reaching for real data.

4. **Subclass `ToolView` in `screen.py`.** Set `ID_PREFIX` to a short namespace and
   `LOAD_LABEL` to what the load downloads. Build the filter bar with
   `compose_filter_bar(...)`, which appends the Refresh button; use `position_select()` if
   the tool filters by position. Add table columns in `on_mount`. Implement `fetch()` to call
   loaders with `force_refresh` passed through every one, and `apply()` to populate filters
   and call `fill_table()`. Write the empty-state message for each table: it is the only
   thing distinguishing a filter that matched nothing from a load that has not happened.

5. **Register it in `app.py`.** Add a `(view_id, label)` pair to `TOOLS` and yield the view
   inside the `ContentSwitcher` with `id=view_id`. The sidebar entry, the switch and the
   first-show activation all key off that id.

A tool that needs a dataset no loader covers adds one loader to `data/loader.py`: a cache key
and a `_cached` call. Everything else — the timeout, the atomic write, the eviction — comes
with it.
