# FFB

NFL data analytics and fantasy football tools.

## Stack

- **Language:** Python
- **Data acquisition:** `nflreadpy` (nflverse parquet files from GitHub Releases — free, no auth)
- **Data processing:** `polars`
- **TUI:** `textual`
- **HTTP (if needed):** `httpx`

## Data Source

All data comes from [nflverse](https://github.com/nflverse/nflverse-data) via `nflreadpy`,
nflverse's own polars-native package. The loaders in `src/ffb/data/loader.py` call these:

```python
import nflreadpy as nfl

nfl.load_pbp([2024, 2025])              # play-by-play with EPA
nfl.load_snap_counts([2024, 2025])      # snap counts
nfl.load_player_stats([2024, 2025])     # weekly player stats
nfl.load_rosters([2024, 2025])          # rosters
nfl.load_injuries([2024, 2025])         # injury reports
nfl.load_schedules([2024, 2025])        # schedules
nfl.load_ff_playerids()                 # pfr_id ↔ gsis_id crosswalk
```

`nflreadpy` publishes further loaders the ones here do not call, among them
`load_nextgen_stats()`, `load_depth_charts()` and `load_contracts()`.

A loader reads one release asset per season and raises on the first it cannot read, so
`_by_season` asks for one season at a time. It raises `ValueError` before any request
for a season outside the range its release covers, and that ceiling moves with the
calendar independently of `seasons.season_for`.

Every loader returns a polars frame under the column names the release carries.
`loader` renames the two that disagree with the frames they are joined to — the roster
`gsis_id` and the weekly `team` — and takes `position` off the rosters rather than off
the weekly asset, which labels a fullback FB.

The weekly stats asset updates through a season rather than landing complete at the
end of one, so a season being played is read from it. `load_weekly_stats` reconstructs
a season the asset does not carry at all from play-by-play rather than dropping it.

## Repo

- **GitHub:** stxkxs/ffb
- **Project board:** https://github.com/users/stxkxs/projects/1
- **Architecture:** `docs/architecture.md` — bounded contexts, the engine/screen split, and how to add a tool

## Supplementary APIs (not used by this codebase)

- **ESPN:** undocumented JSON API for real-time scores and injuries
- **Sleeper:** free JSON API for fantasy league data, roster %, trending players
- **The Odds API:** betting lines (500 free req/mo)
