# FFB

NFL data analytics and fantasy football tools.

## Stack

- **Language:** Python
- **Data acquisition:** `nfl_data_py` (nflverse parquet files from GitHub Releases — free, no auth)
- **Data processing:** `polars` (preferred) or `pandas`
- **TUI:** `textual`
- **HTTP (if needed):** `httpx`

## Data Source

All data comes from [nflverse](https://github.com/nflverse/nflverse-data) via `nfl_data_py`.
The loaders in `src/ffb/data/loader.py` call these importers:

```python
import nfl_data_py as nfl

nfl.import_pbp_data([2024, 2025])            # play-by-play with EPA
nfl.import_snap_counts([2024, 2025])         # snap counts
nfl.import_weekly_data([2024, 2025])         # weekly player stats
nfl.import_seasonal_rosters([2024, 2025])    # rosters
nfl.import_injuries([2024, 2025])            # injury reports
nfl.import_schedules([2024, 2025])           # schedules
nfl.import_ids()                             # pfr_id ↔ gsis_id crosswalk
```

`nfl_data_py` publishes further importers the loaders do not call, among them
`import_ngs_data(stat_type="receiving")` for Next Gen Stats and `import_contracts()`.

nflverse publishes weekly player stats as one release asset per season, so a season in
progress can have no asset at all. `load_weekly_stats` derives those weeks from
play-by-play rather than dropping the season.

## Repo

- **GitHub:** stxkxs/ffb
- **Project board:** https://github.com/users/stxkxs/projects/1
- **Architecture:** `docs/architecture.md` — bounded contexts, the engine/screen split, and how to add a tool

## Supplementary APIs (not used by this codebase)

- **ESPN:** undocumented JSON API for real-time scores and injuries
- **Sleeper:** free JSON API for fantasy league data, roster %, trending players
- **The Odds API:** betting lines (500 free req/mo)
