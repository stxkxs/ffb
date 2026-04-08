# FFB

NFL data analytics and fantasy football tools.

## Stack

- **Language:** Python
- **Data acquisition:** `nfl_data_py` (nflverse parquet files from GitHub Releases — free, no auth)
- **Data processing:** `polars` (preferred) or `pandas`
- **TUI:** `textual`
- **HTTP (if needed):** `httpx`

## Data Source

All data comes from [nflverse](https://github.com/nflverse/nflverse-data) via `nfl_data_py`. Key imports:

```python
import nfl_data_py as nfl

nfl.import_pbp_data([2024, 2025])           # play-by-play with EPA
nfl.import_snap_counts([2024, 2025])        # snap counts
nfl.import_player_stats([2024, 2025])       # player stats
nfl.import_weekly_rosters([2024, 2025])     # rosters
nfl.import_injuries([2024, 2025])           # injuries
nfl.import_schedules([2024, 2025])          # schedules
nfl.import_contracts()                       # contracts
nfl.import_ngs_data(stat_type="receiving")  # Next Gen Stats
```

## Repo

- **GitHub:** stxkxs/ffb
- **Project board:** https://github.com/users/stxkxs/projects/1
- **Research:** see `nfl-data-api-research.md` for full API research and decision rationale

## Supplementary APIs (not in current stack, available if needed)

- **ESPN:** undocumented JSON API for real-time scores/injuries — see research doc for endpoints
- **Sleeper:** free JSON API for fantasy league data, roster %, trending players
- **The Odds API:** betting lines (500 free req/mo)
