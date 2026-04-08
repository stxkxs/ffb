# ffb

NFL data analytics and fantasy football tools. Terminal UI powered by [Textual](https://github.com/Textualize/textual).

## Install

```bash
uv sync
```

## Run

```bash
uv run ffb
```

## Tools

### Snap Share
Weekly snap count trend tracker. Flags players gaining or losing offensive snap share over 3-week rolling windows. Surfaces breakout candidates before box scores reflect it.

- Rolling average, delta, and velocity per player per week
- Breakout detection (crossed from <50% to >60% snap share)
- Rising/falling alert panels
- Filter by position, team, season, week

### Red Zone
Team and player red zone conversion analysis. Red zone behavior is the strongest predictor of fantasy scoring and game totals.

- Team rankings: trips, TDs, conversion rate, pass/rush split, EPA/play
- Player rankings: red zone targets, target share, carries, touches, TDs, TD rate
- Filter by position, team, season

### Injury Impact
When a player gets injured, instantly see the historical fantasy impact on their teammates. Search by player name to see how teammate stats changed when that player was active vs inactive.

- Search autocomplete across all offensive players
- Fantasy points, targets, and touches with/without the player
- Delta and confidence indicators based on sample size
- Multi-season lookback (2023-2025)

### Start/Sit
Matchup-based start/sit projections. Cross-references player baselines against opponent defensive rankings by position to produce matchup-adjusted fantasy point projections.

- Defensive rankings: avg fantasy points allowed per position per game
- Matchup multiplier: opponent strength vs league average
- Confidence tiers: Strong Start, Lean Start, Neutral, Lean Sit, Strong Sit
- Filter by position, team, season, week

### Waiver Wire
Ranks players by a composite usage score combining snap share, target share, and touch share. Flags players with rising 3-week trends — the waiver pickups before they blow up.

- Composite usage score: snap% (40%) + target share (35%) + touch share (25%)
- Rolling average, delta, and velocity trend detection
- Rising/falling alert panels + full player table
- Filter by position, team, season, week

### Trade Value
Rest-of-season trade value chart. Normalizes player value from multiple factors into a single 0-100 score for evaluating trades.

- Production (40%): season PPG × games remaining
- Schedule strength (20%): avg opponent defensive ranking for remaining games
- Usage (20%): snap share percentage
- Health (10%): injury history discount from missed games
- Bye week (10%): penalty if bye is still upcoming

## Stack

- **Data:** [nflverse](https://github.com/nflverse/nflverse-data) via `nfl_data_py` (free, no auth)
- **Processing:** `polars`
- **TUI:** `textual`

## Data Caching

Downloaded data is cached locally at `~/.fantasy/cache/` with a 6-hour TTL. Hit the Refresh button in any tool to force a fresh download.
