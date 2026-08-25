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

The sidebar lists six tools. A tool downloads its data the first time it is opened;
`docs/architecture.md` describes how the tools, the engines and the data layer fit together.

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
When a player gets injured, see the historical fantasy impact on their teammates. Search by player name to compare teammate stats across the weeks that player was active against the weeks they sat out.

- Search autocomplete across every offensive player in the loaded snap counts
- Fantasy points, targets, and touches with/without the player
- Delta per teammate, and a confidence tier keyed to the size of the without-sample: High at 7 games or more, Med at 4 or more, Low below that
- Three-season lookback

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
Rest-of-season trade value chart. Ranks the offensive players of one season through one chosen week and scales the field so the strongest player scores 100.

A player is ranked when the chosen week is week 3 or later, the 18-week regular season
still has a week left to play, and the player has at least three games of results. Four
components carry the score:

- **Production (50%)** — season PPG multiplied by games remaining, divided by the largest
  such figure in the field. Games remaining is the weeks left in the regular season, less
  one when the player's team still has its bye ahead.
- **Schedule strength (20%)** — average fantasy points the remaining opponents allow at the
  player's position, divided by the league average allowed at that position, then scaled
  across the field between its own minimum and maximum. A team whose remaining opponents
  have no ranking takes a neutral 1.00 multiplier.
- **Usage (20%)** — average offensive snap share over the weeks played, as a fraction of
  100. A player with no snap-count row takes 50%.
- **Health (10%)** — one minus the fraction of the loaded seasons' regular-season weeks the
  player was listed Out, that fraction capped at 0.5 so health never falls below 0.5. A
  player with no Out week takes 1.0.

A bye week is not a component of its own: it enters through games remaining, which is the
only term that separates two players with the same PPG.

The weighted sum is rescaled so the field's best score reads 100.0. The table lists PPG,
games played, the schedule multiplier, snap share, health, bye week and the value, and
filters by position, team, season and as-of week.

## Stack

- **Data:** [nflverse](https://github.com/nflverse/nflverse-data) via `nfl_data_py` (free, no auth)
- **Processing:** `polars`
- **TUI:** `textual`

## Configuration

Every setting is a module constant; the application reads no environment variables and
takes no command-line flags.

| Setting | Value | Defined in |
| --- | --- | --- |
| Cache directory | `~/.fantasy/cache` | `CACHE_DIR` in `src/ffb/data/cache.py` |
| Freshness window | 6 hours | `DEFAULT_TTL` in `src/ffb/data/cache.py` |
| Size ceiling | 2 GiB | `MAX_CACHE_BYTES` in `src/ffb/data/cache.py` |
| Download timeout | 120 seconds per download | `_DOWNLOAD_TIMEOUT` in `src/ffb/data/loader.py` |

Each download lands as one Parquet file named for its cache key — `snap_counts_2025.parquet`,
`pbp_2025.parquet`, `player_ids.parquet` — beside a `_meta.json` holding the write and
last-access time of every entry. A dataset published per season is keyed per season, so a
tool asking for two seasons reads or writes two entries. A read of an entry written more than six hours earlier
counts as a miss and triggers a fresh download. Writing an entry reaps records whose Parquet
file is gone and Parquet files no record claims, then evicts entries least recently read
until the directory fits under the ceiling.

To clear the cache, delete the directory:

```bash
rm -rf ~/.fantasy/cache
```

To replace only what one tool reads, press **Refresh** in that tool. Refresh skips the cache
read and overwrites every key the tool loads.

## Troubleshooting

**The first load takes minutes.** Opening a tool downloads whole-season Parquet assets from
nflverse. Red Zone pulls two seasons of play-by-play, and so does any tool whose season has
no published weekly-stats asset; play-by-play is the largest asset the cache holds, which is
what `MAX_CACHE_BYTES` in `src/ffb/data/cache.py` is sized around. Nothing is on disk before
the first download succeeds, so the wait falls once per dataset season and then not again
for six hours.

**Watching a load.** The loading pane names what is downloading and counts elapsed time as
`MM:SS`, updated once a second. `nfl_data_py` downloads a release asset in one blocking call
and reports no progress, so elapsed time is the only honest readout — there is no percentage
or bar.

**Stopping a load.** Press the **Cancel** button, or `escape` while the load is in flight —
the binding is offered in the footer only while there is something to cancel. Cancelling
discards the result and restores the previous content, and posts `Load cancelled.` A download
already under way cannot be interrupted, so it runs to completion or to its timeout on a
background daemon thread with nothing left to hand its result to.

**`Failed to load data: Download timed out after 120s. Check your network connection.`** One
download did not finish inside its 120-second budget. The budget is per download, not per
tool: a tool that loads five datasets gives each its own 120 seconds, and the first to run
out ends the whole load. Datasets that completed are already cached, so a tool that reads
only those opens from disk. Retrying the tool that timed out means pressing **Refresh**,
which forces every dataset it loads — including the ones already cached — to download
again.

**A season nflverse has not published.** The season label rolls over on 1 September,
while nflverse publishes a season's play-by-play, snap counts, weekly stats and injuries
only once its games have been played — so for the opening days of September every tool asks
for a season the source answers `HTTP Error 404` for. That season contributes no rows and
the seasons beside it render: each loader resolves its season list one season at a time, so
an unpublished season costs only itself.

The skip reaches the interface, because every screen builds its season filter from the
seasons its data carries rows for. The unpublished season is absent from the
filter, and the filter opens on the newest season that has data. Start/Sit and Trade Value
read their season options off the results for the same reason, not off the schedule:
nflverse publishes a schedule months before the season opens, so a schedule alone would
offer a season nothing can be projected or ranked for.

Nothing negative is cached, so the season joins the filter on the first load after nflverse
publishes it. The price is one failed request per unpublished asset per load.

**`Failed to load data: nflverse has not published snap counts for season 2026`.** Every
season the tool asked for is unpublished, so the load has nothing to render. The message
names the dataset and the seasons. Loads whose datasets are published are unaffected: a tool
fails only on the dataset it could not resolve at all.

**Any other `Failed to load data:` toast** carries the message the download itself raised. A
season nflverse has not published a weekly-stats asset for is not an error on its own: those
weeks are derived from play-by-play, at the cost of a play-by-play download, and the season
drops out only when neither asset covers it.

**A table shows a sentence where rows would be.** An empty result renders as a message
naming which emptiness it is — a filter combination that matches nothing, a season and week
too early to rank, a season with no completed week behind it, or a search with no split to
report.

## Development

```bash
uv sync                 # install the project and the dev group
uv run ruff check .     # lint
uv run mypy src         # type-check
uv run pytest           # unit tests
uv run pytest --cov     # unit tests, and the coverage floor in pyproject.toml
uv run ffb              # run the TUI
```

Tests run the cache against a redirected cache directory, and the engines, the views and
the app against the synthetic league in `tests/conftest.py`. No test reaches the network.
