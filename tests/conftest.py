"""Shared test fixtures — small synthetic DataFrames for engine tests."""

import polars as pl
import pytest


@pytest.fixture()
def snap_counts() -> pl.DataFrame:
    """Minimal snap counts: 2 players, 6 weeks, 1 team."""
    rows = []
    for week in range(1, 7):
        rows.append({
            "pfr_player_id": "AAA01",
            "player": "Alpha Player",
            "position": "WR",
            "team": "NYG",
            "opponent": "DAL",
            "season": 2025,
            "week": week,
            "game_type": "REG",
            "offense_snaps": 50.0 + week * 5,
            "offense_pct": 0.50 + week * 0.05,
            "defense_snaps": 0.0,
            "defense_pct": 0.0,
            "st_snaps": 0.0,
            "st_pct": 0.0,
            "game_id": f"2025_{week:02d}_NYG_DAL",
            "pfr_game_id": f"202509{week:02d}dal",
        })
        rows.append({
            "pfr_player_id": "BBB02",
            "player": "Beta Player",
            "position": "RB",
            "team": "NYG",
            "opponent": "DAL",
            "season": 2025,
            "week": week,
            "game_type": "REG",
            "offense_snaps": 40.0,
            "offense_pct": 0.40,
            "defense_snaps": 0.0,
            "defense_pct": 0.0,
            "st_snaps": 0.0,
            "st_pct": 0.0,
            "game_id": f"2025_{week:02d}_NYG_DAL",
            "pfr_game_id": f"202509{week:02d}dal",
        })
    return pl.DataFrame(rows)


@pytest.fixture()
def weekly_stats() -> pl.DataFrame:
    """Minimal weekly stats matching snap_counts players."""
    rows = []
    for week in range(1, 7):
        rows.append({
            "player_id": "00-001",
            "player_display_name": "Alpha Player",
            "position": "WR",
            "recent_team": "NYG",
            "opponent_team": "DAL",
            "season": 2025,
            "week": week,
            "season_type": "REG",
            "fantasy_points_ppr": 12.0 + week * 2.0,
            "targets": 6.0 + week,
            "carries": 0.0,
            "receptions": 4.0,
            "receiving_yards": 60.0,
            "receiving_tds": 0.0,
            "rushing_yards": 0.0,
            "rushing_tds": 0.0,
            "passing_yards": 0.0,
            "passing_tds": 0.0,
            "interceptions": 0.0,
            "fumbles_lost": 0.0,
        })
        rows.append({
            "player_id": "00-002",
            "player_display_name": "Beta Player",
            "position": "RB",
            "recent_team": "NYG",
            "opponent_team": "DAL",
            "season": 2025,
            "week": week,
            "season_type": "REG",
            "fantasy_points_ppr": 10.0,
            "targets": 2.0,
            "carries": 15.0,
            "receptions": 1.0,
            "receiving_yards": 10.0,
            "receiving_tds": 0.0,
            "rushing_yards": 70.0,
            "rushing_tds": 0.0,
            "passing_yards": 0.0,
            "passing_tds": 0.0,
            "interceptions": 0.0,
            "fumbles_lost": 0.0,
        })
    return pl.DataFrame(rows)


@pytest.fixture()
def player_ids() -> pl.DataFrame:
    """Crosswalk mapping pfr_id → gsis_id for test players."""
    return pl.DataFrame({
        "pfr_id": ["AAA01", "BBB02"],
        "gsis_id": ["00-001", "00-002"],
        "name": ["Alpha Player", "Beta Player"],
        "position": ["WR", "RB"],
    })


@pytest.fixture()
def schedules() -> pl.DataFrame:
    """Minimal schedule: NYG vs DAL for 6 weeks."""
    rows = []
    for week in range(1, 7):
        rows.append({
            "game_id": f"2025_{week:02d}_NYG_DAL",
            "season": 2025,
            "week": week,
            "game_type": "REG",
            "home_team": "DAL",
            "away_team": "NYG",
        })
    return pl.DataFrame(rows)
