# %%
import asyncio
from pathlib import Path

import httpx
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parent.parent
RAW_DIR = PROJECT_ROOT / "raw" / "athletes"
RAW_DIR.mkdir(parents=True, exist_ok=True)
BASKETBALL_PLAYER_STATS_CATEGORY_MAP = {
    "general": [
        "total_games_played",
        "avg_minutes",
        "avg_personal_fouls",
        "total_double_doubles",
        "total_triple_doubles",
        "total_disqualifications",
        "total_ejections",
        "total_technical_fouls",
        "total_flagrant_fouls",
        "total_minutes",
        "total_rebounds",
        "total_personal_fouls",
        "avg_rebounds",
    ],
    "offensive": [
        "avg_points",
        "avg_field_goal_makes",
        "avg_field_goal_attempts",
        "avg_field_goal_pctg",
        "avg_3pt_makes",
        "avg_3pt_attempts",
        "avg_3pt_pctg",
        "avg_ft_makes",
        "avg_ft_attempts",
        "avg_ft_pctg",
        "avg_assists",
        "avg_turnovers",
        "total_points",
        "total_field_goal_makes",
        "total_field_goal_attempts",
        "total_3pt_makes",
        "total_3pt_attempts",
        "total_ft_makes",
        "total_ft_attempts",
        "total_assists",
        "total_turnovers",
    ],
    "defensive": [
        "avg_steals",
        "avg_blocks",
        "total_steals",
        "total_blocks",
    ],
}


async def async_get(url: str, client: httpx.AsyncClient):
    """Async wrapper for GET requests."""
    resp = await client.get(url)
    resp.raise_for_status()
    return resp.json()


def get_athlete_demographic_data(athlete_data):
    demographics = athlete_data.get("athlete", {})
    return {
        "player_id": demographics.get("id"),
        "team_id": demographics.get("teamId"),
        "sport": demographics.get("type"),
        "first_name": demographics.get("firstName"),
        "last_name": demographics.get("lastName"),
        "full_name": demographics.get("displayName"),
        "team_name": demographics.get("teamName"),
        "stats_page_link": demographics.get("links", [{}])[0].get("href"),
        "position": demographics.get("position", {}).get("slug"),
        "active_status": demographics.get("status", {}).get("name"),
    }


def get_player_stats(athlete_data):
    stat_categories = athlete_data.get("categories", [])
    player_stats = {}
    for category in stat_categories:
        name = category.get("name")
        values = category.get("values", [])
        if name in BASKETBALL_PLAYER_STATS_CATEGORY_MAP:
            fields = BASKETBALL_PLAYER_STATS_CATEGORY_MAP[name]
            mapped = dict(zip(fields, values))
            player_stats.update(mapped)
    return player_stats


async def fetch_page(url: str, page: int, client: httpx.AsyncClient):
    """Fetch one page of athletes from the ESPN API."""
    page_url = f"{url}&page={page}"
    return await async_get(page_url, client)


async def get_all_player_info_async(url: str):
    async with httpx.AsyncClient(timeout=10) as client:
        # 1) Fetch first page to discover total pages
        first_page = await async_get(url, client)
        pagination = first_page.get("pagination", {})
        max_pages = pagination.get("pages", 1)
        # 2) Fetch all remaining pages concurrently
        tasks = [
            asyncio.create_task(fetch_page(url, page, client))
            for page in range(1, max_pages + 1)
        ]
        pages = await asyncio.gather(*tasks)
        # 3) Extract players from all pages
        all_players = []
        for page_data in pages:
            for athlete in page_data.get("athletes", []):
                demo = get_athlete_demographic_data(athlete)
                stats = get_player_stats(athlete)
                all_players.append({**demo, **stats})
        return all_players


players = asyncio.run(
    get_all_player_info_async(
        "https://site.web.api.espn.com/apis/common/v3/sports/basketball/mens-college-basketball/statistics/byathlete?limit=50"
    )
)
df = pd.DataFrame(players)
df.to_csv(RAW_DIR / "all_player_basic_data.csv", index=False)
