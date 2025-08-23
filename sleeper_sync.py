#!/usr/bin/env python3
"""
Sleeper League & Roster Sync (Python MVP)

What this does
- Look up a Sleeper user by username
- List their NFL leagues for a given season
- Fetch users and rosters for a league
- Join rosters with managers & expand player_ids to names/pos/team via players map
- Output JSON to stdout and (optionally) save to a local file or S3

Usage (local):
  pip install httpx boto3 python-dateutil
  python sleeper_sync.py --username YOUR_SLEEPER_USERNAME --season 2025 --league-id <optional>

Optional env vars:
  S3_BUCKET            -> if set, uploads the JSON snapshot to s3://$S3_BUCKET/sleeper_sync/<league_id>.json
  AWS_REGION           -> AWS region for S3 client (default "us-east-1")
  PLAYERS_CACHE_PATH   -> local cache path for players map (default ".players_nfl.json")
  PLAYERS_CACHE_TTL_HR -> hours to reuse cached players map (default 24)

Lambda use:
  - Package this file with its deps as a Lambda. Set S3_BUCKET to write snapshots to S3 on a schedule.
  - Handler: lambda_handler(event, context) with keys {"username":"...","season":2025,"league_id":"..."}

Notes:
  - Uses public Sleeper API; be polite with rate limits.
  - Players map is large; we cache it locally/S3 to avoid repeated downloads.
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

import httpx
from dateutil.parser import isoparse

# ------------------------------
# Constants & Config
# ------------------------------
SLEEPER_BASE = "https://api.sleeper.app/v1"
DEFAULT_SEASON = datetime.now().year
PLAYERS_CACHE_PATH = os.getenv("PLAYERS_CACHE_PATH", ".players_nfl.json")
PLAYERS_CACHE_TTL_HR = int(os.getenv("PLAYERS_CACHE_TTL_HR", "24"))

# ------------------------------
# HTTP helpers
# ------------------------------
async def fetch_json(client: httpx.AsyncClient, url: str) -> Any:
    r = await client.get(url, timeout=30)
    r.raise_for_status()
    return r.json()

# ------------------------------
# Sleeper API wrappers
# ------------------------------
async def get_user(client: httpx.AsyncClient, username_or_id: str) -> Dict[str, Any]:
    url = f"{SLEEPER_BASE}/user/{username_or_id}"
    return await fetch_json(client, url)

async def get_user_leagues(client: httpx.AsyncClient, user_id: str, season: int) -> List[Dict[str, Any]]:
    url = f"{SLEEPER_BASE}/user/{user_id}/leagues/nfl/{season}"
    return await fetch_json(client, url)

async def get_league_users(client: httpx.AsyncClient, league_id: str) -> List[Dict[str, Any]]:
    url = f"{SLEEPER_BASE}/league/{league_id}/users"
    return await fetch_json(client, url)

async def get_league_rosters(client: httpx.AsyncClient, league_id: str) -> List[Dict[str, Any]]:
    url = f"{SLEEPER_BASE}/league/{league_id}/rosters"
    return await fetch_json(client, url)

async def get_players_map(client: httpx.AsyncClient) -> Dict[str, Dict[str, Any]]:
    """Fetch all NFL players. Cache to disk for a short TTL.
    Returns a dict keyed by Sleeper player_id -> player metadata.
    """
    # Try local cache first
    try:
        stat = os.stat(PLAYERS_CACHE_PATH)
        age_hr = (time.time() - stat.st_mtime) / 3600.0
        if age_hr <= PLAYERS_CACHE_TTL_HR:
            with open(PLAYERS_CACHE_PATH, "r", encoding="utf-8") as f:
                return json.load(f)
    except FileNotFoundError:
        pass

    url = f"{SLEEPER_BASE}/players/nfl"
    players = await fetch_json(client, url)

    # Persist cache
    with open(PLAYERS_CACHE_PATH, "w", encoding="utf-8") as f:
        json.dump(players, f)
    return players

# ------------------------------
# Join & transform
# ------------------------------

def index_users(users: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    return {u["user_id"]: u for u in users}

def index_players(players: Dict[str, Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    return players  # already keyed by id


def enrich_rosters(
    rosters: List[Dict[str, Any]],
    users_by_id: Dict[str, Dict[str, Any]],
    players_by_id: Dict[str, Dict[str, Any]],
) -> List[Dict[str, Any]]:
    enriched = []
    for r in rosters:
        owner_id = r.get("owner_id")
        manager = users_by_id.get(owner_id, {}) if owner_id else {}
        roster_players = r.get("players", []) or []
        starters = r.get("starters", []) or []

        def player_view(pid: str) -> Dict[str, Any]:
            p = players_by_id.get(pid) or {}
            return {
                "player_id": pid,
                "name": name_from_player(p),
                "pos": p.get("position"),
                "team": p.get("team"),
                "status": p.get("status"),
                "injury_status": p.get("injury_status"),
            }

        enriched.append(
            {
                "league_id": r.get("league_id"),
                "roster_id": r.get("roster_id"),
                "manager": {
                    "user_id": manager.get("user_id"),
                    "username": manager.get("username"),
                    "display_name": manager.get("display_name"),
                    "team_name": (manager.get("metadata") or {}).get("team_name"),
                },
                "settings": r.get("settings", {}),
                "players": [player_view(pid) for pid in roster_players],
                "starters": [player_view(pid) for pid in starters],
                "taxi": r.get("taxi") or [],
                "reserve": r.get("reserve") or [],
            }
        )
    return enriched


def name_from_player(p: Dict[str, Any]) -> Optional[str]:
    if not p:
        return None
    # Sleeper commonly provides full_name or first_name/last_name
    full = p.get("full_name")
    if full:
        return full
    first, last = p.get("first_name"), p.get("last_name")
    if first or last:
        return " ".join([x for x in [first, last] if x])
    return p.get("search_full_name") or p.get("display_name")

# ------------------------------
# Persistence (optional)
# ------------------------------

def maybe_write_s3(obj: Any, league_id: str) -> Optional[str]:
    bucket = os.getenv("S3_BUCKET")
    if not bucket:
        return None
    try:
        import boto3  # type: ignore

        s3 = boto3.client("s3", region_name=os.getenv("AWS_REGION", "us-east-1"))
        key = f"sleeper_sync/{league_id}.json"
        data = json.dumps(obj, separators=(",", ":")).encode("utf-8")
        s3.put_object(Bucket=bucket, Key=key, Body=data, ContentType="application/json")
        return f"s3://{bucket}/{key}"
    except Exception as e:  # pragma: no cover
        print(f"[warn] failed to write to S3: {e}", file=sys.stderr)
        return None

# ------------------------------
# Orchestration
# ------------------------------
import asyncio

async def sync_league(username: str, season: int, league_id: Optional[str] = None) -> Dict[str, Any]:
    async with httpx.AsyncClient(headers={"User-Agent": "tyler-ai-gm/0.1"}) as client:
        user = await get_user(client, username)
        user_id = user.get("user_id")
        if not user_id:
            raise RuntimeError(f"No user_id found for '{username}'")

        leagues = await get_user_leagues(client, user_id, season)
        if not leagues:
            raise RuntimeError(f"No leagues for user {username} in season {season}")

        # Choose league
        target_league = None
        if league_id:
            target_league = next((l for l in leagues if l.get("league_id") == league_id), None)
            if not target_league:
                raise RuntimeError(f"league_id {league_id} not found for user {username}")
        else:
            # Heuristic: pick the first in-season league, else first
            target_league = next((l for l in leagues if l.get("status") in {"drafting", "in_season"}), leagues[0])

        league_id = target_league.get("league_id")
        league_name = target_league.get("name")

        users, rosters, players_map = await asyncio.gather(
            get_league_users(client, league_id),
            get_league_rosters(client, league_id),
            get_players_map(client),
        )
        users_by_id = index_users(users)
        players_by_id = index_players(players_map)
        enriched = enrich_rosters(rosters, users_by_id, players_by_id)

        snapshot = {
            "fetched_at": datetime.now(timezone.utc).isoformat(),
            "user": {k: user.get(k) for k in ("user_id", "username", "display_name")},
            "season": season,
            "league": {
                "league_id": league_id,
                "name": league_name,
                "status": target_league.get("status"),
                "scoring_settings": target_league.get("scoring_settings"),
                "roster_positions": target_league.get("roster_positions"),
            },
            "teams": enriched,
        }

        # Optional write to S3
        s3_uri = maybe_write_s3(snapshot, league_id)
        if s3_uri:
            snapshot["s3_uri"] = s3_uri
        return snapshot

# ------------------------------
# CLI
# ------------------------------

def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Sync a Sleeper league's rosters to JSON")
    parser.add_argument("--username", required=True, help="Sleeper username (not display name)")
    parser.add_argument("--season", type=int, default=DEFAULT_SEASON, help="Season year, e.g., 2025")
    parser.add_argument("--league-id", help="Specific league_id to sync (optional)")
    parser.add_argument("--out", help="Write JSON to this local file as well (optional)")

    args = parser.parse_args(argv)

    snapshot = asyncio.run(sync_league(args.username, args.season, args.league_id))

    text = json.dumps(snapshot, indent=2, ensure_ascii=False)
    print(text)

    if args.out:
        with open(args.out, "w", encoding="utf-8") as f:
            f.write(text)
        print(f"[ok] wrote {args.out}")

    return 0

# ------------------------------
# Lambda handler
# ------------------------------

def lambda_handler(event, context):  # pragma: no cover
    username = event.get("username")
    season = int(event.get("season", DEFAULT_SEASON))
    league_id = event.get("league_id")
    snap = asyncio.run(sync_league(username, season, league_id))
    return {"statusCode": 200, "body": json.dumps(snap)}


if __name__ == "__main__":
    raise SystemExit(main())
