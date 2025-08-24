#!/usr/bin/env python3
"""
Sleeper League & Roster Sync (Python, decoupled flows)

This file now supports **two independent syncs** so player-syncs can run at most once per day,
while roster-syncs can run many times per day without re-downloading the giant players map.

Subcommands / Lambda handlers
- CLI subcommands:
    - `players-sync` : fetch `/players/nfl` once and publish to S3 (dated + current) and/or local file
    - `roster-sync`  : fetch league rosters and expand using players **from S3 current** (fallback: local cache or live)
- Lambda handlers:
    - `players_lambda_handler(event, context)`
    - `roster_lambda_handler(event, context)`

S3 layout (recommended)
  s3://$S3_BUCKET/$PLAYERS_S3_PREFIX/
    ├── YYYY-MM-DD.json        # full snapshot, immutable per day
    ├── current.json           # overwritten pointer to latest full snapshot
    └── players_core.json      # trimmed map {id: {full_name, position, team, bye_week, status, injury_status}}

ENV VARS
  S3_BUCKET=...               # enable S3 writes/reads
  AWS_REGION=us-east-1        # optional
  PLAYERS_S3_PREFIX=sleeper/players
  PLAYERS_CACHE_PATH=.players_nfl.json
  PLAYERS_CACHE_TTL_HR=24
  ROSTER_S3_PREFIX=sleeper_sync   # where roster snapshots go (league_id.json)
  USE_S3_PLAYERS=1             # roster-sync prefers S3 players current.json (decoupling)

Usage examples
  # Daily players sync (writes to S3 and updates current.json)
  python sleeper_sync.py players-sync

  # Roster sync (every 10m/hour) using the S3 players map
  python sleeper_sync.py roster-sync --username YOUR_USER --season 2025 --league-id <LEAGUE_ID> --out roster.json

EventBridge suggestions
  - Players sync: 06:05 AM America/Los_Angeles daily
  - Roster sync:  every 10 minutes

"""
from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import httpx

SLEEPER_BASE = "https://api.sleeper.app/v1"
DEFAULT_SEASON = datetime.now().year
PLAYERS_CACHE_PATH = os.getenv("PLAYERS_CACHE_PATH", ".players_nfl.json")
PLAYERS_CACHE_TTL_HR = int(os.getenv("PLAYERS_CACHE_TTL_HR", "24"))

# ------------------------------
# HTTP helpers
# ------------------------------
async def fetch_json(client: httpx.AsyncClient, url: str) -> Any:
    r = await client.get(url, timeout=60)
    r.raise_for_status()
    return r.json()

# ------------------------------
# Sleeper API wrappers
# ------------------------------
async def get_user(client: httpx.AsyncClient, username_or_id: str) -> Dict[str, Any]:
    return await fetch_json(client, f"{SLEEPER_BASE}/user/{username_or_id}")

async def get_user_leagues(client: httpx.AsyncClient, user_id: str, season: int) -> List[Dict[str, Any]]:
    return await fetch_json(client, f"{SLEEPER_BASE}/user/{user_id}/leagues/nfl/{season}")

async def get_league_users(client: httpx.AsyncClient, league_id: str) -> List[Dict[str, Any]]:
    return await fetch_json(client, f"{SLEEPER_BASE}/league/{league_id}/users")

async def get_league_rosters(client: httpx.AsyncClient, league_id: str) -> List[Dict[str, Any]]:
    return await fetch_json(client, f"{SLEEPER_BASE}/league/{league_id}/rosters")

async def fetch_players_from_api(client: httpx.AsyncClient) -> Dict[str, Any]:
    return await fetch_json(client, f"{SLEEPER_BASE}/players/nfl")

# ------------------------------
# S3 helpers (optional)
# ------------------------------

def _boto3():
    try:
        import boto3  # type: ignore
        return boto3
    except Exception as e:
        print("[warn] boto3 not available; S3 ops disabled:", e, file=sys.stderr)
        return None


def s3_put_json(obj: Any, key: str) -> Optional[str]:
    bucket = os.getenv("S3_BUCKET")
    if not bucket:
        return None
    b3 = _boto3()
    if not b3:
        return None
    s3 = b3.client("s3", region_name=os.getenv("AWS_REGION", "us-east-1"))
    data = json.dumps(obj, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
    s3.put_object(Bucket=bucket, Key=key, Body=data, ContentType="application/json")
    return f"s3://{bucket}/{key}"


def s3_get_json(key: str) -> Optional[Any]:
    bucket = os.getenv("S3_BUCKET")
    if not bucket:
        return None
    b3 = _boto3()
    if not b3:
        return None
    s3 = b3.client("s3", region_name=os.getenv("AWS_REGION", "us-east-1"))
    try:
        obj = s3.get_object(Bucket=bucket, Key=key)
        body = obj["Body"].read()
        return json.loads(body)
    except Exception as e:
        print(f"[warn] s3_get_json failed {bucket}/{key}: {e}", file=sys.stderr)
        return None

# ------------------------------
# Players map caching & publishing (decoupled)
# ------------------------------

def _players_cache_is_fresh(path: str, ttl_hr: int) -> bool:
    try:
        age_hr = (time.time() - os.stat(path).st_mtime) / 3600.0
        return age_hr <= ttl_hr
    except FileNotFoundError:
        return False


def _write_local(path: str, obj: Any):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f)


def build_players_core(full_map: Dict[str, Any]) -> Dict[str, Any]:
    core = {}
    for pid, p in full_map.items():
        core[pid] = {
            "full_name": p.get("full_name") or (" ".join(filter(None, [p.get("first_name"), p.get("last_name")]))) or p.get("search_full_name") or p.get("display_name"),
            "position": p.get("position"),
            "team": p.get("team"),
            "bye_week": p.get("bye_week"),
            "status": p.get("status"),
            "injury_status": p.get("injury_status"),
        }
    return core


async def players_sync(publish_to_s3: bool = True, out_local: Optional[str] = None) -> Dict[str, Any]:
    async with httpx.AsyncClient(headers={"User-Agent": "tyler-ai-gm/0.2"}) as client:
        full = await fetch_players_from_api(client)

    # Always update local cache
    _write_local(PLAYERS_CACHE_PATH, full)

    result = {"fetched_at": datetime.now(timezone.utc).isoformat(), "count": len(full)}

    if publish_to_s3 and os.getenv("S3_BUCKET"):
        prefix = os.getenv("PLAYERS_S3_PREFIX", "sleeper/players")
        day_key = f"{prefix}/{datetime.now(timezone.utc).date()}.json"
        cur_key = f"{prefix}/current.json"
        core_key = f"{prefix}/players_core.json"
        result["s3_day"] = s3_put_json(full, day_key)
        result["s3_current"] = s3_put_json(full, cur_key)
        result["s3_core"] = s3_put_json(build_players_core(full), core_key)
    else:
        result["warning"] = "S3_BUCKET not set or publish disabled; wrote local cache only"

    if out_local:
        _write_local(out_local, full)
        result["local_out"] = out_local

    return result

# ------------------------------
# Roster sync (reads players from S3 current.json by default)
# ------------------------------

def _name_from_core(p: Dict[str, Any]) -> Optional[str]:
    return p.get("full_name")


def index_users(users: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    return {u["user_id"]: u for u in users}


def enrich_rosters(rosters: List[Dict[str, Any]], users_by_id: Dict[str, Dict[str, Any]], players_by_id: Dict[str, Dict[str, Any]]) -> List[Dict[str, Any]]:
    enriched = []
    def view(pid: str) -> Dict[str, Any]:
        p = players_by_id.get(pid) or {}
        return {
            "player_id": pid,
            "name": _name_from_core(p),
            "pos": p.get("position"),
            "team": p.get("team"),
            "status": p.get("status"),
            "injury_status": p.get("injury_status"),
        }
    for r in rosters:
        owner_id = r.get("owner_id")
        manager = users_by_id.get(owner_id, {}) if owner_id else {}
        roster_players = r.get("players", []) or []
        starters = r.get("starters", []) or []
        enriched.append({
            "league_id": r.get("league_id"),
            "roster_id": r.get("roster_id"),
            "manager": {
                "user_id": manager.get("user_id"),
                "username": manager.get("username"),
                "display_name": manager.get("display_name"),
                "team_name": (manager.get("metadata") or {}).get("team_name"),
            },
            "settings": r.get("settings", {}),
            "players": [view(pid) for pid in roster_players],
            "starters": [view(pid) for pid in starters],
            "taxi": r.get("taxi") or [],
            "reserve": r.get("reserve") or [],
        })
    return enriched


async def roster_sync(username: str, season: int, league_id: Optional[str]) -> Dict[str, Any]:
    async with httpx.AsyncClient(headers={"User-Agent": "tyler-ai-gm/0.2"}) as client:
        user = await get_user(client, username)
        user_id = user.get("user_id")
        if not user_id:
            raise RuntimeError(f"No user_id for '{username}'")
        leagues = await get_user_leagues(client, user_id, season)
        if not leagues:
            raise RuntimeError(f"No leagues for user {username} in {season}")

        # Pick league
        target = None
        if league_id:
            target = next((l for l in leagues if l.get("league_id") == league_id), None)
            if not target:
                raise RuntimeError(f"league_id {league_id} not found for user {username}")
        else:
            target = next((l for l in leagues if l.get("status") in {"drafting", "in_season"}), leagues[0])

        league_id = target.get("league_id")

        # Prefer S3 players current.json if configured
        players_core = None
        if os.getenv("USE_S3_PLAYERS") and os.getenv("S3_BUCKET"):
            prefix = os.getenv("PLAYERS_S3_PREFIX", "sleeper/players")
            players_core = s3_get_json(f"{prefix}/players_core.json") or s3_get_json(f"{prefix}/current.json")
            if players_core and isinstance(players_core, dict) and "full_name" not in next(iter(players_core.values()), {}):
                # current.json may be full map; convert to core structure
                players_core = build_players_core(players_core)

        # Fallback to local cache or live fetch (keeps roster sync resilient)
        if players_core is None:
            # try local cache
            if _players_cache_is_fresh(PLAYERS_CACHE_PATH, PLAYERS_CACHE_TTL_HR):
                with open(PLAYERS_CACHE_PATH, "r", encoding="utf-8") as f:
                    players_core = build_players_core(json.load(f))
            else:
                # live fetch as last resort (still decoupled in practice if S3 is set)
                players_core = build_players_core(await fetch_players_from_api(client))

        users, rosters = await asyncio.gather(
            get_league_users(client, league_id),
            get_league_rosters(client, league_id),
        )
        users_by_id = index_users(users)
        teams = enrich_rosters(rosters, users_by_id, players_core)

        snapshot = {
            "fetched_at": datetime.now(timezone.utc).isoformat(),
            "user": {k: user.get(k) for k in ("user_id", "username", "display_name")},
            "season": season,
            "league": {
                "league_id": league_id,
                "name": target.get("name"),
                "status": target.get("status"),
                "scoring_settings": target.get("scoring_settings"),
                "roster_positions": target.get("roster_positions"),
            },
            "teams": teams,
        }

        # Optional write to S3
        bucket = os.getenv("S3_BUCKET")
        if bucket:
            key = f"{os.getenv('ROSTER_S3_PREFIX', 'sleeper_sync')}/{league_id}.json"
            snapshot["s3_uri"] = s3_put_json(snapshot, key)
        return snapshot

# ------------------------------
# CLI
# ------------------------------

def make_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Sleeper decoupled syncs")
    sub = parser.add_subparsers(dest="cmd", required=True)

    p_players = sub.add_parser("players-sync", help="Fetch players map and publish to S3/local")
    p_players.add_argument("--out", help="Write full players map to local file as well")

    p_roster = sub.add_parser("roster-sync", help="Fetch a league's rosters")
    p_roster.add_argument("--username", required=True)
    p_roster.add_argument("--season", type=int, default=DEFAULT_SEASON)
    p_roster.add_argument("--league-id")
    p_roster.add_argument("--out", help="Write roster snapshot to local file")

    return parser


def main(argv: Optional[List[str]] = None) -> int:
    parser = make_parser()
    args = parser.parse_args(argv)

    if args.cmd == "players-sync":
        res = asyncio.run(players_sync(publish_to_s3=True, out_local=getattr(args, "out", None)))
        print(json.dumps(res, indent=2))
        return 0

    if args.cmd == "roster-sync":
        snap = asyncio.run(roster_sync(args.username, args.season, getattr(args, "league_id", None)))
        text = json.dumps(snap, indent=2, ensure_ascii=False)
        print(text)
        if getattr(args, "out", None):
            with open(args.out, "w", encoding="utf-8") as f:
                f.write(text)
            print(f"[ok] wrote {args.out}")
        return 0

    parser.error("unknown command")
    return 2

# ------------------------------
# Lambda handlers (decoupled)
# ------------------------------

def players_lambda_handler(event, context):  # pragma: no cover
    res = asyncio.run(players_sync(publish_to_s3=True, out_local=event.get("out")))
    return {"statusCode": 200, "body": json.dumps(res)}


def roster_lambda_handler(event, context):  # pragma: no cover
    username = event.get("username")
    season = int(event.get("season", DEFAULT_SEASON))
    league_id = event.get("league_id")
    snap = asyncio.run(roster_sync(username, season, league_id))
    return {"statusCode": 200, "body": json.dumps(snap)}

if __name__ == "__main__":
    raise SystemExit(main())
