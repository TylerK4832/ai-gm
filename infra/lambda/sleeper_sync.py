#!/usr/bin/env python3
"""Decoupled Sleeper sync: players-sync (daily) and roster-sync (frequent)."""
from __future__ import annotations
import argparse, asyncio, json, os, sys, time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
import httpx

SLEEPER_BASE = "https://api.sleeper.app/v1"
DEFAULT_SEASON = datetime.now().year
PLAYERS_CACHE_PATH = os.getenv("PLAYERS_CACHE_PATH", ".players_nfl.json")
PLAYERS_CACHE_TTL_HR = int(os.getenv("PLAYERS_CACHE_TTL_HR", "24"))

async def fetch_json(client: httpx.AsyncClient, url: str) -> Any:
    r = await client.get(url, timeout=60); r.raise_for_status(); return r.json()

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

def _boto3():
    try:
        import boto3; return boto3
    except Exception as e:
        print("[warn] boto3 not available; S3 ops disabled:", e, file=sys.stderr); return None

def s3_put_json(obj: Any, key: str) -> Optional[str]:
    bucket = os.getenv("S3_BUCKET"); 
    if not bucket: return None
    b3 = _boto3(); 
    if not b3: return None
    s3 = b3.client("s3", region_name=os.getenv("AWS_REGION", "us-east-1"))
    data = json.dumps(obj, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
    s3.put_object(Bucket=bucket, Key=key, Body=data, ContentType="application/json")
    return f"s3://{bucket}/{key}"

def s3_get_json(key: str) -> Optional[Any]:
    bucket = os.getenv("S3_BUCKET"); 
    if not bucket: return None
    b3 = _boto3(); 
    if not b3: return None
    s3 = b3.client("s3", region_name=os.getenv("AWS_REGION", "us-east-1"))
    try:
        obj = s3.get_object(Bucket=bucket, Key=key); body = obj["Body"].read(); return json.loads(body)
    except Exception as e:
        print(f"[warn] s3_get_json failed {bucket}/{key}: {e}", file=sys.stderr); return None

def _players_cache_is_fresh(path: str, ttl_hr: int) -> bool:
    try: return (time.time() - os.stat(path).st_mtime)/3600.0 <= ttl_hr
    except FileNotFoundError: return False

def _write_local(path: str, obj: Any):
    with open(path, "w", encoding="utf-8") as f: json.dump(obj, f)

def build_players_core(full_map: Dict[str, Any]) -> Dict[str, Any]:
    core = {}
    for pid, p in full_map.items():
        name = p.get("full_name") or (" ".join(filter(None, [p.get("first_name"), p.get("last_name")]))) or p.get("search_full_name") or p.get("display_name")
        core[pid] = {"full_name": name, "position": p.get("position"), "team": p.get("team"),
                     "bye_week": p.get("bye_week"), "status": p.get("status"), "injury_status": p.get("injury_status")}
    return core

async def players_sync(publish_to_s3: bool = True, out_local: Optional[str] = None) -> Dict[str, Any]:
    async with httpx.AsyncClient(headers={"User-Agent": "tyler-ai-gm/0.2"}) as client:
        full = await fetch_players_from_api(client)
    _write_local(PLAYERS_CACHE_PATH, full)
    result = {"fetched_at": datetime.now(timezone.utc).isoformat(), "count": len(full)}
    if publish_to_s3 and os.getenv("S3_BUCKET"):
        prefix = os.getenv("PLAYERS_S3_PREFIX", "sleeper/players")
        result["s3_day"] = s3_put_json(full, f"{prefix}/{datetime.now(timezone.utc).date()}.json")
        result["s3_current"] = s3_put_json(full, f"{prefix}/current.json")
        result["s3_core"] = s3_put_json(build_players_core(full), f"{prefix}/players_core.json")
    else:
        result["warning"] = "S3_BUCKET not set or publish disabled; wrote local cache only"
    if out_local: _write_local(out_local, full); result["local_out"] = out_local
    return result

def _name_from_core(p: Dict[str, Any]) -> Optional[str]: return p.get("full_name")
def index_users(users: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]: return {u["user_id"]: u for u in users}

def enrich_rosters(rosters: List[Dict[str, Any]], users_by_id: Dict[str, Dict[str, Any]], players_by_id: Dict[str, Dict[str, Any]]) -> List[Dict[str, Any]]:
    enriched = []
    def view(pid: str) -> Dict[str, Any]:
        p = players_by_id.get(pid) or {}
        return {"player_id": pid, "name": _name_from_core(p), "pos": p.get("position"), "team": p.get("team"),
                "status": p.get("status"), "injury_status": p.get("injury_status")}
    for r in rosters:
        owner_id = r.get("owner_id"); manager = users_by_id.get(owner_id, {}) if owner_id else {}
        roster_players = r.get("players", []) or []; starters = r.get("starters", []) or []
        enriched.append({"league_id": r.get("league_id"), "roster_id": r.get("roster_id"),
                         "manager": {"user_id": manager.get("user_id"), "username": manager.get("username"),
                                     "display_name": manager.get("display_name"), "team_name": (manager.get("metadata") or {}).get("team_name")},
                         "settings": r.get("settings", {}), "players": [view(pid) for pid in roster_players],
                         "starters": [view(pid) for pid in starters], "taxi": r.get("taxi") or [], "reserve": r.get("reserve") or []})
    return enriched

async def roster_sync(
    username: Optional[str] = None,
    season: int = DEFAULT_SEASON,
    league_id: Optional[str] = None,
    *,
    user_id: Optional[str] = None,
    league_name: Optional[str] = None,
) -> Dict[str, Any]:
    async with httpx.AsyncClient(headers={"User-Agent": "tyler-ai-gm/0.2"}) as client:
        # Resolve user_id and user info for enrichment
        if not user_id and not username:
            raise RuntimeError("roster_sync requires 'user_id' or 'username'")
        user: Dict[str, Any]
        if user_id and not username:
            # fetch user to include username/display_name in snapshot
            user = await get_user(client, user_id)
            # Normalize to canonical user_id from API response (handles username passed in this field)
            user_id = user.get("user_id") or user_id
        else:
            # username provided; resolve to user object (works for id too)
            user = await get_user(client, username or user_id or "")
            user_id = user.get("user_id") or user_id
        if not user_id:
            raise RuntimeError(f"No user_id for user '{username or 'unknown'}'")

        leagues = await get_user_leagues(client, user_id, season)
        if not leagues:
            raise RuntimeError(f"No leagues for user {username or user_id} in {season}")

        # Pick league by id first, then by name, else choose an active one
        target = None
        if league_id:
            target = next((l for l in leagues if l.get("league_id") == league_id), None)
            if not target:
                raise RuntimeError(f"league_id {league_id} not found for user {username or user_id}")
        elif league_name:
            lname = league_name.strip().lower()
            target = next((l for l in leagues if str(l.get("name", "")).strip().lower() == lname), None)
            if not target:
                raise RuntimeError(f"league_name '{league_name}' not found for user {username or user_id}")
        else:
            target = next((l for l in leagues if l.get("status") in {"drafting", "in_season"}), leagues[0])
        league_id = target.get("league_id")
        players_core = None
        if os.getenv("USE_S3_PLAYERS") and os.getenv("S3_BUCKET"):
            prefix = os.getenv("PLAYERS_S3_PREFIX", "sleeper/players")
            players_core = s3_get_json(f"{prefix}/players_core.json") or s3_get_json(f"{prefix}/current.json")
            if players_core and isinstance(players_core, dict) and "full_name" not in next(iter(players_core.values()), {}):
                players_core = build_players_core(players_core)
        if players_core is None:
            if os.path.exists(PLAYERS_CACHE_PATH) and _players_cache_is_fresh(PLAYERS_CACHE_PATH, PLAYERS_CACHE_TTL_HR):
                with open(PLAYERS_CACHE_PATH, "r", encoding="utf-8") as f:
                    players_core = build_players_core(json.load(f))
            else:
                players_core = build_players_core(await fetch_players_from_api(client))
        users, rosters = await asyncio.gather(get_league_users(client, league_id), get_league_rosters(client, league_id))
        teams = enrich_rosters(rosters, index_users(users), players_core)
        snapshot = {"fetched_at": datetime.now(timezone.utc).isoformat(),
                    "user": {k: user.get(k) for k in ("user_id", "username", "display_name")},
                    "season": season,
                    "league": {"league_id": league_id, "name": target.get("name"), "status": target.get("status"),
                               "scoring_settings": target.get("scoring_settings"), "roster_positions": target.get("roster_positions")},
                    "teams": teams}
        if os.getenv("S3_BUCKET"):
            base = os.getenv('ROSTER_S3_PREFIX', 'sleeper/rosters')
            ts = datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')
            # By league (existing layout)
            dated_key = f"{base}/{season}/{league_id}/{ts}.json"
            latest_key = f"{base}/{season}/{league_id}/latest.json"
            snapshot["s3_uri"] = s3_put_json(snapshot, dated_key)
            snapshot["s3_latest_uri"] = s3_put_json(snapshot, latest_key)
            # By user (preferred stable layout by username)
            uname = (snapshot.get("user") or {}).get("username") or username or "unknown"
            safe_uname = "".join(ch for ch in uname.lower() if ch.isalnum() or ch in ("-", "_", ".")) or "unknown"
            user_base = f"{base}/by_user/{safe_uname}/{season}/{league_id}"
            stable_key = f"{user_base}/roster.json"
            snapshot["s3_user_stable_uri"] = s3_put_json(snapshot, stable_key)
            # Back-compat: also write old by_user layout (user_id based)
            old_uid = (snapshot.get("user") or {}).get("user_id") or user_id or "unknown"
            old_user_base = f"{base}/by_user/{old_uid}/{season}/{league_id}"
            u_dated_key = f"{old_user_base}/{ts}.json"
            u_latest_key = f"{old_user_base}/latest.json"
            snapshot["s3_user_uri"] = s3_put_json(snapshot, u_dated_key)
            snapshot["s3_user_latest_uri"] = s3_put_json(snapshot, u_latest_key)
        return snapshot

def make_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Sleeper decoupled syncs")
    sub = p.add_subparsers(dest="cmd", required=True)
    sp = sub.add_parser("players-sync", help="Fetch players map and publish to S3/local")
    sp.add_argument("--out", help="Write full players map to local file as well")
    sr = sub.add_parser("roster-sync", help="Fetch a league's rosters")
    sr.add_argument("--username", required=False, help="Sleeper username (alternative to --user-id)")
    sr.add_argument("--user-id", required=False, help="Sleeper user_id (alternative to --username)")
    sr.add_argument("--season", type=int, default=DEFAULT_SEASON)
    sr.add_argument("--league-id", help="Target league_id (preferred if known)")
    sr.add_argument("--league-name", help="Target league name (if id unknown)")
    sr.add_argument("--out", help="Write roster snapshot to local file")
    return p

def main(argv: Optional[List[str]] = None) -> int:
    args = make_parser().parse_args(argv)
    if args.cmd == "players-sync":
        res = asyncio.run(players_sync(publish_to_s3=True, out_local=getattr(args, "out", None))); print(json.dumps(res, indent=2)); return 0
    if args.cmd == "roster-sync":
        snap = asyncio.run(roster_sync(
            getattr(args, "username", None),
            args.season,
            getattr(args, "league_id", None),
            user_id=getattr(args, "user_id", None),
            league_name=getattr(args, "league_name", None),
        ))
        text = json.dumps(snap, indent=2, ensure_ascii=False); print(text)
        if getattr(args, "out", None):
            with open(args.out, "w", encoding="utf-8") as f: f.write(text)
            print(f"[ok] wrote {args.out}")
        return 0
    return 2

def players_lambda_handler(event, context):
    res = asyncio.run(players_sync(publish_to_s3=True, out_local=event.get("out"))); return {"statusCode": 200, "body": json.dumps(res)}
def roster_lambda_handler(event, context):
    username = event.get("username"); season = int(event.get("season", DEFAULT_SEASON)); league_id = event.get("league_id")
    user_id = event.get("user_id"); league_name = event.get("league_name")
    snap = asyncio.run(roster_sync(username, season, league_id, user_id=user_id, league_name=league_name)); return {"statusCode": 200, "body": json.dumps(snap)}

def roster_scheduler_handler(event, context):
    key = os.getenv("ROSTER_TARGETS_KEY", "sleeper/config/roster_targets.json")
    targets = s3_get_json(key)
    if not isinstance(targets, list):
        msg = f"No valid targets list at s3://{os.getenv('S3_BUCKET')}/{key}"
        return {"statusCode": 400, "body": json.dumps({"error": msg, "hint": "Upload a JSON array of {username|user_id, league_id|league_name, season?}"})}
    results: List[Dict[str, Any]] = []
    errors = 0
    for t in targets:
        try:
            username = t.get("username")
            user_id = t.get("user_id")
            league_id = t.get("league_id")
            league_name = t.get("league_name")
            season = int(t.get("season") or DEFAULT_SEASON)
            snap = asyncio.run(roster_sync(username, season, league_id, user_id=user_id, league_name=league_name))
            results.append({
                "ok": True,
                "season": season,
                "league_id": (snap.get("league") or {}).get("league_id") or league_id,
                "s3_latest_uri": snap.get("s3_latest_uri"),
            })
        except Exception as e:
            errors += 1
            results.append({"ok": False, "error": str(e), "target": t})
    return {"statusCode": 200, "body": json.dumps({"count": len(targets), "errors": errors, "results": results})}

if __name__ == "__main__":
    raise SystemExit(main())
