# AI GM – AWS CDK Starter

This zip contains a ready-to-deploy AWS CDK stack and the decoupled Sleeper sync (players + roster).

## Structure
infra/
  bin/app.ts
  lib/ai-gm-stack.ts
  lambda/
    sleeper_sync.py
    requirements.txt
  package.json
  tsconfig.json
  cdk.json

## Quickstart
cd infra
npm i
npx cdk bootstrap
npx cdk deploy -c playersPrefix=sleeper/players -c rosterPrefix=sleeper/rosters

Optional: if you want an automatic roster sync schedule for a specific account/league, pass:

npx cdk deploy -c username=YOUR_SLEEPER_USERNAME -c season=2025 -c rosterEvery=10

Notes:
- When no `username` is provided, the Roster schedule is omitted. Invoke the `RosterSyncFn` on-demand and pass `username` or `user_id` and `league_id`/`league_name` in the event payload.
- Players are written to `s3://$BUCKET/sleeper/players/...` and rosters to:
  - by league: `s3://$BUCKET/sleeper/rosters/{season}/{league_id}/...` (dated snapshots + `latest.json`)
  - by user (preferred): `s3://$BUCKET/sleeper/rosters/by_user/{username}/{season}/{league_id}/roster.json`
    - Back-compat: the old user-id layout still updates dated snapshots and `latest.json` under `by_user/{user_id}/{season}/{league_id}/`.

Then invoke Players lambda once to seed players_core.json, and invoke Roster lambda with an event like `{ "username": "YOUR_USER", "season": 2025, "league_id": "LEAGUE_ID" }` (or use `user_id` / `league_name`).
