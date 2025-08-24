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
npx cdk deploy -c username=YOUR_SLEEPER_USERNAME -c season=2025 -c rosterEvery=10 -c playersPrefix=sleeper/players -c rosterPrefix=sleeper_sync

Then invoke Players lambda once to seed players_core.json, and invoke Roster lambda to generate your first roster snapshot.
