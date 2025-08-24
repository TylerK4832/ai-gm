import * as path from 'path';
import * as cdk from 'aws-cdk-lib';
import { Construct } from 'constructs';
import * as s3 from 'aws-cdk-lib/aws-s3';
import * as iam from 'aws-cdk-lib/aws-iam';
import { Runtime, Architecture } from 'aws-cdk-lib/aws-lambda';
import { PythonFunction } from '@aws-cdk/aws-lambda-python-alpha';
import * as logs from 'aws-cdk-lib/aws-logs';
import * as scheduler from 'aws-cdk-lib/aws-scheduler';

export class AiGmSleeperStack extends cdk.Stack {
  constructor(scope: Construct, id: string, props?: cdk.StackProps) {
    super(scope, id, props);

    // Optional context for scheduled roster syncs; functions can be invoked with parameters for any user/league
    const username = this.node.tryGetContext('username') as string | undefined;
    const season = Number(this.node.tryGetContext('season') ?? new Date().getFullYear());
    const leagueId = (this.node.tryGetContext('leagueId') as string) || undefined;
    const rosterEvery = Number(this.node.tryGetContext('rosterEvery') ?? 10);
    const playersPrefix = (this.node.tryGetContext('playersPrefix') as string) || 'sleeper/players';
    const rosterPrefix = (this.node.tryGetContext('rosterPrefix') as string) || 'sleeper/rosters';

    const bucket = new s3.Bucket(this, 'SleeperDataBucket', {
      versioned: true,
      blockPublicAccess: s3.BlockPublicAccess.BLOCK_ALL,
      encryption: s3.BucketEncryption.S3_MANAGED,
      lifecycleRules: [{ id: 'expire-old-versions', noncurrentVersionExpiration: cdk.Duration.days(90) }],
      removalPolicy: cdk.RemovalPolicy.RETAIN,
      autoDeleteObjects: false,
    });

    const lambdaCodePath = path.join(__dirname, '..', 'lambda');
    const baseEnv: Record<string, string> = {
      S3_BUCKET: bucket.bucketName,
      // AWS_REGION: cdk.Stack.of(this).region,
      PLAYERS_S3_PREFIX: playersPrefix,
      ROSTER_S3_PREFIX: rosterPrefix,
      PLAYERS_CACHE_PATH: "/tmp/players_nfl.json",
      ROSTER_TARGETS_KEY: "sleeper/config/roster_targets.json",
    };

    const playersFn = new PythonFunction(this, 'PlayersSyncFn', {
      entry: lambdaCodePath,
      index: 'sleeper_sync.py',
      handler: 'players_lambda_handler',
      runtime: Runtime.PYTHON_3_12,
      architecture: Architecture.ARM_64,
      memorySize: 512,
      timeout: cdk.Duration.seconds(120),
      environment: baseEnv,
      logRetention: logs.RetentionDays.ONE_WEEK,
    });

    const rosterFn = new PythonFunction(this, 'RosterSyncFn', {
      entry: lambdaCodePath,
      index: 'sleeper_sync.py',
      handler: 'roster_lambda_handler',
      runtime: Runtime.PYTHON_3_12,
      architecture: Architecture.ARM_64,
      memorySize: 512,
      timeout: cdk.Duration.seconds(60),
      environment: { ...baseEnv, USE_S3_PLAYERS: '1' },
      logRetention: logs.RetentionDays.ONE_WEEK,
    });

    bucket.grantReadWrite(playersFn);
    bucket.grantReadWrite(rosterFn);

    // Orchestrator function to read S3 targets list and invoke roster syncs hourly
    const rosterOrchestratorFn = new PythonFunction(this, 'RosterOrchestratorFn', {
      entry: lambdaCodePath,
      index: 'sleeper_sync.py',
      handler: 'roster_scheduler_handler',
      runtime: Runtime.PYTHON_3_12,
      architecture: Architecture.ARM_64,
      memorySize: 512,
      timeout: cdk.Duration.seconds(240),
      environment: { ...baseEnv, USE_S3_PLAYERS: '1' },
      logRetention: logs.RetentionDays.ONE_WEEK,
    });
    bucket.grantReadWrite(rosterOrchestratorFn);

    const playersTargetRole = new iam.Role(this, 'PlayersSchedulerRole', {
      assumedBy: new iam.ServicePrincipal('scheduler.amazonaws.com'),
    });
    playersFn.grantInvoke(playersTargetRole);

    new scheduler.CfnSchedule(this, 'PlayersDailySchedule', {
      flexibleTimeWindow: { mode: 'OFF' },
      scheduleExpressionTimezone: 'America/Los_Angeles',
      scheduleExpression: 'cron(5 6 * * ? *)',
      target: { arn: playersFn.functionArn, roleArn: playersTargetRole.roleArn, input: JSON.stringify({}) },
      description: 'Daily Sleeper players map sync (timezone aware)'
    });

    const rosterTargetRole = new iam.Role(this, 'RosterSchedulerRole', {
      assumedBy: new iam.ServicePrincipal('scheduler.amazonaws.com'),
    });
    rosterFn.grantInvoke(rosterTargetRole);

    // Create the roster schedule only if a username is provided; otherwise, invoke the function on-demand with parameters.
    if (username) {
      new scheduler.CfnSchedule(this, 'RosterEveryNMinutes', {
        flexibleTimeWindow: { mode: 'OFF' },
        scheduleExpression: `rate(${rosterEvery} minutes)`,
        target: { arn: rosterFn.functionArn, roleArn: rosterTargetRole.roleArn, input: JSON.stringify({ username, season, league_id: leagueId }) },
        description: `Roster sync every ${rosterEvery} minutes`,
      });
    }

    // Hourly schedule that drives orchestrator across all configured targets
    const rosterBatchRole = new iam.Role(this, 'RosterBatchSchedulerRole', {
      assumedBy: new iam.ServicePrincipal('scheduler.amazonaws.com'),
    });
    rosterOrchestratorFn.grantInvoke(rosterBatchRole);
    new scheduler.CfnSchedule(this, 'RosterHourlySchedule', {
      flexibleTimeWindow: { mode: 'OFF' },
      scheduleExpression: 'cron(0 * * * ? *)',
      target: { arn: rosterOrchestratorFn.functionArn, roleArn: rosterBatchRole.roleArn, input: JSON.stringify({}) },
      description: 'Hourly roster sync for configured targets',
    });

    new cdk.CfnOutput(this, 'BucketName', { value: bucket.bucketName });
    new cdk.CfnOutput(this, 'PlayersFnName', { value: playersFn.functionName });
    new cdk.CfnOutput(this, 'RosterFnName', { value: rosterFn.functionName });
    new cdk.CfnOutput(this, 'RosterOrchestratorFnName', { value: rosterOrchestratorFn.functionName });
  }
}
