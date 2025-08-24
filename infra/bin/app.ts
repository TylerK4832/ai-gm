#!/usr/bin/env node
import 'source-map-support/register';
import * as cdk from 'aws-cdk-lib';
import { AiGmSleeperStack } from '../lib/ai-gm-stack';

const app = new cdk.App();

new AiGmSleeperStack(app, 'AiGmSleeperStack', {
  env: {
    account: process.env.CDK_DEFAULT_ACCOUNT,
    region: process.env.CDK_DEFAULT_REGION || 'us-west-2',
  },
});
