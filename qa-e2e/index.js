#!/usr/bin/env node
/**
 * SignalEngine QA-E2E Agent Entry Point
 */

const SignalEngineE2E = require('./src/suites/signal-engine-e2e');
const GitHubActionsReporter = require('./src/reporters/github-actions');

async function main() {
  const startTime = Date.now();

  const namespace = process.env.TARGET_NAMESPACE;
  const imageTag = process.env.IMAGE_TAG;
  const failFast = process.env.FAIL_FAST !== 'false';
  const timeout = parseInt(process.env.READY_TIMEOUT_SECONDS || '300', 10);

  if (!namespace) {
    console.error('ERROR: TARGET_NAMESPACE is required');
    process.exit(1);
  }
  if (!imageTag) {
    console.error('ERROR: IMAGE_TAG is required');
    process.exit(1);
  }

  console.log('SignalEngine QA-E2E Agent Starting');
  console.log(`  Namespace: ${namespace}`);
  console.log(`  Image Tag: ${imageTag}`);
  console.log(`  Fail Fast: ${failFast}`);
  console.log(`  Timeout: ${timeout}s`);
  console.log('');

  const results = {
    overall: 'pass',
    duration_ms: 0,
    namespace,
    image_tag: imageTag,
    timestamp: new Date().toISOString(),
    suites: []
  };

  try {
    const suite = new SignalEngineE2E(namespace, imageTag, { failFast, timeout });
    const result = await suite.run();
    results.suites.push(result);

    const anyFailed = results.suites.some(s => s.status === 'fail');
    results.overall = anyFailed ? 'fail' : 'pass';
    results.duration_ms = Date.now() - startTime;
  } catch (err) {
    results.overall = 'fail';
    results.error = err.message;
    results.duration_ms = Date.now() - startTime;
  }

  const reporter = new GitHubActionsReporter();
  console.log('\n' + '='.repeat(60));
  console.log(reporter.format(results));
  console.log('='.repeat(60) + '\n');

  console.log('JSON_RESULT_START');
  console.log(reporter.formatJson(results));
  console.log('JSON_RESULT_END');

  process.exit(results.overall === 'pass' ? 0 : 1);
}

process.on('unhandledRejection', (err) => {
  console.error('Unhandled rejection:', err);
  process.exit(1);
});

main().catch((err) => {
  console.error('Fatal:', err);
  process.exit(1);
});
