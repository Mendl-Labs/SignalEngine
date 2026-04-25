#!/usr/bin/env node
/**
 * GitHub Actions Reporter
 * Formats E2E results for GitHub Actions consumption
 */

class GitHubActionsReporter {
  constructor() {
    this.output = [];
  }

  /**
   * Format full test run result
   */
  format(result) {
    const lines = [];
    
    // Overall status
    if (result.overall === 'pass') {
      lines.push('::notice::✅ All E2E tests passed');
    } else {
      lines.push('::error::❌ E2E tests failed');
    }
    
    lines.push('');
    lines.push('## Test Summary');
    lines.push(`- **Namespace:** ${result.namespace}`);
    lines.push(`- **Image Tag:** ${result.image_tag}`);
    lines.push(`- **Duration:** ${this.formatDuration(result.duration_ms)}`);
    lines.push(`- **Overall:** ${result.overall.toUpperCase()}`);
    lines.push('');
    
    // Suite breakdown
    lines.push('## Suite Results');
    for (const suite of result.suites) {
      const icon = suite.status === 'pass' ? '✅' : '❌';
      lines.push(`${icon} **${suite.name}**: ${suite.passed}/${suite.tests} passed (${this.formatDuration(suite.duration_ms)})`);
      
      // GitHub Actions grouping for failed tests
      if (suite.status === 'fail' && suite.tests_detail) {
        lines.push(`::group::${suite.name} failures`);
        for (const test of suite.tests_detail) {
          if (test.status === 'fail') {
            lines.push(`::error::${test.name}: ${test.error || 'Unknown error'}`);
          }
        }
        lines.push('::endgroup::');
      }
    }
    
    lines.push('');
    lines.push('## JSON Output');
    lines.push('```json');
    lines.push(JSON.stringify(result, null, 2));
    lines.push('```');
    
    return lines.join('\n');
  }

  /**
   * Format just the JSON result (for programmatic consumption)
   */
  formatJson(result) {
    return JSON.stringify(result, null, 2);
  }

  /**
   * Format for GitHub Actions step summary (markdown)
   */
  formatSummary(result) {
    const lines = [];
    
    lines.push('# QA E2E Test Results');
    lines.push('');
    lines.push(`| Metric | Value |`);
    lines.push(`|--------|-------|`);
    lines.push(`| Namespace | \`${result.namespace}\` |`);
    lines.push(`| Image Tag | \`${result.image_tag}\` |`);
    lines.push(`| Duration | ${this.formatDuration(result.duration_ms)} |`);
    lines.push(`| Status | ${result.overall === 'pass' ? '✅ PASS' : '❌ FAIL'} |`);
    lines.push('');
    
    lines.push('## Suite Breakdown');
    lines.push('');
    lines.push(`| Suite | Tests | Passed | Failed | Status |`);
    lines.push(`|-------|-------|--------|--------|--------|`);
    
    for (const suite of result.suites) {
      const status = suite.status === 'pass' ? '✅' : '❌';
      lines.push(`| ${suite.name} | ${suite.tests} | ${suite.passed} | ${suite.failed} | ${status} |`);
    }
    
    lines.push('');
    
    // Failed tests details
    const failedTests = result.suites.flatMap(s => 
      (s.tests_detail || []).filter(t => t.status === 'fail').map(t => ({
        suite: s.name,
        test: t.name,
        error: t.error
      }))
    );
    
    if (failedTests.length > 0) {
      lines.push('## Failed Tests');
      lines.push('');
      for (const fail of failedTests) {
        lines.push(`### ${fail.suite} > ${fail.test}`);
        lines.push(`\`\`\``);
        lines.push(fail.error);
        lines.push(`\`\`\``);
        lines.push('');
      }
    }
    
    return lines.join('\n');
  }

  /**
   * Set GitHub Actions output variables
   */
  setOutputs(result) {
    const outputs = [];
    
    // GitHub Actions output syntax
    outputs.push(`overall=${result.overall}`);
    outputs.push(`duration_ms=${result.duration_ms}`);
    outputs.push(`total_tests=${result.suites.reduce((a, s) => a + s.tests, 0)}`);
    outputs.push(`passed=${result.suites.reduce((a, s) => a + s.passed, 0)}`);
    outputs.push(`failed=${result.suites.reduce((a, s) => a + s.failed, 0)}`);
    
    return outputs.map(o => `echo "${o}" >> $GITHUB_OUTPUT`).join('\n');
  }

  formatDuration(ms) {
    if (ms < 1000) return `${ms}ms`;
    if (ms < 60000) return `${(ms / 1000).toFixed(1)}s`;
    const minutes = Math.floor(ms / 60000);
    const seconds = ((ms % 60000) / 1000).toFixed(0);
    return `${minutes}m ${seconds}s`;
  }
}

module.exports = GitHubActionsReporter;
