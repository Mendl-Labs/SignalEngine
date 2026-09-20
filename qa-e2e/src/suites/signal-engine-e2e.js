#!/usr/bin/env node
/**
 * SignalEngine E2E Test Suite
 *
 * SignalEngine doesn't expose a routable HTTP API; this suite focuses on
 * infrastructure-level validations: pod readiness, image tag correctness,
 * helm-managed labels, and log-pattern checks for fatal errors.
 */

const { execSync } = require('child_process');
const Discovery = require('../discovery');

const APP_LABEL = 'app.kubernetes.io/name=signal-engine-helm';
const IMAGE_PREFIX = 'ghcr.io/mendl-labs/signal-engine';

class SignalEngineE2E {
  constructor(namespace, imageTag, options = {}) {
    this.namespace = namespace;
    this.imageTag = imageTag;
    this.failFast = options.failFast !== false;
    this.timeout = options.timeout || 300;
    this.discovery = new Discovery(namespace);
    this.results = [];
  }

  async run() {
    const startTime = Date.now();
    const suiteName = 'signal-engine-e2e';

    try {
      await this.discovery.discover();

      await this.testInfraHealth();
      await this.testImageTags();
      await this.testStartupLogs();
      await this.testDeploymentLabels();
    } catch (err) {
      if (this.failFast) {
        return this.formatResult(suiteName, startTime, err);
      }
    }

    return this.formatResult(suiteName, startTime);
  }

  async testInfraHealth() {
    const testName = 'infra-health';
    const startTime = Date.now();

    try {
      const readyCheck = await this.discovery.waitForReady(this.timeout);

      if (!readyCheck.ready) {
        if (readyCheck.timeout) {
          throw new Error(`Timeout waiting for pods to be ready after ${this.timeout}s`);
        }
        if (readyCheck.failedPods) {
          const names = readyCheck.failedPods.map(p => p.name).join(', ');
          throw new Error(`Pods in failed state: ${names}`);
        }
      }

      this.addResult(testName, 'pass', Date.now() - startTime, {
        podsReady: readyCheck.pods?.length || 0
      });
    } catch (err) {
      this.addResult(testName, 'fail', Date.now() - startTime, {}, err.message);
      if (this.failFast) throw err;
    }
  }

  async testImageTags() {
    const testName = 'image-tag-validation';
    const startTime = Date.now();

    try {
      const pods = await this.discovery.getPods();
      if (pods.length === 0) {
        this.addResult(testName, 'skipped', Date.now() - startTime, {
          reason: 'no pods matched selector'
        });
        return;
      }

      const mismatched = [];
      let imagesChecked = 0;

      for (const pod of pods) {
        if (pod.terminating) {
          continue;
        }
        if (!pod.image.startsWith(IMAGE_PREFIX)) {
          continue;
        }
        imagesChecked++;
        const tag = pod.image.split(':').pop();
        if (tag !== this.imageTag) {
          mismatched.push(`${pod.name}: ${tag} (expected ${this.imageTag})`);
        }
      }

      if (mismatched.length > 0) {
        throw new Error(`Image tag mismatch: ${mismatched.join('; ')}`);
      }

      this.addResult(testName, 'pass', Date.now() - startTime, {
        podsChecked: pods.length,
        imagesChecked,
        tag: this.imageTag
      });
    } catch (err) {
      this.addResult(testName, 'fail', Date.now() - startTime, {}, err.message);
      if (this.failFast) throw err;
    }
  }

  async testStartupLogs() {
    const testName = 'startup-logs';
    const startTime = Date.now();

    try {
      const logs = execSync(
        `kubectl logs -n ${this.namespace} -l ${APP_LABEL} ` +
        `--tail=300 --all-containers`,
        { encoding: 'utf-8', timeout: 30000 }
      );

      const badPatterns = /panic|FATAL/i;
      if (badPatterns.test(logs)) {
        throw new Error('Critical error pattern detected in SignalEngine logs');
      }

      this.addResult(testName, 'pass', Date.now() - startTime, {
        bytesScanned: logs.length
      });
    } catch (err) {
      this.addResult(testName, 'fail', Date.now() - startTime, {}, err.message);
      if (this.failFast) throw err;
    }
  }

  async testDeploymentLabels() {
    const testName = 'deployment-labels';
    const startTime = Date.now();

    try {
      const output = execSync(
        `kubectl get deployments -n ${this.namespace} ` +
        `-l ${APP_LABEL} -o json`,
        { encoding: 'utf-8', timeout: 30000 }
      );
      const data = JSON.parse(output);

      if (!data.items || data.items.length === 0) {
        throw new Error('No signal-engine deployments found');
      }

      const missing = [];
      const checked = [];

      for (const dep of data.items) {
        const labels = dep.metadata.labels || {};
        const depName = dep.metadata.name;

        if (labels['app.kubernetes.io/managed-by'] !== 'Helm') {
          missing.push(`${depName}: managed-by != Helm`);
        }
        if (!labels['app.kubernetes.io/instance']) {
          missing.push(`${depName}: missing instance label`);
        }
        checked.push(depName);
      }

      if (missing.length > 0) {
        throw new Error(`Label validation failed: ${missing.join('; ')}`);
      }

      this.addResult(testName, 'pass', Date.now() - startTime, {
        deploymentsChecked: checked.length,
        checked
      });
    } catch (err) {
      this.addResult(testName, 'fail', Date.now() - startTime, {}, err.message);
      if (this.failFast) throw err;
    }
  }

  // Helper methods
  addResult(name, status, durationMs, extra = {}, error = null) {
    const r = { name, status, duration_ms: durationMs, ...extra };
    if (error) r.error = error;
    this.results.push(r);
  }

  formatResult(suiteName, startTime, fatalErr = null) {
    const durationMs = Date.now() - startTime;
    const passed = this.results.filter(r => r.status === 'pass').length;
    const failed = this.results.filter(r => r.status === 'fail').length;
    const skipped = this.results.filter(r => r.status === 'skipped').length;
    const status = failed > 0 || fatalErr ? 'fail' : 'pass';

    const out = {
      name: suiteName,
      status,
      tests: this.results.length,
      passed,
      failed,
      skipped,
      duration_ms: durationMs,
      tests_detail: this.results
    };
    if (fatalErr) out.error = fatalErr.message || String(fatalErr);
    return out;
  }
}

module.exports = SignalEngineE2E;
