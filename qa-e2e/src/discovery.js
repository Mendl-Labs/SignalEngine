#!/usr/bin/env node
/**
 * QA-E2E Discovery Module
 * Discovers k8s resources in target namespace
 */

const { execSync } = require('child_process');

class Discovery {
  constructor(namespace) {
    this.namespace = namespace;
    this.resources = {};
  }

  /**
   * Run full discovery
   */
  async discover() {
    this.resources = {
      namespace: this.namespace,
      pods: await this.getPods(),
      services: await this.getServices(),
      deployments: await this.getDeployments(),
      secrets: await this.getSecrets(),
      timestamp: new Date().toISOString()
    };
    return this.resources;
  }

  /**
   * Get all pods in namespace
   */
  async getPods() {
    try {
      const output = execSync(
        `kubectl get pods -n ${this.namespace} -o json`,
        { encoding: 'utf-8', timeout: 30000 }
      );
      const data = JSON.parse(output);
      
      // Skip if no pods found
      if (!data.items || data.items.length === 0) {
        return [];
      }
      
      return data.items.map(pod => ({
        name: pod.metadata.name,
        status: pod.status.phase,
        ready: pod.status.containerStatuses?.every(c => c.ready) || false,
        restarts: pod.status.containerStatuses?.[0]?.restartCount || 0,
        image: pod.spec.containers?.[0]?.image || 'unknown',
        ip: pod.status.podIP,
        node: pod.spec.nodeName
      }));
    } catch (err) {
      throw new Error(`Failed to get pods: ${err.message}`);
    }
  }

  /**
   * Get all services in namespace
   */
  async getServices() {
    try {
      const output = execSync(
        `kubectl get svc -n ${this.namespace} -o json`,
        { encoding: 'utf-8', timeout: 30000 }
      );
      const data = JSON.parse(output);
      
      // Return empty array if no services found
      if (!data.items || data.items.length === 0) {
        return [];
      }
      
      return data.items.map(svc => ({
        name: svc.metadata.name,
        type: svc.spec.type,
        clusterIP: svc.spec.clusterIP,
        ports: svc.spec.ports?.map(p => ({
          name: p.name,
          port: p.port,
          targetPort: p.targetPort,
          protocol: p.protocol
        })) || [],
        selector: svc.spec.selector
      }));
    } catch (err) {
      throw new Error(`Failed to get services: ${err.message}`);
    }
  }

  /**
   * Get all deployments in namespace
   */
  async getDeployments() {
    try {
      const output = execSync(
        `kubectl get deployments -n ${this.namespace} -o json`,
        { encoding: 'utf-8', timeout: 30000 }
      );
      const data = JSON.parse(output);
      
      // Return empty array if no deployments found
      if (!data.items || data.items.length === 0) {
        return [];
      }
      
      return data.items.map(dep => ({
        name: dep.metadata.name,
        replicas: dep.spec.replicas,
        available: dep.status.availableReplicas || 0,
        ready: dep.status.readyReplicas || 0,
        updated: dep.status.updatedReplicas || 0,
        image: dep.spec.template?.spec?.containers?.[0]?.image || 'unknown'
      }));
    } catch (err) {
      throw new Error(`Failed to get deployments: ${err.message}`);
    }
  }

  /**
   * Get secret names (not values)
   */
  async getSecrets() {
    try {
      const output = execSync(
        `kubectl get secrets -n ${this.namespace} -o json`,
        { encoding: 'utf-8', timeout: 30000 }
      );
      const data = JSON.parse(output);
      
      // Return empty array if no secrets found
      if (!data.items || data.items.length === 0) {
        return [];
      }
      
      return data.items.map(sec => ({
        name: sec.metadata.name,
        type: sec.type,
        keys: Object.keys(sec.data || {})
      }));
    } catch (err) {
      throw new Error(`Failed to get secrets: ${err.message}`);
    }
  }

  /**
   * Get secret value
   */
  async getSecretValue(secretName, key) {
    try {
      const output = execSync(
        `kubectl -n ${this.namespace} get secret ${secretName} ` +
        `-o jsonpath='{.data.${key}}' | base64 -d`,
        { encoding: 'utf-8', timeout: 30000 }
      );
      return output.trim();
    } catch (err) {
      throw new Error(`Failed to get secret ${secretName}.${key}: ${err.message}`);
    }
  }

  /**
   * Get service ClusterIP
   */
  getServiceClusterIP(serviceName) {
    const svc = this.resources.services?.find(s => s.name === serviceName);
    return svc?.clusterIP;
  }

  /**
   * Get pod by label selector
   */
  async getPodBySelector(selector) {
    const selectorStr = Object.entries(selector)
      .map(([k, v]) => `${k}=${v}`)
      .join(',');
    
    try {
      const output = execSync(
        `kubectl get pods -n ${this.namespace} -l ${selectorStr} -o json`,
        { encoding: 'utf-8', timeout: 30000 }
      );
      const data = JSON.parse(output);
      
      // Return null if no pods matched
      if (!data.items || data.items.length === 0) {
        return null;
      }
      
      const pod = data.items[0];
      return {
        name: pod.metadata.name,
        status: pod.status.phase,
        ready: pod.status.containerStatuses?.every(c => c.ready) || false
      };
    } catch (err) {
      return null;
    }
  }

  /**
   * Wait for all pods to be ready
   */
  async waitForReady(timeoutSeconds = 300) {
    const startTime = Date.now();
    const timeoutMs = timeoutSeconds * 1000;

    while (Date.now() - startTime < timeoutMs) {
      const pods = await this.getPods();
      const allReady = pods.every(p => p.ready && p.status === 'Running');
      
      if (allReady && pods.length > 0) {
        return { ready: true, pods };
      }

      // Check for failures
      const failedPods = pods.filter(p => 
        p.status === 'Error' || 
        p.status === 'CrashLoopBackOff' ||
        p.restarts > 5
      );
      
      if (failedPods.length > 0) {
        return { ready: false, failedPods, pods };
      }

      // Wait 5 seconds before retry
      await new Promise(r => setTimeout(r, 5000));
    }

    return { ready: false, timeout: true };
  }
}

module.exports = Discovery;
