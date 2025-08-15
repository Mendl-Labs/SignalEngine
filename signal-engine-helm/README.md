# SignalEngine Helm Chart

This Helm chart deploys the SignalEngine ultra-low latency trading platform on Kubernetes.

## Overview

The SignalEngine is a high-frequency trading (HFT) platform built in Rust, designed for sub-microsecond signal processing and ultra-low latency order execution across multiple cryptocurrency exchanges.

## Features

- ⚡ **Ultra-Low Latency**: Sub-microsecond signal processing
- 🏗️ **High Availability**: Multi-replica deployment with auto-scaling
- 🔒 **Security Hardened**: Network policies, RBAC, pod security contexts
- 📊 **Comprehensive Monitoring**: Prometheus metrics, Grafana dashboards, alerting
- 🔄 **Multi-Environment**: Dev, QA, and Production configurations
- 💾 **Persistent Storage**: Configurable storage classes and sizes
- 🌐 **Multi-Exchange**: Binance, Coinbase, Kraken support
- 🔍 **Observability**: Distributed tracing with Jaeger

## Quick Start

### Prerequisites

- Kubernetes 1.20+
- Helm 3.2.0+
- StorageClass for persistent volumes
- (Optional) Prometheus Operator for monitoring

### Installation

```bash
# Add the Helm repository
helm repo add signal-engine https://charts.signal-engine.com
helm repo update

# Install for development
helm install signal-engine signal-engine/signal-engine-helm \
  --namespace trading \
  --create-namespace \
  --values values-dev.yaml

# Install for production
helm install signal-engine signal-engine/signal-engine-helm \
  --namespace trading-prod \
  --create-namespace \
  --values values-prod.yaml
```

### Local Development

```bash
# Clone the repository
git clone https://github.com/nwagbara-group/signal-engine.git
cd signal-engine/signal-engine-helm

# Install locally
helm install signal-engine . \
  --namespace trading-dev \
  --create-namespace \
  --values values-dev.yaml
```

## Configuration

### Basic Configuration

| Parameter | Description | Default |
|-----------|-------------|---------|
| `replicaCount` | Number of replicas | `1` |
| `image.repository` | Image repository | `signal-engine` |
| `image.tag` | Image tag | `latest` |
| `image.pullPolicy` | Image pull policy | `IfNotPresent` |

### Environment-Specific Configurations

#### Development (`values-dev.yaml`)
- Single replica
- Minimal resources
- Debug mode enabled
- Mock market data
- Relaxed security

#### QA (`values-qa.yaml`)
- 2 replicas
- Moderate resources
- Testing tools enabled
- Sandbox exchanges
- Load testing configured

#### Production (`values-prod.yaml`)
- 3+ replicas with auto-scaling
- High resource allocation
- All security features enabled
- Production exchanges
- Comprehensive monitoring

### Resource Management

```yaml
resources:
  limits:
    cpu: "4000m"      # 4 CPU cores
    memory: "8Gi"     # 8GB RAM
  requests:
    cpu: "2000m"      # 2 CPU cores guaranteed
    memory: "4Gi"     # 4GB RAM guaranteed
```

### Autoscaling

```yaml
autoscaling:
  enabled: true
  minReplicas: 2
  maxReplicas: 10
  targetCPUUtilizationPercentage: 70
  targetMemoryUtilizationPercentage: 80
  customMetrics:
    - type: Pods
      pods:
        metric:
          name: signal_processing_rate
        target:
          type: AverageValue
          averageValue: "1000"
```

### Monitoring Configuration

```yaml
monitoring:
  prometheus:
    enabled: true
    serviceMonitor:
      enabled: true
      interval: 15s
  grafana:
    enabled: true
    dashboards:
      enabled: true
  tracing:
    enabled: true
    jaeger:
      endpoint: "http://jaeger-collector:14268"
```

### Security Configuration

```yaml
securityContext:
  allowPrivilegeEscalation: false
  readOnlyRootFilesystem: true
  runAsNonRoot: true
  runAsUser: 1000
  capabilities:
    drop:
      - ALL

networkPolicy:
  enabled: true
  ingress:
    - from:
        - namespaceSelector:
            matchLabels:
              name: monitoring
```

## Deployment Examples

### Development Environment

```bash
helm install signal-engine . \
  --namespace trading-dev \
  --create-namespace \
  --set global.environment=dev \
  --set image.tag=dev \
  --set development.enabled=true \
  --values values-dev.yaml
```

### Production Environment

```bash
helm install signal-engine . \
  --namespace trading-prod \
  --create-namespace \
  --set global.environment=prod \
  --set image.tag=v1.0.0 \
  --set autoscaling.enabled=true \
  --set monitoring.prometheus.enabled=true \
  --values values-prod.yaml
```

### Upgrade Deployment

```bash
helm upgrade signal-engine . \
  --namespace trading-prod \
  --set image.tag=v1.1.0 \
  --values values-prod.yaml
```

## Monitoring and Observability

### Prometheus Metrics

The application exposes metrics on port 8081:

- `signal_processing_duration_seconds` - Signal processing latency
- `signals_processed_total` - Total signals processed
- `order_execution_errors_total` - Order execution errors
- `exchange_connection_status` - Exchange connection status

### Grafana Dashboards

Pre-configured dashboards available:
- **SignalEngine Overview** - High-level metrics and KPIs
- **Trading Performance** - Signal processing and execution metrics
- **System Resources** - CPU, memory, and network utilization
- **Exchange Connectivity** - Connection status and latency

### Alerting Rules

Built-in alerting for:
- Signal processing latency > 1ms
- Signal throughput < 1000/sec
- Exchange connection failures
- High error rates
- Resource exhaustion

## Performance Tuning

### Node Selection

```yaml
nodeSelector:
  node-type: "compute-optimized"
  network-tier: "premium"

tolerations:
  - key: "dedicated"
    operator: "Equal"
    value: "trading"
    effect: "NoSchedule"
```

### Pod Affinity

```yaml
affinity:
  podAntiAffinity:
    requiredDuringSchedulingIgnoredDuringExecution:
      - labelSelector:
          matchExpressions:
            - key: app.kubernetes.io/name
              operator: In
              values:
                - signal-engine-helm
        topologyKey: kubernetes.io/hostname
```

### DNS Optimization

```yaml
dnsPolicy: ClusterFirst
dnsConfig:
  options:
    - name: ndots
      value: "1"
    - name: edns0
```

## Troubleshooting

### Common Issues

1. **Pod Stuck in Pending**
   ```bash
   kubectl describe pod <pod-name> -n <namespace>
   # Check for resource constraints or node selection issues
   ```

2. **High Latency**
   ```bash
   # Check CPU throttling
   kubectl top pods -n <namespace>
   
   # Verify node performance
   kubectl describe node <node-name>
   ```

3. **Exchange Connection Issues**
   ```bash
   # Check network policies
   kubectl get networkpolicy -n <namespace>
   
   # Verify DNS resolution
   kubectl exec -it <pod-name> -- nslookup api.binance.com
   ```

### Debugging Commands

```bash
# View all resources
kubectl get all -l app.kubernetes.io/name=signal-engine-helm -n <namespace>

# Check configuration
kubectl get configmap signal-engine-config -o yaml -n <namespace>

# View secrets (redacted)
kubectl get secret signal-engine-secret -o yaml -n <namespace>

# Monitor real-time metrics
kubectl port-forward svc/signal-engine 8081:8081 -n <namespace>
curl http://localhost:8081/metrics

# Access debug sidecar (dev only)
kubectl exec -it deployment/signal-engine -c debug-sidecar -n <namespace> -- /bin/sh
```

## Security Considerations

### Production Security Checklist

- [ ] Enable network policies
- [ ] Configure pod security contexts
- [ ] Use read-only root filesystem
- [ ] Run as non-root user
- [ ] Enable RBAC
- [ ] Use secrets for sensitive data
- [ ] Enable TLS for ingress
- [ ] Configure resource limits
- [ ] Enable audit logging

### Secret Management

Store sensitive configuration in Kubernetes secrets:

```yaml
apiVersion: v1
kind: Secret
metadata:
  name: signal-engine-secrets
type: Opaque
stringData:
  BINANCE_API_KEY: "your-api-key"
  BINANCE_SECRET_KEY: "your-secret-key"
  DATABASE_PASSWORD: "your-db-password"
```

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make changes and test locally
4. Submit a pull request

## Support

- **Documentation**: https://docs.signal-engine.com
- **Issues**: https://github.com/nwagbara-group/signal-engine/issues
- **Email**: engineering@nwagbara-group.com
- **Discord**: https://discord.gg/signal-engine

## License

This Helm chart is licensed under the Apache 2.0 License. See [LICENSE](LICENSE) for details.
