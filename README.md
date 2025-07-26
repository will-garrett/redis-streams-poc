# FastStream Redis Microservice with OpenTelemetry Monitoring

A microservice application using Python FastStream and Redis with comprehensive monitoring and observability.

## Architecture

- **Producer**: Generates sequential messages and sends them to Redis Streams
- **Consumer**: 3 scaled workers that consume messages using Redis Consumer Groups for load balancing
- **Redis**: Message broker using Redis Streams
- **Redis Commander**: Web UI for Redis management
- **OpenTelemetry Collector**: Collects traces and metrics
- **Prometheus**: Metrics storage and query engine
- **Jaeger**: Distributed tracing UI
- **Grafana**: Metrics dashboard

## Services

| Service | Port | Description |
|---------|------|-------------|
| Redis | 6379 | Message broker |
| Redis Commander | 8081 | Redis web UI |
| Producer | 8000 | Message producer API |
| Consumer | - | Message consumers (3 replicas) |
| OpenTelemetry Collector | 4317/4318/8889 | Telemetry collection |
| Prometheus | 9090 | Metrics storage and API |
| Jaeger | 16686 | Tracing UI |
| Grafana | 3000 | Metrics dashboard |

## Quick Start

1. **Start all services:**
   ```bash
   docker-compose up -d
   ```

2. **Monitor the application:**
   - **Redis Commander**: http://localhost:8081
   - **Grafana**: http://localhost:3000 (admin/admin)
   - **Jaeger**: http://localhost:16686

3. **View consumer output files:**
   ```bash
   ls -la consumer_output/
   ```

## Monitoring & Observability

### Metrics Available

- **Producer Metrics:**
  - `messages_produced_total`: Total messages produced
  - Rate of message production

- **Consumer Metrics:**
  - `messages_processed_total`: Total messages processed per consumer
  - Rate of message processing
  - Load balancing distribution

### Traces Available

- Message production spans
- Message consumption spans
- Redis operations
- Consumer group operations

### Dashboards

The Grafana dashboard includes:
- Real-time message production rate
- Real-time message processing rate
- Message distribution across consumers
- Historical trends

## Message Format

```json
{
  "timestamp_producer": 1753553379,
  "payload": {
    "package": 1
  }
}
```

## Load Balancing

The application uses Redis Consumer Groups to ensure:
- Messages are distributed equally between consumers
- No message duplication
- Automatic failover
- Message acknowledgment for reliability

## Troubleshooting

### Check Service Status
```bash
docker-compose ps
```

### View Logs
```bash
# All services
docker-compose logs

# Specific service
docker-compose logs producer
docker-compose logs consumer
```

### Check Redis Stream
```bash
docker-compose exec redis redis-cli XINFO STREAM number_stream
```

### Check Consumer Group
```bash
docker-compose exec redis redis-cli XINFO GROUPS number_stream
```

### Access Consumer Output
```bash
# View consumer output files
cat consumer_output/consumer_*.txt
```

## Development

### Rebuild Services
```bash
docker-compose build --no-cache
docker-compose up -d
```

### Scale Consumers
```bash
docker-compose up -d --scale consumer=5
```

### Stop All Services
```bash
docker-compose down
``` 

### Stop All Services and clear volumes
```bash
docker-compose down -v
``` 