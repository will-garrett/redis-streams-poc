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

## Validation Testing

### Consumer Output Analysis

The application includes a comprehensive test script to validate message processing and identify potential issues:

```bash
# Run the validation test
python tests/outputs.py

# With verbose output
python tests/outputs.py --verbose

# With custom output directory
python tests/outputs.py --output-dir consumer_output
```

#### What the Test Validates:

- **Duplicate Detection**: Identifies packages processed multiple times
- **Missing Packages**: Finds gaps in sequential package numbering  
- **Consumer Load Distribution**: Analyzes processing distribution across consumers
- **Processing Statistics**: Provides comprehensive metrics and analysis

#### Test Output Example:

```
============================================================
CONSUMER OUTPUT ANALYSIS RESULTS
============================================================

📊 SUMMARY STATISTICS:
   Total packages processed: 1140
   Unique packages: 1130
   Duplicate packages: 10
   Package range: 1 - 1130

👥 CONSUMER BREAKDOWN:
   Consumer 1a86f4c0: 378 packages
   Consumer b233a67c: 379 packages
   Consumer bdfbcd64: 378 packages

⚠️  DUPLICATE PACKAGES FOUND:
   5 packages were processed multiple times:
     Package 567: processed 2 times by consumers ['1a86f4c0', 'b233a67c']
     Package 892: processed 2 times by consumers ['b233a67c', 'bdfbcd64']
     ...

✅ NO MISSING PACKAGES
```
> p.s. this shouldn't happen, here is a real output
```
Found 3 consumer output files
Consumer 1a86f4c0: 2996 packages
Consumer b233a67c: 2997 packages
Consumer bdfbcd64: 2997 packages

============================================================
CONSUMER OUTPUT ANALYSIS RESULTS
============================================================

📊 SUMMARY STATISTICS:
   Total packages processed: 8990
   Unique packages: 8990
   Duplicate packages: 0
   Package range: 1 - 8990

👥 CONSUMER BREAKDOWN:
   Consumer 1a86f4c0: 2996 packages
   Consumer b233a67c: 2997 packages
   Consumer bdfbcd64: 2997 packages

✅ NO DUPLICATES FOUND

✅ NO MISSING PACKAGES

============================================================

✅ NO ISSUES DETECTED: All packages processed exactly once
```

#### Exit Codes:

- **0**: No issues detected (all packages processed exactly once)
- **1**: Issues detected (duplicates or missing packages found)
- **2**: Analysis failed (could not read files)

## Monitoring & Observability

### Metrics Available

- **Producer Metrics:**
  - `messages_produced_total`: Total messages produced
  - `stream_cleanup_total`: Total messages cleaned up from stream
  - `stream_length_current`: Current stream length
  - `stream_cleanup_operations_total`: Total cleanup operations

- **Consumer Metrics:**
  - `messages_processed_total`: Total messages processed per consumer
  - Rate of message processing
  - Load balancing distribution

### Traces Available

- Message production spans (`faststream-producer`)
- Message consumption spans (`faststream-consumer`)
- Stream cleanup monitoring spans
- Redis operations
- Consumer group operations

### Dashboards

The Grafana dashboard includes:
- Real-time message production rate
- Real-time message processing rate
- Message distribution across consumers
- Stream cleanup metrics
- Redis performance metrics
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

## Stream Cleanup

The producer automatically manages stream size by:
- Monitoring stream length every 10 seconds
- Trimming stream to 50 messages when it exceeds 100 messages
- Tracking cleanup metrics for monitoring
- Preventing infinite stream growth

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

# Analyze consumer outputs for issues
python tests/outputs.py
```

### Validate Message Processing
```bash
# Run comprehensive validation
python tests/outputs.py --verbose

# Check for specific issues
python tests/outputs.py | grep -E "(DUPLICATE|MISSING|ISSUE)"
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