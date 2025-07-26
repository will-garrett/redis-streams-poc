import asyncio
import time
import os
from faststream import FastStream
from faststream.redis import RedisBroker
import json

# OpenTelemetry imports
from opentelemetry import trace, metrics
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.exporter.otlp.proto.grpc.metric_exporter import OTLPMetricExporter
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
from opentelemetry.instrumentation.asyncio import AsyncioInstrumentor

# Set up OpenTelemetry
def setup_telemetry():
    # Initialize tracing
    trace.set_tracer_provider(TracerProvider())
    tracer = trace.get_tracer(__name__)
    
    # Initialize metrics
    metric_reader = PeriodicExportingMetricReader(
        OTLPMetricExporter(endpoint="http://otel-collector:4317")
    )
    metrics.set_meter_provider(MeterProvider(metric_readers=[metric_reader]))
    meter = metrics.get_meter(__name__)
    
    # Add span processor
    otlp_exporter = OTLPSpanExporter(endpoint="http://otel-collector:4317")
    span_processor = BatchSpanProcessor(otlp_exporter)
    trace.get_tracer_provider().add_span_processor(span_processor)
    
    # Instrument asyncio
    AsyncioInstrumentor().instrument()
    
    return tracer, meter

print(f"[{time.time()}] Starting producer application...")

# Initialize FastStream app and Redis broker
redis_url = os.getenv("REDIS_URL", "redis://redis:6379")
print(f"[{time.time()}] Connecting to Redis at: {redis_url}")

# Try using explicit connection parameters
broker = RedisBroker(
    host="redis",
    port=6379,
    db=0
)
app = FastStream(broker)

# Set up telemetry
tracer, meter = setup_telemetry()
messages_produced_counter = meter.create_counter(
    name="messages_produced_total",
    description="Total number of messages produced"
)

async def produce_messages():
    """Produce messages continuously"""
    counter = 1
    print(f"[{time.time()}] Starting message production...")
    
    # Wait for broker to be ready
    try:
        print(f"[{time.time()}] Waiting for broker to be ready...")
        await broker.connect()
        print(f"[{time.time()}] Broker connected successfully")
    except Exception as e:
        print(f"[{time.time()}] Broker connection failed: {e}")
        raise
    
    while True:
        with tracer.start_as_current_span("produce_message") as span:
            # Create message with the specified JSON structure
            message = {
                "timestamp_producer": int(time.time()), 
                "payload": {"package": counter}
            }
            
            span.set_attribute("message.package", counter)
            span.set_attribute("message.timestamp", message["timestamp_producer"])
            
            # Send message to Redis stream using xadd for proper stream format
            try:
                await broker._connection.xadd("number_stream", {"data": json.dumps(message)})
                print(f"[{time.time()}] Produced message: {message}")
                
                # Increment metrics
                messages_produced_counter.add(1, {"package": str(counter)})
                
            except Exception as e:
                print(f"[{time.time()}] Error producing message: {e}")
                span.record_exception(e)
            
            counter += 1
            await asyncio.sleep(1)  # Send one message per second

if __name__ == "__main__":
    start_time = time.time()
    print(f"[{start_time}] Starting producer...")
    
    try:
        print(f"[{time.time()}] About to run FastStream app...")
        
        # Start message production as a background task
        async def run_app_with_producer():
            # Start the producer task
            producer_task = asyncio.create_task(produce_messages())
            
            # Run the FastStream app
            await app.run()
            
            # Cancel the producer task when app stops
            producer_task.cancel()
        
        asyncio.run(run_app_with_producer())
        
    except Exception as e:
        end_time = time.time()
        print(f"[{end_time}] Producer failed to start after {end_time - start_time:.2f} seconds: {e}")
        import traceback
        traceback.print_exc()