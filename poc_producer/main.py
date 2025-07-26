import asyncio
import time
import os
import logging
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
from opentelemetry.sdk.resources import Resource
from opentelemetry.semconv.resource import ResourceAttributes

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Set up OpenTelemetry
def setup_telemetry():
    # Create resource with service information
    resource = Resource.create({
        ResourceAttributes.SERVICE_NAME: "faststream-producer",
        ResourceAttributes.SERVICE_VERSION: "1.0.0",
        "service.instance.id": "producer-1"
    })
    
    # Initialize tracing with resource
    trace.set_tracer_provider(TracerProvider(resource=resource))
    tracer = trace.get_tracer("faststream-producer")
    
    # Initialize metrics with resource
    metric_reader = PeriodicExportingMetricReader(
        OTLPMetricExporter(endpoint="http://otel-collector:4317")
    )
    metrics.set_meter_provider(MeterProvider(resource=resource, metric_readers=[metric_reader]))
    meter = metrics.get_meter("faststream-producer")
    
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

# Stream cleanup metrics
stream_cleanup_counter = meter.create_counter(
    name="stream_cleanup_total",
    description="Total number of messages cleaned up from stream"
)

stream_length_gauge = meter.create_up_down_counter(
    name="stream_length_current",
    description="Current number of messages in the stream"
)

stream_cleanup_operations_counter = meter.create_counter(
    name="stream_cleanup_operations_total",
    description="Total number of cleanup operations performed"
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
            await asyncio.sleep(0.05)  # Send one message every 50ms

async def monitor_stream_cleanup():
    """Monitor stream cleanup and track metrics"""
    print(f"[{time.time()}] Starting stream cleanup monitoring...")
    
    # Wait for broker to be ready
    try:
        print(f"[{time.time()}] Waiting for broker to be ready for monitoring...")
        await broker.connect()
        print(f"[{time.time()}] Broker connected successfully for monitoring")
    except Exception as e:
        print(f"[{time.time()}] Broker connection failed for monitoring: {e}")
        raise
    
    last_cleanup_count = 0
    last_stream_length = 0
    
    while True:
        try:
            with tracer.start_as_current_span("monitor_stream_cleanup") as span:
                # Get current stream length
                stream_info = await broker._connection.xinfo_stream("number_stream")
                current_length = stream_info['length'] if stream_info else 0
                
                # Update stream length gauge
                stream_length_gauge.add(current_length, {"stream": "number_stream"})
                
                # Get pending messages info
                pending_info = await broker._connection.xpending("number_stream", "consumer_group")
                pending_count = pending_info['pending'] if pending_info else 0
                
                # Calculate acknowledged messages (total produced - pending - current)
                # This is an approximation since we don't have exact tracking
                total_produced = current_length + pending_count
                
                span.set_attribute("stream.length", current_length)
                span.set_attribute("stream.pending", pending_count)
                span.set_attribute("stream.total_produced", total_produced)
                
                print(f"[{time.time()}] Stream monitoring - Length: {current_length}, Pending: {pending_count}")
                
                # Check if cleanup happened (stream length decreased significantly)
                if current_length < last_stream_length and last_stream_length > 0:
                    # Calculate how many messages were cleaned up
                    cleanup_amount = last_stream_length - current_length
                    if cleanup_amount > 0:
                        stream_cleanup_counter.add(cleanup_amount, {"stream": "number_stream"})
                        stream_cleanup_operations_counter.add(1, {"stream": "number_stream"})
                        print(f"[{time.time()}] Detected cleanup of {cleanup_amount} messages")
                
                # Perform cleanup if stream is getting large
                if current_length > 100:  # Increased threshold for more aggressive cleanup
                    try:
                        # Trim to keep only the last 50 messages
                        trimmed = await broker._connection.xtrim("number_stream", maxlen=50, approximate=True)
                        if trimmed > 0:
                            stream_cleanup_counter.add(trimmed, {"stream": "number_stream"})
                            stream_cleanup_operations_counter.add(1, {"stream": "number_stream"})
                            print(f"[{time.time()}] Producer trimmed {trimmed} messages from stream")
                            logger.info(f"Producer trimmed {trimmed} messages from stream")
                            span.set_attribute("messages.trimmed", trimmed)
                    except Exception as e:
                        print(f"[{time.time()}] Error during producer cleanup: {e}")
                        logger.error(f"Error during producer cleanup: {e}")
                
                last_stream_length = current_length
                
        except Exception as e:
            print(f"[{time.time()}] Error in stream monitoring: {e}")
            logger.error(f"Error in stream monitoring: {e}")
        
        # Monitor every 10 seconds
        await asyncio.sleep(10)

async def run_app_with_producer():
    """Run the FastStream app with the producer"""
    # Start the producer task
    producer_task = asyncio.create_task(produce_messages())
    
    # Start the stream monitoring task
    monitoring_task = asyncio.create_task(monitor_stream_cleanup())
    
    # Run the FastStream app
    await app.run()

if __name__ == "__main__":
    start_time = time.time()
    print(f"[{start_time}] Starting producer...")
    
    try:
        print(f"[{time.time()}] About to run FastStream app...")
        
        asyncio.run(run_app_with_producer())
        
    except Exception as e:
        end_time = time.time()
        print(f"[{end_time}] Producer failed to start after {end_time - start_time:.2f} seconds: {e}")
        import traceback
        traceback.print_exc()