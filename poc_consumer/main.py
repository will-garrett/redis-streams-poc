import os
import uuid
import asyncio
import time
import logging
from faststream import FastStream
from faststream.redis import RedisBroker

# OpenTelemetry imports
from opentelemetry import trace, metrics
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.exporter.otlp.proto.grpc.metric_exporter import OTLPMetricExporter
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
from opentelemetry.instrumentation.redis import RedisInstrumentor
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

# Set up comprehensive logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

print(f"[{time.time()}] Starting consumer application...")

# Initialize FastStream app and Redis broker
redis_url = os.getenv("REDIS_URL", "redis://redis:6379")
print(f"[{time.time()}] Connecting to Redis at: {redis_url}")
logger.info(f"Redis URL: {redis_url}")

print(f"[{time.time()}] Creating Redis broker...")
try:
    # Try using explicit connection parameters
    broker = RedisBroker(
        host="redis",
        port=6379,
        db=0
    )
    print(f"[{time.time()}] Redis broker created successfully")
    logger.info("Redis broker created successfully")
except Exception as e:
    print(f"[{time.time()}] Error creating Redis broker: {e}")
    logger.error(f"Error creating Redis broker: {e}")
    raise

print(f"[{time.time()}] Creating FastStream app...")
try:
    app = FastStream(broker)
    print(f"[{time.time()}] FastStream app created successfully")
    logger.info("FastStream app created successfully")
except Exception as e:
    print(f"[{time.time()}] Error creating FastStream app: {e}")
    logger.error(f"Error creating FastStream app: {e}")
    raise

# Generate unique consumer ID for load balancing
consumer_id = str(uuid.uuid4())[:8]
print(f"[{time.time()}] Generated consumer ID: {consumer_id}")
logger.info(f"Consumer ID: {consumer_id}")

# Set up telemetry
tracer, meter = setup_telemetry()
messages_processed_counter = meter.create_counter(
    name="messages_processed_total",
    description="Total number of messages processed"
)

@app.on_startup
async def startup():
    """Start the producer when the app starts"""
    start_time = time.time()
    print(f"[{start_time}] Consumer {consumer_id} startup function called")
    logger.info(f"Consumer {consumer_id} startup function called")
    
    # Wait for broker to be ready
    try:
        print(f"[{time.time()}] Waiting for broker to be ready...")
        await broker.connect()
        print(f"[{time.time()}] Broker connected successfully")
        logger.info("Broker connected successfully")
    except Exception as e:
        print(f"[{time.time()}] Broker connection failed: {e}")
        logger.error(f"Broker connection failed: {e}")
        raise
    
    try:
        print(f"[{time.time()}] Testing Redis connection...")
        await broker._connection.ping()
        print(f"[{time.time()}] Redis connection successful")
        logger.info("Redis connection successful")
    except Exception as e:
        print(f"[{time.time()}] Redis connection failed: {e}")
        logger.error(f"Redis connection failed: {e}")
        raise
    
    try:
        print(f"[{time.time()}] Creating consumer group...")
        # Create consumer group if it doesn't exist
        await broker._connection.xgroup_create("number_stream", "consumer_group", id="0", mkstream=True)
        print(f"[{time.time()}] Consumer group created successfully")
        logger.info("Consumer group created successfully")
    except Exception as e:
        print(f"[{time.time()}] Consumer group creation failed (might already exist): {e}")
        logger.warning(f"Consumer group creation failed (might already exist): {e}")
    
    try:
        print(f"[{time.time()}] Starting consumer group reader task...")
        # Start the consumer group reader as a background task
        task = asyncio.create_task(consume_with_group())
        print(f"[{time.time()}] Consumer group reader task started")
        logger.info("Consumer group reader task started")
    except Exception as e:
        print(f"[{time.time()}] Error starting consumer group reader task: {e}")
        logger.error(f"Error starting consumer group reader task: {e}")
        raise
    
    end_time = time.time()
    print(f"[{end_time}] Consumer {consumer_id} startup completed in {end_time - start_time:.2f} seconds")
    logger.info(f"Consumer {consumer_id} startup completed in {end_time - start_time:.2f} seconds")

async def consume_with_group():
    """Consume messages using Redis consumer groups for load balancing"""
    start_time = time.time()
    print(f"[{start_time}] Consumer {consumer_id} consume_with_group function started")
    logger.info(f"Consumer {consumer_id} consume_with_group function started")
    
    output_file = f"/output/consumer_{consumer_id}.txt"
    print(f"[{time.time()}] Output file: {output_file}")
    logger.info(f"Output file: {output_file}")
    
    message_count = 0
    
    while True:
        loop_start = time.time()
        with tracer.start_as_current_span("consume_messages") as span:
            span.set_attribute("consumer.id", consumer_id)
            
            try:
                # Check if stream exists, if not wait a bit
                print(f"[{time.time()}] Checking if stream exists...")
                stream_exists = await broker._connection.exists("number_stream")
                if not stream_exists:
                    print(f"[{time.time()}] Consumer {consumer_id} waiting for stream to be created...")
                    logger.info(f"Consumer {consumer_id} waiting for stream to be created...")
                    await asyncio.sleep(2)
                    continue
                
                print(f"[{time.time()}] Reading messages from stream...")
                # Read messages from the stream using consumer groups for load balancing
                messages = await broker._connection.xreadgroup(
                    "consumer_group", 
                    consumer_id, 
                    {"number_stream": ">"}, 
                    count=1, 
                    block=1000
                )
                
                if messages:
                    print(f"[{time.time()}] Received {len(messages)} message batches")
                    logger.info(f"Received {len(messages)} message batches")
                    span.set_attribute("messages.received", len(messages))
                
                for stream, stream_messages in messages:
                    print(f"[{time.time()}] Processing {len(stream_messages)} messages from stream {stream}")
                    logger.info(f"Processing {len(stream_messages)} messages from stream {stream}")
                    
                    for message_id, fields in stream_messages:
                        message_count += 1
                        print(f"[{time.time()}] Processing message {message_id} (message #{message_count})")
                        logger.info(f"Processing message {message_id} (message #{message_count})")
                        
                        with tracer.start_as_current_span("process_message") as msg_span:
                            msg_span.set_attribute("message.id", message_id)
                            msg_span.set_attribute("message.number", message_count)
                            
                            if "data" in fields or b"data" in fields:
                                import json
                                # Get the data field, handling both string and bytes keys
                                data_field = fields.get("data") or fields.get(b"data")
                                message = json.loads(data_field)
                                timestamp = message.get("timestamp_producer")
                                payload = message.get("payload", {})
                                package_number = payload.get("package")
                                
                                msg_span.set_attribute("message.package", package_number)
                                msg_span.set_attribute("message.timestamp", timestamp)
                                
                                # Write to file
                                log_entry = f"Consumer {consumer_id} processed package {package_number} (timestamp: {timestamp})\n"
                                try:
                                    with open(output_file, "a") as f:
                                        f.write(log_entry)
                                    print(f"[{time.time()}] Wrote to file: {log_entry.strip()}")
                                    logger.info(f"Wrote to file: {log_entry.strip()}")
                                except Exception as e:
                                    print(f"[{time.time()}] Error writing to file: {e}")
                                    logger.error(f"Error writing to file: {e}")
                                
                                print(f"[{time.time()}] Consumer {consumer_id} processed package {package_number} (timestamp: {timestamp})")
                                logger.info(f"Consumer {consumer_id} processed package {package_number} (timestamp: {timestamp})")
                                
                                # Increment metrics
                                messages_processed_counter.add(1, {"consumer_id": consumer_id, "package": str(package_number)})
                                
                                # Simulate some processing time
                                await asyncio.sleep(0.1)
                                
                                # Acknowledge the message - THIS IS CRITICAL!
                                print(f"[{time.time()}] Acknowledging message {message_id}...")
                                await broker._connection.xack("number_stream", "consumer_group", message_id)
                                print(f"[{time.time()}] Consumer {consumer_id} acknowledged message {message_id}")
                                logger.info(f"Consumer {consumer_id} acknowledged message {message_id}")
                            else:
                                print(f"[{time.time()}] Message {message_id} has no 'data' field")
                                logger.warning(f"Message {message_id} has no 'data' field")
                                print(f"[{time.time()}] Available fields: {list(fields.keys())}")
                                logger.warning(f"Available fields: {list(fields.keys())}")
                
                loop_end = time.time()
                if loop_end - loop_start > 1.0:
                    print(f"[{time.time()}] Loop iteration took {loop_end - loop_start:.2f} seconds")
                    logger.warning(f"Loop iteration took {loop_end - loop_start:.2f} seconds")
                            
            except Exception as e:
                print(f"[{time.time()}] Error reading messages: {e}")
                logger.error(f"Error reading messages: {e}")
                span.record_exception(e)
                import traceback
                traceback.print_exc()
                await asyncio.sleep(1)

if __name__ == "__main__":
    start_time = time.time()
    print(f"[{start_time}] Starting consumer...")
    logger.info("Starting consumer...")

    try:
        print(f"[{time.time()}] About to run FastStream app...")
        logger.info("About to run FastStream app...")
        asyncio.run(app.run())
    except Exception as e:
        end_time = time.time()
        print(f"[{end_time}] Consumer failed to start after {end_time - start_time:.2f} seconds: {e}")
        logger.error(f"Consumer failed to start after {end_time - start_time:.2f} seconds: {e}")
        import traceback
        traceback.print_exc() 