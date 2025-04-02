from pyflink.datastream.connectors.kafka import KafkaSink, KafkaRecordSerializationSchema, KafkaSource, KafkaOffsetsInitializer
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.window import EventTimeSessionWindows
from pyflink.common.watermark_strategy import WatermarkStrategy
from pyflink.common.time import Time

# 🚀 Initialize Flink Streaming Execution Environment
env = StreamExecutionEnvironment.get_execution_environment()
env.set_parallelism(1)

# 🚀 Kafka Source
kafka_source = KafkaSource.builder() \
    .set_bootstrap_servers("host.docker.internal:9092") \
    .set_topics("events") \
    .set_group_id("flink-session-group") \
    .set_starting_offsets(KafkaOffsetsInitializer.earliest()) \
    .set_value_only_deserializer(SimpleStringSchema()) \
    .build()

def parse_csv_event(event):
    """Parse a CSV-formatted Kafka event into a structured dictionary."""
    try:
        parts = event.strip().split(",")
        if len(parts) != 4:
            return None  # Skip invalid rows

        timestamp, user_id, transaction_id, payload_value = parts
        return {
            "timestamp": timestamp.strip(),
            "user_id": int(user_id.strip()),
            "transaction_id": int(transaction_id.strip()),
            "payload_value": float(payload_value.strip())
        }
    except Exception:
        return None  # Ignore malformed records

# 🚀 Define Watermark Strategy
#monotonous for ordered timestamps
#forBoundedOutOfOrderness for un-ordered timestamps
watermark_strategy = WatermarkStrategy.for_monotonous_timestamps()

# 🚀 Create Flink DataStream
ds = env.from_source(kafka_source, watermark_strategy, source_name="Kafka Source") \
    .map(parse_csv_event) \
    .filter(lambda x: x is not None)

# 🚀 Apply Session Windowing (Auto-closes after 30s of inactivity)
# Groups elements by sessions of activity
keyed_ds = ds.key_by(lambda event: event["user_id"])
session_windowed = keyed_ds.window(EventTimeSessionWindows.with_gap(Time.seconds(30)))

def session_aggregator(event1, event2):
    """Aggregate session data by summing payload values and counting occurrences."""
    event1_payload = event1.get("payload_value", event1.get("session_sum", 0))
    event2_payload = event2.get("payload_value", event2.get("session_sum", 0))

    return {
        "user_id": event1["user_id"],
        "session_sum": event1_payload + event2_payload,
        "count": event1.get("count", 1) + event2.get("count", 1)
    }

def compute_avg(event):
    """Compute session average and format output as a clean table row."""
    if not event or "user_id" not in event or "session_sum" not in event or "count" not in event:
        return f"[ERROR] Missing fields in final computation: {event}"

    result = {
        "user_id": event["user_id"],
        "session_sum": int(event["session_sum"]),
        "session_avg": int(round(event["session_sum"] / max(1, event["count"])))
    }

    return f"{result['user_id']:<10} {result['session_sum']:<15} {result['session_avg']:<15}"

# 🚀 Print Report Header Once
col_widths = [10, 15, 15]
print("\n📌 **Session Expiry Report** (All UIDs Processed)")
print(f"{'user_id':<{col_widths[0]}} {'session_sum':<{col_widths[1]}} {'session_avg':<{col_widths[2]}}")
print("=" * sum(col_widths))

# 🚀 Process and Print Results
formatted_ds = session_windowed.reduce(session_aggregator).map(compute_avg)
formatted_ds.print()

env.execute("Flink CSV Session Processing")
