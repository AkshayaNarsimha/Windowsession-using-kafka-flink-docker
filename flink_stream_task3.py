from pyflink.datastream.connectors.kafka import KafkaSink, KafkaRecordSerializationSchema
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.window import EventTimeSessionWindows
from pyflink.common.watermark_strategy import WatermarkStrategy
from pyflink.common.time import Time
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer
import time
from tabulate import tabulate

# 🚀 Initialize Flink Streaming Execution Environment
env = StreamExecutionEnvironment.get_execution_environment()
env.set_parallelism(1)

kafka_source = KafkaSource.builder() \
    .set_bootstrap_servers("host.docker.internal:9092") \
    .set_topics("events") \
    .set_group_id("flink-session-group") \
    .set_starting_offsets(KafkaOffsetsInitializer.earliest()) \
    .set_value_only_deserializer(SimpleStringSchema()) \
    .build()

def parse_csv_event(event):
    """Parse a CSV-formatted Kafka event into a dictionary."""
    try:
        parts = event.strip().split(",")
        if len(parts) != 4:
            print(f"[ERROR] Invalid CSV format: {event}")
            return None  # Skip invalid rows

        timestamp, user_id, transaction_id, payload_value = parts
        parsed_event = {
            "timestamp": timestamp.strip(),
            "user_id": int(user_id.strip()),
            "transaction_id": int(transaction_id.strip()),
            "payload_value": float(payload_value.strip())
        }
        return parsed_event
    except Exception as e:
        print(f"[ERROR] Failed to parse event: {event}, Error: {e}")
        return None  # Ignore malformed records

# Define a watermark strategy for event-time processing
watermark_strategy = WatermarkStrategy.for_monotonous_timestamps()

ds = env.from_source(kafka_source, watermark_strategy, source_name="Kafka Source") \
    .map(parse_csv_event) \
    .filter(lambda x: x is not None)  # Remove invalid records

# 🚀 Group by User ID
keyed_ds = ds.key_by(lambda event: event["user_id"])

# Define custom session timeouts per user category
def get_session_timeout(user_id):
    if user_id == 9999:
        return Time.minutes(5)
    elif 2 <= user_id <= 9:
        return Time.minutes(2)
    elif 100 <= user_id <= 200:
        return Time.seconds(60)
    else:
        return Time.seconds(30)  # Default session timeout

# Apply dynamic session windowing based on user-specific timeouts
def dynamic_session_window(event):
    user_id = event["user_id"]
    return EventTimeSessionWindows.with_gap(get_session_timeout(user_id))

# 🚀 Apply Dynamic Session Windowing
dynamic_windowed = keyed_ds.window(dynamic_session_window)

# 🚀 Reduce Function - Aggregate Sum & Count Per User Session
def session_aggregator(event1, event2):
    """Aggregate session data by summing payload values and counting occurrences."""
    if not event1 or not event2:
        return event1 if event1 else event2  # Return the valid event

    event1_payload = event1.get("payload_value", event1.get("session_sum", 0))
    event2_payload = event2.get("payload_value", event2.get("session_sum", 0))

    aggregated = {
        "user_id": event1["user_id"],
        "session_sum": event1_payload + event2_payload,
        "count": event1.get("count", 1) + event2.get("count", 1)
    }
    return aggregated

def compute_avg(event):
    """Compute session average and return formatted output."""
    if not event or "session_sum" not in event or "count" not in event:
        return ""  # ✅ Ignore invalid events

    result = {
        "user_id": event["user_id"],
        "session_sum": event["session_sum"],
        "session_avg": round(event["session_sum"] / max(1, event["count"]), 2)
    }

    return tabulate([result.values()], tablefmt="grid")  # ✅ Just return, no extra print!


from pyflink.datastream.window import EventTimeSessionWindows
from pyflink.datastream.window import SessionWindowTimeGapExtractor

# 🚀 Define SessionWindowTimeGapExtractor
class DynamicSessionGapExtractor(SessionWindowTimeGapExtractor):
    def extract(self, event):
        return get_session_timeout(event["user_id"])

# 🚀 Apply Dynamic Session Windowing
dynamic_windowed = keyed_ds.window(EventTimeSessionWindows.with_dynamic_gap(DynamicSessionGapExtractor()))



# 🚀 Execute Flink Streaming Job
print("[INFO] Flink Job Started with Dynamic Session Timeout...")
env.execute("Flink CSV Session Processing with Custom Timeouts")