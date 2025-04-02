from pyflink.datastream.connectors.kafka import KafkaSink, KafkaRecordSerializationSchema
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.window import EventTimeSessionWindows
from pyflink.common.watermark_strategy import WatermarkStrategy
from pyflink.common.time import Time
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer
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

# 🚀 Apply Session Windowing (Auto-closes after 30s of inactivity)
session_windowed = keyed_ds.window(EventTimeSessionWindows.with_gap(Time.seconds(30)))

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

# 🚀 Track printed users to avoid duplicate entries
printed_users = set()

def compute_avg(event):
    """Compute session average, filter big customers, and print only unique results."""
    global printed_users

    if not event or "session_sum" not in event or "count" not in event:
        return ""  # ✅ Ignore invalid events

    # ✅ **Filter sessions with `session_sum >= 1,000,000`**
    if event["session_sum"] < 1_000_000:
        return ""  # ✅ Skip small customers

    user_id = event["user_id"]
    
    # ✅ **Avoid duplicate printing for the same user_id**
    if user_id in printed_users:
        return ""  # ✅ Skip duplicates
    printed_users.add(user_id)  # ✅ Mark user as printed

    result = {
        "user_id": user_id,
        "session_sum": event["session_sum"],
        "session_avg": round(event["session_sum"] / max(1, event["count"]), 2)
    }

    # ✅ **Print once when a new big customer appears**
    print("\n📌 **Big Customers Session Report** (Sessions ≥ 1M Payload)")
    print(tabulate([result.values()], headers=result.keys(), tablefmt="grid"))  # ✅ Proper table format

    return result  # ✅ Return for further processing if needed

# 🚀 **Filtering: Only Sessions ≥ 1M**
formatted_ds = session_windowed.reduce(session_aggregator) \
    .map(compute_avg) \
    .filter(lambda x: x != "")  # ✅ Remove empty results

# 🚀 Print results in the terminal before sending to Kafka
formatted_ds.print()  # ✅ Flink processing

# 🚀 Execute Flink Streaming Job
print("[INFO] Flink Job Started: Waiting for Kafka Events...")
env.execute("Flink CSV Session Processing")
