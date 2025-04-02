from confluent_kafka import Producer
import random
from datetime import datetime, timedelta
import time

# Kafka broker configuration
bootstrap_servers = "host.docker.internal:9092"  # Change this to your Kafka broker(s) address
kafka_topic = "events"  # Change this to your Kafka topic

# Create a Kafka producer instance
producer = Producer({"bootstrap.servers": bootstrap_servers})

# Function to generate random events and produce them to Kafka
def generate_and_produce_events(events_per_second, total_events, tenantIDs):
    start_time = datetime(2024, 8, 26, 8, 0, 0)
    for _ in range(total_events):
        timestamp = start_time.strftime("%Y-%m-%d %H:%M:%S")
        userID = random.choice(tenantIDs)
        sessionID = f"{random.randint(1, 1000000)}"
        payload = int(userID)*10
        data = f"{timestamp},{userID},{sessionID},{payload}"
        
        # Produce the data to the Kafka topic
        producer.produce(kafka_topic, key=userID, value=data)
        producer.flush()
        start_time += timedelta(minutes=random.randint(1, 10))

        
        time.sleep(1 / events_per_second)
        print(f"Message sent to topic: {kafka_topic}: value:{payload} UID: {userID}")      
    # Wait for any outstanding messages to be delivered and delivery reports to be received
    producer.flush()


events_per_second = 1000  
total_events = 1000  

tenantIDs_1 = ["4","1", "5", "8", "100", "101", "198","212","213", "214", "301", "1000", "1990", "9999"]
generate_and_produce_events(events_per_second, total_events,tenantIDs_1)

events_per_second = 10  
total_events = 100  
tenantIDs_2 = ["4"]
generate_and_produce_events(events_per_second, total_events,tenantIDs_2)

total_events = 3000
events_per_second = 100 
tenantIDs_3 = ["1","198","1990"]
generate_and_produce_events(events_per_second, total_events,tenantIDs_3)

generate_and_produce_events(events_per_second, 100000,tenantIDs_2)