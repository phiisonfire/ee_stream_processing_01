from kafka import KafkaConsumer

# Define the Kafka broker and topic
broker = 'localhost:9092'
topic = 'user_created'

# Create a Kafka consumer
consumer = KafkaConsumer(
    topic,
    bootstrap_servers=[broker],
    auto_offset_reset='earliest',  # Start reading at the earliest message
    enable_auto_commit=False,       # Commit the offsets automatically
    group_id='my-group',           # Consumer group ID
    value_deserializer=lambda x: x.decode('utf-8')
)

print(f"Connected to Kafka broker at {broker} and consuming from topic '{topic}'")

# Read and print 10 latest messages
i = 0
for message in consumer:
    if i > 10:
        break
    print(f"Received message: {message.value}")
    consumer.commit()
    i += 1
