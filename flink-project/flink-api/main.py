import json

from kafka import KafkaConsumer


def main():
    topic = "orderbooks-topic"
    # Create a Kafka consumer
    consumer = KafkaConsumer(
        "orderbooks-topic",  # Topic name
        bootstrap_servers=["localhost:9092"],  # Kafka broker address
        auto_offset_reset="earliest",  # Start from the earliest message
        enable_auto_commit=True,  # Automatically commit offsets
        group_id="orderbooks-group",  # Consumer group ID
        security_protocol="SSL",
        api_version=(
            0,
            11,
            5,
        ),  # TODO: update
        value_deserializer=lambda x: json.loads(
            x.decode("utf-8")
        ),  # Deserialize JSON messages
        request_timeout_ms=120000,  # Increase timeout to 120 seconds
    )

    print("Consumer started. Waiting for messages...")

    # Process messages from the topic
    for message in consumer:
        print(f"Received message: {message.value}")
        # Process the orderbook data here

    # for message in consumer:
    # # message value and key are raw bytes -- decode if necessary!
    # # e.g., for unicode: `message.value.decode('utf-8')`
    # print ("%s:%d:%d: key=%s value=%s" % (message.topic, message.partition,
    #                                       message.offset, message.key,
    #                                       message.value))


if __name__ == "__main__":
    main()
