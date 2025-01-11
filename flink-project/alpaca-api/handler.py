from kafka.errors import KafkaTimeoutError


async def kafka_produce_message(
    producer,
    topic,
    data,
):
    print(data)
    try:
        producer.send(topic=topic, value=data)
    except KafkaTimeoutError as e:
        print(f"Error sending data to Kafka: {e}")
