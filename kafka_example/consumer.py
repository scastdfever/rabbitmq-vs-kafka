from kafka import KafkaConsumer

from shared.message_deserializer import MessageDeserializer

KAFKA_SERVERS: list[str] = ["localhost:9092"]
TOPIC_NAME: str = "test-topic"


def main() -> None:
    consumer = None

    try:
        consumer = KafkaConsumer(
            TOPIC_NAME,
            bootstrap_servers=KAFKA_SERVERS,
            auto_offset_reset="earliest",
            value_deserializer=MessageDeserializer.deserialize_message,
        )

        print(" [*] Waiting for messages. To exit press CTRL+C")
        for message in consumer:
            print(f" [x] Received {message.value}")
    except KeyboardInterrupt:
        print("\n [*] Stopping consumer...")
    finally:
        if consumer:
            consumer.close()
            print(" [*] Consumer closed")


if __name__ == "__main__":
    main()
