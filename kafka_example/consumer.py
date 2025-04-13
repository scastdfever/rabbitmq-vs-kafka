from kafka import KafkaConsumer
import json
from typing import Any


KAFKA_SERVERS: list[str] = ["localhost:9092"]
TOPIC_NAME: str = "test-topic"


class KafkaMessageDeserializer:
    @staticmethod
    def deserialize_message(message_bytes: bytes) -> dict[str, Any]:
        return json.loads(message_bytes.decode("utf-8"))


def main() -> None:
    consumer = None

    try:
        consumer = KafkaConsumer(
            TOPIC_NAME,
            bootstrap_servers=KAFKA_SERVERS,
            value_deserializer=KafkaMessageDeserializer.deserialize_message,
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
