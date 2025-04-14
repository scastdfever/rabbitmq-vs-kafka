from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
import json
import time
import signal
from typing import Any
from dataclasses import dataclass
import logging


logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(name)s - %(message)s")
logger = logging.getLogger(__name__)


KAFKA_SERVERS: list[str] = ["localhost:9092"]
TOPIC_NAME: str = "test-topic"
NUM_PARTITIONS: int = 1
REPLICATION_FACTOR: int = 1


@dataclass
class Message:
    id: int
    content: str
    timestamp: float

    def to_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "content": self.content,
            "timestamp": self.timestamp,
        }


class KafkaMessageSerializer:
    @staticmethod
    def serialize_message(message: Message) -> bytes:
        return json.dumps(message.to_dict()).encode("utf-8")


class KafkaProducerWrapper:
    def __init__(self) -> None:
        self.producer = None
        self.running = True
        self.__setup_topic()
        self.__setup_producer()
        self.__setup_signal_handlers()

    @staticmethod
    def __setup_topic() -> None:
        try:
            admin_client = KafkaAdminClient(bootstrap_servers=KAFKA_SERVERS)
            existing_topics = admin_client.list_topics()

            if TOPIC_NAME not in existing_topics:
                new_topic = NewTopic(
                    name=TOPIC_NAME,
                    num_partitions=NUM_PARTITIONS,
                    replication_factor=REPLICATION_FACTOR,
                )
                admin_client.create_topics([new_topic])
                logger.info(f"Created topic {TOPIC_NAME}")

            admin_client.close()
        except Exception as e:
            logger.error(f"Error setting up topic: {e}")
            raise

    def __setup_producer(self) -> None:
        try:
            self.producer = KafkaProducer(
                bootstrap_servers=KAFKA_SERVERS,
                value_serializer=KafkaMessageSerializer.serialize_message,
            )
            logger.info("Successfully connected to Kafka")
        except Exception as e:
            logger.error(f"Error setting up producer: {e}")
            raise

    def __setup_signal_handlers(self) -> None:
        signal.signal(signal.SIGINT, self.__handle_shutdown)
        signal.signal(signal.SIGTERM, self.__handle_shutdown)

    def __handle_shutdown(self, _signum: int, _frame: Any) -> None:
        logger.info("Shutting down producer...")
        self.running = False

    def send_message(self, message: Message) -> None:
        try:
            self.producer.send(TOPIC_NAME, message)
            logger.info(f"Sent message {message.id}: {message.content}")
        except Exception as e:
            logger.error(f"Error sending message: {e}")

    def close(self) -> None:
        if self.producer:
            self.producer.close()
            logger.info("Closed Kafka producer")


def main() -> None:
    producer = KafkaProducerWrapper()
    message_id = 0

    try:
        while producer.running:
            message = Message(id=message_id, content=f"Message {message_id}", timestamp=time.time())
            producer.send_message(message)
            message_id += 1
            time.sleep(1)
    except Exception as e:
        logger.error(f"Error in main: {e}")
    finally:
        producer.close()


if __name__ == "__main__":
    main()
