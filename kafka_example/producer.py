import logging
import random
import signal
import time
from typing import Any

from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic

from shared.message import Message
from shared.message_serializer import MessageSerializer

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(name)s - %(message)s")
logger = logging.getLogger(__name__)

KAFKA_SERVERS: list[str] = ["localhost:9092"]
TOPIC_NAME: str = "test-topic"
NUM_PARTITIONS: int = 1
REPLICATION_FACTOR: int = 1


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
                value_serializer=MessageSerializer.serialize_message,
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
            num_messages = random.randint(1, 10)
            logger.info(f"Producing {num_messages} messages")

            for _ in range(num_messages):
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
