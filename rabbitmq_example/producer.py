import logging
import signal
import time
import random
from typing import Any

import pika
from pika.exceptions import AMQPError

from shared.message import Message
from shared.message_serializer import MessageSerializer

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(name)s - %(message)s")
logger = logging.getLogger(__name__)

RABBITMQ_HOST: str = "localhost"
RABBITMQ_PORT: int = 5672
RABBITMQ_USER: str = "guest"
RABBITMQ_PASS: str = "guest"
QUEUE_NAME: str = "test-queue"


class RabbitMQProducer:
    def __init__(self) -> None:
        self.connection = None
        self.channel = None
        self.running = True
        self.__setup_connection()
        self.__setup_signal_handlers()

    def __setup_connection(self) -> None:
        credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASS)
        parameters = pika.ConnectionParameters(
            host=RABBITMQ_HOST,
            port=RABBITMQ_PORT,
            credentials=credentials,
        )

        self.connection = pika.BlockingConnection(parameters)
        self.channel = self.connection.channel()
        self.channel.queue_declare(queue=QUEUE_NAME)
        logger.info("Successfully connected to RabbitMQ")

    def __setup_signal_handlers(self) -> None:
        signal.signal(signal.SIGINT, self.__handle_shutdown)
        signal.signal(signal.SIGTERM, self.__handle_shutdown)

    def __handle_shutdown(self, _signum: int, _frame: Any) -> None:
        logger.info("Shutting down producer...")
        self.running = False

    def send_message(self, message: Message) -> None:
        try:
            self.channel.basic_publish(
                exchange="",
                routing_key=QUEUE_NAME,
                body=MessageSerializer.serialize_message(message),
            )

            logger.info(f"Sent message {message.id}: {message.content}")
        except AMQPError as e:
            logger.error(f"Error sending message: {e}")

    def close(self) -> None:
        if self.connection and not self.connection.is_closed:
            self.connection.close()
            logger.info("Closed RabbitMQ connection")


def main() -> None:
    producer = RabbitMQProducer()
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
