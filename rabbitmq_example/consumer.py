import json
from typing import Any

import pika

RABBITMQ_HOST: str = "localhost"
RABBITMQ_PORT: int = 5672
QUEUE_NAME: str = "test-queue"


class RabbitMQMessageDeserializer:
    @staticmethod
    def deserialize_message(message_bytes: bytes) -> dict[str, Any]:
        return json.loads(message_bytes)


def callback(
    _ch: pika.channel.Channel,
    _method: pika.spec.Basic.Deliver,
    _properties: pika.spec.BasicProperties,
    body: bytes,
) -> None:
    message = RabbitMQMessageDeserializer.deserialize_message(body)
    print(f" [x] Received {message}")


def main() -> None:
    connection = None
    try:
        connection_parameters = pika.ConnectionParameters(host=RABBITMQ_HOST, port=RABBITMQ_PORT)
        connection = pika.BlockingConnection(connection_parameters)
        channel = connection.channel()

        channel.queue_declare(queue=QUEUE_NAME)
        channel.basic_consume(queue=QUEUE_NAME, on_message_callback=callback, auto_ack=True)

        print(" [*] Waiting for messages. To exit press CTRL+C")
        channel.start_consuming()
    except KeyboardInterrupt:
        print("\n [*] Stopping consumer...")
    finally:
        if connection and connection.is_open:
            connection.close()
            print(" [*] Connection closed")


if __name__ == "__main__":
    main()
