# RabbitMQ vs Kafka Comparison

This project provides simple examples to compare RabbitMQ and Kafka message brokers. It includes basic producers and consumers for both systems.

## Project Structure

```
.
├── rabbitmq_example/
│   ├── __init__.py
│   ├── producer.py
│   └── consumer.py
├── kafka_example/
│   ├── __init__.py
│   ├── producer.py
│   └── consumer.py
├── docker-compose.yml
├── pyproject.toml
├── requirements.txt
└── README.md
```

## Setup

1. Install dependencies:

```bash
pip install -r requirements.txt
```

2. Start the services using Docker Compose:

```bash
docker-compose up -d
```

This will start:
- RabbitMQ server on port 5672 (http://localhost:15672)
- Kafka server on port 9092 (http://localhost:9092)
- Zookeeper on port 2181 (http://localhost:2181)

## Examples

### RabbitMQ

The RabbitMQ example demonstrates basic message queuing with non-persistent messages.

1. Start the producer:

```bash
python -m rabbitmq_example.producer
```

2. Start the consumer:

```bash
python -m rabbitmq_example.consumer
```

### Kafka

The Kafka example demonstrates basic message streaming with a single partition.

1. Start the producer:

```bash
python -m kafka_example.producer
```

2. Start the consumer:

```bash
python -m kafka_example.consumer
```

## Features

### RabbitMQ
- Non-persistent messages
- Basic queue configuration
- Continuous message production
- Graceful shutdown handling

### Kafka
- Single partition topic
- Message persistence
- Continuous message production
- Graceful shutdown handling

## Docker Setup

The `docker-compose.yml` file includes:
- RabbitMQ: Latest version
- Kafka: Latest version with Zookeeper
- All necessary ports exposed
- Basic configuration for both services

## Requirements

- Python 3.9+
- Docker and Docker Compose
- Python packages:
  - pika (RabbitMQ client)
  - kafka-python (Kafka client)
  - black (code formatting)
  - isort (import sorting) 