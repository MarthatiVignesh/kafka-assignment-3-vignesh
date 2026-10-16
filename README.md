# Kafka Assignment 3 - Order Processing Events

## Objective
This project uses Python and Apache Kafka to process order messages.

## Tools Used
- Python
- Apache Kafka
- Docker

## Kafka Topic
`intern-orders-vignesh`

## Project Files
- `producer.py` sends order messages to Kafka.
- `consumer.py` reads orders, checks the data, and calculates the total price.
- `docker-compose.yml` runs Kafka.
- `requirements.txt` lists the Python package needed.
- `screenshots/` will contain screenshots of the project output.

## How It Works
1. The producer sends order details as JSON messages.
2. The consumer reads the messages.
3. The consumer checks whether each order is valid.
4. It calculates the order total: quantity × unit price.
5. It displays valid orders, invalid orders, and the total value.

## Example
- Product: Keyboard
- Quantity: 2
- Unit price: Rs. 1500
- Order total: Rs. 3000

## Expected Result
For one batch of 12 messages:
- Valid orders: 10
- Invalid orders: 2
- Total value of valid orders: Rs. 20,850

If the producer is run more than once, Kafka stores the new messages too, so the consumer summary may show higher counts and totals.

## Kafka Concepts
This project demonstrates a producer, consumer, topic, message key, partition, and offset.
