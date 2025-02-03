import os
import csv
import json
import glob
import logging
from kafka import KafkaProducer

# Setup logging
logging.basicConfig(level=logging.INFO)

# Constants
topic_id = "csv-records-topic"  # Kafka topic for CSV data
bootstrap_servers = ['localhost:9092']  # Kafka server address

# Kafka Producer
producer = KafkaProducer(
    bootstrap_servers=bootstrap_servers,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')  # Serialize JSON
)

def publish_csv_to_topic(csv_file_path):
    try:
        with open(csv_file_path, mode='r') as csv_file:
            csv_reader = csv.DictReader(csv_file)
            for row in csv_reader:
                message_data = row  # Keep it as a dictionary for easier handling
                try:
                    # Send the record to the Kafka topic
                    producer.send(topic_id, value=message_data)
                    logging.info(f"Published record to Kafka: {message_data}")
                except Exception as e:
                    logging.error(f"Failed to publish message: {e}")
    except FileNotFoundError as e:
        logging.error(f"CSV file not found: {e}")
    except Exception as e:
        logging.error(f"Failed to read CSV file: {e}")

if __name__ == "__main__":
    csv_file_path = "Labels.csv"  # File path to your CSV
    publish_csv_to_topic(csv_file_path)
