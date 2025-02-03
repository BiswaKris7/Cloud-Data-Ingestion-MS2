import os
import json
import redis
import base64
import six
import logging
from kafka import KafkaConsumer

# Setup logging
logging.basicConfig(level=logging.INFO)

# Redis client setup
r = redis.StrictRedis(host='localhost', port=6379, db=0)

# Constants
bootstrap_servers = ['localhost:9092']  # Kafka server address
topic_id = "image-topic"  # Kafka topic for images

# Kafka Consumer for Images
consumer = KafkaConsumer(
    topic_id,  # Kafka topic for images
    group_id='image_consumer_group',
    bootstrap_servers=bootstrap_servers,
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))  # Deserialize JSON message
)

# Function to store image in Redis
def store_image_in_redis(image_name, serialized_image):
    redis_key = f"{image_name}_{int(time.time())}"  # Use timestamp to prevent overwriting
    try:
        r.set(redis_key, serialized_image)
        logging.info(f"Stored image {image_name} in Redis with key {redis_key}")
    except Exception as e:
        logging.error(f"Failed to store image {image_name} in Redis: {e}")

# Listen for image messages from Kafka
for message in consumer:
    image_data = message.value  # Parse the message
    image_name = image_data['image_name']
    serialized_image = image_data['image_data']
    store_image_in_redis(image_name, serialized_image)  # Store image in Redis
    logging.info(f"Received and stored image: {image_name}")
