import os
import base64
import json
import logging
from kafka import KafkaProducer

# Setup logging
logging.basicConfig(level=logging.INFO)

# Kafka producer setup
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',  # Kafka server address
    value_serializer=lambda v: json.dumps(v).encode('utf-8')  # Serialize JSON
)

# Directory where images are stored
image_dir = "./images"

# Function to serialize an image
def serialize_image(image_path):
    with open(image_path, "rb") as img_file:
        img_data = img_file.read()
        return base64.b64encode(img_data).decode('utf-8')

# Function to publish image to Kafka
def publish_image(image_name):
    image_path = os.path.join(image_dir, image_name)
    if os.path.exists(image_path):
        serialized_image = serialize_image(image_path)
        message = {
            'image_name': image_name,
            'image_data': serialized_image
        }
        try:
            producer.send('image-topic', value=message)
            logging.info(f"Published image {image_name} to Kafka")
        except Exception as e:
            logging.error(f"Failed to publish image {image_name}: {e}")
    else:
        logging.warning(f"Image {image_name} not found in directory {image_dir}")

# Scan folder and publish images
for image_name in os.listdir(image_dir):
    if image_name.endswith(('.png', '.jpg')) and not image_name.startswith('.'):  # Check image extension
        publish_image(image_name)
