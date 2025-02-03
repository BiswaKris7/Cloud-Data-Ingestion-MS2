# Cloud-Data-Ingestion-MS2
The idea of this project is to demonstrate Kafka, which acts like Cloud Pub/Sub, integrated with Redis and MySQL through Python scripts. The system simulates real-time processing of records in CSV and images by publishing and consuming via Kafka topics, image storage in Redis, and insertion of CSV data into MySQL.


Required Python libraries:
kafka-python
redis
mysql-connector-python
json
base64


Run the script by: 
Start the Kafka producer:python producer.py

python publish_images.py

python consumer.py

python image_consumer.py


Producer Scripts:

producer.py sends CSV records from Labels.csv to the Kafka csv-records-topic.
publish_images.py sends images from the images/ folder to Kafka under the image-topic.
Consumer Scripts

The consumer.py script will consume the CSV data from Kafka and write it into the MySQL database.
image_consumer.py consumes the image data from Kafka and writes the base64 encoded images into Redis.
