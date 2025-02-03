import os
import json
import mysql.connector
import logging
from kafka import KafkaConsumer

# Setup logging
logging.basicConfig(level=logging.INFO)

# MySQL connection setup
try:
    db_connection = mysql.connector.connect(
        host="your_mysql_host",
        user="your_mysql_user",
        password="your_mysql_password",
        database="your_mysql_db"
    )
    db_cursor = db_connection.cursor()
    logging.info("Connected to MySQL database.")
except mysql.connector.Error as err:
    logging.error(f"Error connecting to MySQL: {err}")
    exit(1)

# Constants
topic_id = "csv-records-topic"  # Kafka topic for CSV data
bootstrap_servers = ['localhost:9092']  # Kafka server address

# Kafka Consumer for CSV Records
consumer = KafkaConsumer(
    topic_id,  # Kafka topic for data
    group_id='csv_consumer_group',
    bootstrap_servers=bootstrap_servers,
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))  # Deserialize JSON message
)

# Function to insert records into MySQL
def insert_into_mysql(record):
    query = """
    INSERT INTO LabelData (Timestamp, Car1_Location_X, Car1_Location_Y, Car1_Location_Z, 
    Car2_Location_X, Car2_Location_Y, Car2_Location_Z, Occluded_Image_view, Occluding_Car_view, 
    Ground_Truth_View, pedestrianLocationX_TopLeft, pedestrianLocationY_TopLeft, 
    pedestrianLocationX_BottomRight, pedestrianLocationY_BottomRight)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    values = (
        record['Timestamp'], record['Car1_Location_X'], record['Car1_Location_Y'], 
        record['Car1_Location_Z'], record['Car2_Location_X'], record['Car2_Location_Y'], 
        record['Car2_Location_Z'], record['Occluded_Image_view'], record['Occluding_Car_view'], 
        record['Ground_Truth_View'], record['pedestrianLocationX_TopLeft'], 
        record['pedestrianLocationY_TopLeft'], record['pedestrianLocationX_BottomRight'], 
        record['pedestrianLocationY_BottomRight']
    )
    try:
        db_cursor.execute(query, values)
        db_connection.commit()
        logging.info(f"Inserted record into MySQL: {record}")
    except mysql.connector.Error as err:
        logging.error(f"Error inserting record into MySQL: {err}")
        db_connection.rollback()

# Listen for messages from Kafka
for message in consumer:
    record = message.value  # Parse the message
    insert_into_mysql(record)  # Store it into MySQL
