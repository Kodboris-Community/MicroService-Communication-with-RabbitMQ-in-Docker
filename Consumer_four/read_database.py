import os
import json
import logging
from pymongo import MongoClient
import pika
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Logging setup
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Config from environment variables
RABBITMQ_HOST = os.getenv('RABBITMQ_HOST', 'rabbitmq')
RABBITMQ_PORT = int(os.getenv('RABBITMQ_PORT', 5672))
RABBITMQ_USER = os.getenv('RABBITMQ_USER', 'guest')
RABBITMQ_PASS = os.getenv('RABBITMQ_PASS', 'guest')
QUEUE_NAME = os.getenv('QUEUE_NAME', 'read_database')
EXCHANGE_NAME = os.getenv('EXCHANGE_NAME', 'microservices')
ROUTING_KEY = os.getenv('ROUTING_KEY', 'send_database')

MONGO_URI = os.getenv('MONGO_URI', 'mongodb://mongodb:27017/')
MONGO_DB = os.getenv('MONGO_DB', 'database')
MONGO_COLLECTION = os.getenv('MONGO_COLLECTION', 'ccdb')

# Connect to MongoDB
mongo_client = MongoClient(MONGO_URI)
collection = mongo_client[MONGO_DB][MONGO_COLLECTION]

# Connect to RabbitMQ
credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASS)
params = pika.ConnectionParameters(host=RABBITMQ_HOST, port=RABBITMQ_PORT, credentials=credentials)
connection = pika.BlockingConnection(params)
channel = connection.channel()

# Declare the exchange
channel.exchange_declare(exchange=EXCHANGE_NAME, exchange_type='direct', durable=True)

# Declare queues
channel.queue_declare(queue=QUEUE_NAME, durable=True)
channel.queue_declare(queue='send_database', durable=True)

# Bind the 'send_database' queue to the exchange
channel.queue_bind(exchange=EXCHANGE_NAME, queue='send_database', routing_key=ROUTING_KEY)

# Implement basic QoS
channel.basic_qos(prefetch_count=1)

# Define the callback function for consuming messages
def callback(ch, method, properties, body):
    try:
        # Retrieve all records from MongoDB collection
        records = list(collection.find({}, {"_id": 0}))  # Exclude Mongo's default _id field
        
        # Serialize the records as JSON
        records_json = json.dumps(records)

        # Send the records back to the producer via RabbitMQ
        channel.basic_publish(
            exchange=EXCHANGE_NAME, 
            routing_key=ROUTING_KEY, 
            body=records_json
        )
        logging.info(f"Sent {len(records)} records to producer.")

        # Acknowledge the message has been processed
        ch.basic_ack(delivery_tag=method.delivery_tag)
    
    except Exception as e:
        logging.error(f"Error processing message: {e}")
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)

# Start consuming messages
channel.basic_consume(queue=QUEUE_NAME, on_message_callback=callback)

logging.info("Waiting for messages...")
channel.start_consuming()
