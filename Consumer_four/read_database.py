import pika
import pymongo
import json

# RabbitMQ setup
credentials = pika.PlainCredentials(username='guest', password='guest')
parameters = pika.ConnectionParameters(host='rabbitmq', port=5672, credentials=credentials)
connection = pika.BlockingConnection(parameters=parameters)
channel = connection.channel()

# Declare exchange first
channel.exchange_declare(exchange='microservices', exchange_type='direct', durable=True)

# Declare queues
channel.queue_declare(queue='read_database', durable=True)
channel.queue_declare(queue='send_database', durable=True)

# Bind queue to exchange
channel.queue_bind(exchange='microservices', queue='send_database', routing_key='send_database')

# MongoDB setup
client = pymongo.MongoClient("mongodb://mongodb:27017/")
db = client["database"]
collection = db["ccdb"]

# Define callback
def callback(ch, method, properties, body):
    print("Received message to read database.")
    records = list(collection.find({}, {"_id": False}))  # Exclude MongoDB internal _id
    message = json.dumps(records)

    channel.basic_publish(
        exchange='microservices',
        routing_key='send_database',
        body=message
    )
    print("Published records to send_database queue.")
    ch.basic_ack(delivery_tag=method.delivery_tag)

# Start consuming
channel.basic_consume(queue='read_database', on_message_callback=callback)

print('Waiting for messages...')
channel.start_consuming()
