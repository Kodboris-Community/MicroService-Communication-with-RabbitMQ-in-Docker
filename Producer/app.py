import logging
import time
import json
from flask import Flask, request, render_template
import pika
from pika.exceptions import AMQPConnectionError, StreamLostError

app = Flask(__name__, template_folder='templates')

# Setup logging
logging.basicConfig(level=logging.INFO)

# RabbitMQ connection parameters
RABBITMQ_HOST = 'rabbitmq'
RABBITMQ_PORT = 5672
RABBITMQ_USER = 'guest'
RABBITMQ_PASS = 'guest'

def get_channel():
    """Establish a new RabbitMQ channel with heartbeat and retry logic."""
    credentials = pika.PlainCredentials(username=RABBITMQ_USER, password=RABBITMQ_PASS)
    parameters = pika.ConnectionParameters(
        host=RABBITMQ_HOST,
        port=RABBITMQ_PORT,
        credentials=credentials,
        heartbeat=30,
        blocked_connection_timeout=30
    )

    try:
        connection = pika.BlockingConnection(parameters)
        channel = connection.channel()

        # Ensure exchange and queues are declared each time (idempotent)
        channel.exchange_declare(exchange='microservices', exchange_type='direct', durable=True)

        for queue_name in ['health_check', 'insert_record', 'delete_record', 'read_database', 'send_database']:
            channel.queue_declare(queue=queue_name, durable=True)
            if queue_name != 'send_database':
                channel.queue_bind(exchange='microservices', queue=queue_name, routing_key=queue_name)

        return connection, channel

    except AMQPConnectionError as e:
        logging.error("Failed to connect to RabbitMQ: %s", e)
        return None, None

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/health_check', methods=['GET'])
def health_check():
    connection, channel = get_channel()
    if not channel:
        return "Failed to connect to RabbitMQ", 500
    message = 'RabbitMQ connection established successfully'
    channel.basic_publish(exchange='microservices', routing_key='health_check', body=message)
    connection.close()
    return 'Health Check message sent!'

@app.route('/insert_record', methods=['GET'])
def insert_record():
    return render_template('insert.html', message='')

@app.route('/insert_record_actually', methods=['POST'])
def insert_record_actually():
    name = request.form['name']
    srn = request.form['srn']
    section = request.form['section']
    message = json.dumps({'name': name, 'srn': srn, 'section': section})
    logging.info(f"Inserting record: {message}")

    connection, channel = get_channel()
    if not channel:
        return "Failed to connect to RabbitMQ", 500

    try:
        channel.basic_publish(exchange='microservices', routing_key='insert_record', body=message)
    except StreamLostError as e:
        logging.error("RabbitMQ connection lost while publishing: %s", e)
        return "Error sending message", 500
    finally:
        connection.close()

    return render_template('insert.html', message='Record Inserted Successfully!')

@app.route('/delete_record', methods=['GET'])
def delete_record():
    return render_template('delete.html', message='')

@app.route('/delete_record_actually', methods=['POST'])
def delete_record_actually():
    srn = request.form['srn']
    logging.info(f"Deleting record: {srn}")

    connection, channel = get_channel()
    if not channel:
        return "Failed to connect to RabbitMQ", 500

    channel.basic_publish(exchange='microservices', routing_key='delete_record', body=srn)
    connection.close()
    return render_template('delete.html', message='Record Deleted Successfully!')

@app.route('/read_database', methods=['GET'])
def read_database():
    connection, channel = get_channel()
    if not channel:
        return "Failed to connect to RabbitMQ", 500

    channel.basic_publish(exchange='microservices', routing_key='read_database', body='Read database request')
    connection.close()
    return render_template('read.html', message='Read Database message sent!')

@app.route('/read_database_actually', methods=['GET'])
def read_database_actually():
    connection, channel = get_channel()
    if not channel:
        return "Failed to connect to RabbitMQ", 500

    method_frame, header_frame, body = channel.basic_get(queue='send_database')
    if method_frame:
        channel.basic_ack(delivery_tag=method_frame.delivery_tag)
        records = body.decode()
    else:
        records = "No records available"

    connection.close()
    return records

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)