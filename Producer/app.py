import os
import json
import logging
from flask import Flask, request, render_template
import pika
from dotenv import load_dotenv

# Load env vars
load_dotenv()

app = Flask(__name__, template_folder='templates')
logging.basicConfig(level=logging.INFO)

# Get RabbitMQ configuration from env
RABBITMQ_HOST = os.getenv('RABBITMQ_HOST', 'localhost')
RABBITMQ_PORT = int(os.getenv('RABBITMQ_PORT', 5672))
RABBITMQ_USER = os.getenv('RABBITMQ_USER', 'guest')
RABBITMQ_PASS = os.getenv('RABBITMQ_PASS', 'guest')

EXCHANGE_NAME = 'microservices'

def get_rabbitmq_channel():
    try:
        credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASS)
        parameters = pika.ConnectionParameters(
            host=RABBITMQ_HOST,
            port=RABBITMQ_PORT,
            credentials=credentials
        )
        connection = pika.BlockingConnection(parameters)
        channel = connection.channel()

        # Declare exchange and queues if not already declared
        channel.exchange_declare(exchange=EXCHANGE_NAME, exchange_type='direct', durable=True)
        for queue in ['health_check', 'insert_record', 'delete_record', 'read_database', 'send_database']:
            channel.queue_declare(queue=queue, durable=True)
            channel.queue_bind(exchange=EXCHANGE_NAME, queue=queue, routing_key=queue)

        return channel, connection
    except Exception as e:
        logging.error(f"Failed to connect to RabbitMQ: {e}")
        raise

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/health_check', methods=['GET'])
def health_check():
    channel, conn = get_rabbitmq_channel()
    channel.basic_publish(exchange=EXCHANGE_NAME, routing_key='health_check', body='RabbitMQ connection established')
    conn.close()
    return 'Health Check message sent!'

@app.route('/insert_record', methods=['GET'])
def insert_record():
    return render_template('insert.html')

@app.route('/insert_record_actually', methods=['POST'])
def insert_record_actually():
    name = request.form['name']
    srn = request.form['srn']
    section = request.form['section']
    message = json.dumps({'name': name, 'srn': srn, 'section': section})
    
    channel, conn = get_rabbitmq_channel()
    channel.basic_publish(exchange=EXCHANGE_NAME, routing_key='insert_record', body=message)
    conn.close()
    return render_template('insert.html', message='Record Inserted Successfully!')

@app.route('/delete_record', methods=['GET'])
def delete_record():
    return render_template('delete.html')

@app.route('/delete_record_actually', methods=['POST'])
def delete_record_actually():
    srn = request.form['srn']
    
    channel, conn = get_rabbitmq_channel()
    channel.basic_publish(exchange=EXCHANGE_NAME, routing_key='delete_record', body=srn)
    conn.close()
    return render_template('delete.html', message='Record Deleted Successfully!')

@app.route('/read_database', methods=['GET'])
def read_database():
    channel, conn = get_rabbitmq_channel()
    channel.basic_publish(exchange=EXCHANGE_NAME, routing_key='read_database', body='Read database request')
    conn.close()
    return render_template('read.html', message='Read Database message sent!')

@app.route('/read_database_actually', methods=['GET'])
def read_database_actually():
    channel, conn = get_rabbitmq_channel()
    method_frame, header_frame, body = channel.basic_get(queue='send_database')

    if method_frame:
        channel.basic_ack(delivery_tag=method_frame.delivery_tag)
        records = body.decode()
    else:
        records = '{}'

    conn.close()
    return records

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=int(os.getenv('FLASK_RUN_PORT', 5000)))
