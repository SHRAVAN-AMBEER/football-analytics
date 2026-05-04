import eventlet
eventlet.monkey_patch()

import json
from flask import Flask, render_template
from flask_socketio import SocketIO
from kafka import KafkaConsumer
import threading

app = Flask(__name__)
socketio = SocketIO(app, cors_allowed_origins="*")

KAFKA_BROKER = 'localhost:9092'

def background_kafka_consumer(topic, event_name):
    """Starts a Kafka consumer and emits messages via SocketIO"""
    try:
        consumer = KafkaConsumer(
            topic,
            bootstrap_servers=[KAFKA_BROKER],
            value_deserializer=lambda x: json.loads(x.decode('utf-8')),
            auto_offset_reset='latest'
        )
        print(f"Started consuming {topic}...")
        for message in consumer:
            socketio.emit(event_name, message.value)
    except Exception as e:
        print(f"Error consuming {topic}: {e}")

@app.route('/')
def index():
    return render_template('index.html')

if __name__ == '__main__':
    # Start background threads for tracking and insights
    t1 = threading.Thread(target=background_kafka_consumer, args=('player_telemetry', 'telemetry_update'))
    t2 = threading.Thread(target=background_kafka_consumer, args=('tactical_insights', 'insights_update'))
    t1.daemon = True
    t2.daemon = True
    t1.start()
    t2.start()

    print("Starting Flask dashboard on port 5000...")
    socketio.run(app, host='0.0.0.0', port=5000)
