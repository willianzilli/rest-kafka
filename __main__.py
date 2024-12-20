from flask import Flask, request, jsonify
from confluent_kafka import Producer
import os

app = Flask(__name__)

bootstrap_server = ":".join(os.getenv("BOOTSTRAP_SERVER_HOST", 'localhost'), os.getenv("BOOTSTRAP_SERVER_PORT", '9092'))

producer = Producer({'bootstrap.servers': bootstrap_server})

# POST /produce
@app.route('/produce', methods=['POST'])
def produce():
    try:
        data = request.get_json()
        topic = data['topic']
        message = data['message']
        
        producer.produce_message(topic, message)
        
        return jsonify({'message': 'Message sent successfully to Kafka'}), 200
    
    except Exception as e:
        return jsonify({'error': str(e)}), 500

if __name__ == '__main__':
    app.run(host=os.getenv("HOST", "0.0.0.0"), port=os.getenv("PORT", "8080"))