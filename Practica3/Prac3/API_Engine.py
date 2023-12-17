from flask import Flask, jsonify
from kafka import KafkaConsumer
import threading
import json

app = Flask(__name__)

# Kafka Consumer que escucha en un tópico específico
def kafka_listener():
    consumer = KafkaConsumer(
        'drone_position_updates',
        bootstrap_servers='localhost:29092',
        value_deserializer=lambda m: json.loads(m.decode('utf-8'))
    )

    for message in consumer:
        message_data = message.value
        dron_id = message_data['ID']
        position = message_data['Position']

        # Actualizar la posición del dron en el diccionario global
        drone_positions[dron_id] = position
        print(f"Actualización recibida: Drone ID {dron_id}, Posición {position}")


@app.route('/get_drone_positions')
def get_drone_positions():
    return jsonify(drone_positions)


if __name__ == '__main__':
    # Inicia el thread de Kafka
    threading.Thread(target=kafka_listener, daemon=True).start()

    # Inicia la aplicación Flask
    app.run(debug=True, port=5005)
