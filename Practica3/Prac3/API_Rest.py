from flask import Flask, request, jsonify, render_template
from flask_jwt_extended import JWTManager, create_access_token, jwt_required
from pymongo import MongoClient, errors
from bson import ObjectId
from datetime import timedelta
from werkzeug.security import generate_password_hash, check_password_hash
from flask_cors import CORS
import os
import requests
import logging
from api_w import WEATHER_API_KEY
import threading
from kafka import KafkaConsumer, KafkaProducer
import json
import argparse

app = Flask(__name__)
CORS(app)
logging.basicConfig(level=logging.INFO)
#logging.basicConfig(filename='registro_auditoria.log', level=logging.INFO)

def create_app(mongo_address, kafka_address):
    try:
        client = MongoClient(mongo_address, serverSelectionTimeoutMS=5000)
        client.server_info()  # Test MongoDB connection
    except Exception as e:
        logging.error(f"Error al conectar a MongoDB: {e}")
        # Implementar lógica de reintento o manejo de fallo aquí

    try:
        kafka_producer = KafkaProducer(
            bootstrap_servers=kafka_address,
            value_serializer=lambda m: json.dumps(m).encode('utf-8')
        )
        # Test Kafka connection here
    except Exception as e:
        logging.error(f"Error al conectar a Kafka: {e}")
        # Implementar lógica de reintento o manejo de fallo aquí

    db = client.dronedb
    jwt = JWTManager(app)
    app.config["JWT_SECRET_KEY"] = "tu_clave_secreta"

    drone_positions = {}

    BASE_URL = "http://api.openweathermap.org/data/2.5/weather"



    # Esta función obtiene los datos del clima para una ciudad
    def get_weather(city_name):
        params = {
            'q': city_name,
            'appid': WEATHER_API_KEY,
        }
        response = requests.get(BASE_URL, params=params)
        if response.status_code == 200:
            return response.json()  # Retorna el JSON de la respuesta
        else:
            return None
            
            
    @app.route('/')
    def index():
        return render_template('index.html')


    @app.route('/weather/<city>', methods=['GET'])
    def weather(city):
        data = get_weather(city)
        if data is not None:
            custom_data = {
                'coord': data['coord'],
                'weather': data['weather'],
                'base': data['base'],
                'main': data['main'],
                'visibility': data['visibility'],
                'wind': data['wind'],
                'clouds': data['clouds'],
                'dt': data['dt'],
                'sys': data['sys'],
                'timezone': data['timezone'],
                'id': data['id'],
                'name': data['name'],
                'cod': data['cod']
            }
            # Guardar en un archivo JSON
            with open('ciudad.json', 'w') as json_file:
                json.dump(custom_data, json_file, indent=4)
            
            return jsonify(custom_data)
        else:
            return jsonify({'error': 'No se pudo obtener el clima'}), 404




    @app.errorhandler(Exception)
    def error_response(e):
        logging.error(f'Error no capturado: {e}', exc_info=True)
        return jsonify(error=str(e)), 500



    ###### KAFKA ######
    def kafka_listener():
        consumer = KafkaConsumer(
        'drone_position_updates',
        bootstrap_servers=kafka_address,
        value_deserializer=lambda m: json.loads(m.decode('utf-8'))
        )

        for message in consumer:
            message_data = message.value
            dron_id = message_data['ID']
            position = message_data['Position']

            # Actualizar la posición del dron en el diccionario global
            drone_positions[dron_id] = position
            print(f"Actualización recibida: Drone ID {dron_id}, Posición {position}")

    final_drone_positions = {}

    def kafka_final_positions_listener():
        consumer = KafkaConsumer(
            'drone_final_position',  # Asegúrate de usar el tópico correcto
            bootstrap_servers=kafka_address,
            value_deserializer=lambda m: json.loads(m.decode('utf-8'))
        )

        for message in consumer:
            dron_id = message.value['dron_id']
            final_position = message.value['final_position']
            final_drone_positions[dron_id] = final_position

    threading.Thread(target=kafka_final_positions_listener, daemon=True).start()

    @app.route('/get_final_drone_positions', methods=['GET'])
    def get_final_drone_positions():
        return jsonify(final_drone_positions)

    @app.route('/get_drone_positions')
    def get_drone_positions():
        return jsonify(drone_positions)



    ###### SEGURIDAD ######

    @app.route('/registro_usuario', methods=['POST'])
    def registro_usuario():
        username = request.json.get("username")
        password = request.json.get("password")
        password_hash = generate_password_hash(password)

        if db.users.find_one({"username": username}):
            return jsonify({"msg": "El usuario ya existe"}), 409

        db.users.insert_one({"username": username, "password_hash": password_hash})
        return jsonify({"msg": "Usuario registrado exitosamente"}), 201

    




    @app.route('/login', methods=['POST'])
    def login():
        username = request.json.get("username")
        password = request.json.get("password")
        user = db.users.find_one({"username": username})

        if user and check_password_hash(user['password_hash'], password):
            access_token = create_access_token(identity=username, expires_delta=timedelta(seconds=20))
            return jsonify(access_token=access_token)
        return jsonify({"msg": "Credenciales incorrectas"}), 401


    @app.route('/protegido', methods=['GET'])
    @jwt_required()
    def protegido():
        return jsonify(msg="Ruta protegida")


    ##### AUDITORIA #####

    @app.route('/evento', methods=['GET'])
    @jwt_required()
    def evento():
        logging.info("Evento registrado")
        return jsonify(msg="Evento registrado")



    ##### API #####
    @app.errorhandler(Exception)
    def error_response(e):
        logging.error(f'Error: {e}')
        return jsonify(error=str(e)), 500



    @app.route('/registro', methods=['POST'])
    @jwt_required()
    def registro():
        try:
            data = request.json
            Alias = data.get('Alias', '').strip()  # Usar 'Alias' con mayúscula
            drone_id = int(data.get('ID')) # Obtener el ID proporcionado

            if not Alias or not drone_id:
                return jsonify({'error': 'Falta el Alias o el ID del dron'}), 400

            # Verificar si el ID ya existe
            if db.drones.find_one({'ID': drone_id}):
                return jsonify({'error': 'El ID ya existe'}), 409

            new_drone = {
                '_id': ObjectId(),  # Generar ObjectId automáticamente
                'type': 'register',
                'ID': drone_id,  # Usar el ID proporcionado
                'Alias': Alias,
                'InitialPosition': [1, 1]
            }

            db.drones.insert_one(new_drone)

            return jsonify({'status': 'success', 'drone_id': drone_id}), 201
        except errors.PyMongoError as e:
            return jsonify({'error': str(e)}), 500





    @app.route('/listar_drones', methods=['GET'])
    def listar_drones():
        try:
            drones = list(db.drones.find({}, {'ID': 1, 'alias': 1}))
            return jsonify([{'ID': str(drone['ID']), 'Alias': drone.get('Alias', 'Sin alias')} for drone in drones])
        except errors.PyMongoError as e:
            return error_response(f'Error de base de datos: {e}', 500)


    @app.route('/modificar_dron/<drone_id>', methods=['PUT'])
    def modificar_dron(drone_id):
        try:
            data = request.json
            alias = data.get('Alias', '').strip()
            if not alias:
                return error_response('Falta el alias del dron o es inválido', 400)

            result = db.drones.update_one({'_id': ObjectId(drone_id)}, {'$set': {'Alias': alias}})
            if result.matched_count == 0:
                return error_response('Dron no encontrado', 404)
            return jsonify({'message': 'Dron modificado correctamente'}), 200
        except errors.PyMongoError as e:
            return error_response(f'Error de base de datos: {e}', 500)

    @app.route('/borrar_dron/<drone_id>', methods=['DELETE'])
    def borrar_dron(drone_id):
        try:
            if drone_id == "-1":
                # Borrar todos los drones
                result = db.drones.delete_many({})
                return jsonify({'message': f'{result.deleted_count} drones eliminados correctamente'}), 200
            else:
                #Borrar un solo dron
                result = db.drones.delete_one({'ID': int(drone_id)})
                if result.deleted_count == 0:
                    return error_response('Dron no encontrado', 404)
                return jsonify({'message': 'Dron eliminado correctamente'}), 200
        except errors.PyMongoError as e:
            return jsonify({'error': str(e)}), 500
        

    @app.route('/unir_drones_show', methods=['POST'])
    @jwt_required()
    def unir_drones_show():
        data = request.get_json()
        drone_ids = data['drone_ids']

        if not drone_ids:
            return jsonify({'error': 'No se proporcionaron IDs de drones'}), 400

        for dron_id in drone_ids:
            drone = db.drones.find_one({"ID": dron_id})
            if drone:
                # Actualiza el estado del dron en la base de datos
                db.drones.update_one({"ID": dron_id}, {"$set": {"estado": "en_show"}})
                # Enviar mensaje a Kafka
                kafka_producer.send('drone_messages_topic', json.dumps({"type": "join_show", "dron_id": dron_id}))

        return jsonify({'message': 'Drones unidos al show exitosamente'}), 200


    
    

    threading.Thread(target=kafka_listener, daemon=True).start()
    context = ('ssl/certificado_registry.crt', 'ssl/clave_privada_registry.pem')
    #SSL
    #app.run(debug=True, host='registry', ssl_context=context, port=5000)
    app.run(debug=False, host='0.0.0.0', port=5000)

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='API Rest para drones')
    parser.add_argument('--mongo', type=str, default='localhost:27017', help='Dirección de MongoDB (por defecto: localhost:27017)')
    parser.add_argument('--kafka', type=str, default='localhost:29092', help='Dirección de Kafka (por defecto: localhost:9092)')
    args = parser.parse_args()

    app = create_app(args.mongo, args.kafka)