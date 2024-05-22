from flask import Flask, request, jsonify, render_template
from flask_jwt_extended import JWTManager, create_access_token, jwt_required
from pymongo import MongoClient, errors
from bson import ObjectId
from datetime import timedelta
from werkzeug.security import generate_password_hash, check_password_hash
from flask_cors import CORS
from flask import Response
import os
import requests
import logging
from api_w import WEATHER_API_KEY
import threading
from kafka import KafkaConsumer, KafkaProducer
import json
import argparse
import base64
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import padding, rsa
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.backends import default_backend
import time

from datetime import datetime, timedelta
from flask_socketio import SocketIO, emit


app = Flask(__name__)
socketio = SocketIO(app)
CORS(app)
#logging.basicConfig(level=logging.INFO)

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
    app.config["JWT_SECRET_KEY"] = os.getenv(".env", "default_secret_key")
    jwt = JWTManager(app)

    drone_positions = {}

    BASE_URL = "http://api.openweathermap.org/data/2.5/weather"
    

    # Configura el logger de auditoría
    auditoria_logger = logging.getLogger('auditoria')
    auditoria_logger.setLevel(logging.INFO)
    # Crea un manejador de archivo
    file_handler = logging.FileHandler('registro_auditoria.log')
    file_handler.setLevel(logging.INFO)

    # Formato del logger con la ip
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(formatter)

    # Añade el manejador al logger
    auditoria_logger.addHandler(file_handler)
    
    db.auditoria.delete_many({})

    

    # Esta función obtiene los datos del clima para una ciudad
    def get_weather(city_name):
        params = {
            'q': city_name,
            'appid': WEATHER_API_KEY,
        }
        response = requests.get(BASE_URL, params=params, verify=False)
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
            auditoria_logger.info('Evento específico en /weather/<city>')
            return jsonify(custom_data)
        else:
            return jsonify({'error': 'No se pudo obtener el clima'}), 404




    @app.errorhandler(Exception)
    def error_response(e):
        logging.error(f'Error no capturado: {e}', exc_info=True)
        return jsonify(error=str(e)), 500
    
    
    @app.route('/stream_errors')
    def stream_errors():
        def generate():
            while True:
                # Suponiendo que tienes una función que obtiene los errores
                errors = obtener_errores()  
                yield f"data: {json.dumps(errors)}\n\n"
                time.sleep(1)  # Ajustar la frecuencia de actualización según sea necesario

        return Response(generate(), mimetype='text/event-stream')



    ###### KAFKA ######
    def load_drone_keys(drone_id):
        try:
            # Obtener la clave privada del dron desde MongoDB
            key_document = db.Claves.find_one({'ID': drone_id})
            
            # Comprobar si se encontró el documento
            if key_document is None:
                print(f"No se encontró la clave para el dron ID {drone_id}")
                return None
            
            # Cargar la clave privada desde el documento
            private_key_data = key_document['PrivateKey']
            private_key = serialization.load_pem_private_key(
                private_key_data.encode(),
                password=None,
                backend=default_backend()
            )
            return private_key
        except Exception as e:
            print(f"Error al cargar clave privada: {e}")
            return None
        
    def load_engine_keys():
        try:
            # Obtener la clave privada del dron desde MongoDB
            key_document = db.Claves.find_one({'ID': 'ADEngine'})
            
            # Comprobar si se encontró el documento
            if key_document is None:
                print(f"No se encontró la clave para el ADEngine")
                return None
            
            # Cargar la clave privada desde el documento
            private_key_data = key_document['PrivateKey']
            private_key = serialization.load_pem_private_key(
                private_key_data.encode(),
                password=None,
                backend=default_backend()
            )
            return private_key
        except Exception as e:
            print(f"Error al cargar clave privada: {e}")
            return None

    
    
    
    def kafka_listener():
        consumer = KafkaConsumer(
            'drone_position_updates',
            bootstrap_servers=kafka_address,
            value_deserializer=lambda m: m.decode('utf-8'),  # Decodifica como cadena
            ssl_cafile='ssl/certificado_CA.crt',
            ssl_certfile='ssl/certificado_registry.crt',
            ssl_keyfile='ssl/clave_privada_registry.pem'
        )

        for message in consumer:
            message_data = message.value
            try:
                # Intenta convertir la cadena a un diccionario JSON
                message_data = json.loads(message_data)
            except json.JSONDecodeError:
                print(f"El mensaje no es un JSON válido: {message_data}")
                continue  # Salta este mensaje y continúa con el siguiente

            # Ahora puedes acceder a message_data como un diccionario
            dron_id = message_data['ID']
            position = message_data['Position']

            # Actualizar la posición del dron en el diccionario global
            drone_positions[dron_id] = position
            print(f"Actualización recibida: Drone ID {dron_id}, Posición {position}")


    final_drone_positions = {}
    
    def kafka_final_positions_listener():
        consumer = KafkaConsumer(
            'drone_final_position',
            bootstrap_servers=kafka_address,
            value_deserializer=lambda m: json.loads(m.decode('utf-8')) if m else None,
            ssl_cafile='ssl/certificado_CA.crt',
            ssl_certfile='ssl/certificado_registry.crt',
            ssl_keyfile='ssl/clave_privada_registry.pem'
        )

        for message in consumer:
            if not message.value:
                print("Mensaje recibido está vacío.")
                continue

            try:
                # Asumimos que el mensaje es un JSON y lo convertimos a un diccionario
                message_dict = message.value
                dron_id = message_dict['dron_id']
                final_position = message_dict['final_position']
                final_drone_positions[dron_id] = final_position
                print(f"Posición final actualizada: Drone ID {dron_id}, Posición {final_position}")
            except json.JSONDecodeError as e:
                print(f"Error al decodificar el mensaje JSON: {e}")
            except TypeError as e:
                print(f"Error de tipo en el mensaje: {e}")
            except KeyError as e:
                print(f"Falta clave esperada en el mensaje: {e}")



    @app.route('/get_final_drone_positions', methods=['GET'])
    def get_final_drone_positions():
        auditoria_logger.info('Evento específico en /get_final_drone_positions')

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
        auditoria_logger.info('Evento específico en /registro_usuario')
        return jsonify({"msg": "Usuario registrado exitosamente"}), 201

    
    #TODO: Comprobar la expiracion y visualizacion de los token
    @app.route('/login', methods=['POST'])
    def login():
        username = request.json.get("username")
        password = request.json.get("password")
        user = db.users.find_one({"username": username})

        if user and check_password_hash(user['password_hash'], password):
            # Genera el token con una vida útil de 20 segundos
            access_token = create_access_token(identity=username, expires_delta=timedelta(seconds=20))
            print(access_token)
            
            db.tokens.insert_one({
                'username': username,
                'token': access_token,
                'created_at': datetime.now(),
                'expires_at': datetime.now() + timedelta(seconds=20)
            })
            
            # Crear un índice para borrar automáticamente tokens expirados
            db.tokens.create_index("expires_at", expireAfterSeconds=20)

            auditoria_logger.info('Usuario {} ha iniciado sesión'.format(username))
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
    
    @app.route('/auditoria', methods=['GET', 'POST'])
    def auditoria():
        if request.method == 'POST':
            try:
                data = request.json
                evento = data.get('evento')
                descripcion = data.get('descripcion')
                timestamp = data.get('timestamp')
                tipo = data.get('tipo', 'dron')  # Campo para distinguir el tipo de auditoría

                if not evento or not descripcion or not timestamp:
                    return jsonify({"error": "Faltan datos necesarios"}), 400

                log_entry = {
                    'evento': evento,
                    'descripcion': descripcion,
                    'timestamp': timestamp,
                    'tipo': tipo  # Almacenar el tipo de auditoría
                }
                db.auditoria.insert_one(log_entry)
                return jsonify({"message": "Evento de auditoría registrado"}), 201
            except Exception as e:
                logging.error(f"Error al registrar el evento de auditoría: {e}")
                return jsonify({"error": "Error al registrar el evento de auditoría"}), 500

        elif request.method == 'GET':
            try:
                general_logs = list(db.auditoria.find({"tipo": {"$ne": "encrypted"}}, {'_id': 0, 'timestamp': 1, 'evento': 1, 'descripcion': 1, 'tipo': 1}).sort('timestamp', -1))
                encrypted_logs = list(db.auditoria.find({"tipo": "encrypted"}, {'_id': 0, 'timestamp': 1, 'evento': 1, 'descripcion': 1, 'tipo': 1}).sort('timestamp', -1))
                return jsonify({
                    "general": general_logs,
                    "encrypted": encrypted_logs
                }), 200
            except Exception as e:
                logging.error(f"Error al recuperar los eventos de auditoría: {e}")
                return jsonify({"error": "Error al obtener los eventos de auditoría"}), 500


            
    @app.route('/stream_auditoria', methods=['GET'])
    def stream_auditoria():
        def generate():
            while True:
                logs = list(db.auditoria.find({}, {'_id': 0, 'timestamp': 1, 'evento': 1, 'descripcion': 1, 'tipo': 1}).sort('timestamp', -1))
                yield f"data: {json.dumps(logs)}\n\n"
                time.sleep(5)  # Ajusta el intervalo de tiempo según sea necesario

        return Response(generate(), mimetype='text/event-stream')




    ##### API #####
    @app.errorhandler(Exception)
    def error_response(e):
        logging.error(f'Error: {e}')
        return jsonify(error=str(e)), 500
    
    
    @app.route('/get_errors', methods=['GET'])
    def get_errors():
        with open('error_log.txt', 'r') as file:
            errors = file.readlines()
        return jsonify(errors)

    
    @app.route('/errores', methods=['GET'])
    def obtener_errores():
        # Aquí el código para obtener los errores de la base de datos o archivo de registro
        errores = []  # Supongamos que esta es la lista de errores
        return jsonify(errores)
    
    def log_error(error):
        with open('error_log.txt', 'a') as file:
            file.write(f'{error}\n')

    
    @app.errorhandler(Exception)
    def handle_all_errors(e):
        if request.path in ['/weather', '/registro_usuario', '/login', '/registro', '/borrar_dron']:
            logging.error(f'Error en {request.path}: {e}')
        if hasattr(e, 'code') and e.code == 200:
            return e
        log_error(e)  # Función personalizada para registrar errores
        return jsonify({'error': 'Ocurrió un error'}), getattr(e, 'code', 500)


    @app.route('/registroWEB', methods=['POST'])
    @jwt_required()
    def registroWEB():
        try:
            data = request.json
            Alias = data.get('Alias', '').strip()  # Usar 'Alias' con mayúscula
            drone_id = int(data.get('ID')) # Obtener el ID proporcionado

            if not Alias or not drone_id:
                return jsonify({'error': 'Falta el Alias o el ID del dron'}), 400

            # Verificar si el ID ya existe
            if db.drones.find_one({'ID': drone_id}):
                return jsonify({'error': 'El ID ya existe'}), 409
            
            # Generar claves privada y pública
            private_key = rsa.generate_private_key(public_exponent=65537, key_size=2048, backend=default_backend())
            public_key = private_key.public_key()

            # Convertir las claves a formato PEM
            private_key_pem = private_key.private_bytes(
                encoding=serialization.Encoding.PEM,
                format=serialization.PrivateFormat.TraditionalOpenSSL,
                encryption_algorithm=serialization.NoEncryption()
            )
            public_key_pem = public_key.public_bytes(
                encoding=serialization.Encoding.PEM,
                format=serialization.PublicFormat.SubjectPublicKeyInfo
            )
            
            key_document = {
                'ID': drone_id,
                'PrivateKey': private_key.private_bytes(encoding=serialization.Encoding.PEM, format=serialization.PrivateFormat.TraditionalOpenSSL, encryption_algorithm=serialization.NoEncryption()),
                'PublicKey': public_key.public_bytes(encoding=serialization.Encoding.PEM, format=serialization.PublicFormat.SubjectPublicKeyInfo)
            }
            db.Claves.replace_one({'ID': drone_id}, key_document, upsert=True)
            
            new_drone = {
                '_id': ObjectId(),  # Generar ObjectId automáticamente
                'type': 'register',
                'ID': drone_id,  # Usar el ID proporcionado
                'Alias': Alias,
                'InitialPosition': [1, 1]
            }

            db.drones.insert_one(new_drone)
            auditoria_logger.info('Evento específico en /registro_dron')


            return jsonify({'status': 'success', 'drone_id': drone_id}), 201
        except errors.PyMongoError as e:
            return jsonify({'error': str(e)}), 500
        
        
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
            auditoria_logger.info('Evento específico en /registro_dron')

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
                auditoria_logger.info('Evento específico en /borrar_dron/<drone_id>')
                return jsonify({'message': 'Dron eliminado correctamente'}), 200
        except errors.PyMongoError as e:
            return jsonify({'error': str(e)}), 500

    @app.route('/borrar_drones', methods=['POST'])
    def borrar_drones():
        try:
            data = request.json
            drone_ids = data.get('drone_ids')
            
            if not drone_ids:
                return jsonify({'message': 'No se proporcionaron IDs de drones'}), 400

            # Elimina cada dron por su ID
            for drone_id in drone_ids:
                db.drones.delete_one({'ID': int(drone_id)})

            return jsonify({'message': f'Drones eliminados correctamente'}), 200
        except errors.PyMongoError as e:
            return jsonify({'error': str(e)}), 500
        
        
    @app.route('/unir_drones_al_show', methods=['POST'])
    def unir_drones_al_show():
        data = request.json
        drone_ids = data.get('drone_ids')

        if not drone_ids:
            return jsonify({'message': 'No se proporcionaron IDs de drones'}), 400

        for drone_id in drone_ids:
            kafka_producer.send(
                'drone_messages_topic', 
                {'type': 'instruction', 'dron_id': drone_id, 'instruction': 'join_show'}
            )
        return jsonify({'message': f'Drones {drone_ids} invitados a unirse al show'}), 200


    threading.Thread(target=kafka_listener, daemon=True).start()
    context = ('ssl/certificado_registry.crt', 'ssl/clave_privada_registry.pem')
    #SSL
    app.run(debug=True, host='0.0.0.0', ssl_context=context, port=5000)
    #app.run(debug=False, host='0.0.0.0', port=5000)

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='API Rest para drones')
    parser.add_argument('--mongo', type=str, default='localhost:27017', help='Dirección de MongoDB (por defecto: localhost:27017)')
    parser.add_argument('--kafka', type=str, default='localhost:29092', help='Dirección de Kafka (por defecto: localhost:9092)')
    args = parser.parse_args()

    app = create_app(args.mongo, args.kafka)
    