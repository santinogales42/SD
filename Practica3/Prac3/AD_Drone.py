import socket
import json
import threading
from kafka import KafkaProducer, KafkaConsumer
from bson import ObjectId
from flask import Flask
import argparse
import pymongo
import sys
import time
import requests
import ssl
import warnings
from urllib3.exceptions import InsecureRequestWarning
import logging
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import padding
from cryptography.hazmat.primitives import hashes
import base64
import json
import binascii
import datetime


class ADDrone(threading.Thread):
    def __init__(self, engine_address, broker_address, mongo_address, api_address, engine_registry_address):
        super().__init__()
        self.engine_address = engine_address
        self.broker_address = broker_address
        #logging.basicConfig(level=logging.DEBUG)
        #self.kafka_producer = KafkaProducer(bootstrap_servers=self.broker_address)
        self.kafka_producer = KafkaProducer(
            bootstrap_servers=self.broker_address,
            value_serializer=lambda m: json.dumps(m).encode('utf-8'),
            #security_protocol='SSL',
            ssl_cafile='ssl/certificado_CA.crt',
            ssl_certfile='ssl/certificado_registry.crt',
            ssl_keyfile='ssl/clave_privada_registry.pem'
        )
        self.api_address = api_address
        self.registry_address = engine_registry_address  # Usamos el argumento proporcionado
        self.final_position = None
        self.current_position = (1, 1)
        self.base_position = (1, 1)
        self.dron_id = None
        self.alias = None
        self.access_token = None
        self.mongo_client = pymongo.MongoClient(mongo_address)  # Usamos el argumento proporcionado
        self.db = self.mongo_client["dronedb"]
        
        self.registered_drones = {}
        self.in_show_mode = False
        self.token_time_received = None  # Tiempo en el que se recibió el token
        self.last_heartbeat_received = time.time()
        self.heartbeat_timeout = 20  
        self.token_expiration_time = None
        
        self.public_key = None
        self.private_key = None
                
        warnings.filterwarnings('ignore', category=InsecureRequestWarning)
        #logging.basicConfig(level=logging.DEBUG, filename='app.log', filemode='w', format='%(name)s - %(levelname)s - %(message)s')
     
    def start(self):
        self.show_menu()
        
    def is_token_expired(self):
        return datetime.datetime.now() > self.token_expiration_time
                    
    def generate_keys(self):
        try:
            if self.dron_id is None:
                raise ValueError("Drone ID is not set. Can't generate keys.")
            
            private_key = rsa.generate_private_key(public_exponent=65537, key_size=2048, backend=default_backend())
            public_key = private_key.public_key()
            self.private_key = private_key
            self.public_key = public_key

            key_document = {
                'ID': self.dron_id,
                'PrivateKey': private_key.private_bytes(encoding=serialization.Encoding.PEM, format=serialization.PrivateFormat.TraditionalOpenSSL, encryption_algorithm=serialization.NoEncryption()),
                'PublicKey': public_key.public_bytes(encoding=serialization.Encoding.PEM, format=serialization.PublicFormat.SubjectPublicKeyInfo)
            }
            self.db.Claves.replace_one({'ID': self.dron_id}, key_document, upsert=True)
        except Exception as e:
            print(f"Error al generar claves: {e}")
            return None
        
    def start_consuming_messages_sin_cifrar(self):
        print("ADDrone: Esperando mensajes de Kafka...")
        consumer = KafkaConsumer(
            'drone_final_position',
            bootstrap_servers=self.broker_address,
            auto_offset_reset='latest',
            enable_auto_commit=True,
            group_id='drone-group-{}'.format(self.dron_id),
            value_deserializer=lambda x: json.loads(x.decode('utf-8')),
            ssl_cafile='ssl/certificado_CA.crt',
            ssl_certfile='ssl/certificado_registry.crt',
            ssl_keyfile='ssl/clave_privada_registry.pem'
        )

        for message in consumer:
            try:
                # Convertir el mensaje a un diccionario si es necesario
                if isinstance(message.value, str):
                    message_dict = json.loads(message.value)
                else:
                    message_dict = message.value

                if message_dict['type'] == 'final_position':
                    self.handle_final_position_message(message_dict)
                elif message_dict['type'] == 'instruction':
                    self.handle_instruction_message(message_dict)
                else:
                    print("Mensaje no reconocido o falta información clave.")

            except Exception as e:
                print(f"Error al procesar el mensaje de Kafka: {e}")
                print("Mensaje recibido:", message.value)


    def start_consuming_messages(self):
        print("ADDrone: Esperando mensajes de Kafka...1")
        consumer = KafkaConsumer(
            'drone_messages_topic',
            bootstrap_servers=self.broker_address,
            auto_offset_reset='latest',
            enable_auto_commit=True,
            group_id='drone-group-{}'.format(self.dron_id),
            value_deserializer=lambda x: json.loads(x.decode('utf-8')),
            #security_protocol='SSL',
            ssl_cafile='ssl/certificado_CA.crt',
            ssl_certfile='ssl/certificado_registry.crt',
            ssl_keyfile='ssl/clave_privada_registry.pem'
        )

        for message in consumer:
            try:
                try:
                    decoded_message = base64.b64decode(message.value).decode('utf-8')
                    # Intenta descifrar el mensaje
                    decrypted_message_json = self.decrypt_message(decoded_message)
                    if decrypted_message_json is None:
                        continue
                    # Asumir que el mensaje descifrado es un JSON y convertirlo en diccionario
                    decrypted_message = json.loads(decrypted_message_json)
                except (binascii.Error, ValueError, json.JSONDecodeError):
                    decrypted_message = json.loads(message.value)

                # Asegúrate de que decrypted_message es un diccionario
                if not isinstance(decrypted_message, dict):
                    raise ValueError("El mensaje descifrado no es un diccionario")

                # Ahora usa `decrypted_message` para el resto de la lógica
                elif message.topic == 'drone_messages_topic':
                    self.handle_instruction_message(decrypted_message)
                elif message.topic == 'drone_final_position':
                    self.handle_final_position_message(decrypted_message)

            except Exception as e:
                print(f"Error al procesar el mensaje de Kafka: {e}")

    def decrypt_message(self, encrypted_message):
        try:
            # Decodificar el mensaje de Base64
            if not isinstance(encrypted_message, bytes):
            # Si no es bytes, intentar decodificar desde Base64
                encrypted_data = base64.b64decode(encrypted_message)
            else:
                encrypted_data = encrypted_message
            decrypted_data = self.private_key.decrypt(
                encrypted_data,
                padding.OAEP(
                    mgf=padding.MGF1(algorithm=hashes.SHA256()),
                    algorithm=hashes.SHA256(),
                    label=None
                )
            )
            # Convertir datos desencriptados a cadena
            decrypted_message = decrypted_data.decode()
            # Convertir cadena a objeto JSON
            message_json = json.loads(decrypted_message)
            # Aquí puedes agregar validación adicional del formato de mensaje_json
            if not isinstance(message_json, dict):
                logging.error("Mensaje desencriptado no tiene el formato esperado")
                return None
            print("Datos recibidos de Kafka:", message_json)
            return message_json
        except Exception as e:
            print(f"Error al desencriptar mensaje: {e}")
            return None
        

    def handle_instruction_message(self, message):
        try:
            instruction = message.get('instruction')
            if instruction == 'join_show':
                self.in_show_mode = True
                print(f"ADDrone: Uniendo al show con ID {self.dron_id}...")
            if instruction == 'START':
                if self.in_show_mode:
                    self.in_show_mode = True
                    print("ADDrone: Instrucción START recibida, iniciando movimiento hacia posición final...")
                    self.move_to_final_position()
                else:
                    print("ADDrone: Ya en modo show, ignorando instrucción START.")
            elif instruction == 'STOP':
                print("ADDrone: Instrucción STOP recibida, deteniendo y regresando a la base...")
                self.final_position = self.base_position
                self.in_show_mode = False
                self.return_to_base()
                self.in_show_mode = True
        
        except json.JSONDecodeError:
            print(f"Received non-JSON message: {message.value}")
        
            
    def handle_final_position_message(self, message):
        try:
            if 'final_position' in message and 'dron_id' in message:
                message_dron_id = message['dron_id']
                final_position = message['final_position']

                if message_dron_id == self.dron_id:
                    if final_position:
                        self.final_position = tuple(final_position)
                        print(f"ADDrone: Nueva posición final recibida: {self.final_position}")
                        if self.in_show_mode:
                            print("ADDrone: Iniciando movimiento hacia la nueva posición final...")
                            self.move_to_final_position()
            else:
                print(f"ADDrone: Mensaje no contiene 'final_position' o 'dron_id'. Mensaje recibido: {message}")
        except Exception as e:
            print(f"Error al procesar el mensaje de posición final: {e}")
            
            
    def return_to_base(self):
        while self.current_position != self.base_position:
            self.calculate_movement()
            self.send_position_update()
            time.sleep(1)
        print(f"ADDrone: Posición base alcanzada: {self.current_position}")

    def move_to_final_position(self):
        if not self.in_show_mode:
            return
        while self.current_position != self.final_position and self.in_show_mode:
            self.calculate_movement()
            self.send_position_update()
            time.sleep(1)
        if self.current_position == self.final_position:
            self.send_kafka_message('drone_position_reached', {
                'type': 'position_reached',
                'dron_id': self.dron_id,
                'final_position': self.final_position
            })
            print(f"ADDrone: Posición {('final' if self.in_show_mode else 'base')} alcanzada: {self.current_position}")


    def calculate_movement(self):
        next_x, next_y = self.current_position
        final_x, final_y = self.final_position

        if next_x < final_x:
            next_x += 1
        elif next_x > final_x:
            next_x -= 1

        if next_y < final_y:
            next_y += 1
        elif next_y > final_y:
            next_y -= 1

        self.current_position = (next_x, next_y)


    def send_kafka_message(self, topic, message):
        try:
            # Convertir el mensaje original a JSON
            original_message_json = json.dumps(message)

            #Cifrar el mensaje
            encrypted_data = self.public_key.encrypt(
                original_message_json.encode(),
                padding.OAEP(
                    mgf=padding.MGF1(algorithm=hashes.SHA256()),
                    algorithm=hashes.SHA256(),
                    label=None
                )
            )

            #Codificar en base64
            encoded_message = base64.b64encode(encrypted_data).decode('utf-8')
            #Enviar a Kafka
            self.kafka_producer.send(topic, key=str(self.dron_id).encode(), value=encoded_message)
            self.kafka_producer.flush()

            #Almacenar en MongoDB
            kafka_message_document = {
                'Clase': 'ADDrone',
                'Topic': topic,
                'OriginalMessage': message,
                'EncryptedMessage': encoded_message
            }
            self.db.MensajesKafka.insert_one(kafka_message_document)
            #TODO: arreglar
            #self.log_auditoria('Mensaje enviado', f"Mensaje enviado a Kafka: {encoded_message}", tipo='encrypted')

        except Exception as e:
            print(f"Error al enviar mensaje Kafka: {e}")


    def send_position_update(self):
        message = {
            'type': 'position_update',
            'ID': self.dron_id,
            'Position': self.current_position
        }
        #self.send_kafka_message('drone_position_updates', message)
        self.kafka_producer.send('drone_position_updates', value=message)
        self.kafka_producer.flush()
        print(f"Nueva posición: {self.current_position}")
    
    def input_drone_data(self):
        self.ensure_token_valid()
        if not (self.access_token):
            print("Es necesario registrarse y obtener un token antes de unirse al show.")
            return

        headers = {'Authorization': f'Bearer {self.access_token}'}
        while True:
            user_input = input("Introduce el ID del dron (número entre 1 y 99): ")
            alias = input("Introduce el alias del dron: ")

            if user_input.strip() and alias.strip(): 
                try:
                    dron_id = int(user_input)

                    if 1 <= dron_id <= 99:
                        mongo_address = args.mongo_address 
                        mongo_client = pymongo.MongoClient(mongo_address)
                        #mongo_client = pymongo.MongoClient("mongodb://localhost:27017/")
                        db = mongo_client["dronedb"]
                        drones_collection = db["drones"]
                        existing_dron = drones_collection.find_one({"ID": dron_id})
                        
                        if existing_dron:
                            print("ID de dron ya existe. Introduce un ID diferente.")
                        else:
                            self.dron_id = dron_id
                            self.alias = alias
                            self.generate_keys()
                            self.choose_registration_method()
                            break  
                    else:
                        print("El ID del dron debe estar entre 1 y 99. Inténtalo de nuevo.")
                except ValueError:
                    print("Entrada inválida. Debes ingresar un número válido.")
            else:
                print("Ambos campos son obligatorios. Introduce el ID y el alias del dron.")


    def id_exists(self, drone_id):
        return self.db.drones.find_one({'_id': drone_id}) is not None
    

    def register_drone(self):
        try:
            host, port = self.registry_address.split(':')
            registry_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            registry_socket.connect((host, int(port)))
            dron_data = {'ID': self.dron_id, 'Alias': self.alias}

            # Cifrar los datos
            encrypted_data = self.public_key.encrypt(
                json.dumps(dron_data).encode(),
                padding.OAEP(
                    mgf=padding.MGF1(algorithm=hashes.SHA256()),
                    algorithm=hashes.SHA256(),
                    label=None
                )
            )

            # Preparar datos para enviar
            data_to_send = json.dumps({'ID': self.dron_id, 'Data': base64.b64encode(encrypted_data).decode()}).encode()
            # Enviar datos cifrados
            registry_socket.sendall(data_to_send)
            # Recibir y procesar respuesta
            response = registry_socket.recv(1024).decode()
            response_json = json.loads(response)
            registry_socket.close()
            
            if self.id_exists(self.dron_id):
                print("ID de dron ya existe. Introduce un ID diferente.")
                return
            if response_json.get('status') == 'success':
                self.access_token = response_json.get('token', '')
                self.registered_drones[self.dron_id] = self.access_token
                print(f"Registro exitoso del dron {self.dron_id}.")
                
                full_message = {
                    'type': 'register',
                    'ID': self.dron_id,
                    'Alias': self.alias,
                    'InitialPosition': self.current_position
                }
                self.send_kafka_message('drone_messages_topic', full_message)
                self.log_auditoria('Registro', f"Registro exitoso del dron {self.dron_id}", tipo='drone')
            else:
                print(f"Error en el registro: {response_json['message']}")
        except ConnectionRefusedError:
            print("Error: El registro no está funcionando. Por favor, inicia el módulo de registro.")
        except Exception as e:
            print(f"Error inesperado: {e}")
            
                   
    def register_via_api(self):
        self.ensure_token_valid()
        data = {'ID': str(self.dron_id), 'Alias': self.alias}
        headers = {'Authorization': f'Bearer {self.access_token}'}
        response = requests.post(f'{self.api_address}/registro', json=data, headers=headers, verify=False)
        if self.id_exists(self.dron_id):
            print("ID de dron ya existe. Introduce un ID diferente.")
            return
        if response.status_code == 201:
            print(f"Registrado via API para el dron {self.dron_id}.")
            self.log_auditoria('Registro de dron via API', f'Dron {self.dron_id} registrado exitosamente via API.', tipo='drone')
        else:
            print(f"Error al registrar via API: {response.text}")

    def choose_registration_method(self):
        method = input("Elige el método de registro (1 - Registro directo, 2 - Registro via API): ")
        if method == "1":
            self.register_drone()
        if method == "2":
            self.register_via_api()
            

    def delete_drones(self):
        self.ensure_token_valid()
        self.dron_id = input("Introduce el ID del dron: ")

        headers = {'Authorization': f'Bearer {self.access_token}'}
        api_address = args.api_address
        try:
            response = requests.delete(f'{api_address}/borrar_dron/{self.dron_id}', headers=headers, verify=False)
            
            if response.status_code == 200:
                print("Dron eliminado exitosamente.")
                return
            else:
                print(f"Error al eliminar dron en la API: {response.text}")
        except requests.exceptions.ConnectionError:
            print("No se pudo conectar a la API. Intentando eliminar desde la base de datos local.")

        mongo_address = args.mongo_address
        mongo_client = pymongo.MongoClient(mongo_address)
        db = mongo_client["dronedb"]
        drones_collection = db["drones"]

        try:
            result = drones_collection.delete_one({'ID': int(self.dron_id)})
            if result.deleted_count == 1:
                print(f"Dron con ID {self.dron_id} eliminado de la base de datos.")
                self.log_auditoria('Eliminación de dron local', f'Dron {self.dron_id} eliminado de la base de datos local.',tipo='drone')
            else:
                print(f"No se encontró el dron con ID {self.dron_id} en la base de datos.")
        except Exception as e:
            print(f"Error al eliminar dron de la base de datos: {e}")   


    def join_show(self):
        self.ensure_token_valid()
        if not (self.dron_id and self.alias and self.access_token):
            print("Es necesario registrarse y obtener un token antes de unirse al show.")
            return

        headers = {'Authorization': f'Bearer {self.access_token}'}
        waiting_for_figure = True
        while waiting_for_figure:
            try:
                # Crear un contexto SSL para el cliente
                context = ssl.create_default_context(ssl.Purpose.SERVER_AUTH)
                # Cargar el certificado de la autoridad certificadora
                context.load_verify_locations('ssl/certificado_CA.crt')
                context.load_cert_chain(certfile='ssl/certificado_registry.crt', keyfile='ssl/clave_privada_registry.pem')
                #with socket.create_connection((host, port)) as sock:
                #    with context.wrap_socket(sock, server_hostname=host) as secure_sock:
                #with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as raw_socket:
                    # Envolver el socket en el contexto SSL
                #    secure_socket = context.wrap_socket(raw_socket, server_hostname="registry")

                #    secure_socket.connect(self.engine_address)
                #    join_message = {'action': 'join', 'ID': self.dron_id, 'Alias': self.alias}
                #    secure_socket.send(json.dumps(join_message).encode('utf-8'))
                with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as engine_socket:
                    engine_socket.connect(self.engine_address)

                    # Preparar el mensaje para enviar
                    join_message = {'action': 'join', 'ID': self.dron_id, 'Alias': self.alias}
                    encoded_message = json.dumps(join_message).encode()

                    # Cifrar el mensaje
                    encrypted_message = self.public_key.encrypt(
                        encoded_message,
                        padding.OAEP(
                            mgf=padding.MGF1(algorithm=hashes.SHA256()),
                            algorithm=hashes.SHA256(),
                            label=None
                        )
                    )

                    # Preparar el mensaje final con ID y datos cifrados
                    final_message = json.dumps({'ID': self.dron_id, 'Data': base64.b64encode(encrypted_message).decode()}).encode()
                    self.db.MensajesKafka.insert_one({
                        'DroneID': self.dron_id,
                        'MessageType': 'join',
                        'OriginalMessage': join_message,
                        'EncryptedMessage': base64.b64encode(encrypted_message).decode()
                    })
                    engine_socket.sendall(final_message)
                    
                    response_data = engine_socket.recv(1024).decode()
                    if response_data:
                        response = json.loads(response_data)
                        if 'final_position' in response:
                            if isinstance(response['final_position'], list):
                                self.final_position = tuple(response['final_position'])
                                print(f"Drone {self.dron_id} has received final position: {self.final_position}")
                                self.in_show_mode = True
                                threading.Thread(target=self.start_consuming_messages_sin_cifrar, daemon=True).start()
                                waiting_for_figure = False  # Cambia el estado a modo activo
                                self.log_auditoria('Unión al show', f'Dron {self.dron_id} unido al show con posición final {self.final_position}.', tipo='drone')
                            else:
                                print("Invalid format for final position received from the server.")
                        else:
                            print(f"Drone {self.dron_id} is not needed for the current figure. Waiting for the next figure...")
                            time.sleep(10)  # Espera antes de reintentar
                    else:
                        print(f"Drone {self.dron_id} did not receive a response from the server. Retrying in 10 seconds...")
                        time.sleep(10)  # Espera antes de reintentar

            except (ConnectionError, json.JSONDecodeError, socket.timeout) as e:
                print(f"An error occurred: {e}. Retrying in 10 seconds...")
                time.sleep(10)  # Espera antes de reintentar


    def register_user(self):
        while True:
            username = input("Introduce tu nombre de usuario: ")
            password = input("Introduce tu contraseña: ")

            if not username or not password:
                print("Nombre de usuario y contraseña son obligatorios.")
                continue
            response = requests.post(f'{self.api_address}/registro_usuario', json={'username': username, 'password': password}, verify=False)

            if response.status_code == 201:
                print("Usuario registrado exitosamente.")
                break
            elif response.status_code == 409:
                print("El nombre de usuario ya está en uso. Por favor, elige otro.")
            else:
                print(f"Error al registrar usuario: {response.json().get('msg', 'Error desconocido')}")
                break


    def get_jwt_token(self):
        print("¿Ya tienes un usuario? (si/no): ")
        tiene_usuario = input().strip().lower()

        if tiene_usuario != "si":
            self.register_user()

        username = input("Introduce tu nombre de usuario: ")
        password = input("Introduce tu contraseña: ")
        self.access_token = self.request_jwt_token(username, password)
        if self.access_token:
            self.token_time_received = time.time()
            self.token_expiration_time = datetime.datetime.now() + datetime.timedelta(seconds=20)
            print("Token JWT obtenido con éxito.")
            self.log_auditoria('Token obtenido', f"Usuario {username} ha obtenido el token exitoso", tipo='drone')
        else:
            print("Error al obtener token JWT")

    def request_jwt_token(self, username, password):
        try:
            response = requests.post(f'{self.api_address}/login', json={'username': username, 'password': password}, verify=False)
            if response.status_code == 200:
                return response.json().get('access_token')
            else:
                print(f"Error al obtener token JWT: {response.text}")
                return None
        except requests.exceptions.ConnectionError:
            print("No se pudo conectar a la API. Por favor, inicia el módulo de API.")
            return None
    
    def ensure_token_valid(self):
        if self.is_token_expired():
            print("Token expirado, obteniendo uno nuevo...")
            self.get_jwt_token()
        
    #TODO: An error occurred: 'NoneType' object has no attribute 'encrypt'
    def take_over_drone(self):
        print("Seleccionando un dron existente para controlar...")
        drones = self.list_drones_in_db()
        if not drones:
            print("No hay drones disponibles para controlar.")
            return

        for idx, drone in enumerate(drones, start=1):
            print(f"{idx}. ID: {drone['ID']}, Alias: {drone['Alias']}")

        choice = input("Selecciona el número del dron que deseas controlar: ")
        try:
            selected_idx = int(choice) - 1
            if 0 <= selected_idx < len(drones):
                selected_drone = drones[selected_idx]
                self.dron_id = selected_drone['ID']
                self.alias = selected_drone['Alias']
                self.load_drones_keys(self.dron_id)
                print(f"Has tomado el control del dron ID: {self.dron_id}, Alias: {self.alias}")
            else:
                print("Selección inválida.")
        except ValueError:
            print("Por favor, introduce un número válido.")
            
    def load_drones_keys(self,drone_id):
        try:
            key_document = self.db.Claves.find_one({'ID': drone_id})
            if key_document:
                self.private_key = serialization.load_pem_private_key(
                    key_document['PrivateKey'],
                    password=None,
                    backend=default_backend()
                )
                self.public_key = serialization.load_pem_public_key(
                    key_document['PublicKey'],
                    backend=default_backend()
                )
                print(f"Claves cargadas para el dron ID {drone_id}")
            else:
                print(f"No se encontraron claves para el dron ID {drone_id}")
        except Exception as e:
            print(f"Error al cargar claves para el dron ID {drone_id}: {e}")

    def list_drones_in_db(self):
        try:
            drones = list(self.db.drones.find({}, {'ID': 1, 'Alias': 1}))
            return [{'ID': drone['ID'], 'Alias': drone.get('Alias', 'Sin alias')} for drone in drones]
        except Exception as e:
            print(f"Error al listar drones: {e}")
            return []
    
    def cleanbd(self):
        try:
            self.db.Claves.delete_many({})
            self.db.MensajesKafka.delete_many({})
            self.db.drones.delete_many({})
            print("Base de datos limpiada.")
        except Exception as e:
            print(f"Error al limpiar la base de datos: {e}")
    
    #TODO: Implementar el método de auditoría      
    def log_auditoria(self, evento, descripcion, tipo):
        try:
            data = {
                'evento': evento,
                'descripcion': descripcion,
                'timestamp': time.strftime("%Y-%m-%d %H:%M:%S"),
                'tipo':tipo
            }
            response = requests.post(f'{self.api_address}/auditoria', json=data, verify=False)
            if response.status_code == 201:
                print(f"Evento de auditoría registrado: {evento}")
            else:
                print(f"Error al registrar evento de auditoría: {response.text}")
        except requests.exceptions.RequestException as e:
            print(f"Error de conexión al registrar evento de auditoría: {e}")
           
    def show_menu(self):
        options = {
            "1": self.input_drone_data,
            "2": self.join_show,
            "3": self.delete_drones,
            "4": self.get_jwt_token,
            "5": self.take_over_drone,
            "6": self.cleanbd
        }
        try:
            while True:
                if not self.in_show_mode:
                    print("\nDrone Menu:")
                    print("1. Enter Drone Data")
                    print("2. Join Show")
                    print("3. Delete Drone")
                    print("4. Get Token")
                    print("5. Take Over Drone")
                    print("6. Clean Database")
                    print("7. Exit")
                    choice = input("Select an option: ")
                    if choice == "7":
                        try:
                            self.db.Claves.delete_one({'ID': self.dron_id})
                            print(f"Database entries for drone ID {self.dron_id} deleted.")
                        except Exception as e:
                            print(f"Error deleting database entries: {e}")
                        break
                    action = options.get(choice)
                    if action:
                        action()
                    else:
                        print("Invalid option. Please select a valid one.")
        except KeyboardInterrupt:
            print("Ctrl+C pressed. Shutting down...")
        except Exception as e:
            print(f"An error occurred: {e}")
        finally:
            self.mongo_client.close()
            sys.exit()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='ADDrone start-up arguments')
    parser.add_argument('--engine_ip', type=str, default='127.0.0.1', help='IP address of the ADEngine')
    parser.add_argument('--engine_port', type=int, default=8080, help='Port number of the ADEngine')
    parser.add_argument('--engine_registry_address', type=str, default='localhost:8081', help='Address of the ADEngine Registry')
    parser.add_argument('--broker_address', type=str, default='localhost:29092', help='Address of the Kafka broker')
    parser.add_argument('--mongo_address', type=str, default='localhost:27017', help='Address of the MongoDB server')
    parser.add_argument('--api_address', type=str, default='https://localhost:5000', help='Address of the API server')
    parser.add_argument('--drones_count', type=int, default=1, help='Number of drones to automatically register and join')
    args = parser.parse_args()
    
    drones = []
    dron = ADDrone(
        (args.engine_ip, args.engine_port),
        args.broker_address,
        args.mongo_address,
        args.api_address,
        args.engine_registry_address
    )
    drones.append(dron)

    for dron in drones:
        dron.start()
        
    #python AD_Drone.py --engine_ip 172.20.10.6 --engine_port 8080 --api_address https://172.20.10.7:5000