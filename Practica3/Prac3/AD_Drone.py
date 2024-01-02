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
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import padding
from cryptography.hazmat.primitives import hashes
import base64


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
        
        self.public_key = None
        self.private_key = None
        
        warnings.filterwarnings('ignore', category=InsecureRequestWarning)

        
    def start(self):
        self.show_menu()
        
        
    def generate_keys(self):
        if self.dron_id is None:
            raise ValueError("Drone ID is not set. Can't generate keys.")
        
        # Generar y guardar las claves
        private_key = rsa.generate_private_key(public_exponent=65537, key_size=2048, backend=default_backend())
        public_key = private_key.public_key()
        self.private_key = private_key
        self.public_key = public_key

        private_key_file = f"private_key_{self.dron_id}.pem"
        public_key_file = f"public_key_{self.dron_id}.pem"

        with open(private_key_file, 'wb') as f:
            f.write(private_key.private_bytes(encoding=serialization.Encoding.PEM, format=serialization.PrivateFormat.TraditionalOpenSSL, encryption_algorithm=serialization.NoEncryption()))

        with open(public_key_file, 'wb') as f:
            f.write(public_key.public_bytes(encoding=serialization.Encoding.PEM, format=serialization.PublicFormat.SubjectPublicKeyInfo))

        
    def handle_drone_registered_message(self, message):
        if message.value.get('ID') == self.dron_id:
            print(f"ADDrone: Registrado exitosamente")
    

    def start_consuming_messages(self):
        print("ADDrone: Esperando mensajes de Kafka...")
        consumer = KafkaConsumer(
            'drone_messages_topic',
            'drones_registered',
            'drone_final_position',
            bootstrap_servers=self.broker_address,
            auto_offset_reset='latest',
            enable_auto_commit=True,
            group_id='drone-group-{}'.format(self.dron_id),
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            #security_protocol='SSL',
            ssl_cafile='ssl/certificado_CA.crt',
            ssl_certfile='ssl/certificado_registry.crt',
            ssl_keyfile='ssl/clave_privada_registry.pem'
        )

        for message in consumer:
            if message.topic == 'drones_registered':
                self.handle_drone_registered_message(message)
            if message.topic == 'drone_messages_topic':
                self.handle_instruction_message(message)
            if message.topic == 'drone_final_position':
                self.handle_final_position_message(message)
                
        

    def handle_instruction_message(self, message):
        instruction = message.value.get('instruction')
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
            self.move_to_final_position()

    def handle_final_position_message(self, message):
        if message.value.get('dron_id') == self.dron_id:
            final_position = message.value.get('final_position')
            if final_position:
                self.final_position = tuple(final_position)



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

            # Cifrar el mensaje
            encrypted_data = self.public_key.encrypt(
                original_message_json.encode(),
                padding.OAEP(
                    mgf=padding.MGF1(algorithm=hashes.SHA256()),
                    algorithm=hashes.SHA256(),
                    label=None
                )
            )

            # Codificar en base64
            encoded_message = base64.b64encode(encrypted_data).decode('utf-8')

            # Enviar a Kafka
            self.kafka_producer.send(topic, value=encoded_message)
            self.kafka_producer.flush()
            print("Mensaje Kafka enviado (cifrado y codificado):", encoded_message)

            # Almacenar en MongoDB
            kafka_message_document = {
                'Clase': 'ADDrone',
                'OriginalMessage': message,
                'EncryptedMessage': encoded_message
            }
            self.db.MensajesKafka.insert_one(kafka_message_document)

        except Exception as e:
            print(f"Error al enviar mensaje Kafka: {e}")





    def send_position_update(self):
        message = {
            'type': 'position_update',
            'ID': self.dron_id,
            'Position': self.current_position
        }
        self.kafka_producer.send('drone_position_updates', value=json.dumps(message).encode('utf-8'))
        self.kafka_producer.flush()
        print(f"Nueva posición: {self.current_position}")
    
    def input_drone_data(self):
        while True:
            user_input = input("Introduce el ID del dron (número entre 1 y 99): ")
            alias = input("Introduce el alias del dron: ")

            if user_input.strip() and alias.strip(): 
                try:
                    dron_id = int(user_input)

                    if 1 <= dron_id <= 99:
                        mongo_address = args.mongo_address  # args.mongo_address es el argumento que recibes
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

            public_key_file = f"public_key_{self.dron_id}.pem"

            # Carga la clave pública del archivo
            with open(public_key_file, "rb") as key_file:
                public_key = serialization.load_pem_public_key(
                    key_file.read(),
                    backend=default_backend()
                )

            # Datos del dron
            dron_data = {'ID': self.dron_id, 'Alias': self.alias}

            # Cifrar los datos
            encrypted_data = public_key.encrypt(
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
                print(f"Registro exitoso. Token de acceso: {self.access_token}")
                
                full_message = {
                    'type': 'register',
                    'ID': self.dron_id,
                    'Alias': self.alias,
                    'InitialPosition': self.current_position
                }
                #Cifrar y enviar el mensaje a Kafka
                self.send_kafka_message('drone_messages_topic', full_message)
            else:
                print(f"Error en el registro: {response_json['message']}")
        except ConnectionRefusedError:
            print("Error: El registro no está funcionando. Por favor, inicia el módulo de registro.")
        except Exception as e:
            print(f"Error inesperado: {e}")
            
            
            
    def register_via_api(self):
        data = {'ID': str(self.dron_id), 'Alias': self.alias}
        headers = {'Authorization': f'Bearer {self.access_token}'}
        api_address = args.api_address  # args.api_address es el argumento que recibes
        response = requests.post(f'{api_address}/registro', json=data, headers=headers, verify=False)
        #response = requests.post('http://localhost:5000/registro', json=data, headers=headers)
        if self.id_exists(self.dron_id):
            print("ID de dron ya existe. Introduce un ID diferente.")
            return
        if response.status_code == 201:
            print(f"Registrado via API para el dron {self.dron_id}.")
        else:
            print(f"Error al registrar via API: {response.text}")

    def choose_registration_method(self):
        method = input("Elige el método de registro (1 - Registro directo, 2 - Registro via API): ")
        if method == "1":
            self.register_drone()
        if method == "2":
            self.register_via_api()
    
    
    
    def request_final_position_from_db(self):
        # Conectar a la base de datos de MongoDB
        mongo_address = args.mongo_address  # args.mongo_address es el argumento que recibes
        mongo_client = pymongo.MongoClient(mongo_address)
        #mongo_client = pymongo.MongoClient("mongodb://localhost:27017/")
        db = mongo_client["dronedb"]
        drones_collection = db["drones_fp"]

        # Consultar la posición final del dron por su ID
        drone_data = drones_collection.find_one({"ID": self.dron_id})

        if drone_data and "FinalPosition" in drone_data:
            # Si la posición final ha cambiado, imprimir el mensaje de la nueva figura
            if self.final_position and self.final_position != drone_data["FinalPosition"]:
                print("\nSiguiente figura con esta posición final:")
            
            self.final_position = drone_data["FinalPosition"]
            print(f"{self.final_position}")
        else:
            print(f"No se pudo obtener la posición final para el dron ID: {self.dron_id}")
        # Cerrar la conexión con la base de datos
        mongo_client.close()

    def delete_drones(self):
        self.dron_id = input("Introduce el ID del dron: ")

        # Primero intenta eliminar el dron a través de la API
        headers = {'Authorization': f'Bearer {self.access_token}'}
        api_address = args.api_address  # args.api_address es el argumento que recibes
        response = requests.delete(f'{api_address}/borrar_dron/{self.dron_id}', headers=headers, verify=False)
        #response = requests.delete(f'http://localhost:5000/borrar_dron/{self.dron_id}', headers=headers)
        
        if response.status_code == 200:
            print("Dron eliminado exitosamente.")
        else:
            print(f"Error al eliminar dron en la API: {response.text}")

            # Si la API falla, intenta eliminarlo de la base de datos local
            mongo_address = args.mongo_address  # args.mongo_address es el argumento que recibes
            mongo_client = pymongo.MongoClient(mongo_address)
            #mongo_client = pymongo.MongoClient("mongodb://localhost:27017/")
            db = mongo_client["dronedb"]
            drones_collection = db["drones"]

            try:
                result = drones_collection.delete_one({'ID': int(self.dron_id)})
                if result.deleted_count == 1:
                    print(f"Dron con ID {self.dron_id} eliminado de la base de datos.")
                else:
                    print(f"No se encontró el dron con ID {self.dron_id} en la base de datos.")
            except Exception as e:
                print(f"Error al eliminar dron de la base de datos: {e}")


    def join_show(self):
        if not (self.dron_id and self.alias and self.access_token):
            print("Es necesario registrarse y obtener un token antes de unirse al show.")
            return

        headers = {'Authorization': f'Bearer {self.access_token}'}
        
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
                join_message = {'action': 'join', 'ID': self.dron_id, 'Alias': self.alias}
                engine_socket.send(json.dumps(join_message).encode('utf-8'))
                
                response_data = engine_socket.recv(1024).decode()
            #    response_data = secure_socket.recv(1024).decode()
                if response_data:
                    response = json.loads(response_data)
                    if 'final_position' in response:
                        if isinstance(response['final_position'], list):
                            self.final_position = tuple(response['final_position'])
                        else:
                            print("Invalid format for final position received from the server.")
                        print(f"Drone {self.dron_id} has received final position: {self.final_position}")

                        # Iniciar el consumo de mensajes en un hilo separado
                        self.in_show_mode = True
                        threading.Thread(target=self.start_consuming_messages, daemon=True).start()
                    else:
                        print(f"Drone {self.dron_id} failed to join the show. Server response: {response}")
                else:
                    print(f"Drone {self.dron_id} failed to join the show. No response from server.")
        except ssl.SSLError as e:
            print(f"Error SSL: {e}")
        except json.JSONDecodeError:
            print("Failed to decode the server response. Ensure the server sends a valid JSON.")
        except ConnectionError as e:
            print(f"Unable to connect to the ADEngine: {e}")



    def register_user(self):
        while True:
            username = input("Introduce tu nombre de usuario: ")
            password = input("Introduce tu contraseña: ")

            if not username or not password:
                print("Nombre de usuario y contraseña son obligatorios.")
                continue
            api_address = args.api_address  # args.api_address es el argumento que recibes
            response = requests.post(f'{api_address}/registro_usuario', json={'username': username, 'password': password}, verify=False)
            #response = requests.post('http://localhost:5000/registro_usuario', json={'username': username, 'password': password})

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
            print("Token JWT obtenido con éxito.")
        else:
            print("Error al obtener token JWT")

    def request_jwt_token(self, username, password):
        # Solicitar el token JWT a la API
        api_address = args.api_address  # args.api_address es el argumento que recibes
        response = requests.post(f'{api_address}/login', json={'username': username, 'password': password}, verify=False)
        #response = requests.post('http://localhost:5000/login', json={'username': username, 'password': password})
        if response.status_code == 200:
            return response.json().get('access_token')
        else:
            print(f"Error al obtener token JWT: {response.text}")
            return None
        
        
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
                print(f"Has tomado el control del dron ID: {self.dron_id}, Alias: {self.alias}")
            else:
                print("Selección inválida.")
        except ValueError:
            print("Por favor, introduce un número válido.")

    def list_drones_in_db(self):
        try:
            drones = list(self.db.drones.find({}, {'ID': 1, 'Alias': 1}))
            return [{'ID': drone['ID'], 'Alias': drone.get('Alias', 'Sin alias')} for drone in drones]
        except Exception as e:
            print(f"Error al listar drones: {e}")
            return []

               
    def show_menu(self):
        options = {
            "1": self.input_drone_data,
            "2": self.join_show,
            "3": self.delete_drones,
            "4": self.get_jwt_token,
            "5": self.take_over_drone  # Agregar la nueva opción aquí
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
                    print("6. Exit")
                    choice = input("Select an option: ")
                    if choice == "6":
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
    args = parser.parse_args()
    #python tu_script.py --mongo_address mi_servidor_mongodb:27017 --api_address http://mi_servidor_api:5000 --engine_registry_address mi_servidor_engine_registry:8081
    
    drones = []
    dron = ADDrone(
        (args.engine_ip, args.engine_port),
        args.broker_address,
        args.mongo_address,
        args.api_address,
        args.engine_registry_address
    )
    drones.append(dron)

    # Inicia cada dron como un hilo separado
    for dron in drones:
        dron.start()
