import socket, ssl
import json
from kafka import KafkaProducer, KafkaConsumer
import pymongo
import logging
import threading
import argparse
import time
from flask import Flask
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import padding, rsa
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.backends import default_backend
import base64
import requests
import warnings
from urllib3.exceptions import InsecureRequestWarning
from pynput import keyboard  # Correct import for pynput
import sys
import select

# Ignore InsecureRequestWarning
warnings.simplefilter('ignore', InsecureRequestWarning)


#logging.basicConfig(level=logging.DEBUG, filename='app.log', filemode='w', format='%(name)s - %(levelname)s - %(message)s')


class ADEngine:
    def __init__(self, listen_port, max_drones, broker_address, database_address, api_address):
        self.listen_port = listen_port
        self.max_drones = max_drones
        self.broker_address = broker_address
        self.database_address = database_address
        self.api_address = api_address
        self.client = pymongo.MongoClient(self.database_address)
        self.db = self.client["dronedb"]
        self.state_lock = threading.Lock()
        self.drones_state = {}
        self.final_positions = {}
        self.current_positions = {}
        self.connected_drones = set()
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        # Para conectarse desde cualquier IP de la misma red
        self.server_socket.bind(("0.0.0.0", self.listen_port))
        self.server_socket.listen(15)
        self.kafka_producer = KafkaProducer(
            bootstrap_servers=[self.broker_address],
            value_serializer=lambda x: json.dumps(x).encode('utf-8'),
            #security_protocol='SSL',
            ssl_cafile='ssl/certificado_CA.crt',  # El certificado de la Autoridad Certificadora
            ssl_certfile='ssl/certificado_registry.crt',  # Certificado de tu servidor
            ssl_keyfile='ssl/clave_privada_registry.pem'  # Clave privada de tu servidor
        )
        self.accept_thread = threading.Thread(target=self.accept_connections)
        self.accept_thread.start()
        self.last_weather_check = {"city_name": None, "temp_celsius": 10}
        self.private_key = rsa.generate_private_key(
            public_exponent=65537,
            key_size=2048
        )     
        self.public_key = self.private_key.public_key()
        self.drones_involucrados = set()
        self.new_instructions_sent = False
        self.reset_event = threading.Event()

        #Para conexiones seguras
        #self.context = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)
        #self.context.load_cert_chain(certfile="ssl/certificado_registry.crt", keyfile="ssl/clave_privada_registry.pem")

    #SANTI NO PUEDE
    #def accept_connections(self):
        #while True:
            #client_socket, _ = self.server_socket.accept()
            #try:
                # Envolver la conexión del cliente con el contexto SSL
            #    secure_socket = self.context.wrap_socket(client_socket, server_side=True)
            #    threading.Thread(target=self.handle_drone_connection, args=(secure_socket,)).start()
            #except ssl.SSLError as e:
            #    print(f"Error SSL: {e}")
    def accept_connections(self):
        while True:
            client_socket, _ = self.server_socket.accept()
            threading.Thread(target=self.handle_drone_connection, args=(client_socket,)).start()
                
    def handle_drone_connection(self, client_socket):
        try:
            data = client_socket.recv(1024).decode('utf-8')
            data_dict = json.loads(data)

            # Suponiendo que se recibe ID y datos cifrados
            drone_id = data_dict['ID']
            encrypted_data = base64.b64decode(data_dict['Data'])

            # Cargar clave privada y descifrar datos
            private_key = self.load_drone_keys(drone_id)
            if private_key is None:
                raise ValueError(f"No se pudo cargar la clave privada para el dron {drone_id}")

            decrypted_data = private_key.decrypt(
                encrypted_data,
                padding.OAEP(
                    mgf=padding.MGF1(algorithm=hashes.SHA256()),
                    algorithm=hashes.SHA256(),
                    label=None
                )
            )
            self.db.MensajesKafka.insert_one({
                'DroneID': drone_id,
                'Type': 'Incoming a ADEngine',
                'EncryptedMessage': data_dict['Data'],
                'DecryptedMessage': decrypted_data.decode('utf-8')
            })

            #Procesar datos descifrados
            request_json = json.loads(decrypted_data.decode('utf-8'))
            print(f"Datos recibidos del dron {drone_id}: {request_json}")

            if request_json.get('action') == 'join':
                if drone_id in self.final_positions:
                    self.connected_drones.add(drone_id)
                    self.drones_involucrados.add(drone_id)
                    if self.are_all_drones_connected():
                        print("Todos los drones necesarios están conectados. Preparando para iniciar el espectáculo.")
                        self.send_positions_and_start_commands()
                    remaining_drones = self.required_drones - len(self.connected_drones)
                    print(f"Drone ID {drone_id} has joined. {remaining_drones} drones are still required.")
                    self.log_auditoria('JOIN', f"El dron {drone_id} se ha unido al espectáculo.", tipo='engine')
                    final_position = self.final_positions[drone_id]
                    response = {'status': 'success', 'final_position': final_position}
                else:
                    response = {'status': 'not_required'}
            else:
                print(f"Received unknown action from drone: {request_json.get('action')}")
                response = {'status': 'error', 'message': 'Unknown action'}
            
            client_socket.send(json.dumps(response).encode('utf-8'))

        except Exception as e:
            print(f"Error al manejar la conexión del dron: {e}")
        finally:
            client_socket.close()
        
    def load_drone_keys(self, drone_id):
        #Cargar la clave privada del dron desde MongoDB
        try:            
            #obtener la clave privada del dron
            key_document = self.db.Claves.find_one({'ID': drone_id})
            
            #Comprobar si se encontró el documento
            if key_document is None:
                print(f"No se encontró la clave para el dron ID {drone_id}")
                return None
            
            #Cargar la clave privada desde el documento
            private_key_data = key_document['PrivateKey']
            private_key = serialization.load_pem_private_key(
                private_key_data,
                password=None,
                backend=default_backend()
            )
            return private_key

        except Exception as e:
            print(f"Error al cargar clave privada: {e}")
            return None
        
    def encrypted_message(self, topic, message):
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

            # Verificar que los datos cifrados no son None
            if encrypted_data is None:
                raise ValueError("Los datos cifrados son None")
            # Codificar en base64
            encoded_message = base64.b64encode(encrypted_data).decode('utf-8')
            # Verificar que el mensaje codificado no es None
            if encoded_message is None:
                raise ValueError("El mensaje codificado es None")


            # Almacenar en MongoDB
            kafka_message_document = {
                'Clase': 'ADEngine',
                'Topic': topic,
                'OriginalMessage': message,
                'EncryptedMessage': encoded_message
            }
            self.db.MensajesKafka.insert_one(kafka_message_document)
        except Exception as e:
            print(f"Error al enviar mensaje Kafka: {e}")
        
            
            
    def send_encrypted_kafka_message(self, topic, message):
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

            # Verificar que los datos cifrados no son None
            if encrypted_data is None:
                raise ValueError("Los datos cifrados son None")

            # Codificar en base64
            encoded_message = base64.b64encode(encrypted_data).decode('utf-8')

            # Verificar que el mensaje codificado no es None
            if encoded_message is None:
                raise ValueError("El mensaje codificado es None")

            # Enviar a Kafka
            self.kafka_producer.send(topic, value=encoded_message)
            self.encrypted_message(topic, message)
            self.kafka_producer.flush()


            # Almacenar en MongoDB
            kafka_message_document = {
                'Clase': 'ADEngine',
                'Topic': topic,
                'OriginalMessage': message,
                'EncryptedMessage': encoded_message
            }
            self.db.MensajesKafka.insert_one(kafka_message_document)
        except Exception as e:
            print(f"Error al enviar mensaje Kafka: {e}")

            
    def send_instruction_to_drone(self, dron_id, instruction):
        final_position = [1, 1] if instruction == 'RESET' else self.final_positions.get(dron_id, [1, 1])

        message = {
            'type': 'instruction',
            'dron_id': dron_id,
            'instruction': instruction,
            'final_position': final_position
        }
        self.send_encrypted_kafka_message('drone_messages_topic', message)
        self.kafka_producer.flush()
        print(f"Instrucción {instruction} enviada al dron {dron_id}")
        with self.state_lock:
            self.drones_state[dron_id] = {'instruction': instruction, 'reached': False}


            
    def are_all_drones_connected(self):
        return len(self.connected_drones) >= self.required_drones


    def check_weather_warnings(self):
        while True:
            with open('ciudad.json', 'r') as file:
                weather_data = json.load(file)
                city_name = weather_data['name']
                temp_kelvin = weather_data['main']['temp']
                temp_celsius = temp_kelvin - 273.15

                # Comprobar si la temperatura ha cambiado
                if city_name != self.last_weather_check['city_name'] or temp_celsius != self.last_weather_check['temp_celsius']:
                    print(f"Temperatura actual en {city_name}: {temp_celsius}°C")
                    if temp_celsius < 0:
                        self.send_warning_to_drones(city_name, temp_celsius)
                    elif self.last_weather_check['temp_celsius'] < 0 and temp_celsius >= 0:                        
                        self.resend_positions_and_restart_drones()

                    # Actualizar la última comprobación de clima
                    self.last_weather_check['city_name'] = city_name
                    self.last_weather_check['temp_celsius'] = temp_celsius

            time.sleep(5)
            
    def resend_positions_and_restart_drones(self):
        print("Temperatura normalizada. Reenviando posiciones finales y reiniciando drones.")
        for dron_id in self.connected_drones:
            self.send_final_position(dron_id, self.final_positions[dron_id])
            self.send_instruction_to_drone(dron_id, 'START')

    def send_warning_to_drones(self, city_name, temp_celsius):
        warning_message = {
            'type': 'instruction',
            'dron_id': 'all',  # O especificar un ID si solo se dirige a un dron específico
            'instruction': 'STOP',
            'reason': f"Advertencia de clima frío en {city_name}: {temp_celsius}°C"
        }
        self.kafka_producer.send('drone_final_position', warning_message)
        self.encrypted_message('drone_final_position', warning_message)
        self.kafka_producer.flush()
        print(f"Enviada advertencia de clima frío para todos los drones.")
        self.log_auditoria('STOP', 'Advertencia de clima frío enviada a todos los drones.', tipo='engine')


    def reset_engine(self):
        self.reset_event.set()
        # Reset the drones to initial positions
        for dron_id in self.connected_drones:
            self.send_final_position(dron_id, (1, 1))  # Sending initial position (1, 1)
            self.send_instruction_to_drone(dron_id, 'RESET')

        time.sleep(20)
    
        # Reset engine state and load the first figure
        self.final_positions.clear()
        self.current_positions.clear()
        self.drones_state.clear()
        self.indice_figura_actual = 0  # Set index to load the first figure
        self.cargar_figura(self.indice_figura_actual)
        self.send_positions_and_start_commands()  # Start the first figure

        self.reset_event.clear()

    def handle_drone_position_reached(self, dron_id, final_position):
        if self.reset_event.is_set():
            return
        final_position_tuple = tuple(final_position) if isinstance(final_position, list) else final_position
        self.current_positions[dron_id] = final_position_tuple
        with self.state_lock:
            self.drones_state[dron_id]['reached'] = True
        if final_position_tuple == (1, 1):  # Check if drone reached initial position
            self.cargar_figura(0)
        self.gestionar_estado_drones()

    def gestionar_estado_drones(self):
        if self.check_all_drones_in_position():
            print("Todos los drones han alcanzado su posición final.")
            if self.reset_event.is_set():
                self.load_next_figure()  # Load the first figure after reset
            else:
                self.load_next_figure()
        else:
            print("Aún hay drones que no han alcanzado su posición final.")

    def check_all_drones_in_position(self):
        with self.state_lock:
            # Verificar todos los drones requeridos para la figura actual
            for dron_id in self.final_positions.keys():
                # Si algún dron no ha alcanzado su posición, devuelve False
                if dron_id not in self.drones_state or not self.drones_state[dron_id]['reached']:
                    return False
            # Todos los drones han alcanzado su posición
            print("Todos los drones han alcanzado su posición final.")
            return True



    def send_final_position(self, dron_id, final_position):
        message = {
            'type': 'final_position',
            'dron_id': dron_id,
            'final_position': final_position
        }
        self.kafka_producer.send('drone_final_position', message)
        self.encrypted_message('drone_final_position', message)
        self.kafka_producer.flush()


    def start(self):
        print(f"AD_Engine en funcionamiento. Escuchando en el puerto {self.listen_port}...")
        self.kafka_consumer_thread = threading.Thread(target=self.start_kafka_consumer)
        self.kafka_consumer_thread.start()
        self.weather_warning_thread = threading.Thread(target=self.check_weather_warnings)
        self.weather_warning_thread.start()
        self.heartbeat_thread = threading.Thread(target=self.send_heartbeat_messages)
        self.heartbeat_thread.start()
        key_listener_thread = threading.Thread(target=self.listen_for_key_presses)
        key_listener_thread.start()
        
        try:
            while True:
                
                client_socket, addr = self.server_socket.accept()
                print(f"Nueva conexión desde {addr}")
                drone_connection_thread = threading.Thread(target=self.handle_drone_connection, args=(client_socket,))
                drone_connection_thread.start()
        except KeyboardInterrupt:
            print("AD_Engine detenido por el usuario.")
        finally:
            self.close()
            
    def listen_for_key_presses(self):
        while True:
            i, o, e = select.select([sys.stdin], [], [], 1)
            if i:
                input_char = sys.stdin.read(1).strip().lower()
                if input_char == 'r':
                    print("R key pressed. Resetting engine...")
                    self.reset_engine()
    
            
    def send_heartbeat_messages(self):
        while True:
            heartbeat_message = {'type': 'heartbeat'}
            self.kafka_producer.send('engine_heartbeat_topic', heartbeat_message)
            self.kafka_producer.flush()
            time.sleep(5)


    def end_show(self):
        print("El espectáculo ha finalizado.")
        while True:
            user_input = input("¿Deseas reiniciar el espectáculo? (y/n): ").strip().lower()
            if user_input == 'y':
                self.reset_engine()
                return  # Salir del bucle y del método
            elif user_input == 'n':
                self.connected_drones.clear()
                self.final_positions.clear()
                self.current_positions.clear()
                self.drones_state.clear()
                print("El espectáculo ha terminado definitivamente.")
                return  # Salir del bucle y del método
            else:
                print("Entrada no válida. Por favor, introduce 'y' para sí o 'n' para no.")



    def close(self):
        self.stop_event.set()
        if self.kafka_consumer_thread:
            self.kafka_consumer_thread.join()
        self.server_socket.close()
        self.kafka_producer.close()
        print("AD_Engine ha cerrado todos los recursos.")
        
    
    def start_kafka_consumer(self):
        consumer = KafkaConsumer(
            'drone_position_reached',
            bootstrap_servers=self.broker_address,
            auto_offset_reset='latest',
            enable_auto_commit=True,
            group_id='engine-group',
            key_deserializer=lambda x: x.decode('utf-8') if x else None,
            value_deserializer=lambda x: x.decode('utf-8') if x else None,
            ssl_cafile='ssl/certificado_CA.crt',
            ssl_certfile='ssl/certificado_registry.crt',
            ssl_keyfile='ssl/clave_privada_registry.pem'
        )

        for message in consumer:
            try:
                # Convertir el dron_id a entero
                dron_id = int(message.key) if message.key and message.key.isdigit() else None
                if dron_id is None:
                    print(f"ID de dron inválido: {message.key}")
                    continue

                # Decodificar y descifrar
                private_key = self.load_drone_keys(dron_id)
                if private_key is None:
                    print(f"Clave privada no encontrada para el dron ID {dron_id}")
                    continue

                encrypted_data = base64.b64decode(message.value)
                decrypted_data = private_key.decrypt(
                    encrypted_data,
                    padding.OAEP(
                        mgf=padding.MGF1(algorithm=hashes.SHA256()),
                        algorithm=hashes.SHA256(),
                        label=None
                    )
                )
                message_data = json.loads(decrypted_data.decode('utf-8'))

                # Asegurarnos de que message_data es un diccionario
                if not isinstance(message_data, dict):
                    print(f"Formato inesperado de mensaje: {message_data}")
                    continue

                # Procesar el mensaje si es del tipo esperado
                if message_data.get('type') == 'position_reached':
                    final_position = message_data.get('final_position')
                    if final_position is not None:
                        self.handle_drone_position_reached(dron_id, final_position)
                    else:
                        print(f"Posición final no encontrada en el mensaje: {message_data}")
                else:
                    print(f"Tipo de mensaje no reconocido: {message_data.get('type')}")

            except json.JSONDecodeError:
                print("Error al decodificar el mensaje JSON.")
            except TypeError as e:
                print(f"Error en el formato del mensaje: {e}. Se esperaba un diccionario.")
            except KeyError as e:
                print(f"Clave no encontrada en el mensaje: {e}. Mensaje recibido: {message_data}")
            except Exception as e:
                print(f"Error inesperado al procesar el mensaje de Kafka: {e}")
                
    def load_next_figure(self):
        self.indice_figura_actual += 1
        if self.indice_figura_actual < len(self.figuras):
            self.cargar_figura(self.indice_figura_actual)
            self.send_positions_and_start_commands()
        else:
            print("Todas las figuras han sido completadas.")
            self.end_show()

    def procesar_datos_json(self, ruta_archivo_json):
        with open(ruta_archivo_json, 'r') as archivo:
            datos = json.load(archivo)

        self.figuras = datos['figuras']  # Lista de todas las figuras
        self.indice_figura_actual = 0  # Índice para seguir la figura actual
        self.cargar_figura(self.indice_figura_actual)


    def cargar_figura(self, indice_figura):
        figura_actual = self.figuras[indice_figura]
        print(f"Cargando nueva figura: {figura_actual['Nombre']}")
        self.required_drones = len(figura_actual['Drones'])
        self.final_positions.clear()

        # Asegurar el acceso al diccionario de estados
        with self.state_lock:
            for dron in figura_actual['Drones']:
                dron_id = dron['ID']
                final_position = tuple(map(int, dron['POS'].split(',')))
                self.final_positions[dron_id] = final_position
                print(f"Posición final del dron {dron_id}: {final_position}")
                # Actualizar estado del dron
                self.drones_state[dron_id] = {'instruction': 'PENDING', 'reached': False}
        

    def send_positions_and_start_commands(self):
        for dron_id in self.connected_drones:
            if dron_id in self.final_positions:
                self.send_final_position(dron_id, self.final_positions[dron_id])
            else:
                print(f"Advertencia: El dron ID {dron_id} no tiene una posición final asignada en la figura actual.")
        
        if self.are_all_drones_connected():
            for dron_id in self.connected_drones:
                self.send_instruction_to_drone(dron_id, 'START')
            print("Instrucción START enviada a todos los drones.")
            self.log_auditoria('START', 'Instrucción START enviada a todos los drones.', tipo='engine')

            
    def log_auditoria(self,evento, descripcion, tipo='engine'):
        try:
            data = {
                'evento': evento,
                'descripcion': descripcion,
                'timestamp': time.strftime("%Y-%m-%d %H:%M:%S"),
                'tipo': tipo  # Especificar el tipo de auditoría
            }
            # Enviar evento de auditoría a la API
            response = requests.post(f'{args.api_address}/auditoria', json=data, verify=False)
            if response.status_code == 201:
                print(f"Evento de auditoría registrado: {evento}")
            else:
                print(f"Error al registrar evento de auditoría: {response.text}")
        except requests.exceptions.RequestException as e:
            print(f"Error de conexión al registrar evento de auditoría: {e}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='AD_Engine start-up arguments')
    parser.add_argument('--listen_port', type=int, default=8080, help='Port to listen on for drone connections')
    parser.add_argument('--max_drones', type=int, default=20, help='Maximum number of drones to support')
    parser.add_argument('--broker_address', default="127.0.0.1:29092", help='Address of the Kafka broker')
    parser.add_argument('--database_address', default="localhost:27017", help='MongoDB URI for the drones database')
    parser.add_argument('--json', default="PRUEBAS/AwD_figuras.json", help='Path to the JSON file with figures configuration')
    parser.add_argument('--api_address', type=str, default='https://localhost:5000', help='Address of the API server')
    args = parser.parse_args()

    # Inicializar ADEngine con los argumentos parseados
    engine = ADEngine(
        listen_port=args.listen_port,
        max_drones=args.max_drones,  # Asegúrate de manejar este argumento en tu clase ADEngine
        broker_address=args.broker_address,
        database_address=args.database_address,
        api_address=args.api_address
    )

    engine.procesar_datos_json(args.json)
    engine.start()
