import socket
import json
import threading
from kafka import KafkaProducer, KafkaConsumer
import argparse
import pymongo
import sys
import time
import requests

class ADDrone(threading.Thread):
    def __init__(self, engine_address, broker_address):
        super().__init__()
        self.engine_address = engine_address
        self.broker_address = broker_address
        self.kafka_producer = KafkaProducer(bootstrap_servers=self.broker_address)
        self.registry_address = ('localhost', 8081)
        self.final_position = None
        self.current_position = (1, 1)
        self.base_position = (1, 1)
        self.dron_id = None
        self.alias = None
        self.access_token = None
        self.mongo_client = pymongo.MongoClient("mongodb://localhost:27017/")
        self.db = self.mongo_client["dronedb"]
        
        self.registered_drones = {}
        self.in_show_mode = False
        
    def start(self):
        self.show_menu()
        

    def connect_to_engine(self):
        engine_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        engine_socket.connect(self.engine_address)
        engine_socket.send(json.dumps({'action': 'join', 'ID': self.dron_id}).encode())
        response = json.loads(engine_socket.recv(1024).decode())
        self.final_position = tuple(response['final_position'])
        engine_socket.close()
        
        
    def handle_drone_registered_message(self, message):
        if message.value.get('ID') == self.dron_id:
            self.access_token = message.value.get('AccessToken')
            print(f"ADDrone: Registrado exitosamente con AccessToken: {self.access_token}")
    

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
            value_deserializer=lambda m: json.loads(m.decode('utf-8'))
        )

        for message in consumer:
            if message.topic == 'drones_registered':
                self.handle_drone_registered_message(message)
            if message.topic == 'drone_messages_topic':
                self.handle_instruction_message(message)
            elif message.topic == 'drone_final_position':
                self.handle_final_position_message(message)

    def handle_instruction_message(self, message):
        instruction = message.value.get('instruction')
        if instruction == 'START':
            if self.final_position != self.current_position:
                print("ADDrone: Instrucción START recibida, moviéndose hacia la posición final...")
                self.move_to_final_position()
            else:
                print("ADDrone: Ya en posición final, ignorando instrucción START.")
        elif instruction == 'STOP':
            print("ADDrone: Instrucción STOP recibida, deteniendo y regresando a la base...")
            self.current_position = self.base_position
            self.in_show_mode = False
            self.send_position_update()

    def handle_final_position_message(self, message):
        if message.value.get('dron_id') == self.dron_id:
            final_position = message.value.get('final_position')
            if final_position:
                self.final_position = tuple(final_position)

    def move_to_final_position(self):
        while self.current_position != self.final_position:
            self.calculate_movement()
            self.send_position_update()
            time.sleep(1)

        # Envía un mensaje Kafka una vez que el dron ha llegado a su posición final
        self.send_kafka_message('drone_position_reached', {
            'type': 'position_reached',
            'dron_id': self.dron_id,
            'final_position': self.final_position
        })
        print(f"ADDrone: Posición final alcanzada: {self.final_position}")


    def calculate_movement(self):
        # Ejemplo simple de movimiento: moverse en línea recta hacia el objetivo
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
            self.kafka_producer.send(topic, value=json.dumps(message).encode('utf-8'))
            self.kafka_producer.flush()
            print(f"Mensaje Kafka enviado: {message}")
        except Exception as e:
            print(f"Error al enviar mensaje Kafka: {e}")


    def send_position_update(self):
        message = {
            'type': 'position_update',
            'ID': self.dron_id,
            'Position': self.current_position
        }
        self.kafka_producer.send('drone_position_updates', json.dumps(message).encode('utf-8'))
        self.kafka_producer.flush()
        print(f"Nueva posición: {self.current_position}")
    
    def input_drone_data(self):
        while True:
            user_input = input("Introduce el ID del dron (número entre 1 y 99): ")
            alias = input("Introduce el alias del dron: ")

            if user_input.strip() and alias.strip():  # Verifica que ambas entradas no estén vacías
                try:
                    dron_id = int(user_input)

                    if 1 <= dron_id <= 99:
                        # Conectar a la base de datos de MongoDB y verificar si el ID ya existe
                        mongo_client = pymongo.MongoClient("mongodb://localhost:27017/")
                        db = mongo_client["dronedb"]
                        drones_collection = db["drones"]

                        existing_dron = drones_collection.find_one({"ID": dron_id})
                        
                        if existing_dron:
                            print("ID de dron ya existe. Introduce un ID diferente.")
                        else:
                            # ID válido y no duplicado, se puede continuar
                            self.dron_id = dron_id
                            self.alias = alias
                            self.choose_registration_method()
                            break  # La entrada es un número válido y está en el rango, sal del bucle
                    else:
                        print("El ID del dron debe estar entre 1 y 99. Inténtalo de nuevo.")
                except ValueError:
                    print("Entrada inválida. Debes ingresar un número válido.")
            else:
                print("Ambos campos son obligatorios. Introduce el ID y el alias del dron.")


    def register_drone(self):
        try:
            # Conectar al módulo de registro (AD_Registry) para registrar el dron
            registry_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            registry_socket.connect(self.registry_address)

            dron_data = {
                'ID': self.dron_id,
                'Alias': self.alias
            }
            registry_socket.send(json.dumps(dron_data).encode())
            response = registry_socket.recv(1024).decode()
            registry_socket.close()

            response_json = json.loads(response)
            if response_json['status'] == 'success':
                self.access_token = response_json['token']
                self.registered_drones[self.dron_id] = self.access_token
                print(f"Registro exitoso. Token de acceso: {self.access_token}")
                
                self.send_kafka_message('drone_registered', {
                    'type': 'register',
                    'ID': self.dron_id,
                    'Alias': self.alias,
                    'AccessToken': self.access_token,
                    'InitialPosition': self.current_position
                })
            else:
                print(f"Error en el registro: {response_json['message']}")
        except ConnectionRefusedError:
            print("Error: El registro no está funcionando. Por favor, inicia el módulo de registro.")
        except Exception as e:
            print(f"Error inesperado: {e}")
            
            
            
            
            
    def register_via_api(self):
        data = {'ID': str(self.dron_id), 'Alias': self.alias}
        response = requests.post('http://localhost:5000/registro', json=data)
        if response.status_code == 201:
            self.access_token = response.json().get('token')
            print(f"Registrado via API. Token: {self.access_token}")
        else:
            print(f"Error al registrar via API: {response.text}")

    def choose_registration_method(self):
        method = input("Elige el método de registro (1: Socket, 2: API): ")
        if method == "1":
            self.register_drone()
        elif method == "2":
            self.register_via_api()
        else:
            print("Método de registro no válido.")
    
    
    
    
    
    def request_final_position_from_db(self):
        # Conectar a la base de datos de MongoDB
        mongo_client = pymongo.MongoClient("mongodb://localhost:27017/")
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
        #Input para buscar el ID del dron
        self.dron_id=int(input("Introduce el ID del dron: "))
        
        # Conectar a la base de datos de MongoDB
        mongo_client = pymongo.MongoClient("mongodb://localhost:27017/")
        db = mongo_client["dronedb"]
        drones_collection = db["drones"]

        # Recuperar el dron desde la base de datos
        drone = drones_collection.find_one({"ID": self.dron_id})

        if drone:
            result = drones_collection.delete_one({"ID": self.dron_id})
            if result.deleted_count == 1:
                print(f"Dron con ID {self.dron_id} eliminado.")
                self.dron_id = None
                self.access_token = None
            else:
                print(f"No se encontró el dron.")
        else:
            print(f"No se encontró el dron con ID {self.dron_id} en la base de datos.")


    def list_drones(self):
        # Conectar a la base de datos de MongoDB
        mongo_client = pymongo.MongoClient("mongodb://localhost:27017/")
        db = mongo_client["dronedb"]
        drones_collection = db["drones"]

        # Recuperar todos los drones desde la base de datos
        drones = drones_collection.find()

        # Imprimir la información de los drones
        for drone in drones:
            print(f"ID: {drone['ID']}")
            print(f'Alias: {drone["Alias"]}')
            print()  # Línea en blanco para separar los drones
        # Cerrar la conexión con la base de datos
        mongo_client.close()
            
    def join_show(self):
        if not self.dron_id or not self.alias:
            print("Drone ID and alias must be set before joining the show.")
            return

        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as engine_socket:
                engine_socket.connect(self.engine_address)
                join_message = {'action': 'join', 'ID': self.dron_id, 'Alias': self.alias}
                engine_socket.send(json.dumps(join_message).encode('utf-8'))
                
                response_data = engine_socket.recv(1024).decode()
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
        except json.JSONDecodeError:
            print("Failed to decode the server response. Ensure the server sends a valid JSON.")
        except ConnectionError as e:
            print(f"Unable to connect to the ADEngine: {e}")

            
    def show_menu(self):
        options = {
            "1": self.input_drone_data,
            "2": self.join_show,
            "3": self.list_drones,
            "4": self.delete_drones
        }
        try:
            while True:
                if not self.in_show_mode:
                    print("\nDrone Menu:")
                    print("1. Enter Drone Data")
                    print("2. Join Show")
                    print("3. List Drones")
                    print("4. Delete Drone")
                    print("5. Exit")
                    choice = input("Select an option: ")
                    if choice == "5":
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
    parser.add_argument('--broker_address', type=str, default='localhost:29092', help='Address of the Kafka broker')
    args = parser.parse_args()


    drones = []
    dron = ADDrone((args.engine_ip, args.engine_port), args.broker_address)
    drones.append(dron)

    # Inicia cada dron como un hilo separado
    for dron in drones:
        dron.start()
