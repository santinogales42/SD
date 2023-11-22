import socket
import json
import threading
from kafka import KafkaProducer, KafkaConsumer
import argparse
import sys
import pymongo

# Clase ADDrone modificado para trabajar con hilos
class ADDrone(threading.Thread):
    def __init__(self, engine_address, broker_address):
        super().__init__()
        self.engine_address = engine_address
        self.broker_address = broker_address
        self.kafka_producer = KafkaProducer(bootstrap_servers=self.broker_address)
        self.final_position = None
        self.current_position = (1, 1)
        
    def run(self):
        # Aquí puedes iniciar la lógica de conexión al ADEngine y recibir la posición final
        self.show_menu()

    def connect_to_engine(self):
        # Conectar al ADEngine para recibir la posición final
        engine_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        engine_socket.connect(self.engine_address)
        engine_socket.send(json.dumps({'action': 'join', 'ID': self.dron_id}).encode())
        response = json.loads(engine_socket.recv(1024).decode())
        self.final_position = tuple(response['final_position'])
        engine_socket.close()

    def start_consuming_messages(self):
        # Conectar al Kafka Consumer para recibir mensajes
        consumer = KafkaConsumer(
            'drone_messages_topic',
            bootstrap_servers=self.broker_address,
            auto_offset_reset='latest',
            enable_auto_commit=True,
            group_id=f'drone_{self.dron_id}',
            value_deserializer=lambda m: json.loads(m.decode('utf-8'))
        )
        for message in consumer:
            # Aquí puedes implementar la lógica para manejar los mensajes
            pass

    def send_position_update(self):
        # Enviar la posición actualizada al ADEngine
        message = {
            'type': 'position_update',
            'ID': self.dron_id,
            'position': self.current_position
        }
        self.kafka_producer.send('drone_position_updates', json.dumps(message).encode('utf-8'))
        self.kafka_producer.flush()

    def calculate_movement(self):
        # Calcular la ruta hacia la posición final y actualizar la posición actual
        pass  # Implementar la lógica de movimiento aquí
    
    def authenticate(self):
        if self.dron_id in self.registered_drones:
            # Si el ID del dron está registrado, verificar el token
            stored_token = self.registered_drones[self.dron_id]
            if stored_token == self.access_token:
                return True
        return False
    
    
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
            
            
            
    def show_menu(self):
        try:
            while True:
                print("\nDron Menu:")
                print("1. Registrar dron")
                print("2. Unirse al espectáculo")
                print("3. Listar todos los drones")
                print("4. Eliminar drones")
                print("5. Salir")

                choice = input("Seleccione una opción: ")

                if choice == "1":
                    if not self.dron_id:
                        self.input_drone_data()
                    if not self.access_token:
                        self.register_drone()
                elif choice == "2":
                    if self.access_token:
                        dron_id = self.dron_id
                        t = threading.Thread(target=self.join_show, args=(dron_id,))
                        t.start()
                        self.joined_drones.append(dron_id)
                    else:
                        print("Debe registrar el dron primero.")
                elif choice == "3":
                    self.list_drones()
                elif choice == "4":
                    self.delete_drones()
                elif choice == "5":
                    break
                else:
                    print("Opción no válida. Seleccione una opción válida.")
        except KeyboardInterrupt:
            print("Se ha presionado Ctrl+C. Saliendo del programa...")
        except ConnectionResetError:
            print("Error de conexión. Saliendo del programa...")
        finally:
            sys.exit()
    
    
    

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='ADDrone start-up arguments')
    parser.add_argument('--engine_ip', type=str, default='127.0.0.1', help='IP address of the ADEngine')
    parser.add_argument('--engine_port', type=int, default=8080, help='Port number of the ADEngine')
    parser.add_argument('--broker_address', type=str, default='localhost:29092', help='Address of the Kafka broker')
    args = parser.parse_args()

    dron = ADDrone((args.engine_ip, args.engine_port), args.broker_address)
    dron.start()

        

    

            
            
    
        

    