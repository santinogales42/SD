import socket
import json
import time
from kafka import KafkaConsumer, KafkaProducer
import ConsumerProducer as cp
import pymongo
import threading
import sys

class ADDrone:
    def __init__(self, engine_address, registry_address, broker_address):
        self.engine_address = engine_address
        self.registry_address = registry_address
        self.dron_id = None
        self.alias = None
        self.access_token = None
        
        self.status = "IDLE"
        self.registered_drones = {}
        self.joined_drones = []
        self.current_position = (1,1)
        self.broker_address = broker_address
        self.consumer_producer = cp.ConsumerProducer(self.broker_address)
        self.kafka_producer = KafkaProducer(bootstrap_servers=self.broker_address)
        

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

    def send_kafka_message(self, topic, message):
        try:
            # El valor ya debe ser serializado a bytes antes de esta llamada
            # Asegúrate de que el mensaje esté en formato JSON y codificado a bytes
            self.kafka_producer.send(topic, value=json.dumps(message).encode('utf-8'))
            self.kafka_producer.flush()
        except Exception as e:
            print(f"Error al enviar mensaje Kafka: {e}")

        
    

    def authenticate(self):
        if self.dron_id in self.registered_drones:
            # Si el ID del dron está registrado, verificar el token
            stored_token = self.registered_drones[self.dron_id]
            if stored_token == self.access_token:
                return True
        return False



    def consume_kafka_messages(self):
        # Configurar el consumidor de Kafka
        consumer = KafkaConsumer(
            'drone_messages_topic',  # Cambiado a un tópico genérico para todos los mensajes de drones
            bootstrap_servers=self.broker_address,
            auto_offset_reset='latest',
            enable_auto_commit=True,  # Asegúrate de que esto esté habilitado
            group_id='drone-group-{}'.format(self.dron_id)
        )

        try:
            for message in consumer:
                # Decodificar el mensaje de bytes a dict
                message_data = json.loads(message.value.decode('utf-8'))
                if message_data.get('ID') == self.dron_id:
                    # Verificar si el mensaje es un registro o una instrucción
                    if 'type' in message_data and message_data['type'] == 'register':
                        # Manejar registro de drones
                        drone_id = message_data['ID']
                        self.registered_drones[drone_id] = {
                            'alias': message_data['Alias'],
                            'access_token': message_data['AccessToken'],
                            'initial_position': message_data['InitialPosition']
                        }
                        print(f"Drone {drone_id} registered with alias {message_data['Alias']}.")
                    elif 'type' in message_data and message_data['type'] == 'instruction':
                        # Procesar la instrucción específica
                        instruction = message_data['instruction']
                        if instruction == "MOVE":
                            # Extraer detalles de la instrucción MOVE y actuar en consecuencia
                            pass  # Reemplazar con la lógica de movimiento
                        elif instruction == "SHOW_MAP":
                            # Mostrar el mapa o realizar la acción correspondiente
                            pass  # Reemplazar con la lógica para mostrar el mapa
                        # Añadir más instrucciones según sea necesario
                    else:
                        print(f"Unknown message type received: {message_data}")

        except Exception as e:
            print(f"Error while consuming messages: {e}")
        finally:
            consumer.close()

    
    
    def register_to_engine(self):
        # Conectar con AD_Registry para obtener el token
        # Conectar con AD_Engine para unirse al espectáculo
        pass

    def send_movement_update(self, position):
        # Enviar actualización de movimiento a AD_Engine
        pass

    def start_moving(self):
        # Comenzar a moverse hacia la posición de destino asignada
        pass
    

    def join_show(self, dron_id):
        if not self.authenticate():
            print("Autenticación fallida. El dron no puede unirse al espectáculo.")
            return

        producer_thread = cp.ProducerShow(self.broker_address, self.dron_id)
        producer_thread.start()
            
        try:
            engine_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            engine_socket.connect(self.engine_address)
            
            # Envía mensaje al Engine informando que el dron con ID se ha unido
            join_message = {'action': 'join', 'ID': self.dron_id}
            engine_socket.send(json.dumps(join_message).encode('utf-8'))
            
            # Conectar a Kafka para recibir actualizaciones de posición del dron
            consumer_producer = cp.ConsumerProducer(broker_address, dron_id)
            consumer_producer.start_consumer()
            print(f"El dron con ID {self.dron_id} se ha unido al espectáculo.")
                
            try:
                while True:
                    instruction = self.consume_kafka_messages()
                    if instruction is None:
                        print("No se recibió ninguna instrucción válida.")
                        break

                    if instruction == "EXIT":
                        print("Saliendo del programa.")
                        break
                    elif instruction == "SHOW_MAP":
                        self.show_map(engine_socket)
                    elif "MOVE" in instruction:
                        try:
                            # Split y analiza la cadena de instrucción
                            parts = instruction.split(',')
                            x = int(parts[0].split('[')[1])
                            y = int(parts[1].split(']')[0])
                            dron_id = int(parts[2].split('ID: ')[1])
                            print(f"Received MOVE instruction: X: {x}, Y: {y}, ID: {dron_id}")

                            # Luego puedes usar x, y y dron_id como necesites
                            target_x = x
                            target_y = y
                            self.move_to_position(target_x, target_y)
                            self.show_map(engine_socket)
                        except Exception as e:
                            print("ERROR: ", e)
                    elif "STOP" in instruction:
                        print("STOP instruction received.")
                        self.status = "IDLE"
                    else:
                        print(f"Instrucción desconocida: {instruction}")
            except ConnectionResetError:
                # Manejar la excepción de desconexión del motor
                print("Error: AD_Engine se ha desconectado.")
                # Restablecer la posición de todos los drones a (1, 1)
                self.reset_all_drones_position()
                engine_socket.close()
                # Puedes agregar aquí la lógica necesaria para manejar esta desconexión (por ejemplo, notificar al sistema)
        except ConnectionRefusedError:
            print(f"Error: No se pudo conectar al motor (Connection Refused).")


    def reset_all_drones_position(self):
        # Conectar a la base de datos de MongoDB
        mongo_client = pymongo.MongoClient("mongodb://localhost:27017/")
        db = mongo_client["dronedb"]
        drones_collection = db["drones"]

        # Actualizar la posición de todos los drones en la base de datos
        drones_collection.update_many({}, {"$set": {"InitialPosition": (1, 1)}})

        # Cerrar la conexión con la base de datos
        mongo_client.close()
    

    def show_map(self, engine_socket):
        engine_socket.send(json.dumps({'ID': self.dron_id, 'AccessToken': self.access_token}).encode())
        map_state = engine_socket.recv(1024).decode()
        print("Mapa actualizado:")
        print(map_state)
        return map_state  # Devuelve el mapa actualizado



    def move_to_position(self, target_x, target_y):
        if self.current_position == (target_x, target_y):
            print(f"El dron {self.dron_id} ya está en la posición deseada.")
        else:
            print(f"Moviendo el dron {self.dron_id} a la posición ({target_x}, {target_y})")

            while self.current_position != (target_x, target_y):
                if self.current_position[0] < target_x:
                    self.current_position = (self.current_position[0] + 1, self.current_position[1])
                elif self.current_position[0] > target_x:
                    self.current_position = (self.current_position[0] - 1, self.current_position[1])

                if self.current_position[1] < target_y:
                    self.current_position = (self.current_position[0], self.current_position[1] + 1)
                elif self.current_position[1] > target_y:
                    self.current_position = (self.current_position[0], self.current_position[1] - 1)

                time.sleep(1)  # Simular el movimiento de una celda a otra

            print(f"Dron {self.dron_id} ha llegado a la posición ({target_x}, {target_y}).")
            self.update_position()
       
    def update_position(self):
        # Conectar a la base de datos de MongoDB
        mongo_client = pymongo.MongoClient("mongodb://localhost:27017/")
        db = mongo_client["dronedb"]
        drones_collection = db["drones"]

        # Actualizar la posición del dron en la base de datos
        drones_collection.update_one({"ID": self.dron_id}, {"$set": {"InitialPosition": self.current_position}})

       
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
    engine_address = ("127.0.0.1", 8080)
    registry_address = ("127.0.0.1", 8081)
    broker_address = "localhost:29092"

    dron = ADDrone(engine_address, registry_address, broker_address)
    dron.show_menu()
