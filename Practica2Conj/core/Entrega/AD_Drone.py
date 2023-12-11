import socket
import json
import time
from kafka import KafkaConsumer, KafkaProducer
import ConsumerProducer as cp
import pymongo
import threading

class ADDrone:
    def __init__(self, engine_address, registry_address, broker_address):
        self.engine_address = engine_address
        self.registry_address = registry_address
        self.dron_id = None
        self.alias = None
        self.access_token = None
        
        self.status = "IDLE"
        self.registered_drones = {}
        self.current_position = (1,1)
        self.broker_address = broker_address
        self.consumer_producer = cp.ConsumerProducer(self.broker_address)
        self.kafka_producer = KafkaProducer(bootstrap_servers='localhost:29092')
        



    def input_drone_data(self):
        while True:
            user_input = input("Introduce el ID del dron (número entre 1 y 99): ")
            alias = input("Introduce el alias del dron: ")

            try:
                dron_id = int(user_input)
                if 1 <= dron_id <= 99:
                    mongo_client = pymongo.MongoClient("mongodb://localhost:27017/")
                    db = mongo_client["dronedb"]
                    drones_collection = db["drones"]

                    existing_dron = drones_collection.find_one({"ID": dron_id})

                    if existing_dron:
                        print("ID de dron ya existe. Introduce un ID diferente.")
                    else:
                        self.dron_id = dron_id
                        self.alias = alias
                        break 
                else:
                    print("El ID del dron debe estar entre 1 y 99. Inténtalo de nuevo.")
            except ValueError:
                print("Entrada inválida. Debes ingresar un número válido.")

        mongo_client.close()

        return True

    def register_drone(self):
        try:
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
            else:
                print(f"Error en el registro: {response_json['message']}")
        except ConnectionRefusedError:
            print("Error: El registro no está funcionando. Por favor, inicia el módulo de registro.")
        except Exception as e:
            print(f"Error inesperado: {e}")

            

    def authenticate(self):
        if self.dron_id in self.registered_drones:
            stored_token = self.registered_drones[self.dron_id]
            if stored_token == self.access_token:
                return True
        return False



    def consume_kafka_messages(self):
        consumer = KafkaConsumer(
            'register_movement', 
            bootstrap_servers='127.0.0.1:29092', 
            auto_offset_reset='latest',  
            enable_auto_commit=True,
            group_id=None
        )

        for message in consumer:
            instruction = message.value.decode()
            return instruction
    

    def join_show(self):
        try:
            if not self.authenticate():
                print("Autenticación fallida. El dron no puede unirse al espectáculo.")
                return

            producer_thread = cp.ProducerShow(self.broker_address, self.dron_id)
            producer_thread.start()
            
            print(f"El dron con ID {self.dron_id} se ha unido al espectáculo.")
            

            engine_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            engine_socket.connect(self.engine_address)
            
            dron_id = self.dron_id  
            consumer_producer = cp.ConsumerProducer(broker_address, dron_id)
            consumer_producer.start_consumer()
            print("Conectado a Kafka para recibir actualizaciones de posición del dron.")
            
            try:

                while True:
                    instruction = self.consume_kafka_messages()

                    if instruction == "EXIT":
                        print("Saliendo del programa.")
                        break
                    elif instruction == "SHOW_MAP":
                        self.show_map(engine_socket)
                    elif "MOVE" in instruction:
                        try:
                            parts = instruction.split(',')
                            x = int(parts[0].split('[')[1])
                            y = int(parts[1].split(']')[0])
                            dron_id = int(parts[2].split('ID: ')[1])
                            print(f"Received MOVE instruction: X: {x}, Y: {y}, ID: {dron_id}")

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
                print("Error: AD_Engine se ha desconectado.")
                self.reset_all_drones_position()
        except KeyboardInterrupt:
            print("Saliendo del programa.")


    def reset_all_drones_position(self):
        mongo_client = pymongo.MongoClient("mongodb://localhost:27017/")
        db = mongo_client["dronedb"]
        drones_collection = db["drones"]

        drones_collection.update_many({}, {"$set": {"InitialPosition": (1, 1)}})
        mongo_client.close()
    

    def show_map(self, engine_socket):
        engine_socket.send(json.dumps({'ID': self.dron_id, 'AccessToken': self.access_token}).encode())
        map_state = engine_socket.recv(1024).decode()
        print("Mapa actualizado:")
        print(map_state)


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

                time.sleep(1)

            print(f"Dron {self.dron_id} ha llegado a la posición ({target_x}, {target_y}).")
            self.update_position()
       
    def update_position(self):
        mongo_client = pymongo.MongoClient("mongodb://localhost:27017/")
        db = mongo_client["dronedb"]
        drones_collection = db["drones"]

        drones_collection.update_one({"ID": self.dron_id}, {"$set": {"InitialPosition": self.current_position}})

       
        mongo_client.close() 
        
            
    def modify_drones(self):
        mongo_client = pymongo.MongoClient("mongodb://localhost:27017/")
        db = mongo_client["dronedb"]
        drones_collection = db["drones"]
        
        self.dron_id=int(input("Introduce el ID del dron: "))
        
        drones = drones_collection.find_one({"ID": self.dron_id})
        if drones:
            new_alias =input("Introduce el nuevo alias: ")
            drones_collection.update_one({"ID": self.dron_id}, {"$set": {"Alias": new_alias}})
            print(f"Dron con ID {self.dron_id} actualizado con el nuevo alias: {new_alias}")
        else:
            print(f"No se encontro el dron con id {self.dron_id}")
        
        mongo_client.close()
    
    
    def delete_drones(self):
        self.dron_id=int(input("Introduce el ID del dron: "))
        
        mongo_client = pymongo.MongoClient("mongodb://localhost:27017/")
        db = mongo_client["dronedb"]
        drones_collection = db["drones"]

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
        mongo_client = pymongo.MongoClient("mongodb://localhost:27017/")
        db = mongo_client["dronedb"]
        drones_collection = db["drones"]

        drones = drones_collection.find()

        for drone in drones:
            print(f"ID: {drone['ID']}")
            print(f'Alias: {drone["Alias"]}')
            print()
        mongo_client.close()
            
            
            
    def show_menu(self):
        while True:
            print("\nDron Menu:")
            print("1. Registrar dron")
            print("2. Unirse al espectáculo")
            print("3. Listar todos los drones")
            print("4. Modificar drones")
            print("5. Eliminar drones")
            print("6. Salir")
            
            choice = input("Seleccione una opción: ")

            if choice == "1":
                if not self.dron_id:
                    self.input_drone_data()
                if not self.access_token:
                    self.register_drone()
            elif choice == "2":
                if self.access_token:
                    self.join_show()
                else:
                    print("Debe registrar el dron primero.")
            elif choice == "3":
                self.list_drones()
            elif choice =="4":
                self.modify_drones()
            elif choice =="5":
                self.delete_drones()
            elif choice == "6":
                break
            else:
                print("Opción no válida. Seleccione una opción válida.")
                

if __name__ == "__main__":
    engine_address = ("127.0.0.1", 8080)
    registry_address = ("127.0.0.1", 8081)
    broker_address = "localhost:29092"

    dron = ADDrone(engine_address, registry_address, broker_address)
    dron.show_menu()
