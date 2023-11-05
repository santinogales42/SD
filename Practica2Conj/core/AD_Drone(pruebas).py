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
        

    # Resto del código de la clase ADDrone


        
    def input_drone_data(self):
        while True:
            user_input = input("Introduce el ID del dron (número entre 1 y 99): ")
            alias = input("Introduce el alias del dron: ")

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

        # Cerrar la conexión con la base de datos
        mongo_client.close()

        return True

    def register_drone(self):
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
        else:
            print(f"Error en el registro: {response_json['message']}")
            

    def authenticate(self):
        if self.dron_id in self.registered_drones:
            # Si el ID del dron está registrado, verificar el token
            stored_token = self.registered_drones[self.dron_id]
            if stored_token == self.access_token:
                return True
        return False



    def consume_kafka_messages(self):
        consumer = KafkaConsumer(
            'register_movement',  # Cambia según la configuración de tu servidor Kafka
            bootstrap_servers='127.0.0.1:29092',  # Cambia según la configuración de tu servidor Kafka
            auto_offset_reset='latest',  # Puedes configurarlo según tus necesidades
            enable_auto_commit=True,  # Puedes configurarlo según tus necesidades
            group_id=None  # Puedes configurarlo según tus necesidades
        )

        for message in consumer:
            instruction = message.value.decode()
            return instruction
    

    def join_show(self):
        if not self.authenticate():
            print("Autenticación fallida. El dron no puede unirse al espectáculo.")
            return

        producer_thread = cp.ProducerShow(self.broker_address, self.dron_id)
        producer_thread.start()
        
        print(f"El dron con ID {self.dron_id} se ha unido al espectáculo.")
        

        engine_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        engine_socket.connect(self.engine_address)
        
        # Conectar a Kafka para recibir actualizaciones de posición del dron
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
            # Puedes agregar aquí la lógica necesaria para manejar esta desconexión (por ejemplo, notificar al sistema)


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
        
            
    def modify_drones(self):
        # Conectar a la base de datos de MongoDB
        mongo_client = pymongo.MongoClient("mongodb://localhost:27017/")
        db = mongo_client["dronedb"]
        drones_collection = db["drones"]
        
        self.dron_id=int(input("Introduce el ID del dron: "))
        
        # Recuperar todos los drones desde la base de datos
        drones = drones_collection.find_one({"ID": self.dron_id})
        if drones:
            new_alias =input("Introduce el nuevo alias: ")
            drones_collection.update_one({"ID": self.dron_id}, {"$set": {"Alias": new_alias}})
            print(f"Dron con ID {self.dron_id} actualizado con el nuevo alias: {new_alias}")
        else:
            print(f"No se encontro el dron con id {self.dron_id}")
        
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
