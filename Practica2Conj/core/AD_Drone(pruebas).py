import socket
import json
import time
from kafka import KafkaConsumer, KafkaProducer
import pymongo
import os


class ADDrone:
    def __init__(self, engine_address, registry_address):
        self.engine_address = engine_address
        self.registry_address = registry_address
        self.dron_id = None
        self.access_token = None
        self.status = "IDLE"
        self.registered_drones = {}
        self.current_position = (1,1)

    def input_drone_data(self):
        while True:
            dron_id = int(input("Introduce el ID del dron (número entre 1 y 99): "))
            alias = input("Introduce el alias del dron: ")

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
                    break
            else:
                print("ID de dron inválido. Debe estar entre 1 y 99.")

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
    
    
    def process_instruction(self, instruction):
        if instruction.startswith("MOVE"):
            move_data = json.loads(instruction[4:])
            target_x, target_y = move_data["X"], move_data["Y"]
            self.move_to_position(target_x, target_y)
        elif instruction == "STOP":
            # Realiza la acción correspondiente cuando se recibe la instrucción "STOP".
            pass  # Puedes agregar tu lógica aquí.
        elif instruction == "EXIT":
            # Realiza la acción correspondiente cuando se recibe la instrucción "EXIT".
            pass  # Puedes agregar tu lógica aquí.
    
    
    def consume_kafka_messages(self):
        consumer = KafkaConsumer(
            'register_dron',  # Cambia según la configuración de tu servidor Kafka
            bootstrap_servers='127.0.0.1:29092',  # Cambia según la configuración de tu servidor Kafka
            auto_offset_reset='latest',  # Puedes configurarlo según tus necesidades
            enable_auto_commit=True,  # Puedes configurarlo según tus necesidades
            group_id=None  # Puedes configurarlo según tus necesidades
        )

        for message in consumer:
            instruction = message.value.decode()
            self.process_instruction(instruction)
    

    def join_show(self):
        if not self.authenticate():
            print("Autenticación fallida. El dron no puede unirse al espectáculo.")
            return

        print(f"Autenticación correcta. El dron con ID: {self.dron_id} se va a unir al espectáculo.")

        engine_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        engine_socket.connect(self.engine_address)
        
        # Llamar a la función consume_kafka_messages para recibir instrucciones de movimiento
        self.consume_kafka_messages()

        while True:
            # Recibir instrucciones del motor (AD_Engine)
            instruction = engine_socket.recv(1024).decode()
            if instruction == "EXIT":
                print("Saliendo del programa.")
                break
            elif instruction == "SHOW_MAP":
                self.show_map(engine_socket)
            elif instruction.startswith("MOVE"):
                move_data = json.loads(instruction[4:])
                target_x, target_y = move_data["X"], move_data["Y"]
                self.move_to_position(target_x, target_y)
            else:
                print(f"Instrucción desconocida: {instruction}")


    def show_map(self, engine_socket):
        engine_socket.send(json.dumps({'ID': self.dron_id, 'AccessToken': self.access_token}).encode())
        map_state = engine_socket.recv(1024).decode()
        print("Mapa actualizado:")
        print(map_state)
        
        
    def move_to_position(self, x, y):
        if self.current_position == (x, y):
            print("El dron ya está en la posición deseada.")
            return

        while self.current_position != (x, y):
            next_x, next_y = self.calculate_next_position(x, y)
            self.current_position = (next_x, next_y)
            print(f"Dron {self.dron_id} se ha movido a la posición ({next_x}, {next_y}).")
            time.sleep(1)  # Simular el movimiento de una celda a otra
            

    def calculate_next_position(self, target_x, target_y):
        current_x, current_y = self.current_position

        # Calcular la siguiente posición adyacente
        if current_x < target_x:
            current_x += 1
        elif current_x > target_x:
            current_x -= 1

        if current_y < target_y:
            current_y += 1
        elif current_y > target_y:
            current_y -= 1

        return current_x, current_y
        
        
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
        if not self.dron_id:
            print("Dron no registrado.")
            return
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
                
                
    def log_to_file(self, message, log_file='drone_log.txt'):
        # Obtener el nombre del archivo actual
        current_file = os.path.basename(__file__)

        # Formatear el mensaje con el nombre del archivo
        log_message = f"AD_{current_file}: {message}\n"

        # Registrar el mensaje en el archivo de registro
        with open(log_file, 'a') as f:
            f.write(log_message)
                
                

if __name__ == "__main__":
    engine_address = ("127.0.0.1", 8080)
    registry_address = ("127.0.0.1", 8081)

    dron = ADDrone(engine_address, registry_address)
    dron.show_menu()
