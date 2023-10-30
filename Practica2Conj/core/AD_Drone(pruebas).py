import socket
import json
import time
from kafka import KafkaConsumer, KafkaProducer
import pymongo


class ADDrone:
    def __init__(self, engine_address, registry_address):
        self.engine_address = engine_address
        self.registry_address = registry_address
        self.dron_id = None
        self.access_token = None

    def input_drone_data(self):
        # Método para ingresar los datos del dron desde el usuario
        self.dron_id = int(input("Introduce el ID del dron (número entre 1 y 99): "))
        if 1 <= self.dron_id <= 99:
            return True
        else:
            print("ID de dron inválido. Debe estar entre 1 y 99.")
            return False

    def register_drone(self):
        # Conectar al módulo de registro (AD_Registry) para registrar el dron
        registry_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        registry_socket.connect(self.registry_address)

        dron_data = {
            'ID': self.dron_id,
            'Alias': f'Dron_{self.dron_id}'
        }
        registry_socket.send(json.dumps(dron_data).encode())
        response = registry_socket.recv(1024).decode()
        registry_socket.close()

        response_json = json.loads(response)
        if response_json['status'] == 'success':
            self.access_token = response_json['token']
            print(f"Registro exitoso. Token de acceso: {self.access_token}")
        else:
            print(f"Error en el registro: {response_json['message']}")

    def join_show(self):
        engine_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        engine_socket.connect(self.engine_address)

        while True:
            # Recibir instrucciones del motor (AD_Engine)
            instruction = engine_socket.recv(1024).decode()
            if instruction == "EXIT":
                print("Saliendo del programa.")
                break
            elif instruction == "SHOW_MAP":
                self.show_map(engine_socket)
            else:
                print(f"Instrucción desconocida: {instruction}")

    def show_map(self, engine_socket):
        engine_socket.send(json.dumps({'ID': self.dron_id, 'AccessToken': self.access_token}).encode())
        map_state = engine_socket.recv(1024).decode()
        print("Mapa actualizado:")
        print(map_state)
        
        
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
        # Conectar a la base de datos de MongoDB
        mongo_client = pymongo.MongoClient("mongodb://localhost:27017/")
        db = mongo_client["dronedb"]
        drones_collection = db["drones"]
        
        self.dron_id=int(input("Introduce el ID del dron: "))
        
        # Recuperar todos los drones desde la base de datos
        drones = drones_collection.find_one({"ID": self.dron_id})
        
        if not self.dron_id:
            print("Dron no registrado.")
            return
        else:
            result= drones_collection.delete_one({"ID": self.dron_id})
            
            if result.deleted_count==1:
                print(f"Dron con ID {self.dron_id} eliminado. ")
                self.dron_id = None
                self.access_token = None
            else:
                print(f"No se encontro el dron.")


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

    dron = ADDrone(engine_address, registry_address)
    dron.show_menu()
