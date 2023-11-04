import socket
import json
from kafka import KafkaConsumer, KafkaProducer
import pymongo

class ADEngine:
    def __init__(self, listen_port, broker_address, database_address):
        self.listen_port = listen_port
        self.broker_address = broker_address
        self.database_address = database_address
        
        if self.database_address:
            self.client = pymongo.MongoClient(self.database_address)
            self.db = self.client["dronedb"]
        
        self.kafka_producer = KafkaProducer(bootstrap_servers=self.broker_address)
        self.map_size = 20
        self.map = [[0 for _ in range(self.map_size)] for _ in range(self.map_size)]
        self.current_positions = {}  # Almacenar las posiciones actuales de los drones
        self.final_positions = {}  # Almacenar las posiciones finales de los drones
        
    
    

    def save_figura_info(self, figura, dron_id):
        figura_info = {
            "Figura": figura["Nombre"],
            "DronID": dron_id,
            "PosicionFinal": tuple(map(int, figura["Drones"][dron_id - 1]["POS"].split(',')))
        }

        existing_info = self.db.figuras.find_one({"DronID": dron_id})
        if existing_info is None:
            self.db.figuras.insert_one(figura_info)
        else:
            # Actualizar la posición final si ya existe
            self.db.figuras.update_one({"DronID": dron_id}, {"$set": {"PosicionFinal": figura_info["PosicionFinal"]}})


    def procesar_datos_json(self, ruta_archivo_json):
        datos = self.cargar_datos_desde_json(ruta_archivo_json)
        if datos:
            figuras = datos["figuras"]
            for figura in figuras:
                nombre_figura = figura["Nombre"]
                drones = figura["Drones"]
                print(f"Procesando Figura: {nombre_figura}")
                for dron in drones:
                    dron_id = dron["ID"]
                    posicion_str = dron["POS"]
                    posicion = [int(coord) for coord in posicion_str.split(',')]
                    self.final_positions[dron_id] = posicion
                    print(f"ID del dron: {dron_id}, Posición final: {posicion}")
                    self.save_figura_info(figura, dron_id)
        else:
            print("No se pudieron cargar los datos del archivo JSON.")

    def cargar_datos_desde_json(self, ruta_archivo_json):
        try:
            with open(ruta_archivo_json, "r") as archivo:
                datos = json.load(archivo)
                return datos
        except FileNotFoundError:
            print(f"No se encontró el archivo JSON en la ruta: {ruta_archivo_json}")
            return None
        except json.JSONDecodeError as e:
            print(f"Error al decodificar el archivo JSON: {str(e)}")
            return None

    def calculate_next_position(self, current_position, final_position):
        if current_position is not None:
            current_x, current_y = current_position
            # Resto del código aquí
        else:
            raise ValueError("current_position es None y no debería serlo en este punto.")

        target_x, target_y = final_position

        if current_x < target_x:
            current_x += 1
        elif current_x > target_x:
            current_x -= 1

        if current_y < target_y:
            current_y += 1
        elif current_y > target_y:
            current_y -= 1

        return current_x, current_y

    def calculate_move_instructions(self, dron_id, final_position):
        move_instructions = []

        current_position = self.get_initial_position_from_database(dron_id)
        final_position = self.get_final_position_from_database(dron_id)
        
        if current_position != final_position:
            next_position = self.calculate_next_position(current_position, final_position)
            move_instructions.append({
                "type": "MOVE",
                "X": next_position[0],
                "Y": next_position[1]
            })
            current_position = next_position
            print(f"Proxima posicion: {next_position}")
        else:
            print(f"El dron con ID {dron_id} ya ha alcanzado su posición final.")
        
        return move_instructions


    def get_initial_position_from_database(self, dron_id):
        if self.database_address:
            drones_collection = self.db["drones"]
            dron_data = drones_collection.find_one({"ID": dron_id})
            if dron_data:
                return dron_data.get("InitialPosition")
            else:
                print(f"El dron con ID {dron_id} no está registrado en la base de datos.")
                
    def get_final_position_from_database(self, dron_id):
        if self.database_address:
            figuras_collection = self.db["figuras"]
            figura_data = figuras_collection.find_one({"DronID": dron_id})
            if figura_data:
                return figura_data.get("PosicionFinal", (1, 1))
            else:
                print(f"El dron con ID {dron_id} no está registrado en la base de datos.")

    def start(self):
        server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_socket.bind(("127.0.0.1", self.listen_port))
        server_socket.listen(1)
        print(f"AD_Engine en funcionamiento. Escuchando en el puerto {self.listen_port}...")

        initial_positions = {}
            
        while True:
            client_socket, addr = server_socket.accept()
            print(f"Nueva conexión desde {addr}")
            
            kafka_consumer = KafkaConsumer('register_dron', bootstrap_servers=self.broker_address, auto_offset_reset='latest', group_id='ade_group')

            dron_id = None

            for message in kafka_consumer:
                message_value = message.value.decode('utf-8')
                print(f"Mensaje recibido: {message_value}")
                if "Registro de dron: ID=" in message_value:
                    parts = message_value.split(" ")
                    for part in parts:
                        if part.startswith("ID="):
                            dron_id_str = part[3:]
                            dron_id = int(''.join(filter(str.isdigit, dron_id_str)))
                    break
            
            kafka_consumer.close()
            
            initial_position = self.get_initial_position_from_database(dron_id)
            current_position = initial_position 

            if initial_position:
                initial_positions[dron_id] = initial_position
                self.current_positions[dron_id] = initial_position
            
            while True:
                print("Hola")
                move_instructions = self.calculate_move_instructions(dron_id, self.final_positions[dron_id])
                self.send_move_instructions(dron_id, move_instructions)
                
                # Ahora manejar el recibo de mensajes de actualización de posición
                kafka_consumer = KafkaConsumer('update_position', bootstrap_servers=self.broker_address, auto_offset_reset='latest', group_id='ade_group')
                for message in kafka_consumer:
                    message_value = message.value.decode('utf-8')
                    print(f"Mensaje recibido: {message_value}")
                    
                    if "UPDATE_POSITION" in message_value and f"ID: {dron_id}" in message_value:
                        pos_str = message_value.split("[")[1].split("]")[0]
                        pos_x, pos_y = map(int, pos_str.split(", "))
                        self.current_positions[dron_id] = (pos_x, pos_y)
                        print(f"Posición actualizada del dron con ID {dron_id}: {self.current_positions[dron_id]}")
                    # Puedes agregar lógica adicional aquí según las actualizaciones recibidas


    def send_map_state(self, client_socket):
        map_state = json.dumps(self.map)
        client_socket.send(map_state.encode())
        
    def send_message_to_dron(self, dron_id, message):
        message = f"Dron con ID {dron_id} ya puede despegar"
        topic = f'dron_{dron_id}'
        self.kafka_producer.send("register_dron", value=message.encode())

    def send_move_instructions(self, dron_id, move_instructions):
        for instruction in move_instructions:
            x = instruction["X"]
            y = instruction["Y"]
            message = f"MOVE: [{x}, {y}], ID: {dron_id}"
            self.kafka_producer.send("register_dron", value=message.encode())

if __name__ == "__main__":
    listen_port = 8080
    broker_address = "127.0.0.1:29092"
    database_address = "mongodb://localhost:27017/"
    
    engine_address = ADEngine(listen_port, broker_address, database_address)
    engine_address.procesar_datos_json("PRUEBAS/AwD_figuras.json")
    engine_address.start()
