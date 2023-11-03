import socket
import json
import random
import time
import os
from kafka import KafkaConsumer, KafkaProducer
import pymongo

class ADEngine:
    def __init__(self, listen_port, max_drones, broker_address, weather_address, database_address):
        # Constructor de la clase ADEngine
        self.listen_port = listen_port
        self.max_drones = max_drones
        self.broker_address = broker_address
        self.weather_address = weather_address
        self.database_address = database_address  
        
        if self.database_address:
            self.client = pymongo.MongoClient(self.database_address)
            self.db = self.client["dronedb"]

        # Configurar un productor de Kafka para enviar mensajes a los drones
        self.broker_address = "127.0.0.1:29092"

        self.kafka_producer = KafkaProducer(bootstrap_servers=self.broker_address)

        # Inicializar el mapa 2D (matriz de bytes) para representar el espacio aéreo
        self.map_size = 20  # Tamaño del mapa (20x20)
        self.map = [[0 for _ in range(self.map_size)] for _ in range(self.map_size)]
        
        self.final_positions = {}  # Diccionario para almacenar las posiciones finales de los drones
        self.current_positions = {}  # Diccionario para almacenar las posiciones actuales de los drones
        self.figuras = []  # Lista para almacenar las figuras del archivo JSON        
        
        
    def verificar_drones_en_posiciones_finales(self, figura):
        drones_figura = figura["Drones"]
        for dron in drones_figura:
            dron_id = dron["ID"]
            final_position = dron["POS"]
            current_position = self.current_positions.get(dron_id)
            if final_position != current_position:
                return False  # Si al menos un dron no está en su posición final, retorna False
        return True  # Si todos los drones están en sus posiciones finales, retorna True

        
        
    def save_figura_info(self, figura, dron_id):
        figura_info = {
            "Figura": figura["Nombre"],
            "DronID": dron_id,
            "PosicionFinal": figura["Drones"][dron_id - 1]["POS"]
        }
        
        # Verificar si la información ya existe en la base de datos
        existing_info = self.db.figuras.find_one(figura_info)
        if existing_info is None:
            self.db.figuras.insert_one(figura_info)  # Inserta información de la figura en la colección figuras
        
        
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
                    posicion = dron["POS"]
                    self.final_positions[dron_id] = posicion
                    print(f"ID del dron: {dron_id}, Posición final: {posicion}")
                    # Almacenar la información de la figura actual en la base de datos
                    self.save_figura_info(figura, dron_id)
        else:
            print("No se pudieron cargar los datos del archivo JSON.")

        
        
    def calculate_move_instructions(self, current_positions, final_positions):
        move_instructions = {}
        for dron_id, current_position in current_positions.items():
            final_position = final_positions.get(dron_id)
            if final_position:
                if current_position != final_position:
                    move_instructions[dron_id] = {
                        "type": "MOVE",
                        "X": final_position[0],  # Aquí debes calcular la próxima posición X
                        "Y": final_position[1]  # Aquí debes calcular la próxima posición Y
                    }
                else:
                    move_instructions[dron_id] = {
                        "type": "STOP"
                    }
        return move_instructions
    
    
    def get_initial_position_from_database(self, dron_id):
        if self.database_address:
            # Conéctate a la colección de drones
            drones_collection = self.db["drones"]

            # Realiza una consulta para obtener la posición inicial del dron con la ID proporcionada
            dron_data = drones_collection.find_one({"ID": dron_id})

            if dron_data:
                return dron_data.get("InitialPosition", (1, 1))
            else:
                print(f"El dron con ID {dron_id} no está registrado en la base de datos.")
        
        # Si no hay una dirección de base de datos válida, devuelve una posición inicial predeterminada
        #return (1, 1)
            

    def start(self):
        server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_socket.bind(("127.0.0.1", self.listen_port))
        server_socket.listen(1)
        print(f"AD_Engine en funcionamiento. Escuchando en el puerto {self.listen_port}...")

        # Diccionario para rastrear las posiciones iniciales de los drones que se han unido
        initial_positions = {}
        
        while True:
            client_socket, addr = server_socket.accept()
            print(f"Nueva conexión desde {addr}")
            

            # Lee el archivo de registro y obtén el último mensaje
            with open(log_file_name, 'r') as log_file:
                lines = log_file.readlines()

            if lines:
                last_message = lines[-1]
                if "Registro de dron: ID=" in last_message:
                    parts = last_message.split(" ")
                    for part in parts:
                        if part.startswith("ID="):
                            dron_id_str = part[3:]  # Obtiene el ID del dron como una cadena
                            # Elimina caracteres no numéricos, como la coma
                            dron_id_str = ''.join(filter(str.isdigit, dron_id_str))
                            dron_id = int(dron_id_str)
                            print(f"ID del dron: {dron_id}")
                else:
                    print("No se encontró información de ID de dron en el último mensaje.")
            else:
                print("El archivo de registro está vacío o no se pudo leer.")

            
            # Crear un consumidor de Kafka para recibir mensajes de un dron específico
            consumer = KafkaConsumer(f'dron_{addr[1]}', bootstrap_servers=self.broker_address, auto_offset_reset='latest')

            # Consultar la base de datos para obtener la posición inicial del dron
            initial_position = self.get_initial_position_from_database(dron_id)

            if initial_position:
                # Almacenar la posición inicial en el diccionario
                initial_positions[dron_id] = initial_position
                
            # Mensaje a Kafka de dron que se ha unido con su posición inicial y final
            message = f"Dron con ID {dron_id} se ha unido. Posición inicial: {initial_position}, Posición final: {self.final_positions.get(dron_id)}"
            self.kafka_producer.send("register_dron", value=message.encode())
            #self.send_message_to_dron(dron_id, message)
            self.log_to_file(message)

            # Procesar los drones
            move_instructions = self.calculate_move_instructions(initial_positions, self.final_positions)
            self.send_move_instructions(move_instructions)

            # Ejecutar los movimientos y actualizar las posiciones actuales de los drones
            for dron_id, instructions in move_instructions.items():
                current_position = self.current_positions.get(dron_id, initial_positions[dron_id])
                new_position = (instructions["X"], instructions["Y"])
                self.current_positions[dron_id] = new_position

            # Enviar el estado actual del mapa al dron
            self.send_map_state(client_socket)



        

    def send_map_state(self, client_socket):
        map_state = json.dumps(self.map)  # Convertir el mapa a formato JSON
        client_socket.send(map_state.encode())
        
            
    def send_message_to_dron(self, dron_id, message):
        # Construir el mensaje
        message = f"Dron con ID {dron_id} ya puede despegar"
        topic = f'dron_{dron_id}'
        # Enviar un mensaje a un dron específico
        self.kafka_producer.send("register_dron", value=message.encode())
        self.log_to_file(message)

        
    
    def send_move_instructions(self, move_instructions):
        for dron_id, instructions in move_instructions.items():
            message = f"{instructions['type']}, X: {instructions['X']}, Y: {instructions['Y']}"
            # Envía el mensaje a un dron específico
            self.kafka_producer.send("register_dron", value=message.encode())
            self.log_to_file(message)

            
    def log_to_file(self, message, log_file='drone_log.txt'):
        # Obtener el nombre del archivo actual
        current_file = os.path.basename(__file__)

        # Formatear el mensaje con el nombre del archivo
        log_message = f"{current_file}: {message}\n"

        # Registrar el mensaje en el archivo de registro
        with open(log_file, 'a') as f:
            f.write(log_message)
        

if __name__ == "__main__":
    # Punto de entrada principal para ejecutar AD_Engine como un script independiente
    # Configuración de argumentos desde la línea de comandos (ejemplo)
    listen_port = 8080
    max_drones = float('inf')
    broker_address = ("127.0.0.1", 29092)
    weather_address = ("127.0.0.1", 8081)
    #MongoBD
    database_address = "mongodb://localhost:27017/"
    log_file_name = "drone_log.txt"
    
    engine = ADEngine(listen_port, max_drones, broker_address, weather_address, database_address)
    engine.procesar_datos_json("PRUEBAS/AwD_figuras.json")  # Reemplaza con la ruta correcta a tu archivo JSON
    engine.start()
