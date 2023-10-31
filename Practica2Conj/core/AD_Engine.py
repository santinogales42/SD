import socket
import json
import random
import time
from kafka import KafkaConsumer, KafkaProducer

class ADEngine:
    def __init__(self, listen_port, max_drones, broker_address, weather_address, database_address=None):
        # Constructor de la clase ADEngine
        self.listen_port = listen_port
        self.max_drones = max_drones
        self.broker_address = broker_address
        self.weather_address = weather_address
        self.database_address = database_address  
        # Puede ser None si no se utiliza una base de datos

        # Configurar un productor de Kafka para enviar mensajes a los drones
        self.broker_address = "127.0.0.1:29092"

        self.kafka_producer = KafkaProducer(bootstrap_servers=self.broker_address)

        # Inicializar el mapa 2D (matriz de bytes) para representar el espacio aéreo
        self.map_size = 20  # Tamaño del mapa (20x20)
        self.map = [[0 for _ in range(self.map_size)] for _ in range(self.map_size)]
        
        
        
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
                print(f"Figura: {nombre_figura}")
                for dron in drones:
                    dron_id = dron["ID"]
                    posicion = dron["POS"]
                    print(f"ID del dron: {dron_id}, Posición final: {posicion}")
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
    

    def start(self):
        server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_socket.bind(("127.0.0.1", self.listen_port))
        server_socket.listen(1)
        print(f"AD_Engine en funcionamiento. Escuchando en el puerto {self.listen_port}...")

        while True:
            client_socket, addr = server_socket.accept()
            print(f"Nueva conexión desde {addr}")
            print(f"El dron con ID {addr[1]} se ha unido")

            # Crear un consumidor de Kafka para recibir mensajes de un dron específico
            
            
            consumer = KafkaConsumer(f'dron_{addr[1]}', bootstrap_servers=self.broker_address, auto_offset_reset='latest')


            # Simular la obtención de posiciones iniciales y finales de los drones
            current_positions = {}  # Llena este diccionario con las posiciones iniciales
            final_positions = {}  # Llena este diccionario con las posiciones finales

            # Calcular las instrucciones de movimiento
            move_instructions = self.calculate_move_instructions(current_positions, final_positions)

                # Enviar las instrucciones de movimiento a los drones
            self.send_move_instructions(move_instructions)

                # Enviar el estado actual del mapa al dron
            self.send_map_state(client_socket)

            client_socket.close()

            

    def send_map_state(self, client_socket):
        map_state = json.dumps(self.map)  # Convertir el mapa a formato JSON
        client_socket.send(map_state.encode())
        
            
    def send_message_to_dron(self, dron_id, message):
        # Construir el mensaje
        message = f"Dron con ID {dron_id} ya puede despegar"
        topic = f'dron_{dron_id}'
        # Enviar un mensaje a un dron específico
        self.kafka_producer.send(topic, value=message)
        
    
    def send_move_instructions(self, move_instructions):
        # Lógica para enviar las instrucciones de movimiento a los drones
        for dron_id, instructions in move_instructions.items():
            if instructions["type"] == "MOVE":
                message = json.dumps(instructions)
            elif instructions["type"] == "STOP":
                message = json.dumps(instructions)
            elif instructions["type"] == "EXIT":
                message = json.dumps(instructions)
            else:
                print(f"Instrucción desconocida para el dron {dron_id}: {instructions}")
                continue

            topic = f'dron_{dron_id}'
            # Envía el mensaje a un dron específico
            self.kafka_producer.send(topic, value=message)

        

if __name__ == "__main__":
    # Punto de entrada principal para ejecutar AD_Engine como un script independiente
    # Configuración de argumentos desde la línea de comandos (ejemplo)
    listen_port = 8080
    max_drones = float('inf')
    broker_address = ("127.0.0.1", 29092)
    weather_address = ("127.0.0.1", 8081)
    database_address = None  # Puede proporcionarse si se utiliza una base de datos

    engine = ADEngine(listen_port, max_drones, broker_address, weather_address, database_address)
    engine.procesar_datos_json("PRUEBAS/AwD_figuras.json")  # Reemplaza con la ruta correcta a tu archivo JSON
    engine.start()
