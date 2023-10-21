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
        self.database_address = database_address  # Puede ser None si no se utiliza una base de datos

        # Configurar un productor de Kafka para enviar mensajes a los drones
        self.kafka_producer = KafkaProducer(bootstrap_servers=self.broker_address)

        # Inicializar el mapa 2D (matriz de bytes) para representar el espacio aéreo
        self.map_size = 20  # Tamaño del mapa (20x20)
        self.map = [[0 for _ in range(self.map_size)] for _ in range(self.map_size)]

    def start(self):
        # Método para iniciar la lógica del Engine
        # Configurar el socket para escuchar en el puerto especificado
        server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_socket.bind(("127.0.0.1", self.listen_port))
        server_socket.listen(1)
        print(f"AD_Engine en funcionamiento. Escuchando en el puerto {self.listen_port}...")

        while True:
            # Aceptar conexiones entrantes de drones
            client_socket, addr = server_socket.accept()
            print(f"Nueva conexión desde {addr}")
            
            # Aquí se debe implementar la lógica para gestionar las interacciones con los drones
            # Recibir instrucciones de los drones y actualizar el mapa 2D
            # Consultar el servidor de clima para obtener información climática
            # Aplicar medidas necesarias en función del clima y las posiciones de los drones

            # Crear un consumidor de Kafka para recibir mensajes de un dron específico
            consumer = KafkaConsumer(f'dron_{addr[1]}', bootstrap_servers=self.broker_address, auto_offset_reset='latest')

            for message in consumer:
            # Procesar el mensaje recibido de un dron y actualizar el mapa o realizar otras acciones
            # Enviar el estado actual del mapa a cada dron
                self.send_map_state(client_socket)

            client_socket.close()
    def send_message_to_dron(self, dron_id, message):
        
        # Construir el mensaje
        message = f"Dron con ID {dron_id} ya puede despegar"
        topic = f'dron_{dron_id}'
        # Enviar un mensaje a un dron específico
        self.kafka_producer.send(topic, value=message)

    def send_map_state(self, client_socket):
        # Enviar el estado actual del mapa a un dron
        map_state = json.dumps(self.map)  # Convertir el mapa a formato JSON
        client_socket.send(map_state.encode())

if __name__ == "__main__":
    # Punto de entrada principal para ejecutar AD_Engine como un script independiente
    # Configuración de argumentos desde la línea de comandos (ejemplo)
    listen_port = 8080
    max_drones = 10
    broker_address = ("127.0.0.1", 9092)
    weather_address = ("127.0.0.1", 8081)
    database_address = None  # Puede proporcionarse si se utiliza una base de datos

    engine = ADEngine(listen_port, max_drones, broker_address, weather_address, database_address)
    engine.start()
