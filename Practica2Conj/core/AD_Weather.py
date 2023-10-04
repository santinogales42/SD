import socket
import sqlite3
import random

class ADWeather:
    def __init__(self, listen_port, database_path):
        self.listen_port = listen_port
        self.database_path = database_path

    def get_random_temperature(self, city):
        # Generar una temperatura aleatoria para la ciudad (entre -10 y 40 grados Celsius)
        return random.uniform(-10, 40)

    def start(self):
        # Conectar a la base de datos SQLite
        conn = sqlite3.connect(self.database_path)
        cursor = conn.cursor()

        # Configurar el socket para escuchar en el puerto especificado
        server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_socket.bind(("127.0.0.1", self.listen_port))
        server_socket.listen(1)
        print(f"AD_Weather en funcionamiento. Escuchando en el puerto {self.listen_port}...")

        while True:
            # Aceptar conexiones entrantes desde AD_Engine
            client_socket, addr = server_socket.accept()
            print(f"Solicitud de clima desde {addr}")

            # Recibir la ciudad para la cual se solicita el clima desde AD_Engine
            city = client_socket.recv(1024).decode()
            # Obtener la temperatura para la ciudad desde la base de datos (o generar aleatoriamente si no está en la base de datos)
            temperature = self.get_random_temperature(city)
            # Enviar la temperatura al AD_Engine
            client_socket.send(str(temperature).encode())

            client_socket.close()

if __name__ == "__main__":
    # Configuración de argumentos desde la línea de comandos (ejemplo)
    listen_port = 8082
    database_path = "cities.db"  # Archivo de la base de datos SQLite

    weather = ADWeather(listen_port, database_path)
    weather.start()
