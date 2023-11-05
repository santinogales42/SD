import socket
import json
import time
import random
from kafka import KafkaProducer
import ConsumerProducer as cp

class ADWeather:
    def __init__(self, listen_port):
        self.listen_port = listen_port
        self.city_data = self.load_city_data()
        self.clima = {}
        
        self.broker_adress = "localhost:29092"

    def load_city_data(self):
        try:
            with open('ciudades.json', 'r') as file:
                city_data = json.load(file)
            return city_data
        except FileNotFoundError:
            print("El archivo 'ciudades.json' no se encontró. Asegúrate de que el archivo exista en el directorio actual.")
            return {}
        
    def get_temperature(self, city):
        if not self.city_data:
            print("No se han cargado datos de ciudades.")
            return None

        temperature = self.city_data.get(city, 0)
        
        return temperature
    
    
    def get_city_and_temperature(self, dron_id):
        return True

    def start(self):
        self.load_city_data()
        server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_socket.bind(("127.0.0.1", self.listen_port))
        server_socket.listen(1)
        print(f"AD_Weather en funcionamiento. Escuchando en el puerto {self.listen_port}...")

        while True:
            client_socket, addr = server_socket.accept()
            print(f"Solicitud de información del clima desde {addr}")

            data = client_socket.recv(1024).decode()
            request = json.loads(data)

            if 'city' in request:
                city = request['city']
                temperature = self.get_temperature(city)
                print(f"Temperatura actual en {city}: {temperature}")
                self.clima[city] = temperature
                
                producer_thread = cp.WeatherProducer(self.broker_address, self.city_data)
                producer_thread.start()


                if temperature < 0:
                    return False
            else:
                print("Solicitud de información incorrecta")

if __name__ == "__main__":
    listen_port = 8082
    city_data_file = "ciudades.json"
    weather_app = ADWeather(listen_port)
    weather_app.start()
