import socket
import json
import time
import random
from kafka import KafkaProducer
import ConsumerProducer as cp

class ADWeather:
    def __init__(self, listen_port):
        self.listen_port = listen_port
        self.city_temperatures = self.load_city_temperatures()
        self.broker_address = "localhost:29092"
        self.chosen_city = None  # Almacena la ciudad elegida para el show

    def load_city_temperatures(self):
        try:
            with open('ciudades.json', 'r') as file:
                city_temperatures = json.load(file)
                # Asigna temperaturas iniciales aleatorias en lugar de null
                for city in city_temperatures:
                    city_temperatures[city] = random.randint(1, 40)
                return city_temperatures
        except FileNotFoundError:
            print("El archivo 'ciudades.json' no se encontró. Asegúrate de que el archivo exista en el directorio actual.")
            return {}

    def choose_random_city(self):
        if self.city_temperatures:
            self.chosen_city = random.choice(list(self.city_temperatures.keys()))
        return self.chosen_city

    def start(self):
        try:
            server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            server_socket.bind(("127.0.0.1", self.listen_port))
            server_socket.listen(1)
            print(f"AD_Weather en funcionamiento. Escuchando en el puerto {self.listen_port}")
            city = self.choose_random_city()
            initial_temperature = random.randint(0, 40)  # Asegura que la temperatura inicial sea mayor o igual a 0
            self.city_temperatures[self.chosen_city] = initial_temperature

            # Realizar la elección aleatoria de la ciudad una vez al comienzo
            weather_producer = cp.WeatherProducer(self.broker_address, self.city_temperatures, self.chosen_city)
            weather_producer.start()

            while True:
                client_socket, addr = server_socket.accept()
                print(f"Solicitud de información del clima desde {addr}")
                print(f"Ciudad elegida para el show: {self.chosen_city}")

                while True:
                    temperature = random.randint(-20, 40)
                    self.city_temperatures[self.chosen_city] = temperature
                    print(f"Temperatura actual en {city}: {temperature}")

                    if city == self.chosen_city:
                        weather_producer.send_temperature_to_engine(city, temperature)

                    # Simular cambios de temperatura cada 10 segundos
                    time.sleep(10)

        except OSError as e:
            print(f"OSError al intentar enlazar el socket al puerto: {str(e)}")
        except KeyboardInterrupt:
            print("Interrupción de teclado. Deteniendo el servidor ADWeather.")
            server_socket.close()



if __name__ == "__main__":
    listen_port = 8082
    weather_app = ADWeather(listen_port)
    weather_app.start()
