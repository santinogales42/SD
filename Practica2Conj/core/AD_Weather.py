import socket
import json
import threading
import random
from kafka import KafkaProducer
import time


class ADWeather:
    def __init__(self, listen_port, broker_address):
        self.listen_port = listen_port
        self.broker_address = broker_address
        self.city_temperatures = self.load_city_temperatures()
        self.chosen_city = None  # Almacena la ciudad elegida para el show
        
        self.kafka_producer = KafkaProducer(
            bootstrap_servers=[broker_address],
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )

    def load_city_temperatures(self):
        # Intenta cargar la temperatura de las ciudades desde un archivo JSON
        try:
            with open('ciudades.json', 'r') as file:
                city_temperatures = json.load(file)
            return city_temperatures
        except FileNotFoundError:
            print("El archivo 'ciudades.json' no se encontró.")
            return {}

    def choose_random_city(self):
        # Elige una ciudad aleatoria de las disponibles y asigna una temperatura inicial mayor a 0
        self.chosen_city = random.choice(list(self.city_temperatures.keys()))
        self.city_temperatures[self.chosen_city] = random.randint(1, 40)  # Asegura que la primera temperatura sea mayor a 0
        return self.chosen_city

    def start_weather_producer(self):
        while True:
            # Actualiza la temperatura de la ciudad elegida aleatoriamente entre -10 y 40
            self.city_temperatures[self.chosen_city] = random.randint(-10, 40)
            # Construye el mensaje
            weather_update = {
                'city': self.chosen_city,
                'temperature': self.city_temperatures[self.chosen_city]
            }
            # Enviar la actualización de la temperatura a través de Kafka
            self.kafka_producer.send('get_temperature', value=weather_update)
            self.kafka_producer.flush()
            time.sleep(15)
            
            

    def handle_client_request(self, client_socket):
        # Maneja las solicitudes entrantes de los clientes
        try:
            request_data = client_socket.recv(1024).decode('utf-8')
            if not request_data:  # Verifica que se haya recibido algún dato
                raise ValueError("No se recibieron datos o la cadena está vacía.")

            request_json = json.loads(request_data)  # Intenta decodificar el JSON
            # ... procesa la solicitud ...
            if request_json['action'] == 'get_temperature':
                self.send_weather_info(client_socket)
               

            # Puedes añadir más acciones según sea necesario
        except Exception as e:
            print(f"Error al manejar la solicitud del cliente: {e}")
        finally:
            client_socket.close()

    def send_weather_info(self, client_socket):
        # Envía la información del clima al cliente
        weather_info = {
            'city': self.chosen_city,
            'temperature': self.city_temperatures[self.chosen_city]
        }
        client_socket.send(json.dumps(weather_info).encode('utf-8'))

    def start(self):
        # Configura el socket del servidor y comienza a escuchar las solicitudes
        server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_socket.bind(("127.0.0.1", self.listen_port))
        server_socket.listen(1)
        print(f"AD_Weather en funcionamiento. Escuchando en el puerto {self.listen_port}")

        # Elige una ciudad y su temperatura al azar al principio
        self.choose_random_city()

        # Inicia el productor de clima en un hilo separado
        weather_producer_thread = threading.Thread(target=self.start_weather_producer)
        weather_producer_thread.start()
        #Falta el random a la temperatura
        try:
            while True:
                client_socket, addr = server_socket.accept()
                print(f"Solicitud de información del clima desde {addr}")
                # Maneja cada solicitud del cliente en su propio hilo
                client_thread = threading.Thread(target=self.handle_client_request, args=(client_socket,))
                client_thread.start()
        except KeyboardInterrupt:
            print("Servidor AD_Weather detenido por el usuario.")
        except Exception as e:
            print(f"Error del servidor: {e}")
        finally:
            server_socket.close()
            weather_producer_thread.join()  # Asegúrate de que el hilo del productor termina correctamente

if __name__ == "__main__":
    # Estos valores podrían venir de un archivo de configuración o variables de entorno
    listen_port = 8082
    broker_address = "localhost:29092"
    weather_app = ADWeather(listen_port, broker_address)
    weather_app.start()
