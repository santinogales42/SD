import threading
from kafka import KafkaConsumer, KafkaProducer
import kafka
import time
import json

# Cambio de nombres de los módulos para adecuarlos a tu práctica
import AD_Engine as engine


FORMAT = 'utf-8'

KAFKA_SERVER = 'localhost:29092'

class ConsumerProducer:
    def __init__(self, broker_address, dron_id=None):
        self.broker_address = broker_address
        self.dron_id = dron_id
        self.kafka_producer = KafkaProducer(bootstrap_servers=self.broker_address)
        self.consumer_thread =  None
        self.consumer_threads = {}

    def start_consumer(self):

        self.consumer_thread = ConsumerDronUpdates(self.broker_address, self.dron_id)
        self.consumer_thread.start()
        
        
    
    def extract_dron_id_from_message(self, message):
        # Analiza el mensaje para extraer el ID del dron
        # La lógica para extraer el ID del dron depende de la estructura de tus mensajes
        # Reemplaza esto con la lógica real necesaria
        try:
            # Ejemplo: Si el mensaje es JSON y contiene un campo "dron_id"
            data = json.loads(message)
            return data.get("self.dron_id")
        except json.JSONDecodeError:
            # Manejo de errores
            return None
        

    def start(self):
        consumer_thread = ConsumerEngine(self.broker_address, self.consumer_threads)
        consumer_thread.start()

        consumer_thread_movement = ConsumerMovement(self.broker_address)
        consumer_thread_movement.start()

        producer_thread_show = ProducerShow(self.broker_address, self.dron_id)
        producer_thread_show.start()

        consumer_thread_city_temperature = CityTemperatureConsumer(self.broker_address)
        consumer_thread_city_temperature.start()

        # Esperar a que los hilos terminen
        consumer_thread.join()
        consumer_thread_movement.join()
        producer_thread_show.join()
        consumer_thread_city_temperature.join()



# Actualización del nombre del tópico para recibir mensajes de registro de drones
class ConsumerEngine(threading.Thread):
    def __init__(self, KAFKA_SERVER, consumer_threads):
        threading.Thread.__init__(self)
        self.stop_event = threading.Event()
        self.server = KAFKA_SERVER
        self.consumer_threads = consumer_threads

    def stop(self):
        self.stop_event.set()

    def run(self):
        consumer = kafka.KafkaConsumer(
            'register_dron', bootstrap_servers=self.server, auto_offset_reset='latest')

        while not self.stop_event.is_set():
            for message in consumer:
                # Obtener el ID del dron a partir del mensaje (cambia esto según tu implementación real)
                dron_id = self.extract_dron_id_from_message(message.value)

                # Comprobar si ya existe un hilo de consumidor para este dron
                if dron_id not in self.consumer_threads:
                    consumer_thread = ConsumerDronUpdates(self.server, dron_id)
                    self.consumer_threads[dron_id] = consumer_thread
                    consumer_thread.start()

                # Procesa el mensaje de registro del dron (cambia esto según tu implementación real)
                print(f"Mensaje de registro del dron: {message.value.decode('utf-8')}")

                if self.stop_event.is_set():
                    break

        consumer.close()

# Resto del código ...

# Clase para el hilo de consumidor de actualizaciones de posición del dron
class ConsumerDronUpdates(threading.Thread):
    def __init__(self, KAFKA_SERVER, dron_id):
        threading.Thread.__init__(self)
        self.stop_event = threading.Event()
        self.server = KAFKA_SERVER
        self.dron_id = dron_id

    def stop(self):
        self.stop_event.set()

    def run(self):
        consumer = kafka.KafkaConsumer(
            'update_position', bootstrap_servers=self.server, auto_offset_reset='latest')

        while not self.stop_event.is_set():
            for message in consumer:
                # Procesa las actualizaciones de posición del dron (cambia esto según tu implementación real)
                print(f"Actualización de posición del dron {self.dron_id}: {message.value.decode('utf-8')}")
                if self.stop_event.is_set():
                    break

        consumer.close()
        
        

# Consumer para recibir instrucciones de movimiento de drones
class ConsumerMovement(threading.Thread):
    def __init__(self, KAFKA_SERVER):
        threading.Thread.__init__(self)
        self.stop_event = threading.Event()
        self.server = KAFKA_SERVER

    def stop(self):
        self.stop_event.set()

    def run(self):
        consumer = kafka.KafkaConsumer(
            'register_movement', bootstrap_servers=self.server, auto_offset_reset='latest')

        while not self.stop_event.is_set():
            for message in consumer:
                # Procesa las instrucciones de movimiento de drones (cambia este nombre y la lógica a la real en tu código)
                engine.process_movement_instructions(message.value)  # Reemplaza con la lógica real
                if self.stop_event.is_set():
                    break

        consumer.close()

        
#Producer para enviar uniones al show de los drones
class ProducerShow(threading.Thread):
    def __init__(self, KAFKA_SERVER, dron_id):
        threading.Thread.__init__(self)
        self.stop_event = threading.Event()
        self.server = KAFKA_SERVER

        self.dron_id = dron_id  # Almacenar el ID del dron

    def stop(self):
        self.stop_event.set()

    def run(self):
        producer = kafka.KafkaProducer(bootstrap_servers=self.server)

        while not self.stop_event.is_set():
            # Utilizar el ID del dron registrado en AD_Registry
            dron_id = self.dron_id
            
            # Construir el mensaje de registro del dron
            registration_message = f"Autenticación correcta. El dron con ID: {dron_id} se va a unir al espectáculo."
            
            # Enviar el mensaje de registro del dron al motor
            producer.send('register_dron', registration_message.encode(FORMAT))
            
            if self.stop_event.is_set():
                break

        producer.close()

        
        
class ProducerMovements(threading.Thread):
    def __init__(self, KAFKA_SERVER, dron_id):
        threading.Thread.__init__(self)
        self.stop_event = threading.Event()
        self.server = KAFKA_SERVER
        self.dron_id = dron_id
        self.producer = kafka.KafkaProducer(bootstrap_servers=self.server)

    def stop(self):
        self.stop_event.set()
        
    def send_movement_instructions(self, instructions):
        for instruction in instructions:
            if instruction["type"] == "STOP":
                print("Instrucción de STOP recibida.")
            else:
                x, y = instruction["X"], instruction["Y"]
                movement_message = f"MOVE: [{x}, {y}], ID: {self.dron_id}"
                self.producer.send('register_movement', movement_message.encode(FORMAT))

    def run(self):
        while not self.stop_event.is_set():
            if self.stop_event.is_set():
                break

        self.producer.close()
        
        

class WeatherProducer(threading.Thread):
    def __init__(self, KAFKA_SERVER, city_data, chosen_city):
        threading.Thread.__init__(self)
        self.stop_event = threading.Event()
        self.server = KAFKA_SERVER
        self.city_data = city_data  # Datos de las ciudades y temperaturas
        self.chosen_city = chosen_city  # Ciudad elegida para el show
        self.producer = kafka.KafkaProducer(bootstrap_servers=self.server)

    def stop(self):
        self.stop_event.set()

    def send_temperature_to_engine(self, city, temperature):
        temperature_message = f"Temperatura actual en {city}: {temperature}"
        self.producer.send('city_temperature', temperature_message.encode(FORMAT))
        self.producer.flush()

    def run(self):
        while not self.stop_event.is_set():
            for city, temperature in self.city_data.items():
                if city == self.chosen_city:
                    # Solo envía la temperatura de la ciudad elegida al motor
                    self.send_temperature_to_engine(city, temperature)

                if self.stop_event.is_set():
                    break

            # Esperar un intervalo de tiempo (por ejemplo, 10 segundos) antes de enviar las actualizaciones nuevamente
            time.sleep(10)

        self.producer.close()
        
        
# En la clase ADEngine o en un módulo separado
class CityTemperatureConsumer(threading.Thread):
    def __init__(self, KAFKA_SERVER):
        threading.Thread.__init__(self)
        self.stop_event = threading.Event()
        self.server = KAFKA_SERVER

    def stop(self):
        self.stop_event.set()

    def run(self):
        consumer = kafka.KafkaConsumer(
            'city_temperature', bootstrap_servers=self.server, auto_offset_reset='latest')

        while not self.stop_event.is_set():
            for message in consumer:
                # Procesa las actualizaciones de temperatura de la ciudad (cambia este nombre y la lógica a la real en tu código)
                print(f"Actualización de temperatura de la ciudad: {message.value.decode('utf-8')}")
                if self.stop_event.is_set():
                    break

        consumer.close()
