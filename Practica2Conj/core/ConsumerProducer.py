import threading
from kafka import KafkaConsumer, KafkaProducer
import kafka

# Cambio de nombres de los módulos para adecuarlos a tu práctica
import AD_Engine as engine


FORMAT = 'utf-8'

KAFKA_SERVER = 'localhost:29092'

class ConsumerProducer:
    def __init__(self, broker_address, dron_id=None):
        self.broker_address = broker_address
        self.dron_id = dron_id
        self.kafka_producer = KafkaProducer(bootstrap_servers=self.broker_address)
        self.consumer_thread = None  # Agrega esta línea para inicializar consumer_thread

    def start_consumer(self):
        self.consumer_thread = ConsumerDronUpdates(self.broker_address, self.dron_id)
        self.consumer_thread.start()
        

    def start(self):
        # Crear e iniciar un hilo para consumir mensajes de registro de drones
        consumer_thread = ConsumerEngine(self.KAFKA_SERVER)
        consumer_thread.start()

        # Crear e iniciar un hilo para producir mensajes de registro de drones
        producer_thread = ProducerDron(self.KAFKA_SERVER, self.dron_id)
        producer_thread.start()

        # Esperar a que los hilos terminen
        consumer_thread.join()
        producer_thread.join()



# Actualización del nombre del tópico para recibir mensajes de registro de drones
class ConsumerEngine(threading.Thread):
    def __init__(self, KAFKA_SERVER):
        threading.Thread.__init__(self)
        self.stop_event = threading.Event()
        self.server = KAFKA_SERVER

    def stop(self):
        self.stop_event.set()

    def run(self):
        consumer = kafka.KafkaConsumer(
            'register_dron', bootstrap_servers=self.server, auto_offset_reset='latest')

        while not self.stop_event.is_set():
            for message in consumer:
                # Llama a la función de registro del dron (cambia este nombre a la función real en tu código)
                engine.register_dron(message.value)
                if self.stop_event.is_set():
                    break

        consumer.close()
        
# Consumer para recibir actualizaciones de posición del dron y otras actualizaciones
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
                # Procesa las actualizaciones de posición del dron y otras actualizaciones (cambia este nombre y la lógica a la real en tu código)
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


# Producer para enviar registros de drones
class ProducerDron(threading.Thread):
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
            registration_message = f"Registro de dron: ID={dron_id}"
            
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