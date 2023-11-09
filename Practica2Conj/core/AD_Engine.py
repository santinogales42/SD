import socket
import json
from kafka import KafkaConsumer, KafkaProducer
import pymongo
import threading
import time

# Constantes para instrucciones
INSTRUCTION_START = 'START'
INSTRUCTION_STOP = 'STOP'
INSTRUCTION_MOVE = 'MOVE'

class DroneThread(threading.Thread):
    def __init__(self, dron_id, engine_instance):
        super().__init__()
        self.dron_id = dron_id
        self.engine_instance = engine_instance
        self._stop_event = threading.Event()
        self.is_started = False
        self.is_completed = False

    def run(self):
        # Esperar a recibir la instrucción START
        while not self.is_started and not self._stop_event.is_set():
            time.sleep(1)
        # Una vez iniciado, procesar las posiciones
        while not self.is_completed and not self._stop_event.is_set():
            current_position = self.engine_instance.get_current_position(self.dron_id)
            final_position = self.engine_instance.get_final_position(self.dron_id)
            if current_position == final_position:
                self.engine_instance.send_instruction_to_drone(self.dron_id, INSTRUCTION_STOP)
                self.is_completed = True
            else:
                next_position = self.engine_instance.calculate_next_position(current_position, final_position)
                self.engine_instance.send_instruction_to_drone(self.dron_id, f"{INSTRUCTION_MOVE}:{next_position}")
                time.sleep(5)  # Intervalo para simular tiempo de movimiento

    def stop(self):
        self._stop_event.set()

class ADEngine:
    def __init__(self, listen_port, broker_address, database_address, weather_address):
        self.listen_port = listen_port
        self.broker_address = broker_address
        self.database_address = database_address
        self.weather_address = weather_address
        weather_ip, weather_port_str = weather_address.split(':')
        self.weather_address = (weather_ip, int(weather_port_str))
        
        
        # Conexión con la base de datos MongoDB
        self.client = pymongo.MongoClient(self.database_address)
        self.db = self.client["dronedb"]
        
        # Configuración inicial
        self.current_positions = {}
        self.final_positions = {}
        self.required_drones = None
        self.connected_drones = set()
        self.drone_threads = {}

        # Configuración del socket del servidor
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.bind(("127.0.0.1", self.listen_port))
        self.server_socket.listen(15)
        
        self.stop_event = threading.Event()
        self.weather_thread = None
        
        self.kafka_producer = KafkaProducer(
            bootstrap_servers=[broker_address],
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        
        # Hilos para manejar conexiones y mensajes
        self.accept_thread = threading.Thread(target=self.accept_connections)
        self.accept_thread.start()


    def start(self):
        print(f"AD_Engine en funcionamiento. Escuchando en el puerto {self.listen_port}...")
        self.weather_thread = threading.Thread(target=self.update_weather_conditions)
        self.weather_thread.start()
        self.kafka_consumer_thread = threading.Thread(target=self.start_kafka_consumer)
        self.kafka_consumer_thread.start()
        try:
            while True:
                client_socket, addr = self.server_socket.accept()
                print(f"Nueva conexión desde {addr}")
                drone_connection_thread = threading.Thread(target=self.handle_drone_connection, args=(client_socket,))
                drone_connection_thread.start()
        except KeyboardInterrupt:
            print("AD_Engine detenido por el usuario.")
        finally:
            self.close()


    def accept_connections(self):
        while not self.stop_event.is_set():
            client_socket, addr = self.server_socket.accept()
            print(f"Conexión aceptada de {addr}")
            drone_connection_thread = threading.Thread(target=self.handle_drone_connection, args=(client_socket,))
            drone_connection_thread.start()

    def handle_drone_connection(self, client_socket):
        try:
            data = client_socket.recv(1024).decode('utf-8')
            if not data:
                raise ValueError("No se recibieron datos.")

            message = json.loads(data)
            if message.get('action') == 'join':
                dron_id = message['ID']
                if dron_id not in self.connected_drones:
                    self.connected_drones.add(dron_id)
                    self.current_positions[dron_id] = self.get_initial_position(dron_id)
                    self.final_positions[dron_id] = self.get_final_position(dron_id)
                    print(f"Drone con ID={dron_id} se ha unido.")
                    self.check_all_drones_connected()
                else:
                    print(f"Drone con ID={dron_id} ya está unido.")
        except Exception as e:
            print(f"Error al manejar la conexión del dron: {e}")
        finally:
            client_socket.close()
            
    def respond_to_final_position_request(self, dron_id):
        final_position = self.get_final_position(dron_id)
        if final_position:
            message = {
                'type': 'final_position',
                'dron_id': dron_id,
                'final_position': final_position
            }
            self.kafka_producer.send('drone_messages_topic', message)
            self.kafka_producer.flush()


    def check_all_drones_connected(self):
        if self.required_drones is not None and len(self.connected_drones) == self.required_drones:
            self.start_show()
        else:
            remaining_drones = self.required_drones - len(self.connected_drones)
            print(f"Esperando por {remaining_drones} drones más.")

    def start_show(self):
        print("Todos los drones requeridos están conectados. El espectáculo ha comenzado.")
        self.send_start_instructions()
        # Iniciar los hilos de movimiento después de una breve pausa para dar tiempo a que todos los drones reciban START
        time.sleep(5)  # Pausa para asegurar que los drones reciben la instrucción START
        self.initiate_movement_sequence()
        
    def send_instruction_to_drone(self, dron_id, instruction):
        message = {
            'type': 'instruction',
            'dron_id': dron_id,
            'instruction': instruction
        }
        self.kafka_producer.send('drone_messages_topic', message)
        self.kafka_producer.flush()
        print(f"Instrucción {instruction} enviada al dron {dron_id}")
        
    def initiate_movement_sequence(self):
        for dron_id in self.connected_drones:
            drone_thread = DroneThread(dron_id, self)
            drone_thread.start()
            self.drone_threads[dron_id] = drone_thread

    def send_start_instructions(self):
        for dron_id in self.connected_drones:
            self.send_instruction_to_drone(dron_id, INSTRUCTION_START)
    
    def send_stop_instructions(self):
        for dron_id in self.connected_drones:
            self.send_movement_instructions_to_drone(dron_id, INSTRUCTION_STOP)

    def send_movement_instructions_to_drone(self, dron_id, target_position):
        message = {
            'type': 'instruction',
            'dron_id': dron_id,
            'instruction': f"{INSTRUCTION_MOVE}:{target_position}"
        }
        self.kafka_producer.send('drone_messages_topic', message)
        self.kafka_producer.flush()
        print(f"Instrucción {message} enviada al dron {dron_id}")

    def calculate_next_position(self, current_position, final_position):
        # Desempaca las posiciones actuales y finales
        current_x, current_y = current_position
        final_x, final_y = final_position
        
        # Calcula la diferencia en ambos ejes
        delta_x = final_x - current_x
        delta_y = final_y - current_y

        # Determina el movimiento en el eje x
        if delta_x > 0:
            next_x = current_x + 1  # Mover hacia la derecha
        elif delta_x < 0:
            next_x = current_x - 1  # Mover hacia la izquierda
        else:
            next_x = current_x  # No se mueve en x

        # Determina el movimiento en el eje y
        if delta_y > 0:
            next_y = current_y + 1  # Mover hacia abajo
        elif delta_y < 0:
            next_y = current_y - 1  # Mover hacia arriba
        else:
            next_y = current_y  # No se mueve en y

        # Devuelve la siguiente posición
        return (next_x, next_y)
        
    def start_kafka_consumer(self):
        # Configurar el consumidor de Kafka
        consumer = KafkaConsumer(
            'drone_position_updates', # Asegúrate de que el tópico sea el correcto
            bootstrap_servers=self.broker_address,
            auto_offset_reset='latest',
            enable_auto_commit=True,
            group_id='engine-group'  # Un group_id para el engine
        )

        for message in consumer:
            message_data = json.loads(message.value.decode('utf-8'))
            if message_data['type'] == 'position_update':
                # Actualiza la posición actual del dron en AD_Engine
                self.update_drone_position(message_data['ID'], message_data['new_position'])
            if message_data['type'] == 'request_final_position':
                dron_id = message_data['dron_id']   
                # Responde a la solicitud de posición final
                self.respond_to_final_position_request(dron_id)
        
    def end_show(self):
        print("El espectáculo ha finalizado debido a condiciones climáticas adversas.")
        # Aquí iría cualquier lógica adicional para manejar el fin del espectáculo.
        # Por ejemplo, podrías querer enviar un mensaje a todos los drones para que regresen a su base.
        base_position = (1, 1)
        for dron_id in self.connected_drones:
            self.send_movement_instructions_to_drone(dron_id, base_position)

        # Finalmente, limpia o reinicia variables si es necesario.
        self.connected_drones.clear()
        self.current_positions.clear()
        self.final_positions.clear()
        

    def close(self):
        self.stop_event.set()
        if self.weather_thread:
            self.weather_thread.join()
        if self.kafka_consumer_thread:
            self.kafka_consumer_thread.join()
        self.server_socket.close()
        self.kafka_producer.close()
        print("AD_Engine ha cerrado todos los recursos.")

    def procesar_datos_json(self, ruta_archivo_json):
        with open(ruta_archivo_json, 'r') as archivo:
            datos = json.load(archivo)

        self.figuras = datos['figuras']  # Lista de todas las figuras
        self.indice_figura_actual = 0  # Índice para seguir la figura actual
        self.cargar_figura(self.indice_figura_actual)

    def cargar_figura(self, indice_figura):
        figura_actual = self.figuras[indice_figura]
        print(f"Figura actual: {figura_actual['Nombre']}")
        self.required_drones = len(figura_actual['Drones'])  # Número de drones necesarios para la figura actual
        print(f"Se requieren {self.required_drones} drones para esta figura.")
        self.final_positions.clear()  # Limpiar las posiciones finales anteriores

        # Acceder a la nueva colección para las posiciones finales
        drones_fp_collection = self.db["drones_fp"]

        for dron in figura_actual['Drones']:
            dron_id = dron['ID']
            final_position = tuple(map(int, dron['POS'].split(',')))  # Convertir la posición a una tupla de enteros

            # Actualizar la nueva colección con la posición final
            drones_fp_collection.update_one(
                {"ID": dron_id},
                {"$set": {"FinalPosition": final_position}},
                upsert=True
            )

            self.final_positions[dron_id] = final_position  # Nuevas posiciones finales
            print(f"Dron ID: {dron_id}, Posición final: {final_position}")  # Imprime el ID del dron y su posición final

        # Asegúrate de que todos los drones requeridos están conectados antes de comenzar
        self.check_all_drones_connected()


    def check_figure_completion(self):
        all_in_position = all(
            self.current_positions.get(dron_id) == self.final_positions.get(dron_id)
            for dron_id in self.connected_drones 
        )
        if all_in_position:
            print(f"Figura {self.indice_figura_actual + 1} completada.")
            self.indice_figura_actual += 1  # Moverse a la siguiente figura
            if self.indice_figura_actual < len(self.figuras):
                self.cargar_figura(self.indice_figura_actual)  # Cargar la siguiente figura
            else:
                print("Todas las figuras se han completado.")
                self.end_show()  # Finalizar el espectáculo si todas las figuras se completaron
                
    def initiate_movement_sequence(self):
        for dron_id in self.connected_drones:
            drone_thread = DroneThread(dron_id, self)
            drone_thread.start()  # Esto iniciará el método run() del hilo en paralelo
            # Puedes mantener una referencia a los hilos si necesitas interactuar con ellos más tarde
            self.drone_threads[dron_id] = drone_thread
            
    def show_in_progress(self):
        for dron_id, position in self.current_positions.items():
            if position != self.final_positions.get(dron_id, position):
                # Si algún dron no está en su posición final, el espectáculo sigue en progreso
                return True
        # Si todos los drones están en su posición final, el espectáculo no está en progreso
        return False

    def monitor_progress(self):
        all_drones_started = False  # Añadido para controlar el estado del inicio
        while self.show_in_progress():
            for dron_id in self.connected_drones:
                drone_thread = self.drone_threads[dron_id]
                if drone_thread.is_completed:
                    continue

                current_position = self.get_current_position(dron_id)
                final_position = self.get_final_position(dron_id)
                if current_position != final_position:
                    # Esperar a que todos los drones hayan iniciado antes de enviar MOVE
                    if all_drones_started:
                        self.send_instruction_to_drone(dron_id, f"{INSTRUCTION_MOVE}:{final_position}")
                else:
                    drone_thread.set_completed()  # Marcar el dron como completado
                    self.send_instruction_to_drone(dron_id, INSTRUCTION_STOP)  # Instruir al dron a detenerse
            # Comprobar si todos los drones han iniciado después de la primera pasada
            if not all_drones_started:
                all_drones_started = all(
                    not drone_thread.is_alive() or drone_thread.is_started
                    for drone_thread in self.drone_threads.values()
                )
            time.sleep(1)  
    
    def get_initial_position(self, dron_id):
        dron_data = self.db.drones.find_one({"ID": dron_id})
        # Si el dron se encuentra en la base de datos, devuelve su posición inicial
        if dron_data:
            return dron_data.get('InitialPosition')  # Devuelve una posición predeterminada si no se encuentra 'InitialPosition'

    def get_current_position(self, dron_id):
        # Supongamos que las posiciones actuales se almacenan en el estado de la clase
        return self.current_positions.get(dron_id)
    
    def get_final_position(self, dron_id):
        # Supongamos que las posiciones finales se almacenan en un diccionario
        return self.final_positions.get(dron_id)
    
    
    def update_current_position(self, dron_id, position):
        # Actualiza la posición actual del dron en el sistema
        self.current_positions[dron_id] = position

    def show_in_progress(self):
        for dron_id, position in self.current_positions.items():
            print(f"Dron {dron_id} está en la posición {position}")
            
    def fetch_weather_data(self):
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                sock.connect(self.weather_address)
                sock.send(json.dumps({'action': 'get_temperature'}).encode('utf-8'))
                weather_data = json.loads(sock.recv(1024).decode())
                print(f"Datos del clima recibidos: {weather_data}")
                return weather_data['temperature']
        except Exception as e:
            print(f"Error al conectar con AD_Weather: {e}")
            return None  # O manejar de otra manera

    def update_weather_conditions(self):
        while not self.stop_event.is_set():
            temperature = self.fetch_weather_data()
            if temperature is not None and temperature < 0:
                print("Condiciones climáticas adversas. Espectáculo finalizado.")
                self.end_show()
            time.sleep(15)

    def update_drone_position(self, dron_id, position):
        # Actualiza la posición actual del dron en el sistema
        self.current_positions[dron_id] = position
        # Verifica si el dron ha llegado a la posición final
        final_position = self.final_positions.get(dron_id)
        if final_position is None:
            print(f"Error: No se encontró la posición final para el dron {dron_id}.")
            return  # Salir del método si no hay posición final

        next_position = self.calculate_next_position(position, final_position)
        
        if position == self.final_positions.get(dron_id):
            print(f"Dron {dron_id} ha confirmado llegada a la posición final.")
            # Aquí podrías marcar al dron como que ha alcanzado su posición final
            # para evitar enviarle más instrucciones
            # Por ejemplo, podrías tener un conjunto de drones 'en posición'
        else:
            print(f"Dron {dron_id} ha confirmado llegada a la posición intermedia {position}.")
            # Actualiza la posición actual del dron y calcula el siguiente movimiento
            next_position = self.calculate_next_position(position, self.final_positions.get(dron_id))
            # Si la siguiente posición es diferente a la actual, envía la nueva instrucción
            if next_position != position:
                self.send_movement_instructions_to_drone(dron_id, next_position)
                self.current_positions[dron_id] = position
                self.check_figure_completion()


if __name__ == "__main__":
    listen_port = 8080
    broker_address = "127.0.0.1:29092"
    database_address = "mongodb://localhost:27017/"
    weather_address = "127.0.0.1:8082"
    
    engine = ADEngine(listen_port, broker_address, database_address, weather_address)
    engine.procesar_datos_json("PRUEBAS/AwD_figuras_Correccion.json")
    engine.start()