import socket
import json
import ConsumerProducer as cp
from kafka import KafkaConsumer, KafkaProducer
import pymongo
import re
import threading
import time


class DroneThread(threading.Thread):
    def __init__(self, dron_id, engine_instance):
        super().__init__()
        self.dron_id = dron_id
        self.engine_instance = engine_instance
        self._stop_event = threading.Event()

    def run(self):
        try:
            while not self._stop_event.is_set():
                # Sincronizar el acceso a los datos compartidos para evitar condiciones de carrera
                with self.engine_instance.drones_lock:
                    current_position = self.engine_instance.current_positions.get(self.dron_id)
                    final_position = self.engine_instance.final_positions.get(self.dron_id)

                # Si hay una posición actual y final definida para el dron
                if current_position and final_position:
                    # Calcular la siguiente posición hacia la cual el dron debe moverse
                    next_position = self.engine_instance.calculate_next_position(current_position, final_position)
                    # Si el dron aún no ha llegado a su posición final
                    if current_position != final_position:
                        # Enviar instrucciones de movimiento al dron
                        self.engine_instance.send_movement_instructions_to_dron(self.dron_id, next_position)
                    else:
                        # El dron ha llegado a su posición final
                        print(f"Dron {self.dron_id} ha llegado a su posición final.")
                        break

                # Esperar antes de la próxima actualización para no sobrecargar el CPU
                time.sleep(1)
        except Exception as e:
            print(f"Error en DroneThread {self.dron_id}: {e}")

    def stop(self):
        # Método para detener el hilo de forma segura
        self._stop_event.set()

        

class ADEngine:
    def __init__(self, listen_port, broker_address, database_address, weather_address):
        self.listen_port = listen_port
        self.broker_address = broker_address
        self.database_address = database_address
        self.weather_address = weather_address
        
        # Conexión con la base de datos MongoDB
        self.client = pymongo.MongoClient(self.database_address)
        self.db = self.client["dronedb"]
        
        # Configuración inicial
        self.current_positions = {}
        self.final_positions = {}
        self.required_drones = None
        self.connected_drones = set()

        # Configuración del socket del servidor
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.bind(("127.0.0.1", self.listen_port))
        self.server_socket.listen(15)
        
        self.kafka_producer = KafkaProducer(
            bootstrap_servers=[broker_address],
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        
        # Hilos para manejar conexiones y mensajes
        self.accept_thread = threading.Thread(target=self.accept_connections)
        self.accept_thread.start()


    def start(self):
        print(f"AD_Engine en funcionamiento. Escuchando en el puerto {self.listen_port}...")
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

    def handle_drone_connection(self, client_socket):
        try:
            data = client_socket.recv(1024)
            if not data:
                raise ValueError("No se recibieron datos.")

            message = json.loads(data.decode('utf-8'))
            if message.get('action') == 'join':
                dron_id = message['ID']
                self.connected_drones.add(dron_id)
                print(f"Drone con ID={dron_id} se ha unido.")
                self.check_all_drones_connected()
        except Exception as e:
            print(f"Error al manejar la conexión del dron: {e}")
        finally:
            client_socket.close()

    def check_all_drones_connected(self):
        if self.required_drones is not None:
            remaining_drones = self.required_drones - len(self.connected_drones)
            if remaining_drones > 0:
                print(f"Esperando por {remaining_drones} drones más.")
            else:
                print("Todos los drones necesarios se han conectado. ¡Comienza el espectáculo!")
                self.start_show()

    def start_show(self):
        print("El espectáculo ha comenzado.")
        # Implementación para iniciar el espectáculo

    def close(self):
        self.server_socket.close()
        print("AD_Engine ha cerrado todos los recursos.")

    def accept_connections(self):
        while True:
            client_socket, addr = self.server_socket.accept()
            print(f"Conexión aceptada de {addr}")
            # Procesamiento de la conexión

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
        for dron in figura_actual['Drones']:
            dron_id = dron['ID']
            final_position = tuple(map(int, dron['POS'].split(',')))  # Convertir la posición a una tupla de enteros
            self.final_positions[dron_id] = final_position  # Nuevas posiciones finales
            print(f"Dron ID: {dron_id}, Posición final: {final_position}")  # Imprime el ID del dron y su posición final

        # Asegúrate de que todos los drones requeridos están conectados antes de comenzar
        self.check_all_drones_connected()

    def check_figure_completion(self):
        all_in_position = all(
            self.current_positions[dron_id] == self.final_positions[dron_id]
            for dron_id in self.final_positions
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
            initial_position = self.get_initial_position(dron_id)
            self.send_movement_instructions_to_drone(dron_id, initial_position)
            # Puede haber lógica adicional para manejar la sincronización

    def monitor_progress(self):
        while self.show_in_progress:
            for dron_id in self.connected_drones:
                current_position = self.get_current_position(dron_id)
                final_position = self.final_positions[dron_id]
                if current_position != final_position:
                    self.send_movement_instructions_to_drone(dron_id, final_position)
                else:
                    # El dron ha llegado a su posición final, potencialmente marca como completo
                    pass
            time.sleep(1)
            
    def send_movement_instructions_to_drone(self, dron_id, target_position):
        try:
            # Suponemos que el método send() del productor de Kafka está configurado para enviar mensajes.
            # La key asegura que todos los mensajes para un dron particular vayan a la misma partición y,
            # por lo tanto, se procesen en el orden correcto.
            message = {
                'type': 'instruction',
                'dron_id': dron_id,
                'target_position': target_position
            }
            self.kafka_producer.send(
                'drone_messages_topic', 
                key=str(dron_id).encode(), 
                value=json.dumps(message).encode('utf-8')
            )
            self.kafka_producer.flush()
            print(f"Instrucciones de movimiento enviadas al dron {dron_id} para moverse a {target_position}")
        except Exception as e:
            print("ERROR {e}")
    
    def get_initial_position(self, dron_id):
        # Supongamos que las posiciones iniciales están guardadas en una base de datos o en una variable
        return self.db.drones.find_one({"ID": dron_id})['InitialPosition']

    def get_current_position(self, dron_id):
        # Supongamos que las posiciones actuales se almacenan en el estado de la clase
        return self.current_positions.get(dron_id)

    def show_in_progress(self):
        for dron_id, position in self.current_positions.items():
            print(f"Dron {dron_id} está en la posición {position}")


if __name__ == "__main__":
    listen_port = 8080
    broker_address = "127.0.0.1:29092"
    database_address = "mongodb://localhost:27017/"
    weather_address = "127.0.0.1:8082"
    
    engine = ADEngine(listen_port, broker_address, database_address, weather_address)
    engine.procesar_datos_json("PRUEBAS/AwD_figuras_Correccion.json")
    engine.start()