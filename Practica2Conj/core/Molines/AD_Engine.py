import socket
import json
from kafka import KafkaProducer
import pymongo
import threading
import argparse
import time

class ADEngine:
    def __init__(self, listen_port, max_drones, broker_address, database_address, weather_address):
        self.listen_port = listen_port
        self.max_drones = max_drones
        self.broker_address = broker_address
        self.database_address = database_address
        self.weather_address = weather_address
        weather_ip, weather_port_str = weather_address.split(':')
        self.weather_address = (weather_ip, int(weather_port_str))
        self.client = pymongo.MongoClient(self.database_address)
        self.db = self.client["dronedb"]
        self.final_positions = {}
        self.connected_drones = set()
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.bind(("127.0.0.1", self.listen_port))
        self.server_socket.listen(15)
        self.kafka_producer = KafkaProducer(
            bootstrap_servers=[self.broker_address],
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        self.accept_thread = threading.Thread(target=self.accept_connections)
        self.accept_thread.start()

    def accept_connections(self):
        while True:
            client_socket, _ = self.server_socket.accept()
            threading.Thread(target=self.handle_drone_connection, args=(client_socket,)).start()

    def handle_drone_connection(self, client_socket):
        try:
            data = client_socket.recv(1024).decode('utf-8')
            message = json.loads(data)
            if message.get('action') == 'join':
                dron_id = message['ID']
                self.connected_drones.add(dron_id)
                final_position = self.final_positions.get(dron_id)
                if final_position:
                    self.send_final_position(dron_id, final_position)
        finally:
            client_socket.close()

    def send_final_position(self, dron_id, final_position):
        message = {
            'type': 'final_position',
            'dron_id': dron_id,
            'final_position': final_position
        }
        self.kafka_producer.send('drone_final_position', value=message)
        self.kafka_producer.flush()

    def procesar_datos_json(self, ruta_archivo_json):
        with open(ruta_archivo_json, 'r') as archivo:
            datos = json.load(archivo)
        self.final_positions = {dron['ID']: tuple(map(int, dron['POS'].split(',')))
                                for figura in datos['figuras']
                                for dron in figura['Drones']}


    def start(self):
        print(f"AD_Engine en funcionamiento. Escuchando en el puerto {self.listen_port}...")
        self.weather_thread = threading.Thread(target=self.update_weather_conditions)
        self.weather_thread.start()
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

            
    def respond_to_final_position_request(self, dron_id):
        final_position = self.get_final_position(dron_id)
        if final_position:
            message = {
                'type': 'final_position',
                'dron_id': dron_id,
                'final_position': final_position
            }
            self.kafka_producer.send('drone_final_position', message)
            self.kafka_producer.flush()


    def check_all_drones_connected(self):
        # Comprueba si todos los drones requeridos para la figura actual están conectados.
        connected_required_drones = [dron_id for dron_id in self.connected_drones if dron_id in self.final_positions]
        if len(connected_required_drones) == self.required_drones:
            print("Todos los drones requeridos para la figura están conectados. Iniciando la figura.")
            self.start_show()
        else:
            remaining_drones = self.required_drones - len(connected_required_drones)
            print(f"Esperando por {remaining_drones} drones más para iniciar la figura.")
            # Actualiza el estado de los drones que ahora son necesarios
        
    def send_instruction_to_drone(self, dron_id, instruction):
        message = {
            'type': 'instruction',
            'dron_id': dron_id,
            'instruction': instruction
        }
        self.kafka_producer.send('drone_messages_topic', message)
        self.kafka_producer.flush()
        print(f"Instrucción {instruction} enviada al dron {dron_id}")
        if instruction.startswith("MOVE") and self.get_final_position(dron_id) is None:
            print(f"No se puede mover el dron {dron_id}: posición final no establecida.")
            return

    def end_show(self):
        print("El espectáculo ha finalizado.")
        # Finalmente, limpia o reinicia variables si es necesario.
        self.connected_drones.clear()
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
        self.last_figure_time = time.time()

    def check_drone_position(self, dron_id):
        current_position = self.get_current_position(dron_id)
        final_position = self.get_final_position(dron_id)
        if current_position == final_position:
            self.drones_completed[dron_id] = True  # Marca como completado
            print(f"Dron {dron_id} ha confirmado llegada a la posición final.")
        else:
            print(f"Dron {dron_id} aún no ha llegado a la posición final.")
            
    def show_in_progress(self):
        for dron_id, position in self.current_positions.items():
            if position != self.final_positions.get(dron_id, position):
                print(f"Dron {dron_id} está en la posición {position}")
                # Si algún dron no está en su posición final, el espectáculo sigue en progreso
                return True
        # Si todos los drones están en su posición final, el espectáculo no está en progreso
        return False
    
    def get_initial_position(self, dron_id):
        dron_data = self.db.drones.find_one({"ID": dron_id})
        # Si el dron se encuentra en la base de datos, devuelve su posición inicial
        if dron_data:
            return dron_data.get('InitialPosition')  # Devuelve una posición predeterminada si no se encuentra 'InitialPosition'

    def get_current_position(self, dron_id):
        # Supongamos que las posiciones actuales se almacenan en el estado de la clase
        return self.current_positions.get(dron_id, self.get_initial_position(dron_id))
    
    def get_final_position(self, dron_id):
        # Supongamos que las posiciones finales se almacenan en un diccionario
        return self.final_positions.get(dron_id)
            
            
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
        while True:
            temperature = self.fetch_weather_data()
            if temperature is not None and temperature < 0:
                print("Condiciones climáticas adversas. Espectáculo finalizado.")
                self.end_show()
            time.sleep(15)

    def update_drone_position(self, dron_id, position):
        # Solo verifica si el dron ha llegado a la posición final
        final_position = self.final_positions.get(dron_id)
        if final_position and tuple(position) == final_position:
            print(f"Dron {dron_id} ha confirmado llegada a la posición final.")
            self.send_instruction_to_drone(dron_id, 'END')  # Enviar señal de que ha llegado a la posición final
            #self.send_message_to_map_viewer(dron_id, position, state)
        
    def send_message_to_map_viewer(self, dron_id, position, state):
        message = {
            'ID': dron_id,
            'Position': position,
            'State': state
        }
        # Elige el tópico correcto basado en el estado del dron
        topic = 'final_positions_topic' if state == 'FINAL' else 'drone_position_updates'
        try:
            self.kafka_producer.send(topic, value=message)
            self.kafka_producer.flush()
        except Exception as e:
            print(f"Error al enviar mensaje al MapViewer: {e}")


    def start_map_viewer(self):
        message = {
            'type': 'control',
        }
        self.kafka_producer.send('map_control', message)
        self.kafka_producer.flush()
        print("Se ha enviado la señal de inicio al visualizador del mapa.")

if __name__ == "__main__":
    # Crear el analizador de argumentos
    parser = argparse.ArgumentParser(description='AD_Engine start-up arguments')

    # Agregar argumentos esperados con valores por defecto
    parser.add_argument('--listen_port', type=int, default=8080, help='Port to listen on for drone connections')
    parser.add_argument('--max_drones', type=int, default=20, help='Maximum number of drones to support')
    parser.add_argument('--broker_address', default="127.0.0.1:29092", help='Address of the Kafka broker')
    parser.add_argument('--weather_address', default="127.0.0.1:8082", help='Address of the weather service')
    parser.add_argument('--database_address', default="mongodb://localhost:27017/", help='MongoDB URI for the drones database')
    parser.add_argument('--json', default="PRUEBAS/AwD_figuras_Correccion.json", help='Path to the JSON file with figures configuration')

    # Parsear los argumentos
    args = parser.parse_args()

    # Inicializar ADEngine con los argumentos parseados
    engine = ADEngine(
        listen_port=args.listen_port,
        max_drones=args.max_drones,  # Asegúrate de manejar este argumento en tu clase ADEngine
        broker_address=args.broker_address,
        weather_address=args.weather_address,
        database_address=args.database_address
    )

    # Cargar configuración de figuras desde el archivo JSON
    engine.procesar_datos_json(args.json)

    # Iniciar el motor (AD_Engine)
    engine.start()
