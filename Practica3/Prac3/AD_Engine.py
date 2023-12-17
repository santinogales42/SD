import socket, ssl
import json
from kafka import KafkaProducer, KafkaConsumer
import pymongo
import logging
import threading
import argparse
import time
from MapViewer import run_map_viewer
from flask import Flask


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
        self.state_lock = threading.Lock()
        self.drones_state = {}
        self.final_positions = {}
        self.current_positions = {}
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
        #Para conexiones seguras
        self.context = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)
        self.context.load_cert_chain(certfile="ssl/service.crt", keyfile="ssl/service.key")
    
    #def accept_connections(self):
    #    while True:
    #        client_socket, _ = self.server_socket.accept()
    #        secure_socket = self.context.wrap_socket(client_socket, server_side=True)
    #        threading.Thread(target=self.handle_drone_connection, args=(secure_socket,)).start()

    def accept_connections(self):
        while True:
            client_socket, _ = self.server_socket.accept()
            threading.Thread(target=self.handle_drone_connection, args=(client_socket,)).start()

    # En la clase ADEngine
    def handle_drone_connection(self, client_socket):
        try:
            data = client_socket.recv(1024).decode('utf-8')
            message = json.loads(data)
            
            if message.get('action') == 'join':
                dron_id = message['ID']
                
                if dron_id in self.final_positions:
                    # Se verifica si el dron es necesario para la figura actual
                    self.connected_drones.add(dron_id)
                    remaining_drones = self.required_drones - len(self.connected_drones)
                    print(f"Drone ID {dron_id} has joined. {remaining_drones} drones are still required.")
                    
                    # Enviar la posición final al dron si es necesario
                    final_position = self.final_positions[dron_id]
                    response_message = {
                        'status': 'success',
                        'final_position': final_position
                    }

                    # Verifica si es el momento de iniciar el show
                    self.check_all_drones_connected()
                else:
                    # El dron no es necesario para la figura actual
                    print(f"Drone ID {dron_id} has joined but is not needed for the current figure.")
                    response_message = {'status': 'not_required'}
                
                client_socket.send(json.dumps(response_message).encode('utf-8'))
            else:
                print(f"Received unknown action from drone: {message.get('action')}")
        
        except Exception as e:
            print(f"An error occurred while handling drone connection: {e}")
        finally:
            client_socket.close()

    def check_all_drones_connected(self):
        if len(self.connected_drones) == self.required_drones:
            print("Todos los drones necesarios para la figura actual están conectados.")
            for dron_id in self.connected_drones:
                self.send_final_position(dron_id, self.final_positions[dron_id])
                self.send_instruction_to_drone(dron_id, 'START')
            print("Instrucciones START y posiciones finales enviadas a todos los drones.")

    def send_instruction_to_drone(self, dron_id, instruction):
        message = {
            'type': 'instruction',
            'dron_id': dron_id,
            'instruction': instruction
        }
        self.kafka_producer.send('drone_messages_topic', message)
        self.kafka_producer.flush()
        print(f"Instrucción {instruction} enviada al dron {dron_id}")
        with self.state_lock:  # Asegurar el acceso al diccionario de estados
            self.drones_state[dron_id] = {'instruction': instruction, 'reached': False}


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
        
        self.kafka_consumer_thread = threading.Thread(target=self.start_kafka_consumer)
        self.kafka_consumer_thread.start()
        
        
        run_map_viewer()
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


    def end_show(self):
        print("El espectáculo ha finalizado.")
        # Finalmente, limpia o reinicia variables si es necesario.
        self.connected_drones.clear()
        self.final_positions.clear()
        if self.map_viewer_process:
            self.map_viewer_process.terminate()
            print("Mapa visual finalizado.")

    def close(self):
        self.stop_event.set()
        if self.kafka_consumer_thread:
            self.kafka_consumer_thread.join()
        self.server_socket.close()
        self.kafka_producer.close()
        print("AD_Engine ha cerrado todos los recursos.")
        
        
        
    def start_kafka_consumer(self):
        consumer = KafkaConsumer(
            'drone_position_reached',
            bootstrap_servers=self.broker_address,
            auto_offset_reset='latest',
            enable_auto_commit=True,
            group_id='engine-group',
            value_deserializer=lambda x: json.loads(x.decode('utf-8'))
        )

        for message in consumer:
            if message.value['type'] == 'position_reached':
                dron_id = message.value['dron_id']
                final_position = message.value['final_position']
                self.handle_drone_position_reached(dron_id, final_position)

    def handle_drone_position_reached(self, dron_id, final_position):
        # Convertir la posición reportada a una tupla si es una lista
        final_position_tuple = tuple(final_position) if isinstance(final_position, list) else final_position
        self.current_positions[dron_id] = final_position_tuple
        print(f"Drone {dron_id} ha alcanzado su posición final: {final_position_tuple}")
        with self.state_lock:
            if dron_id in self.drones_state:
                self.drones_state[dron_id]['reached'] = True
        if self.check_all_drones_in_position():
            self.load_next_figure()
                
    #Cambiado
    def check_all_drones_in_position(self):
        all_in_position = True
        for dron_id, state in self.drones_state.items():
            if not state['reached']:
                all_in_position = False
                break
        if all_in_position:
            logging.info("Todos los drones han alcanzado sus posiciones finales.")
        return all_in_position          
    

    def check_all_drones_connected(self):
        if len(self.connected_drones) == self.required_drones:
            print("Todos los drones están conectados para la figura actual.")
            for dron_id in self.connected_drones:
                # Enviar la nueva posición final y luego la instrucción de START
                self.send_final_position(dron_id, self.final_positions[dron_id])
                self.send_instruction_to_drone(dron_id, 'START')


    def load_next_figure(self):
        self.indice_figura_actual += 1
        if self.indice_figura_actual < len(self.figuras):
            self.cargar_figura(self.indice_figura_actual)
            self.send_positions_and_start_commands()
        else:
            print("Todas las figuras han sido completadas.")
            self.end_show()

    def procesar_datos_json(self, ruta_archivo_json):
        with open(ruta_archivo_json, 'r') as archivo:
            datos = json.load(archivo)

        self.figuras = datos['figuras']  # Lista de todas las figuras
        self.indice_figura_actual = 0  # Índice para seguir la figura actual
        self.cargar_figura(self.indice_figura_actual)

    # En la clase ADEngine

    def cargar_figura(self, indice_figura):
        figura_actual = self.figuras[indice_figura]
        print(f"Cargando nueva figura: {figura_actual['Nombre']}")
        self.required_drones = len(figura_actual['Drones'])
        self.final_positions.clear()

        # Asegurar el acceso al diccionario de estados
        with self.state_lock:
            for dron in figura_actual['Drones']:
                dron_id = dron['ID']
                final_position = tuple(map(int, dron['POS'].split(',')))
                self.final_positions[dron_id] = final_position
                print(f"Posición final del dron {dron_id}: {final_position}")
                # Actualizar estado del dron
                self.drones_state[dron_id] = {'instruction': 'PENDING', 'reached': False}

        self.send_positions_and_start_commands()
        final_positions_message = {
            'type': 'final_positions_update',
            'final_positions': [(dron['ID'], tuple(map(int, dron['POS'].split(','))))
                                for dron in figura_actual['Drones']]
        }
        self.kafka_producer.send('final_positions_topic', final_positions_message)        
        self.kafka_producer.flush()

    def send_positions_and_start_commands(self):
        for dron_id in self.connected_drones:
            self.send_final_position(dron_id, self.final_positions[dron_id])
            self.send_instruction_to_drone(dron_id, 'START')

# Resto del código...


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
