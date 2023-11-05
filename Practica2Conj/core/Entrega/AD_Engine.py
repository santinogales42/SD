import socket
import json
import ConsumerProducer as cp
from kafka import KafkaConsumer, KafkaProducer
import pymongo
import re
import threading
import time

class ADEngine:
    def __init__(self, listen_port, broker_address, database_address, weather_address):
        self.listen_port = listen_port
        self.broker_address = broker_address
        self.database_address = database_address
        self.weather_address = weather_address
        
        
        
        if self.database_address:
            self.client = pymongo.MongoClient(self.database_address)
            self.db = self.client["dronedb"]
        self.dron_id = None
        
        self.consumer_producer = cp.ConsumerDronUpdates(broker_address, self.dron_id)
        self.kafka_producer = KafkaProducer(bootstrap_servers=self.broker_address)
        self.map_size = 20
        self.map = [[{"dron_id": None} for _ in range(self.map_size)] for _ in range(self.map_size)]
        
        self.current_positions = {}
        self.final_positions = {}
    
    
    
    def check_weather_and_take_actions(self):
        city_name = "CiudadEjemplo"
        temperature = self.get_temperature_from_weather_service(city_name)

        if temperature < 0:
            self.finalizar_espectaculo()

    def get_temperature_from_weather_service(self, city_name):
        temperature = self.weather_app.get_temperature(city_name)
        return temperature

    def finalizar_espectaculo(self):
        print("CONDICIONES CLIMATICAS ADVERSAS. ESPECTACULO FINALIZADO")
        
    
    def drones_posicionados_finalmente(self):
        return len(self.final_positions) == len(self.current_positions)


    def save_figura_info(self, figura, dron_id):
        figura_info = {
            "Figura": figura["Nombre"],
            "DronID": dron_id,
            "PosicionFinal": tuple(map(int, figura["Drones"][dron_id - 1]["POS"].split(',')))
        }

        existing_info = self.db.figuras.find_one({"DronID": dron_id})
        if existing_info is None:
            self.db.figuras.insert_one(figura_info)
        else:
            self.db.figuras.update_one({"DronID": dron_id}, {"$set": {"PosicionFinal": figura_info["PosicionFinal"]}})


    def procesar_datos_json(self, ruta_archivo_json):
        datos = self.cargar_datos_desde_json(ruta_archivo_json)
        if datos:
            figuras = datos["figuras"]
            for figura in figuras:
                nombre_figura = figura["Nombre"]
                drones = figura["Drones"]
                print(f"Procesando Figura: {nombre_figura}")
                for dron in drones:
                    dron_id = dron["ID"]
                    posicion_str = dron["POS"]
                    posicion = [int(coord) for coord in posicion_str.split(',')]
                    self.final_positions[dron_id] = posicion
                    print(f"ID del dron: {dron_id}, Posición final: {posicion}")
                    self.save_figura_info(figura, dron_id)
        else:
            print("No se pudieron cargar los datos del archivo JSON.")

    def cargar_datos_desde_json(self, ruta_archivo_json):
        try:
            with open(ruta_archivo_json, "r") as archivo:
                datos = json.load(archivo)
                return datos
        except FileNotFoundError:
            print(f"No se encontró el archivo JSON en la ruta: {ruta_archivo_json}")
            return None
        except json.JSONDecodeError as e:
            print(f"Error al decodificar el archivo JSON: {str(e)}")
            return None
        

    def calculate_next_position(self, current_position, final_position):
        if current_position is not None:
            current_x, current_y = current_position
        else:
            raise ValueError("current_position es None y no debería serlo en este punto.")

        target_x, target_y = final_position

        if current_x < target_x:
            current_x += 1
        elif current_x > target_x:
            current_x -= 1

        if current_y < target_y:
            current_y += 1
        elif current_y > target_y:
            current_y -= 1

        return current_x, current_y

    def calculate_move_instructions(self, dron_id, final_position):
        move_instructions = []

        current_position = self.get_initial_position_from_database(dron_id)
        final_position = self.get_final_position_from_database(dron_id)
        
        if current_position != final_position:
            next_position = self.calculate_next_position(current_position, final_position)
            move_instructions.append({
                "type": "MOVE",
                "X": next_position[0],
                "Y": next_position[1]
            })
            current_position = next_position
            print(f"Proxima posicion: {next_position}")
        else:
            move_instructions.append({"type": "STOP"})
            print(f"El dron con ID {dron_id} ya ha alcanzado su posición final.")
        
        return move_instructions


    def get_initial_position_from_database(self, dron_id):
        if self.database_address:
            drones_collection = self.db["drones"]
            try:
                dron_data = drones_collection.find_one({"ID": dron_id})
                if dron_data:
                    return dron_data.get("InitialPosition")
                else:
                    print(f"El dron con ID {dron_id} no está registrado en la base de datos.")
            except Exception as e:
                print(f"Error al acceder a la base de datos: {str(e)}")
        return None

    def get_final_position_from_database(self, dron_id):
        if self.database_address:
            figuras_collection = self.db["figuras"]
            try:
                figura_data = figuras_collection.find_one({"DronID": dron_id})
                if figura_data:
                    return figura_data.get("PosicionFinal")
                else:
                    print(f"El dron con ID {dron_id} no está registrado en la base de datos.")
            except Exception as e:
                print(f"Error al acceder a la base de datos: {str(e)}")
        return None
                

    def receive_dron_id(self, client_socket):
        try:
            kafka_consumer = KafkaConsumer('register_dron', bootstrap_servers=self.broker_address, auto_offset_reset='latest', group_id='ade_group')
            keep_waiting = True 

            while keep_waiting:
                for message in kafka_consumer:
                    message_value = message.value.decode('utf-8')
                    print(f"Mensaje recibido: {message_value}")
                    keep_waiting = False 
                    match = re.search(r'El dron con ID: (\d+) se va a unir al espectáculo', message_value)
                    if match:
                        dron_id = int(match.group(1))
                        return dron_id
        except Exception as e:
            print(f"Error al recibir el ID del dron: {str(e)}")
            
            
    def process_dron_updates(dron_id, update_data):
        print(f"Procesando actualizaciones del dron {dron_id}: {update_data}")

    

    def start(self):
        try:
            server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            server_socket.bind(("127.0.0.1", self.listen_port))
            server_socket.listen(1)
            print(f"AD_Engine en funcionamiento. Escuchando en el puerto {self.listen_port}...")

            initial_positions = {}

            temperature_consumer_thread = cp.CityTemperatureConsumer(self.broker_address)
            temperature_consumer_thread.start()

            while True:
                client_socket, addr = server_socket.accept()
                print(f"Nueva conexión desde {addr}")

                dron_id = self.receive_dron_id(client_socket)

                initial_position = self.get_initial_position_from_database(dron_id)
                current_position = initial_position

                if initial_position:
                    initial_positions[dron_id] = initial_position
                    self.current_positions[dron_id] = initial_position
                print(f"Posición inicial del dron con ID {dron_id}: {initial_position}")

                producer_threads = {}
                drones_terminados = []

                try:

                    while True:
                        move_instructions = self.calculate_move_instructions(dron_id, self.final_positions[dron_id])
                        print(f"Instrucciones de movimiento: {move_instructions}")

                        if dron_id not in producer_threads:
                            producer_thread = cp.ProducerMovements(self.broker_address, dron_id)
                            producer_threads[dron_id] = producer_thread
                            producer_thread.start()

                        producer_threads[dron_id].send_movement_instructions(move_instructions)

                        final_position = self.get_final_position_from_database(dron_id)
                        new_position = self.calculate_next_position(current_position, final_position)

                        current_position = new_position

                        initial_position = self.get_initial_position_from_database(dron_id)
                        print(f"Posición actual: {initial_position}")

                        time.sleep(2)

                        self.send_map_state(client_socket)

                        if move_instructions and move_instructions[0].get("type") == "STOP":
                            drones_terminados.append(dron_id)
                            break 

                except ConnectionResetError:
                    print(f"El dron con ID {dron_id} se ha desconectado.")
                    initial_position = self.get_initial_position_from_database(dron_id)
                    if initial_position:
                        self.current_positions[dron_id] = (1, 1)
                        self.db.drones.update_one({"ID": dron_id}, {"$set": {"InitialPosition": self.current_positions[dron_id]}})
                    print(f"El dron con ID {dron_id}, vuelve a {self.current_positions[dron_id]}")
                    continue
                
                if drones_terminados:
                    print("Drones que han llegado a la posición final:")
                    for dron_id in drones_terminados:
                        print(f"Dron ID: {dron_id}")

                drones_posicionados_finalmente = self.drones_posicionados_finalmente()
                if drones_posicionados_finalmente:
                    print("Todos los drones han llegado a su posición final.")
                    break 

        except KeyboardInterrupt:
            print("El motor se apagó bruscamente. Realizando acciones de cierre...")
            server_socket.close()
            print("Motor apagado bruscamente. ¡Hasta luego!")  
            

    def update_map_with_dron_position(self, dron_id, new_position):
        if dron_id in self.current_positions:
            current_x, current_y = self.current_positions[dron_id]
            self.map[current_x][current_y] = {"dron_id": None}
        x, y = new_position
        self.map[x][y] = {"dron_id": dron_id}
        self.current_positions[dron_id] = new_position
    

    def send_map_state(self, client_socket):
        map_state = json.dumps(self.map)
        client_socket.send(map_state.encode())
        

if __name__ == "__main__":
    listen_port = 8080
    broker_address = "127.0.0.1:29092"
    database_address = "mongodb://localhost:27017/"
    weather_address = "127.0.0.1:8082"
    
    engine_address = ADEngine(listen_port, broker_address, database_address, weather_address)
    engine_address.procesar_datos_json("PRUEBAS/AwD_figuras.json")
    engine_address.start()
    
    #POR COMANDOS
    #parser = argparse.ArgumentParser(description='AD Engine Application')
    #parser.add_argument('--listen_port', type=int, default=8080, help='Port for listening')
    #parser.add_argument('--broker_address', type=str, default='127.0.0.1:29092', help='Broker address')
    #parser.add_argument('--database_address', type=str, default='mongodb://localhost:27017/', help='Database address')
    #parser.add_argument('--weather_address', type=str, default='127.0.0.1:8082', help='Weather service address')

    # Parsear los argumentos
    #args = parser.parse_args()
