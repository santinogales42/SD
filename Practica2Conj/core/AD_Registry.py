import socket
import json
import os
from pymongo import MongoClient
from kafka import KafkaProducer

class ADRegistry:
    def __init__(self, listen_port, db_host, db_port, db_name, log_file):
        self.listen_port = listen_port
        self.db_host = db_host
        self.db_port = db_port
        self.db_name = db_name
        self.log_file = log_file

        self.client = MongoClient(self.db_host, self.db_port)
        self.db= self.client[self.db_name]
        self.kafka_producer = KafkaProducer(bootstrap_servers='localhost:29092')  # Asegúrate de que esto sea la dirección y puerto correctos de tu servidor Kafka


    def start(self):
        server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_socket.bind(("127.0.0.1", self.listen_port))
        server_socket.listen(1)
        print(f"AD_Registry en funcionamiento. Escuchando en el puerto {self.listen_port}...")

        while True:
            client_socket, addr = server_socket.accept()
            print(f"Nueva solicitud de registro desde {addr}")

            # Manejar solicitud de registro
            request_data = client_socket.recv(1024).decode()
            try:
                request_json = json.loads(request_data)
                if 'ID' in request_json and 'Alias' in request_json:
                    drone_id = request_json['ID']
                    alias = request_json['Alias']
                    addr = addr[1]
                    # Registrar dron en el archivo de registro y en Kafka
                    access_token = self.register_drone(drone_id, alias, addr)
                    response = {'status': 'success', 'message': 'Registro exitoso', 'token': access_token}
                else:
                    response = {'status': 'error', 'message': 'Solicitud de registro incorrecta'}
            except json.JSONDecodeError:
                response = {'status': 'error', 'message': 'Formato JSON inválido'}
            
            # Enviar respuesta al dron
            response_json = json.dumps(response)
            client_socket.send(response_json.encode())
            client_socket.close()
    
    #ADDR
    def register_drone(self, drone_id, alias, addr):
        # Generar un token de acceso único para el dron
        access_token = f"Token_{addr}"
        
        #Posicion inicial del dron
        initial_position = (1,1)
    
        #Guardar el addr del dron y juntarlo con el id
        
        # Crear un diccionario con los datos del dron
        drone_data = {
            'ID': drone_id,
            'Alias': alias,
            'AccessToken': access_token,
            'InitialPosition': initial_position
        }

        # Registrar el dron en la base de datos MongoDB
        self.db.drones.insert_one(drone_data)

        # Enviar un mensaje a Kafka utilizando self.kafka_producer
        kafka_message = f"Registro de dron: ID={drone_id}, Alias={alias}"
        self.kafka_producer.send("register_dron", value=kafka_message.encode())
        self.log_to_file(kafka_message)

        return access_token


    def log_to_file(self, message, log_file='drone_log.txt'):
        # Obtener el nombre del archivo actual
        current_file = os.path.basename(__file__)

        # Formatear el mensaje con el nombre del archivo
        log_message = f"{current_file}: {message}\n"

        # Registrar el mensaje en el archivo de registro
        with open(log_file, 'a') as f:
            f.write(log_message)
            

if __name__ == "__main__":
    # Configuración de argumentos desde la línea de comandos (ejemplo)
    listen_port = 8081
    db_host = 'localhost'
    db_port = 27017
    db_name = 'dronedb'
    log_file = 'drone_log.txt'  # Nombre del archivo de registro

    registry = ADRegistry(listen_port, db_host, db_port, db_name, log_file)
    registry.start()
