import socket
import json
from pymongo import MongoClient

class ADRegistry:
    def __init__(self, listen_port, db_host, db_port, db_name):
        self.listen_port = listen_port
        self.db_host = db_host
        self.db_port = db_port
        self.db_name = db_name
        self.client = MongoClient(self.db_host, self.db_port)
        self.db = self.client[self.db_name]

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
                    # Registrar dron en la base de datos MongoDB
                    access_token = self.register_drone(drone_id, alias)
                    response = {'status': 'success', 'message': 'Registro exitoso', 'token': access_token}
                else:
                    response = {'status': 'error', 'message': 'Solicitud de registro incorrecta'}
            except json.JSONDecodeError:
                response = {'status': 'error', 'message': 'Formato JSON inválido'}
            
            # Enviar respuesta al dron
            response_json = json.dumps(response)
            client_socket.send(response_json.encode())
            client_socket.close()

    def register_drone(self, drone_id, alias):
        # Generar un token de acceso único para el dron
        access_token = f"TOKEN_{drone_id}"
        # Registrar el dron en la base de datos MongoDB
        drone_data = {
            'ID': drone_id,
            'Alias': alias,
            'AccessToken': access_token
        }
        self.db.drones.insert_one(drone_data)
        print(f"Dron {alias} registrado con ID: {drone_id} en la base de datos.")
        return access_token

if __name__ == "__main__":
    # Configuración de argumentos desde la línea de comandos (ejemplo)
    listen_port = 8081
    db_host = 'localhost'
    db_port = 27017
    db_name = 'dronedb'

    registry = ADRegistry(listen_port, db_host, db_port, db_name)
    registry.start()
