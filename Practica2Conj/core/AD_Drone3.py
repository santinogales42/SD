import socket
import json
import random

class ADDrone:
    def __init__(self, engine_address, registry_address):
        self.engine_address = engine_address
        self.registry_address = registry_address
        self.dron_id = None
        self.access_token = None

    def register_drone(self):
        # Conectar al módulo de registro (AD_Registry) para registrar el dron
        registry_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        registry_socket.connect(self.registry_address)

        dron_data = {
            'ID': self.dron_id,
            'Alias': f'Dron_{self.dron_id}'
        }
        registry_socket.send(json.dumps(dron_data).encode())
        response = registry_socket.recv(1024).decode()
        registry_socket.close()

        response_json = json.loads(response)
        if response_json['status'] == 'success':
            self.access_token = response_json['token']
            print(f"Registro exitoso. Token de acceso: {self.access_token}")
        else:
            print(f"Error en el registro: {response_json['message']}")

    def join_show(self):
        # Conectar al motor (AD_Engine) usando el token de acceso
        engine_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        engine_socket.connect(self.engine_address)

        while True:
            # Simular movimiento del dron
            next_x = random.randint(1, 20)
            next_y = random.randint(1, 20)
            movement_data = {
                'ID': self.dron_id,
                'AccessToken': self.access_token,
                'X': next_x,
                'Y': next_y
            }
            engine_socket.send(json.dumps(movement_data).encode())
            map_state = engine_socket.recv(1024).decode()
            print(f"Mapa actualizado: {map_state}")

            # Simular intervalos de tiempo entre movimientos
            time.sleep(1)

if __name__ == "__main__":
    # Configuración de argumentos desde la línea de comandos (ejemplo)
    engine_address = ("127.0.0.1", 8080)  # Dirección del motor (AD_Engine)
    registry_address = ("127.0.0.1", 8081)  # Dirección del registro (AD_Registry)
    dron_id = random.randint(1, 99)  # ID del dron (generado aleatoriamente)

    dron = ADDrone(engine_address, registry_address)
    dron.dron_id = dron_id
    dron.register_drone()
    dron.join_show()
