import socket
import json
import random
import time

class ADDrone:
    def __init__(self, engine_address, registry_address):
        self.engine_address = engine_address
        self.registry_address = registry_address
        self.dron_id = None
        self.access_token = None
        self.state = "IDLE"  # El estado se inicia como "IDLE" (en espera)
        self.x, self.y = 1, 1  # Iniciar en la coordenada (1, 1)
        
    def register_drone(self, dron_id, alias):
        # Conectar al módulo de registro (AD_Registry) para registrar el dron
        registry_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        registry_socket.connect(self.registry_address)

        dron_data = {
            'ID': dron_id,
            'Alias': alias
        }
        registry_socket.send(json.dumps(dron_data).encode())
        response = registry_socket.recv(1024).decode()
        registry_socket.close()

        response_json = json.loads(response)
        if response_json['status'] == 'success':
            self.dron_id = dron_id
            self.access_token = response_json['token']
            print(f"Registro exitoso. Token de acceso: {self.access_token}")
            self.state = "IDLE"  # Cambiar el estado a "IDLE" después del registro
        else:
            print(f"Error en el registro: {response_json['message']}")

    def move(self, new_x, new_y):
        # Mover el dron a una nueva posición y notificar al motor (AD_Engine)
        if self.state == "RUN":
            movement_data = {
                'ID': self.dron_id,
                'AccessToken': self.access_token,
                'X': new_x,
                'Y': new_y
            }
            return movement_data

    def show_map(self, engine_socket):
        # Solicitar al motor (AD_Engine) el estado actual del mapa y actualizar la posición del dron
        engine_socket.send(json.dumps({'ID': self.dron_id, 'AccessToken': self.access_token}).encode())
        map_state = engine_socket.recv(1024).decode()
        print(f"Mapa actualizado: {map_state}")
    
    def run(self, engine_socket):
        while True:
            if self.state == "RUN":
                next_x = self.x + random.choice([-1, 1])  # Moverse en X en cualquier dirección
                next_y = self.y + random.choice([-1, 1])  # Moverse en Y en cualquier dirección
                
                # Limitar las coordenadas al rango [1, 20]
                next_x = max(1, min(20, next_x))
                next_y = max(1, min(20, next_y))
                
                movement_data = self.move(next_x, next_y)
                if movement_data:
                    engine_socket.send(json.dumps(movement_data).encode())
                    map_state = engine_socket.recv(1024).decode()
                    print(f"Mapa actualizado: {map_state}")
                    self.x, self.y = next_x, next_y
                else:
                    print("El dron no se está moviendo.")
                    
                time.sleep(1)
                
    def show_menu(self, engine_socket):
        while True:
            print("\nDron Menu:")
            print("1. Registrar dron")
            print("2. Iniciar vuelo")
            print("3. Mostrar mapa")
            print("4. Salir")
            
            choice = input("Seleccione una opción: ")
            
            if choice == "1":
                if self.state == "IDLE":
                    self.register_drone()
                else:
                    print("El dron ya está registrado.")
            elif choice == "2":
                if self.state == "IDLE":
                    self.state = "RUN"
                    print("El dron ha iniciado el vuelo.")
                    self.run(engine_socket)
                else:
                    print("El dron ya está en vuelo.")
            elif choice == "3":
                self.show_map(engine_socket)
            elif choice == "4":
                return
            else:
                print("Opción no válida. Seleccione una opción válida.")

if __name__ == "__main":
    # Configuración de argumentos desde la línea de comandos (ejemplo)
    engine_address = ("127.0.0.1", 8080)  # Dirección del motor (AD_Engine)
    registry_address = ("127.0.0.1", 8081)  # Dirección del registro (AD_Registry)
    dron_id = random.randint(1, 99)  # ID del dron (generado aleatoriamente)

    dron = ADDrone(engine_address, registry_address)
    dron.dron_id = dron_id
    dron.show_menu()
