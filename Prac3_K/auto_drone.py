import argparse
import subprocess
import time
from AD_Drone import ADDrone

def create_and_register_drones(drones_count, engine_ip, engine_port, broker_address, mongo_address, api_address, engine_registry_address):
    drones = []

    # Fase de creación y registro de drones
    for i in range(drones_count):
        dron = ADDrone(
            (engine_ip, engine_port),
            broker_address,
            mongo_address,
            api_address,
            engine_registry_address
        )
        dron.dron_id = i + 1  # Asigna un ID secuencialmente
        dron.alias = f"Dron-{dron.dron_id}"
        dron.generate_keys()
        dron.get_jwt_token()  # Solo hacerlo una vez
        dron.register_drone()
        drones.append(dron)
        time.sleep(1)

    return drones

def launch_drones_in_new_terminals(drones, engine_ip, engine_port, broker_address, mongo_address, api_address, engine_registry_address):
    for dron in drones:
        # Prepara el comando para lanzar cada dron en una nueva terminal
        command = [
            "gnome-terminal", "--", "python3", "single_drone.py",
            "--engine_ip", engine_ip,
            "--engine_port", str(engine_port),
            "--broker_address", broker_address,
            "--mongo_address", mongo_address,
            "--api_address", api_address,
            "--engine_registry_address", engine_registry_address,
            "--drone_id", str(dron.dron_id)
        ]

        # Lanza el comando
        subprocess.Popen(command)
        time.sleep(1)  # Pausa para evitar sobrecargar el servidor

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Automate drone creation and join show')
    parser.add_argument('--engine_ip', type=str, default='127.0.0.1', help='IP address of the ADEngine')
    parser.add_argument('--engine_port', type=int, default=8080, help='Port number of the ADEngine')
    parser.add_argument('--engine_registry_address', type=str, default='localhost:8081', help='Address of the ADEngine Registry')
    parser.add_argument('--broker_address', type=str, default='localhost:29092', help='Address of the Kafka broker')
    parser.add_argument('--mongo_address', type=str, default='localhost:27017', help='Address of the MongoDB server')
    parser.add_argument('--api_address', type=str, default='https://localhost:5000', help='Address of the API server')
    parser.add_argument('--drones_count', type=int, default=1, help='Number of drones to automatically register and join')
    args = parser.parse_args()

    # Crear y registrar drones
    drones = create_and_register_drones(
        args.drones_count,
        args.engine_ip,
        args.engine_port,
        args.broker_address,
        args.mongo_address,
        args.api_address,
        args.engine_registry_address
    )
    
    # Lanzar drones en nuevas terminales
    launch_drones_in_new_terminals(
        drones,
        args.engine_ip,
        args.engine_port,
        args.broker_address,
        args.mongo_address,
        args.api_address,
        args.engine_registry_address
    )
