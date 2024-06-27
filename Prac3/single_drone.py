import argparse
import time
from AD_Drone import ADDrone

def automate_single_drone(drone_id, engine_ip, engine_port, broker_address, mongo_address, api_address, engine_registry_address):
    dron = ADDrone(
        (engine_ip, engine_port),
        broker_address,
        mongo_address,
        api_address,
        engine_registry_address
    )
    dron.dron_id = drone_id  # Asigna el ID pasado por argumento
    dron.alias = f"Dron-{dron.dron_id}"
    dron.load_drones_keys(dron.dron_id)  # Asegúrate de que las claves se carguen correctamente
    dron.get_jwt_token()
    dron.join_show()
    while True:
        time.sleep(1)  # Mantener el proceso vivo

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Automate single drone creation and join show')
    parser.add_argument('--engine_ip', type=str, default='127.0.0.1', help='IP address of the ADEngine')
    parser.add_argument('--engine_port', type=int, default=8080, help='Port number of the ADEngine')
    parser.add_argument('--engine_registry_address', type=str, default='localhost:8081', help='Address of the ADEngine Registry')
    parser.add_argument('--broker_address', type=str, default='localhost:29092', help='Address of the Kafka broker')
    parser.add_argument('--mongo_address', type=str, default='localhost:27017', help='Address of the MongoDB server')
    parser.add_argument('--api_address', type=str, default='https://localhost:5000', help='Address of the API server')
    parser.add_argument('--drone_id', type=int, required=True, help='ID of the drone')
    args = parser.parse_args()

    automate_single_drone(
        args.drone_id,
        args.engine_ip,
        args.engine_port,
        args.broker_address,
        args.mongo_address,
        args.api_address,
        args.engine_registry_address
    )
