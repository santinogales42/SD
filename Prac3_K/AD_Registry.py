import socket
import json
import time
import requests
import threading
from pymongo import MongoClient
from kafka import KafkaProducer
from os import getenv
from uuid import uuid4
import argparse
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import padding, rsa
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.backends import default_backend
import base64
import warnings
from urllib3.exceptions import InsecureRequestWarning

# Desactivar solo las advertencias de solicitudes HTTPS no verificadas
warnings.simplefilter('ignore', InsecureRequestWarning)


class ADRegistry:
    def __init__(self, listen_port, db_host, db_port, broker_address, api_address):
        self.listen_port = listen_port
        self.db_host = db_host
        self.db_port = db_port

        self.clients = {}
        self.broker_address = broker_address
        self.api_address = api_address
        self.private_key = rsa.generate_private_key(
            public_exponent=65537,
            key_size=2048
        )
        self.public_key = self.private_key.public_key()

#####SOCKET#####
    def load_drone_keys(self, drone_id):
        try:
            db = self.get_db(drone_id)
            key_document = db.Claves.find_one({'ID': drone_id})
            if key_document is None:
                print(f"No se encontró la clave para el dron ID {drone_id}")
                return None

            private_key_data = key_document['PrivateKey']
            private_key = serialization.load_pem_private_key(
                private_key_data,
                password=None,
                backend=default_backend()
            )
            return private_key
        except Exception as e:
            print(f"Error al cargar clave privada: {e}")
            return None

    def get_db(self, drone_id):
        if drone_id not in self.clients:
            client = MongoClient(self.db_host, self.db_port)
            self.clients[drone_id] = client[f'dronedb_{drone_id}']
        return self.clients[drone_id]


    def handle_client(self, client_socket, addr):
        try:
            request_data = client_socket.recv(1024).decode()
            data_dict = json.loads(request_data)
            drone_id = data_dict['ID']
            encrypted_data = base64.b64decode(data_dict['Data'])

            private_key = self.load_drone_keys(drone_id)
            if private_key is None:
                raise ValueError(f"No se pudo cargar la clave privada para el dron {drone_id}")

            decrypted_data = private_key.decrypt(
                encrypted_data,
                padding.OAEP(
                    mgf=padding.MGF1(algorithm=hashes.SHA256()),
                    algorithm=hashes.SHA256(),
                    label=None
                )
            )
            request_json = json.loads(decrypted_data.decode('utf-8'))
            
            if 'ID' in request_json and 'Alias' in request_json:
                drone_id = request_json['ID']
                alias = request_json['Alias']

                drone_data = self.register_drone(drone_id, alias)
                print(f"Registro exitoso del dron {drone_id} con alias {alias}")
                response = {'status': 'success', 'message': 'Registro exitoso'}
                
            else:
                response = {'status': 'error', 'message': 'Solicitud de registro incorrecta'}
        except json.JSONDecodeError:
            response = {'status': 'error', 'message': 'Formato JSON inválido'}
        except Exception as e:
            response = {'status': 'error', 'message': str(e)}

        response_json = json.dumps(response)
        client_socket.send(response_json.encode())
        client_socket.close()

    def start(self):
        server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_socket.bind(("0.0.0.0", self.listen_port))
        server_socket.listen(15)
        print(f"AD_Registry en funcionamiento. Escuchando en el puerto {self.listen_port}...")
        
        try:
            while True:
                client_socket, addr = server_socket.accept()
                print(f"Nueva solicitud de registro desde {addr}")
                client_thread = threading.Thread(target=self.handle_client, args=(client_socket, addr))
                client_thread.start()
                
        except KeyboardInterrupt:
            print("AD_Registry detenido por el usuario")
        finally:
            server_socket.close()
            client_thread.close()

    def register_drone(self, drone_id, alias):
        try:
            # Generar clave privada y pública para el dron
            private_key = rsa.generate_private_key(
                public_exponent=65537,
                key_size=2048,
                backend=default_backend()
            )
            public_key = private_key.public_key()

            # Serializar las claves
            private_key_pem = private_key.private_bytes(
                encoding=serialization.Encoding.PEM,
                format=serialization.PrivateFormat.TraditionalOpenSSL,
                encryption_algorithm=serialization.NoEncryption()
            )
            public_key_pem = public_key.public_bytes(
                encoding=serialization.Encoding.PEM,
                format=serialization.PublicFormat.SubjectPublicKeyInfo
            )

            # Guardar claves en la base de datos específica del dron
            db = self.get_db(drone_id)
            db.Claves.insert_one({
                'ID': drone_id,
                'PrivateKey': private_key_pem.decode('utf-8'),
                'PublicKey': public_key_pem.decode('utf-8')
            })

            initial_position = [1, 1]
            drone_data_message = {
                'type': 'register',
                'ID': drone_id,
                'Alias': alias,
                'InitialPosition': initial_position
            }

            encrypted_message = self.public_key.encrypt(
                json.dumps(drone_data_message).encode(),
                padding.OAEP(
                    mgf=padding.MGF1(algorithm=hashes.SHA256()),
                    algorithm=hashes.SHA256(),
                    label=None
                )
            )
            encoded_message = base64.b64encode(encrypted_message).decode('utf-8')
            producer = KafkaProducer(
                bootstrap_servers=self.broker_address,
                key_serializer=str.encode,
                value_serializer=lambda m: json.dumps(m).encode('utf-8')
            )
            producer.send('drone_messages_topic', key=str(drone_id), value=encoded_message)
            producer.flush()
            producer.close()

            db.drones.insert_one(drone_data_message)
            kafka_message_document = {
                'Clase': 'ADRegistry',
                'type': 'register',
                'OriginalMessage': drone_data_message,
                'EncryptedMessage': encoded_message
            }
            db.MensajesKafka.insert_one(kafka_message_document)
            self.log_auditoria('Registro', f"Registro exitoso del dron {drone_id}", tipo='registry')


            return drone_data_message
        except Exception as e:
            print(f"Error al registrar el dron: {e}")

    def register_drone_via_api(self, drone_id, alias):
        url = f"{self.api_address}/registro"
        data = {'ID': drone_id, 'Alias': alias}
        response = requests.post(url, json=data)
        if response.status_code == 200:
            return response.json()
        else:
            return {'status': 'error', 'message': 'Error en la solicitud API'}
        
    def log_auditoria(self, evento, descripcion, tipo="registry"):
        try:
            data = {
                'evento': evento,
                'descripcion': descripcion,
                'timestamp': time.strftime("%Y-%m-%d %H:%M:%S"),
                'tipo': tipo
            }
            response = requests.post(f'{self.api_address}/auditoria', json=data, verify=False)
            if response.status_code == 201:
                print(f"Evento de auditoría registrado: {evento}")
            else:
                print(f"Error al registrar evento de auditoría: {response.text}")
        except requests.exceptions.RequestException as e:
            print(f"Error de conexión al registrar evento de auditoría: {e}")
            


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="ADRegistry Configuration")
    parser.add_argument('--listen_port', type=int, default=8081, help='Listen port for ADRegistry')
    parser.add_argument('--db_host', type=str, default='localhost', help='MongoDB host')
    parser.add_argument('--db_port', type=int, default=27017, help='MongoDB port')
    parser.add_argument('--broker_address', type=str, default='localhost:29092', help='Kafka broker address')
    parser.add_argument('--api_address', type=str, default='https://localhost:5000', help='API address')
    args = parser.parse_args()

    registry = ADRegistry(args.listen_port, args.db_host, args.db_port, args.broker_address, args.api_address)
    registry.start()
