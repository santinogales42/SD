from flask import Flask, request, jsonify, render_template
from flask_jwt_extended import JWTManager, create_access_token, jwt_required
from pymongo import MongoClient, errors
from bson import ObjectId
from datetime import timedelta
from cryptography.fernet import Fernet
from werkzeug.security import generate_password_hash, check_password_hash
from flask_cors import CORS
import os
import requests
import logging
from api_w import WEATHER_API_KEY

app = Flask(__name__)
CORS(app)
client = MongoClient(os.environ.get('MONGO_URI', 'mongodb://localhost:27017/'))
db = client.dronedb
app.config["JWT_SECRET_KEY"] = "tu_clave_secreta"  # Cambia esto por una clave segura
jwt = JWTManager(app)

BASE_URL = "http://api.openweathermap.org/data/2.5/weather"

def get_weather(city_name):
    params = {
        'q': city_name,
        'appid': WEATHER_API_KEY,
        'units': 'metric'
    }
    response = requests.get(BASE_URL, params=params)
    if response.status_code == 200:
        data = response.json()
        temperature = data['main']['temp']
        return temperature
    else:
        return None

@app.route('/')
def index():
    return render_template('index.html')


@app.route('/weather/<city>', methods=['GET'])
def weather(city):
    temperature = get_weather(city)
    if temperature is not None:
        return jsonify({'city': city, 'temperature': temperature})
    else:
        return jsonify({'error': 'No se pudo obtener el clima'}), 404


def error_response(message, status_code):
    return jsonify({'error': message}), status_code


###### SEGURIDAD ######

@app.route('/registro_usuario', methods=['POST'])
def registro_usuario():
    username = request.json.get("username")
    password = request.json.get("password")
    password_hash = generate_password_hash(password)

    if db.users.find_one({"username": username}):
        return jsonify({"msg": "El usuario ya existe"}), 409

    db.users.insert_one({"username": username, "password_hash": password_hash})
    return jsonify({"msg": "Usuario registrado exitosamente"}), 201


@app.route('/login', methods=['POST'])
def login():
    username = request.json.get("username")
    password = request.json.get("password")
    user = db.users.find_one({"username": username})

    if user and check_password_hash(user['password_hash'], password):
        access_token = create_access_token(identity=username, expires_delta=timedelta(seconds=20))
        return jsonify(access_token=access_token)
    return jsonify({"msg": "Credenciales incorrectas"}), 401


@app.route('/protegido', methods=['GET'])
@jwt_required()
def protegido():
    return jsonify(msg="Ruta protegida")


##### AUDITORIA #####
#logging.basicConfig(filename='registro_auditoria.log', level=logging.INFO)

#@app.route('/evento', methods=['GET'])
#@jwt_required()
#def evento():
    #logging.info("Evento registrado")
    #return jsonify(msg="Evento registrado")



##### API #####
@app.errorhandler(Exception)
def error_response(e):
    logging.error(f'Error: {e}')
    return jsonify(error=str(e)), 500



@app.route('/registro', methods=['POST'])
@jwt_required()
def registro():
    try:
        data = request.json
        Alias = data.get('Alias', '').strip()  # Usar 'Alias' con mayúscula
        drone_id = int(data.get('ID')) # Obtener el ID proporcionado

        if not Alias or not drone_id:
            return jsonify({'error': 'Falta el Alias o el ID del dron'}), 400

        # Verificar si el ID ya existe
        if db.drones.find_one({'ID': drone_id}):
            return jsonify({'error': 'El ID ya existe'}), 409

        new_drone = {
            '_id': ObjectId(),  # Generar ObjectId automáticamente
            'type': 'register',
            'ID': drone_id,  # Usar el ID proporcionado
            'Alias': Alias,
            'InitialPosition': [1, 1]
        }

        db.drones.insert_one(new_drone)

        return jsonify({'status': 'success', 'drone_id': drone_id}), 201
    except errors.PyMongoError as e:
        return jsonify({'error': str(e)}), 500





@app.route('/listar_drones', methods=['GET'])
def listar_drones():
    try:
        drones = list(db.drones.find({}, {'ID': 1, 'alias': 1}))
        return jsonify([{'ID': str(drone['ID']), 'Alias': drone.get('Alias', 'Sin alias')} for drone in drones])
    except errors.PyMongoError as e:
        return error_response(f'Error de base de datos: {e}', 500)


@app.route('/modificar_dron/<drone_id>', methods=['PUT'])
def modificar_dron(drone_id):
    try:
        data = request.json
        alias = data.get('Alias', '').strip()
        if not alias:
            return error_response('Falta el alias del dron o es inválido', 400)

        result = db.drones.update_one({'_id': ObjectId(drone_id)}, {'$set': {'Alias': alias}})
        if result.matched_count == 0:
            return error_response('Dron no encontrado', 404)
        return jsonify({'message': 'Dron modificado correctamente'}), 200
    except errors.PyMongoError as e:
        return error_response(f'Error de base de datos: {e}', 500)

@app.route('/borrar_dron/<drone_id>', methods=['DELETE'])
def borrar_dron(drone_id):
    try:
        if drone_id == "-1":
            # Borrar todos los drones
            result = db.drones.delete_many({})
            return jsonify({'message': f'{result.deleted_count} drones eliminados correctamente'}), 200
        else:
            #Borrar un solo dron
            result = db.drones.delete_one({'ID': int(drone_id)})
            if result.deleted_count == 0:
                return error_response('Dron no encontrado', 404)
            return jsonify({'message': 'Dron eliminado correctamente'}), 200
    except errors.PyMongoError as e:
        return jsonify({'error': str(e)}), 500


if __name__ == '__main__':
    context = ('ssl/service.crt', 'ssl/service.key')
    #SSL
    #app.run(debug=True, ssl_context=context, host='0.0.0.0', port=5000)
    app.run(debug=False, host='0.0.0.0', port=5000)
