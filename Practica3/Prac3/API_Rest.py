from flask import Flask, request, jsonify, render_template
from pymongo import MongoClient, errors
from bson import ObjectId
import os
import requests


app = Flask(__name__)
client = MongoClient(os.environ.get('MONGO_URI', 'mongodb://localhost:27017/'))
db = client.dronedb

WEATHER_API_KEY = '3fe46e00ff563d40f636df014ce6073e'
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

@app.route('/registro', methods=['POST'])
def registro():
    try:
        print(request.data)  # Ver los datos brutos que llegan
        print(request.json)  # Ver la interpretación JSON de Flask
        data = request.json
        drone_id = str(data.get('ID', '')).strip()
        alias = data.get('Alias', '').strip()
        initial_position = [1, 1]  # Posición inicial predeterminada

        if not alias or not drone_id:
            return jsonify({'error': 'Falta el alias o el ID del dron'}), 400

        if db.drones.find_one({'_id': drone_id}):
            return jsonify({'error': 'El ID ya existe'}), 409

        db.drones.insert_one({'_id': drone_id, 'alias': alias, 'initial_position': initial_position})
        return jsonify({'status': 'success', 'drone_id': drone_id}), 201
    except errors.PyMongoError as e:
        return jsonify({'error': str(e)}), 500


@app.route('/listar_drones', methods=['GET'])
def listar_drones():
    try:
        drones = list(db.drones.find({}, {'_id': 1, 'alias': 1}))
        return jsonify([{'id': str(drone['_id']), 'alias': drone.get('alias', 'Sin alias')} for drone in drones])
    except errors.PyMongoError as e:
        return error_response(f'Error de base de datos: {e}', 500)


@app.route('/modificar_dron/<drone_id>', methods=['PUT'])
def modificar_dron(drone_id):
    try:
        data = request.json
        alias = data.get('alias', '').strip()
        if not alias:
            return error_response('Falta el alias del dron o es inválido', 400)

        result = db.drones.update_one({'_id': ObjectId(drone_id)}, {'$set': {'alias': alias}})
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
            # Borrar un solo dron
            result = db.drones.delete_one({'_id': ObjectId(drone_id)})
            if result.deleted_count == 0:
                return error_response('Dron no encontrado', 404)
            return jsonify({'message': 'Dron eliminado correctamente'}), 200
    except errors.PyMongoError as e:
        return error_response(f'Error de base de datos: {e}', 500)



@app.route('/reiniciar_registro', methods=['DELETE'])
def reiniciar_registro():
    try:
        db.drones.delete_many({})
        return jsonify({'message': 'Registro reiniciado correctamente'}), 200
    except errors.PyMongoError as e:
        return error_response(f'Error de base de datos: {e}', 500)

if __name__ == '__main__':
    context = ('ssl/certificado_CA.crt', 'ssl/clave_privada_CA.pem')
    #SSL
    #app.run(debug=True, ssl_context=context, host='0.0.0.0', port=5000)
    app.run(debug=False, host='0.0.0.0', port=5000)
