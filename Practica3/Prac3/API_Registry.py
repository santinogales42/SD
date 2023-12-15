from flask import Flask, request, jsonify
from pymongo import MongoClient, errors
from bson import ObjectId
import os

app = Flask(__name__)
client = MongoClient(os.environ.get('MONGO_URI', 'mongodb://localhost:27017/'))
db = client.dronedb

def error_response(message, status_code):
    return jsonify({'error': message}), status_code

@app.route('/registro', methods=['POST'])
def registro():
    try:
        data = request.json
        alias = data.get('alias', '').strip()
        drone_id = data.get('id', '').strip()
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
        return jsonify([{'id': str(drone['_id']), 'alias': drone['alias']} for drone in drones])
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
    app.run(debug=True, ssl_context=context, host='0.0.0.0', port=5000)
