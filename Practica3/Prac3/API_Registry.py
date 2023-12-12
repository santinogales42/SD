from flask import Flask, request, jsonify
from pymongo import MongoClient
from bson import ObjectId
import os

app = Flask(__name__)
client = MongoClient(os.environ.get('MONGO_URI', 'mongodb://localhost:27017/'))
db = client.dronedb

@app.route('/registro', methods=['POST'])
def registro():
    try:
        data = request.json
        if 'alias' not in data or not data['alias']:
            return jsonify({'error': 'Falta el alias del dron'}), 400

        result = db.drones.insert_one({'alias': data['alias']})
        return jsonify({'status': 'success', 'drone_id': str(result.inserted_id)}), 201
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/listar_drones', methods=['GET'])
def listar_drones():
    drones = list(db.drones.find({}, {'_id': 1, 'alias': 1}))
    return jsonify([{'id': str(drone['_id']), 'alias': drone['alias']} for drone in drones])

@app.route('/modificar_dron/<drone_id>', methods=['PUT'])
def modificar_dron(drone_id):
    try:
        data = request.json
        if 'alias' not in data or not data['alias']:
            return jsonify({'error': 'Falta el alias del dron'}), 400

        result = db.drones.update_one({'_id': ObjectId(drone_id)}, {'$set': {'alias': data['alias']}})
        if result.matched_count:
            return jsonify({'message': 'Dron modificado correctamente'}), 200
        return jsonify({'error': 'Dron no encontrado'}), 404
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/borrar_dron/<drone_id>', methods=['DELETE'])
def borrar_dron(drone_id):
    try:
        result = db.drones.delete_one({'_id': ObjectId(drone_id)})
        if result.deleted_count:
            return jsonify({'message': 'Dron eliminado correctamente'}), 200
        return jsonify({'error': 'Dron no encontrado'}), 404
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/reiniciar_registro', methods=['DELETE'])
def reiniciar_registro():
    db.drones.delete_many({})
    return jsonify({'message': 'Registro reiniciado correctamente'}), 200

if __name__ == '__main__':
    context = ('ssl/certificado_CA.crt', 'ssl/clave_privada_CA.pem')  # Rutas a tu certificado y clave privada
    app.run(debug=True, ssl_context=context, host='0.0.0.0', port=5000)