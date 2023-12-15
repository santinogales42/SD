from flask import Flask, request, jsonify, render_template
from flask_jwt_extended import JWTManager, create_access_token, jwt_required
from pymongo import MongoClient, errors
from bson import ObjectId
from datetime import timedelta
import os
import logging

app = Flask(__name__)
client = MongoClient(os.environ.get('MONGO_URI', 'mongodb://localhost:27017/'))
db = client.dronedb
app.config["JWT_SECRET_KEY"] = "tu_clave_secreta"  # Cambia esto por una clave segura
jwt = JWTManager(app)


@app.route('/')
def home():
    return render_template('index.html')


###### SEGURIDAD ######
@app.route('/login', methods=['POST'])
def login():
    username = request.json.get("username", None)
    password = request.json.get("password", None)
    
    # Aquí debes validar las credenciales del dron
    if username != "dron_test" or password != "password_test":
        return jsonify({"msg": "Credenciales incorrectas"}), 401

    expires = timedelta(seconds=20)
    access_token = create_access_token(identity=username, expires_delta=expires)

    return jsonify(access_token=access_token)

@app.route('/protegido', methods=['GET'])
@jwt_required()
def protegido():
    return jsonify(msg="Ruta protegida")


##### AUDITORIA #####
#logging.basicConfig(filename='registro_auditoria.log', level=logging.INFO)

#@app.route('/evento', methods=['GET'])
#def evento():
    #logging.info("Evento registrado")
    #return jsonify(msg="Evento registrado")



##### API #####

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
        drones = list(db.drones.find({}, {'ID': 1, 'Alias': 1}))
        return jsonify([{'ID': str(drone['ID']), 'Alias': drone['Alias']} for drone in drones])
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
    context = ('ssl/service.crt', 'ssl/service.key')
    #SSL
    #app.run(debug=True, ssl_context=context, host='0.0.0.0', port=5000)
    app.run(debug=True, host='0.0.0.0', port=5000)
