from flask import Flask, request, jsonify
from pymongo import MongoClient

app = Flask(__name__)
client = MongoClient('mongodb://localhost:27017/')
db = client.dronedb

@app.route('/registro', methods=['POST'])
def register_drone():
    # Aquí procesas la solicitud POST para registrar un dron.
    data = request.json
    drone_id = data.get('ID')
    alias = data.get('Alias')
    # Lógica para registrar el dron en MongoDB
    result = db.drones.insert_one({'ID': drone_id, 'Alias': alias})
    return jsonify({'status': 'success', 'drone_id': result.inserted_id})

if __name__ == '__main__':
    app.run(debug=True)
