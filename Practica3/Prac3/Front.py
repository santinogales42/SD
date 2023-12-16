from flask import Flask, render_template, request, jsonify
import requests
from api_w import WEATHER_API_KEY


front = Flask(__name__)
front.debug = True

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

@front.route('/')
def index():
    return render_template('index.html')

@front.route('/weather/<city>', methods=['GET'])
def weather(city):
    temperature = get_weather(city)
    if temperature is not None:
        return jsonify({'city': city, 'temperature': temperature})
    else:
        return jsonify({'error': 'No se pudo obtener el clima'}), 404

if __name__ == '__main__':
    front.run(host='0.0.0.0', port=5000)
