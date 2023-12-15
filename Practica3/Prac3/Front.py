from flask import Flask, render_template, request

front = Flask(__name__)
front.debug = True

@front.route('/')
def index():
    return render_template('index.html')

if __name__=='__main__':
    front.run(host='0.0.0.0', port=5000)