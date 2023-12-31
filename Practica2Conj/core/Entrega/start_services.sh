#!/bin/bash

# Iniciar compose en una terminal
gnome-terminal -- bash -c 'docker-compose up'

# Iniciar MongoDB en una terminal
gnome-terminal -- bash -c 'mongod'

# Esperar un momento para que MongoDB se inicie completamente
sleep 5

# Iniciar AD_Registry en una terminal
gnome-terminal -- bash -c 'python AD_Registry.py 8081 localhost:27017'

# Esperar un momento para que AD_Registry se inicie completamente
sleep 2

# Iniciar AD_Engine en una terminal
gnome-terminal -- bash -c 'python AD_Engine.py 8080 10 localhost:9092 localhost:8081 localhost:27017'

# Esperar un momento para que AD_Engine se inicie completamente
sleep 2

for(i=1;i)
# Iniciar AD_Drone en una terminal
gnome-terminal -- bash -c 'python AD_Drone(pruebas).py localhost:8080 localhost:9092 localhost:8081'

# Esperar un momento para que AD_Drone se inicie completamente
sleep 2

# Iniciar AD_Weather en una terminal
gnome-terminal -- bash -c 'python AD_Weather.py 8082'

echo "Todos los servicios se han iniciado correctamente en terminales separadas."
