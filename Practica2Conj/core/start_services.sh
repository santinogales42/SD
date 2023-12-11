# Iniciar MongoDB en una terminal
gnome-terminal -- bash -c 'mongod'
# Esperar un momento para que MongoDB se inicie completamente
sleep 5


# Iniciar múltiples instancias de AD_Drone en terminales separadas
for ((i=0;i<15;i++))
do
    gnome-terminal -- bash -c 'python AD_Drone.py'
done

# Esperar un momento para que AD_Drone se inicie completamente
sleep 5

echo "Todos los servicios se han iniciado correctamente en terminales separadas."

