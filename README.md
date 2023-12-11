# SD
Sistemas distribuidos

Para ejecutar la práctica que mencionaste, que involucra componentes como AD_Registry, AD_Engine, y AD_Drone, generalmente sigues estos pasos:

    Configurar el entorno: Asegúrate de que tengas todas las dependencias necesarias instaladas. Esto podría incluir Python, MongoDB, Kafka (u otra cola de mensajes, según la implementación), y otras bibliotecas específicas de Python utilizadas en el proyecto.

    Ejecutar servicios necesarios:

        MongoDB: Inicia el servicio de MongoDB. Usualmente puedes hacerlo ejecutando mongod en la terminal.

        Kafka (si es necesario): Si utilizas Kafka como sistema de mensajería, debes iniciar el servidor de Kafka y crear los temas (topics) necesarios.

    Iniciar AD_Registry:

        Desde la terminal, navega al directorio donde se encuentra el archivo AD_Registry.py.

        Ejecuta el servidor AD_Registry proporcionando la información necesaria, como el puerto en el que escuchará y la dirección del servidor de base de datos MongoDB, por ejemplo:

        shell

    python AD_Registry.py 8081 localhost:27017

Iniciar AD_Engine:

    Desde la terminal, navega al directorio donde se encuentra el archivo AD_Engine.py.

    Ejecuta el servidor AD_Engine, proporcionando detalles como el puerto en el que escuchará y la dirección del servidor de mensajes (Kafka o similar), por ejemplo:

    shell

    python AD_Engine.py 8080 10 localhost:9092 localhost:8081 localhost:27017

Iniciar AD_Drone:

    Desde la terminal, navega al directorio donde se encuentra el archivo AD_Drone.py.

    Ejecuta AD_Drone proporcionando la dirección del servidor AD_Engine y del servidor AD_Registry, por ejemplo:

    shell

    python AD_Drone.py localhost:8080 localhost:9092 localhost:8081

Interactuar con los drones: Ejecuta tantas instancias de AD_Drone como desees en diferentes terminales. Cada instancia debería permitirte unirte al espectáculo y mover el dron.

Depurar errores: Si encuentras errores, verifica los registros (logs) y las excepciones que se muestran en la terminal. Estos mensajes te ayudarán a identificar los problemas y solucionarlos.

Detener los servicios: Cuando hayas terminado con la práctica, asegúrate de detener los servicios de MongoDB, Kafka y los programas AD_Registry y AD_Engine para liberar los recursos.
