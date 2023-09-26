# main.py

from core.AD_Engine import AD_Engine
from core.AD_Registry import AD_Registry
from core.AD_Drone import AD_Drone
from core.AD_Weather import AD_Weather

def main():
    # Configuración de los componentes
    engine = AD_Engine()
    registry = AD_Registry()
    drone = AD_Drone()
    weather = AD_Weather()

    # Iniciar los componentes
    engine.start()
    registry.start()
    drone.start()
    weather.start()

if __name__ == "__main__":
    main()
