import pygame
import sys
import json
import threading
from kafka import KafkaConsumer
import time

# Constantes de configuración
WINDOW_WIDTH = 800
WINDOW_HEIGHT = 600
CELL_SIZE = 20
MAP_OFFSET = 300

# Colores
BEIGE = (245, 245, 220)
BLACK = (0, 0, 0)

INSTRUCTION_START = 'START'

# Inicializar Pygame
pygame.init()

# Configurar la ventana
screen = pygame.display.set_mode((WINDOW_WIDTH, WINDOW_HEIGHT))
pygame.display.set_caption("Drone Map Visualization")

# Función para dibujar el mapa
def draw_map():
    screen.fill(BEIGE)  # Color de fondo
    # Dibujar la cuadrícula
    for x in range(0, WINDOW_WIDTH - MAP_OFFSET, CELL_SIZE):
        for y in range(0, WINDOW_HEIGHT, CELL_SIZE):
            rect = pygame.Rect(MAP_OFFSET + x, y, CELL_SIZE, CELL_SIZE)
            pygame.draw.rect(screen, BLACK, rect, 1)

# Función para dibujar los drones en el mapa
def draw_drones(drones_info):
    # Configurar fuente
    font = pygame.font.Font(None, 24)
    for drone in drones_info:
        # Dibujar dron
        drone_pos = drones_info[drone]
        pygame.draw.circle(screen, BLACK, (MAP_OFFSET + drone_pos[0]*CELL_SIZE + CELL_SIZE//2, drone_pos[1]*CELL_SIZE + CELL_SIZE//2), CELL_SIZE//2 - 1)
        # Dibujar ID del dron
        text = font.render(str(drone), True, BEIGE)
        screen.blit(text, (MAP_OFFSET + drone_pos[0]*CELL_SIZE + CELL_SIZE//4, drone_pos[1]*CELL_SIZE + CELL_SIZE//4))

# Función para obtener la información de los drones desde Kafka
def get_drones_info_from_kafka(consumer, drones_info):
    for message in consumer:
        drone_update = message.value  # Asumiendo que message.value es un diccionario
        dron_id = drone_update.get('ID')
        if 'Position' in drone_update:
            drones_info[dron_id] = drone_update['Position']
            draw_map()
            draw_drones(drones_info)
            pygame.display.flip()
        else:
            print(f"No se encontró la clave 'Position' en el mensaje para el dron {dron_id}")
            # Maneja la situación, por ejemplo, asignando un valor por defecto o ignorando la actualización

        

# Bucle principal para la visualización de drones
def map_viewer_loop():
    # Diccionario para almacenar la información de los drones
    drones_info = {}

    kafka_consumer = KafkaConsumer(
        'drone_position_updates',
        bootstrap_servers=['localhost:29092'],
        auto_offset_reset='latest',
        group_id='map_viewer_group',
        value_deserializer=lambda m: json.loads(m.decode('utf-8'))
    )
    show_started = False
    while not show_started:
        for message in kafka_consumer:
            if message.value.get('type') == 'control' and message.value.get('instruction') == INSTRUCTION_START:
                show_started = True
                break
        time.sleep(1)  # Espera un segundo antes de verificar de nuevo
    running = True
    while running:
        for event in pygame.event.get():
            if event.type == pygame.QUIT:
                running = False

        # Obtener información de drones desde Kafka
        get_drones_info_from_kafka(kafka_consumer, drones_info)

        # Limitar la velocidad de actualización
        pygame.time.wait(100)

    pygame.quit()
    sys.exit()

# Función para iniciar el visualizador de mapa
def run_map_viewer():
    map_viewer_thread = threading.Thread(target=map_viewer_loop)
    map_viewer_thread.start()
    return map_viewer_thread

# Si MapViewer es el punto de entrada principal
if __name__ == "__main__":
    run_map_viewer()
