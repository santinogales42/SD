import pygame
import sys
import json
from kafka import KafkaConsumer

# Inicializar Pygame
pygame.init()

# Constantes de colores
BEIGE = (245, 245, 220)
BLACK = (0, 0, 0)
GREEN = (0, 255, 0)
RED = (255, 0, 0)

# Dimensiones de la ventana
WINDOW_WIDTH = 800
WINDOW_HEIGHT = 600
CELL_SIZE = 20
MAP_OFFSET = 300

# Tamaño del mapa
MAP_WIDTH = CELL_SIZE * 20
MAP_HEIGHT = CELL_SIZE * 20

# Configurar la ventana
screen = pygame.display.set_mode((WINDOW_WIDTH, WINDOW_HEIGHT))
pygame.display.set_caption("Drone Map Visualization")

# Función para dibujar el mapa
def draw_map():
    screen.fill(BEIGE)  # Color de fondo
    for x in range(0, MAP_WIDTH, CELL_SIZE):
        for y in range(0, MAP_HEIGHT, CELL_SIZE):
            rect = pygame.Rect(MAP_OFFSET + x, y, CELL_SIZE, CELL_SIZE)
            pygame.draw.rect(screen, BLACK, rect, 1)

# Función para dibujar la tabla de drones
def draw_drones_table(drones_info):
    # Dibuja el fondo de la tabla
    pygame.draw.rect(screen, BEIGE, (0, 0, MAP_OFFSET, WINDOW_HEIGHT))
    # Configurar fuente
    font = pygame.font.Font(None, 24)
    # Dibuja los encabezados de la tabla
    headings = ["ID", "Position"]
    for i, heading in enumerate(headings):
        text = font.render(heading, True, BLACK)
        screen.blit(text, (10, i * CELL_SIZE))
    
    # Dibuja la información de los drones
    for i, drone in enumerate(drones_info):
        # ID del dron
        text_id = font.render(str(drone['ID']), True, BLACK)
        screen.blit(text_id, (10, (i+1) * CELL_SIZE))
        # Posición del dron
        text_pos = font.render(str(drone['Position']), True, BLACK)
        screen.blit(text_pos, (10 + MAP_OFFSET // len(headings), (i+1) * CELL_SIZE))
        
        
kafka_consumer = KafkaConsumer(
    'drone_position_updates',
    bootstrap_servers=['localhost:29092'],
    auto_offset_reset='latest',
    group_id='map_viewer_group'
)

# Función para obtener la información actualizada de los drones desde Kafka
def get_drones_info_from_kafka(consumer):
    drones_info = []
    for message in consumer:
        # Decodifica el mensaje de bytes a dict
        drone_update = json.loads(message.value)
        drones_info.append(drone_update)
        # Dependiendo de cómo quieras manejar la actualización,
        # puedes actualizar la lista completa de drones o solo el dron específico.
    return drones_info

# Bucle principal
running = True
while running:
    for event in pygame.event.get():
        if event.type == pygame.QUIT:
            running = False

    # Intenta consumir mensajes de Kafka para obtener la información más reciente de los drones
    try:
        drones_info = get_drones_info_from_kafka(kafka_consumer)
    except:
        # Maneja aquí cualquier excepción que pueda ocurrir durante el consumo de Kafka
        # Por ejemplo, podrías querer imprimir un error y continuar con la última información conocida de drones_info
        pass

    # Dibujar mapa y tabla con la información actualizada de los drones
    draw_map()
    draw_drones_table(drones_info)

    # Actualizar la ventana
    pygame.display.flip()

    # Puedes ajustar este tiempo de espera para controlar la frecuencia de actualización de la pantalla
    pygame.time.wait(15)

# Salir
pygame.quit()
sys.exit()