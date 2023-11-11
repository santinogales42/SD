import tkinter as tk
import json
import threading
from kafka import KafkaConsumer
from queue import Queue

# Constantes de configuración
WINDOW_WIDTH = 800
WINDOW_HEIGHT = 600
CELL_SIZE = 20
MAP_OFFSET = 300

# Colores
DRONE_MOVING_COLOR = "green"
DRONE_FINAL_COLOR = "red"
FINAL_POSITION_COLOR = "gray"

# Función para actualizar los drones y su color según el estado
def update_drones(canvas, drones_info, drones_drawings, drone_update):
    dron_id = drone_update['ID']
    position = drone_update['Position']
    state = drone_update.get('State', 'MOVING')  # El estado por defecto es 'MOVING'

    # Determinar el color del dron basado en su estado
    if state == 'FINAL':
        color = DRONE_FINAL_COLOR
    elif state == 'MOVING':
        color = DRONE_MOVING_COLOR
    else:
        color = "black"  # Color por defecto

    # Si el dron ya se dibujó, actualiza su posición y color
    if dron_id in drones_drawings:
        canvas.itemconfig(drones_drawings[dron_id], fill=color)
        canvas.coords(drones_drawings[dron_id],
                      MAP_OFFSET + position[0]*CELL_SIZE, 
                      position[1]*CELL_SIZE, 
                      MAP_OFFSET + (position[0]+1)*CELL_SIZE, 
                      (position[1]+1)*CELL_SIZE)
    # De lo contrario, dibuja un nuevo dron
    else:
        drone = canvas.create_oval(MAP_OFFSET + position[0]*CELL_SIZE, 
                                   position[1]*CELL_SIZE, 
                                   MAP_OFFSET + (position[0]+1)*CELL_SIZE, 
                                   (position[1]+1)*CELL_SIZE, 
                                   fill=color)
        drones_drawings[dron_id] = drone

    # Actualizar la información almacenada del dron
    drones_info[dron_id] = position

# Esta es la función que será llamada en el hilo secundario
def kafka_listener(canvas, drones_info, drones_drawings, queue):
    consumer = KafkaConsumer(
        'drone_position_updates',
        bootstrap_servers=['localhost:29092'],
        auto_offset_reset='latest',
        group_id='map_viewer_group',
        value_deserializer=lambda m: json.loads(m.decode('utf-8'))
    )
    
    for message in consumer:
        drone_update = message.value
        if drone_update.get('Position'):
            # En lugar de actualizar la interfaz directamente,
            # ponemos el mensaje en una cola que el hilo principal estará revisando.
            queue.put(lambda: update_drones(canvas, drones_info, drones_drawings, drone_update))

# Esta es la función que se ejecutará en el hilo principal
def process_queue(canvas, queue):
    try:
        while True:
            update_func = queue.get_nowait()
            canvas.after(0, update_func)
    except queue.Empty:
        # Programamos la función process_queue para que se ejecute nuevamente después de un corto tiempo
        canvas.after(100, process_queue, canvas, queue)

# Supongamos que tienes una función que puede obtener las posiciones finales desde AD_Engine
def get_final_positions_from_engine(canvas, final_positions_drawings):
    # Esta función debe conectarse al AD_Engine y obtener las posiciones finales.
    consumer = KafkaConsumer(
        'final_positions_topic',
        bootstrap_servers=['localhost:29092'],
        auto_offset_reset='latest',
        group_id='final_positions_group',
        value_deserializer=lambda m: json.loads(m.decode('utf-8'))
    )

    for message in consumer:
        final_positions = message.value
        if 'final_positions' in final_positions:
            # Dibujar las posiciones finales en el mapa
            for final_pos in final_positions['final_positions']:
                x, y = final_pos
                rect = canvas.create_rectangle(MAP_OFFSET + x*CELL_SIZE, 
                                               y*CELL_SIZE, 
                                               MAP_OFFSET + (x+1)*CELL_SIZE, 
                                               (y+1)*CELL_SIZE, 
                                               fill=FINAL_POSITION_COLOR)
                final_positions_drawings[(x, y)] = rect

            # Podemos detener la escucha una vez que tenemos las posiciones finales
            break

# Función para dibujar las posiciones finales
def draw_final_positions(canvas, final_positions):
    for final_pos in final_positions:
        canvas.create_rectangle(MAP_OFFSET + final_pos[0]*CELL_SIZE, 
                                final_pos[1]*CELL_SIZE, 
                                MAP_OFFSET + (final_pos[0]+1)*CELL_SIZE, 
                                (final_pos[1]+1)*CELL_SIZE, 
                                fill=FINAL_POSITION_COLOR)

# Función para iniciar el visualizador de mapa
def run_map_viewer():
    root = tk.Tk()
    root.title("Drone Map Visualization")

    # Crear un lienzo (Canvas) para dibujar el mapa
    canvas = tk.Canvas(root, width=WINDOW_WIDTH, height=WINDOW_HEIGHT, bg="beige")
    canvas.pack()

    # Dibujar la cuadrícula en el mapa
    for x in range(MAP_OFFSET, WINDOW_WIDTH, CELL_SIZE):
        for y in range(0, WINDOW_HEIGHT, CELL_SIZE):
            canvas.create_rectangle(x, y, x+CELL_SIZE, y+CELL_SIZE, outline="black")

    # Diccionarios para almacenar la información de los drones y sus representaciones en el lienzo
    drones_info = {}
    drones_drawings = {}
    final_positions_drawings = {}
    queue = queue.Queue()  # Importar queue desde el módulo 'queue'

    # Iniciar el hilo de escucha de Kafka para las actualizaciones de los drones
    threading.Thread(target=kafka_listener, args=(canvas, drones_info, drones_drawings, queue), daemon=True).start()

    # Procesar la cola en el hilo principal
    process_queue(canvas, queue)
    # Llama a la función para obtener las posiciones finales desde el motor de drones
    get_final_positions_from_engine(canvas, final_positions_drawings)

    # Iniciar el bucle principal de Tkinter
    root.mainloop()

# Si MapViewer es el punto de entrada principal
if __name__ == "__main__":
    run_map_viewer()