import tkinter as tk
import json
import threading
import time
from kafka import KafkaConsumer
from queue import Queue, Empty

# Constantes de configuración
WINDOW_WIDTH = 1000
WINDOW_HEIGHT = 600
CELL_SIZE = 20
MAP_OFFSET = 300
LEFT_PANEL_WIDTH = 200

# Colores
DRONE_MOVING_COLOR = "green"
DRONE_FINAL_COLOR = "red"
FINAL_POSITION_COLOR = "gray"
TEXT_COLOR = "black"

# Diccionarios para almacenar información y representaciones de los drones
drones_info = {}
drones_drawings = {}
final_positions_drawings = {}
last_update_time = {}

def update_drones(canvas, drone_update):
    if 'Position' not in drone_update:
        print("Advertencia: el mensaje recibido no contiene la clave 'Position'.")
        return  # Salir de la función si no hay información de posición
    dron_id = drone_update['ID']
    position = drone_update['Position']
    state = drone_update.get('State', 'MOVING')
    
    # Determinar el color del dron basado en su estado
    color = DRONE_MOVING_COLOR if state == 'MOVING' else DRONE_FINAL_COLOR

    # Si el dron no se ha movido en 3-4 segundos, cambiar el color a rojo
    current_time = time.time()
    if dron_id in last_update_time and current_time - last_update_time[dron_id] > 3:
        color = DRONE_FINAL_COLOR
    
    # Actualizar el tiempo de la última actualización
    last_update_time[dron_id] = current_time
    
    # Actualizar el dron en el lienzo
    drone_shape = drones_drawings.get(dron_id)
    if drone_shape:
        canvas.itemconfig(drone_shape, fill=color)
        canvas.coords(drone_shape,
                      MAP_OFFSET + position[0]*CELL_SIZE,
                      position[1]*CELL_SIZE,
                      MAP_OFFSET + (position[0]+1)*CELL_SIZE,
                      (position[1]+1)*CELL_SIZE)
    else:
        drone_shape = canvas.create_oval(MAP_OFFSET + position[0]*CELL_SIZE,
                                         position[1]*CELL_SIZE,
                                         MAP_OFFSET + (position[0]+1)*CELL_SIZE,
                                         (position[1]+1)*CELL_SIZE,
                                         fill=color)
        drones_drawings[dron_id] = drone_shape
    drones_info[dron_id] = position
    
    # Actualizar la tabla de drones
    update_drone_table(canvas)

def update_drone_table(canvas):
    canvas.delete("drone_table")
    y = 20
    for dron_id, position in sorted(drones_info.items()):
        canvas.create_text(LEFT_PANEL_WIDTH / 2, y, text=f"Drone {dron_id}: {position}", tag="drone_table", fill=TEXT_COLOR)
        y += 20

def kafka_listener(canvas, queue):
    consumer = KafkaConsumer(
        'drone_position_updates',
        'final_positions_topic',
        bootstrap_servers=['localhost:29092'],
        auto_offset_reset='earliest',
        group_id='map_viewer_group',
        value_deserializer=lambda m: json.loads(m.decode('utf-8'))
    )

    for message in consumer:
        if message.topic == 'drone_position_updates':
            drone_update = message.value
            queue.put(lambda: update_drones(canvas, drone_update))
        elif message.topic == 'final_positions_topic':
            final_positions_update = message.value
            queue.put(lambda: update_final_positions(canvas, final_positions_update))

def update_final_positions(canvas, final_positions_update):
    if 'final_positions' in final_positions_update:
        # Dibujar las posiciones finales en el mapa
        for final_pos in final_positions_update['final_positions']:
            x, y = final_pos
            if (x, y) not in final_positions_drawings:
                rect = canvas.create_rectangle(MAP_OFFSET + x*CELL_SIZE, 
                                               y*CELL_SIZE, 
                                               MAP_OFFSET + (x+1)*CELL_SIZE, 
                                               (y+1)*CELL_SIZE, 
                                               fill=FINAL_POSITION_COLOR)
                final_positions_drawings[(x, y)] = rect

def process_queue(canvas, queue):
    try:
        while True:
            update_func = queue.get_nowait()
            canvas.after(0, update_func)
    except Empty:
        canvas.after(100, lambda: process_queue(canvas, queue))

def run_map_viewer():
    root = tk.Tk()
    root.title("Drone Map Visualization")
    canvas = tk.Canvas(root, width=WINDOW_WIDTH, height=WINDOW_HEIGHT, bg="beige")
    canvas.pack(fill=tk.BOTH, expand=True)

    # Dibujar la tabla de drones a la izquierda
    canvas.create_rectangle(0, 0, LEFT_PANEL_WIDTH, WINDOW_HEIGHT, fill='white')
    
    # Dibujar la cuadrícula en el mapa
    for x in range(MAP_OFFSET, WINDOW_WIDTH, CELL_SIZE):
        for y in range(0, WINDOW_HEIGHT, CELL_SIZE):
            canvas.create_rectangle(x, y, x + CELL_SIZE, y + CELL_SIZE, outline="black")

    queue = Queue()

    # Iniciar el hilo de escucha de Kafka para las actualizaciones de los drones
    threading.Thread(target=kafka_listener, args=(canvas, queue), daemon=True).start()

    process_queue(canvas, queue)

    root.mainloop()

# Si MapViewer es el punto de entrada principal
if __name__ == "__main__":
    run_map_viewer()
