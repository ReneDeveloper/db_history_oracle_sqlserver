import tkinter as tk
from tkinter import ttk
import json
import os

# Define the entry_nombre, entry_url, combo_servers, and combo_servers_reporte as global variables
entry_nombre = None
entry_url = None
combo_servers = None
combo_servers_reporte = None

def guardar_servidor():
    global entry_nombre, entry_url, combo_servers, combo_servers_reporte  # Declare the variables as global
    nombre = entry_nombre.get()
    url = entry_url.get()

    servidor = {
        "nombre": nombre,
        "url": url
    }

    with open("servidores.json", "a") as f:
        json.dump(servidor, f)
        f.write('\n')

    # Update the comboboxes after saving the new server
    combo_servers['values'] = get_server_names()
    combo_servers_reporte['values'] = get_server_names()

    entry_nombre.delete(0, tk.END)
    entry_url.delete(0, tk.END)

# Rest of the code remains the same
# ...

def procesar_servidor():
    selected_server = combo_servers.get()
    # Here, you can add the logic to process the selected server

def generar_reporte():
    selected_server = combo_servers_reporte.get()
    # Here, you can add the logic to generate the report for the selected server

def get_server_names():
    # This function retrieves the server names from the JSON file
    if not os.path.exists("servidores.json"):
        return []
    server_names = []
    with open("servidores.json", "r") as f:
        for line in f:
            servidor = json.loads(line)
            server_names.append(servidor["nombre"])
    return server_names




def crear_formulario():
    global entry_nombre, entry_url, combo_servers, combo_servers_reporte  # Declare the variables as global
    root = tk.Tk()
    root.title("Formulario de Servidores")

    # Opción Nuevo Servidor
    frame_nuevo_servidor = tk.Frame(root)
    frame_nuevo_servidor.pack(pady=10)
    label_nombre = tk.Label(frame_nuevo_servidor, text="Nombre del servidor:")
    label_nombre.grid(row=0, column=0)
    entry_nombre = tk.Entry(frame_nuevo_servidor)
    entry_nombre.grid(row=0, column=1)
    label_url = tk.Label(frame_nuevo_servidor, text="URL del servidor:")
    label_url.grid(row=1, column=0)
    entry_url = tk.Entry(frame_nuevo_servidor)
    entry_url.grid(row=1, column=1)
    boton_guardar = tk.Button(frame_nuevo_servidor, text="Guardar", command=guardar_servidor)
    boton_guardar.grid(row=2, column=0)
    boton_borrar = tk.Button(frame_nuevo_servidor, text="Borrar", command=lambda: (entry_nombre.delete(0, tk.END), entry_url.delete(0, tk.END)))
    boton_borrar.grid(row=2, column=1)

    # Opción Procesar Servidor
    frame_procesar_servidor = tk.Frame(root)
    frame_procesar_servidor.pack(pady=10)
    label_procesar = tk.Label(frame_procesar_servidor, text="Seleccione un servidor:")
    label_procesar.grid(row=0, column=0)
    combo_servers = ttk.Combobox(frame_procesar_servidor, values=get_server_names())
    combo_servers.grid(row=0, column=1)
    boton_procesar = tk.Button(frame_procesar_servidor, text="Procesar", command=procesar_servidor)
    boton_procesar.grid(row=0, column=2)

    # Opción Reporte
    frame_reporte = tk.Frame(root)
    frame_reporte.pack(pady=10)
    label_reporte = tk.Label(frame_reporte, text="Seleccione un servidor:")
    label_reporte.grid(row=0, column=0)
    combo_servers_reporte = ttk.Combobox(frame_reporte, values=get_server_names())
    combo_servers_reporte.grid(row=0, column=1)
    boton_reporte = tk.Button(frame_reporte, text="Generar Reporte", command=generar_reporte)
    boton_reporte.grid(row=0, column=2)

    # Opción Salir
    boton_salir = tk.Button(root, text="Salir", command=root.quit)
    boton_salir.pack(pady=10)

    root.mainloop()

crear_formulario()
