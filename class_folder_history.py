import os
import time

def count_files_by_date_and_extension(directory):
    # Diccionario para contar archivos por fecha
    date_count = {}
    # Diccionario para contar archivos por extensión
    extension_count = {}
    # Contador de subdirectorios
    directory_count = 0
    # Contador de archivos totales
    file_count = 0

    def analyze_directory(path):
        nonlocal directory_count
        nonlocal file_count
        # Recorrer cada elemento en el directorio actual
        for item in os.scandir(path):
            # Si el elemento es un archivo
            if item.is_file():
                # Obtener fecha de creación del archivo
                file_ctime = time.ctime(item.stat().st_ctime)
                # Obtener extensión del archivo
                file_extension = os.path.splitext(item.name)[1] or 'SIN_EXTENSION'
                # Formatear fecha como YYYYMMDD
                file_date = time.strftime("%Y%m%d", time.gmtime(item.stat().st_ctime))
                # Contar archivo por fecha
                date_count[file_date] = date_count.get(file_date, 0) + 1
                # Contar archivo por extensión
                extension_count[file_extension] = extension_count.get(file_extension, 0) + 1
                # Incrementar el contador de archivos totales
                file_count += 1
            # Si el elemento es un subdirectorio
            elif item.is_dir() and item.name != '.git':
                # Incrementar contador de subdirectorios
                directory_count += 1
                # Analizar el subdirectorio
                analyze_directory(item.path)

    # Llamar a la función para analizar el directorio raíz
    analyze_directory(directory)
    # Imprimir resultados
    print('Archivos por fecha:')
    for date, count in date_count.items():
        print(f'{date}: {count} archivos')
    print('Archivos por extensión:')
    for extension, count in extension_count.items():
        print(f'{extension}: {count} archivos')
    print(f'Cantidad de subdirectorios: {directory_count}')
    print(f'Cantidad total de archivos: {file_count}')

# Uso de ejemplo
#count_files_by_date_and_extension('/path/to/directory')

# Ejemplo de uso
folder = "C:/Users/rcastillosi/__REPOS__/db_history_oracle_sqlserver/"
count_files_by_date_and_extension(folder)
