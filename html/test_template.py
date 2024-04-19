#test_template.py

from class_html_generator import HTMLGenerator
from class_html_detail import HTMLDetail

# Uso del generador HTML
template_file = "template.html"

generator = HTMLGenerator(template_file)
generator.replace_variables(
    server_name="Servidor de ejemplo",
    database_name="Base de datos de ejemplo",
    size="Tamaño total",
    size_last_12="Tamaño últimos 12 años",
    size_last_08="Tamaño últimos 8 años",
    size_last_04="Tamaño últimos 4 años"
)

data = [
    {
        'owner': 'Owner1',
        'table_name': 'Table1',
        'total_size': '100 MB',
        'total_rows': '1000',
        'size_last_12': '50 MB',
        'size_last_08': '30 MB',
        'size_last_04': '20 MB'
    },
    {
        'owner': 'Owner2',
        'table_name': 'Table2',
        'total_size': '200 MB',
        'total_rows': '2000',
        'size_last_12': '80 MB',
        'size_last_08': '50 MB',
        'size_last_04': '30 MB'
    },
    {
        'owner': 'Owner3',
        'table_name': 'Table3',
        'total_size': '150 MB',
        'total_rows': '1500',
        'size_last_12': '70 MB',
        'size_last_08': '40 MB',
        'size_last_04': '25 MB'
    }
]

html_detail = HTMLDetail('template_detail.html')
html_detail.generate_html(data)

