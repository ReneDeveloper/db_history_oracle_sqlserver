#class_html_detail.py

from string import Template
from class_character_replacer import CharacterReplacer


class HTMLDetail:
    def __init__(self, template_file):
        self.template_file = template_file
        self.template = None

    def load_template(self):
        with open(self.template_file, 'r') as file:
            self.template = Template(file.read())

    def generate_html(self, table_data):
        if not self.template:
            self.load_template()

        # Generar filas de la tabla
        table_rows = ""
        for row in table_data:
            row_html = "<tr>"
            row_html += f"<td>{CharacterReplacer.replace_characters(row['owner'])}</td>"
            row_html += f"<td>{CharacterReplacer.replace_characters(row['table_name'])}</td>"
            row_html += f"<td>{CharacterReplacer.replace_characters(row['total_size'])}</td>"
            row_html += f"<td>{CharacterReplacer.replace_characters(row['total_rows'])}</td>"
            row_html += f"<td>{CharacterReplacer.replace_characters(row['size_last_12'])}</td>"
            row_html += f"<td>{CharacterReplacer.replace_characters(row['size_last_08'])}</td>"
            row_html += f"<td>{CharacterReplacer.replace_characters(row['size_last_04'])}</td>"
            row_html += "</tr>"
            table_rows += row_html

        # Reemplazar variables en el template
        contenido_html = self.template.substitute(table_rows=table_rows)

        # Guardar el archivo HTML en disco
        output_file = "detalle_tablas.html"
        with open(output_file, 'w') as file:
            file.write(contenido_html)

        print(f"Archivo HTML generado correctamente. Nombre del archivo: {output_file}")
