from string import Template

from class_character_replacer import CharacterReplacer

class HTMLGenerator:
    def __init__(self, template_file):
        self.template_file = template_file
        self.template = None

    def load_template(self):
        with open(self.template_file, 'r') as file:
            self.template = Template(file.read())

    def replace_variables(self, server_name, database_name, size, size_last_12, size_last_08, size_last_04):
        if not self.template:
            self.load_template()

        # Reemplazar caracteres especiales en los valores
        server_name = CharacterReplacer.replace_characters(server_name)
        database_name = CharacterReplacer.replace_characters(database_name)
        size = CharacterReplacer.replace_characters(size)
        size_last_12 = CharacterReplacer.replace_characters(size_last_12)
        size_last_08 = CharacterReplacer.replace_characters(size_last_08)
        size_last_04 = CharacterReplacer.replace_characters(size_last_04)

        # Reemplazar variables en el template
        contenido_html = self.template.substitute(
            server_name=server_name,
            database_name=database_name,
            size=size,
            size_last_12=size_last_12,
            size_last_08=size_last_08,
            size_last_04=size_last_04
        )

        # Guardar el archivo HTML en disco
        output_file = "documento.html"
        with open(output_file, 'w') as file:
            file.write(contenido_html)

        print(f"Archivo HTML generado correctamente. Nombre del archivo: {output_file}")


# Uso del generador HTML
"""
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
"""