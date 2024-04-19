import sys
from PyQt5.QtWidgets import QApplication, QMainWindow, QAction, QMenu, QWidget, QVBoxLayout, QLabel, QLineEdit, QPushButton

class NuevoServerForm(QWidget):
    def __init__(self, parent=None):
        super().__init__(parent)

        self.init_ui()

    def init_ui(self):
        layout = QVBoxLayout()

        self.label_nombre = QLabel("Nombre del nuevo servidor:", self)
        layout.addWidget(self.label_nombre)

        self.input_nombre = QLineEdit(self)
        layout.addWidget(self.input_nombre)

        self.button_guardar = QPushButton("Guardar", self)
        self.button_guardar.clicked.connect(self.guardar_nuevo_server)
        layout.addWidget(self.button_guardar)

        self.setLayout(layout)

    def guardar_nuevo_server(self):
        nombre = self.input_nombre.text()
        if nombre:
            print(f"Guardando nuevo servidor con nombre: {nombre}")
        else:
            print("El nombre del servidor no puede estar vacío")

class MainWindow(QMainWindow):
    def __init__(self):
        super().__init__()

        options = {
            "Nuevo": self.show_nuevo_server_form,
            "Analizar": lambda: print("Función analizar server"),
            "Reporte": lambda: print("Función reporte server"),
            "...": lambda: print("Función ..."),
            "Salir": lambda: app.quit()
        }

        # Creamos un menú desplegable
        self.menu = QMenu("Archivo", self)

        # Agregamos cada opción del diccionario como una acción en el menú desplegable
        for option, function in options.items():
            action = QAction(option, self)
            action.triggered.connect(function)
            self.menu.addAction(action)

        # Agregamos el menú desplegable a la barra de menú de la ventana
        self.menuBar().addMenu(self.menu)
        self.setGeometry(0, 0, QApplication.desktop().screenGeometry().width() - 100, QApplication.desktop().screenGeometry().height() - 100)

    def show_nuevo_server_form(self):
        form = NuevoServerForm(self)  # Establecemos la ventana principal como padre
        form.show()

if __name__ == "__main__":
    app = QApplication(sys.argv)
    window = MainWindow()
    window.show()
    sys.exit(app.exec_())
