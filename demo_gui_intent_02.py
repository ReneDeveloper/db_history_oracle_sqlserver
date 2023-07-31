"""demo_gui_intent_02.py"""

import sys
from PyQt5.QtWidgets import QApplication, QMainWindow, QAction, QMenu

app = QApplication(sys.argv)

options = {
    "Nuevo": lambda: print("Funcion nuevo server"),
    "Analizar": lambda: print("Función analizar server"),
    "Reporte": lambda: print("Función reporte server"),
    "...": lambda: print("Función ..."),
    "Salir": lambda: app.quit()
}

class MainWindow(QMainWindow):
    def __init__(self):
        super().__init__()

        # Creamos un menú desplegable
        self.menu = QMenu("Archivo", self)

        # Agregamos cada opción del diccionario como una acción en el menú desplegable
        for option, function in options.items():
            action = QAction(option, self)
            action.triggered.connect(function)
            self.menu.addAction(action)

        # Agregamos el menú desplegable a la barra de menú de la ventana
        self.menuBar().addMenu(self.menu)
        #self.showFullScreen()
        self.setGeometry(0, 0, QApplication.desktop().screenGeometry().width() -100, QApplication.desktop().screenGeometry().height()-100)


if __name__ == "__main__":
    window = MainWindow()
    window.show()
    sys.exit(app.exec_())

