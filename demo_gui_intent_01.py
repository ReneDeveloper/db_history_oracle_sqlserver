"""demo_gui_intent_01.py"""
from sqlalchemy import create_engine
from PyQt5.QtWidgets import QApplication, QMainWindow, QTableView,QLabel,QLineEdit,QPushButton,QVBoxLayout,QWidget
from PyQt5.QtCore import QAbstractTableModel, Qt
import pandas as pd

class PandasModel(QAbstractTableModel):
    def __init__(self, data):
        QAbstractTableModel.__init__(self)
        self._data = data

    def rowCount(self, parent=None):
        return len(self._data.values)

    def columnCount(self, parent=None):
        return self._data.columns.size

    def data(self, index, role=Qt.DisplayRole):
        if index.isValid():
            if role == Qt.DisplayRole:
                return str(self._data.values[index.row()][index.column()])
        return None

    def headerData(self, col, orientation, role):
        if orientation == Qt.Horizontal and role == Qt.DisplayRole:
            return self._data.columns[col]
        return None


class MainWindow(QMainWindow):
    def __init__(self):
        super().__init__()

        self.setWindowTitle("Query Tool")

        # Create widgets
        self.db_label = QLabel("Database:")
        self.db_text = QLineEdit()
        self.query_button = QPushButton("Execute Query")
        self.table_view = QTableView()

        # Add widgets to layout
        layout = QVBoxLayout()
        layout.addWidget(self.db_label)
        layout.addWidget(self.db_text)
        layout.addWidget(self.query_button)
        layout.addWidget(self.table_view)

        # Set layout
        widget = QWidget()
        widget.setLayout(layout)
        self.setCentralWidget(widget)

        # Connect button to function
        self.query_button.clicked.connect(self.execute_query)

    def execute_query(self):
        # Get database name from text box
        db_name = self.db_text.text()

        # Connect to database
        engine = create_engine(f"sqlite:///{db_name}")

        # Execute query
        query = "SELECT 1 as a"
        df = pd.read_sql_query(query, engine)

        # Show results in table view
        model = PandasModel(df)
        self.table_view.setModel(model)



import sys
from PyQt5.QtWidgets import QApplication

if __name__ == '__main__':
    app = QApplication(sys.argv)
    window = MainWindow()
    window.show()
    sys.exit(app.exec_())

