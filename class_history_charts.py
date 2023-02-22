from class_config import Config
from class_base_class import BaseClass
from sqlalchemy import create_engine
import sqlalchemy
import pandas as pd
import matplotlib.pyplot as plt


cfg = Config()

class HistoryCharts(BaseClass):
    """Generate the history charts of the database"""
    def __init__(self,report_name__,__target_url__,__flavor__,__log_active__):
        super().__init__(__log_active__)
        self.report_name__ = report_name__
        self.__flavor__ = __flavor__
        self.__target_url__ = __target_url__
    def get_engine_target(self):
        """function get_engine_target"""
        engine = create_engine(self.__target_url__)
        return engine

    def read_sql_query(self,sql_):
        """function read_sql_query"""
        data = None
        try:
            self._log(f"target_execute_select:START:{sql_}")
            engine = self.get_engine_target()
            cnx=engine.connect()
            data = pd.read_sql_query(sql_, engine)
            #data = cnx.execute(sql_)
        except sqlalchemy.exc.OperationalError:
            self._log(f"target_execute:OperationalError:{sql_}")
        finally:
            self._log(f"target_execute:finally:{sql_}")
            cnx.close()
            engine.dispose()
        self._log(f"target_execute:END:{sql_}")
        return data

    def img_001_table_vs_index(self):

        owners_ = "SELECT owner as Grupo, total_mb_table as mb_table, total_mb_index as mb_index FROM v_METADATA_OWNER_SPACE ORDER BY total_mb_table desc"
        df = self.read_sql_query(owners_)

        #df = pd.DataFrame(data).set_index('Grupo')
        df = df.set_index('Grupo')

        # Normalizar los datos
        df = df.div(df.sum(axis=1), axis=0) * 100

        # Normalize the data while ignoring the first column
        #df.iloc[:, 1:] = df.iloc[:, 1:].div(df.iloc[:, 1:].sum(axis=1), axis=0) * 100

        # Definir los colores de las barras
        #colors = ['#FF4136' , '#FFDC00', '#2ECC40' ]#ryb
        colors = ['#2ECC40' , '#0074D9' ]#gb

        # Crear el gráfico de barras apiladas horizontal con los colores definidos
        ax = df.plot(kind='barh', stacked=True, width=0.8, figsize=(8,6), color=colors)

        ax.set_title('MB in tables vs MB in indexes')
        ax.set_xlabel('Percent (%)')
        ax.set_ylabel('Databases')
        ax.set_xlim([0, 100])
        ax.tick_params(axis='y', labelsize=8)
        ax.tick_params(axis='x', labelsize=8)
        # Agregar etiquetas en el centro de cada barra
        for container in ax.containers:
            ax.bar_label(container, label_type='center', labels=[f'{x:.2f}%' for x in container.datavalues], fontsize=6)

            #ax.bar_label(container, label_type='center', labels=[f'{x:.0f}' for x in container.datavalues], fontsize=10)

        # Ajustar el tamaño de la figura para que quepan todas las etiquetas del eje Y
        #plt.subplots_adjust(left=0.4, right=0.9, top=0.9, bottom=0.1)
        plt.subplots_adjust(left=0.2)

        out__ = cfg.get_par('out_path') #+ "reports"

        plt.savefig(f'{out__}img_001_table_vs_index.png')


    def report_v1(self):
        owners_ = "SELECT * FROM v_METADATA_OWNER_SPACE ORDER BY total_mb_table desc"
        df_metadata = self.read_sql_query(owners_)
        df_metadata['total_mb_table'] = df_metadata['total_mb_table'].round(0).astype(int)
        df_metadata['mb_BIG'] = df_metadata['total_mb_table'].round(0).astype(int)
        

        # Set the 'text-align' CSS style for the 'Age' column to 'right'
        styles = [{'selector': '.dataframe td', 'props': [('text-align', 'right')]}]
        df_metadata.style.set_table_styles(styles)

        html = df_metadata.to_html()
        # Save the HTML code to a file


        # HistoryCharts (report_name__,__target_url__,__flavor__,__log_active__)
        #history_charts= HistoryCharts(self.report_name__,self.__target_url__,self.__flavor__,self.__log_active__)

        self.img_001_table_vs_index()

        header_000 = f'<div style="text-align:center"><h1>Database History Report:{self.report_name__}</h1></div>'
        

        header_001 = '<div style="text-align:center"><h2>MB in Tables versus MB in Indexes</h2></div>'


        img_001_table_vs_index = '<img src="img_001_table_vs_index.png"></img>'

        header_002 = '<div style="text-align:center"><h2>Owners Detail - MB by Table sizes</h2></div>'

        out__ = f"{cfg.get_par('out_path')}{self.report_name__}.html" #+ "reports"
        with open(out__, 'w') as f:
            f.write(header_000)
            f.write(header_001)
            f.write(img_001_table_vs_index)
            f.write(header_002)
            f.write(html)

