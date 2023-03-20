"""class_chart_bar.py"""

import os
import random
#from class_config import Config
from class_base_class import BaseClass

class ChartBarClass(ChartBaseClass):

    def img_bar(self, cfg_, group_desc, group_name):
        
        owners_ = f"""
        SELECT
        a.owner as Grupo,
        total_LAST_4,
        total_LAST_8,
        total_LAST_12
        FROM v_HISTORY_YEARS_MB_OWNER A WHERE
        CASE
        WHEN total_mb_owner between 0 and 1000 THEN 'LITE'
        WHEN total_mb_owner between 1000 and 100000 THEN 'MED'
        WHEN total_mb_owner between 100000 and 999999999 THEN 'BIG'
        END
        ='{group_name}'
        """
        df = self.read_sql_query(owners_)

        #df = pd.DataFrame(data).set_index('Grupo')
        df = df.set_index('Grupo')

        # Normalizar los datos
        df = df.div(df.sum(axis=1), axis=0) * 100

        # Normalize the data while ignoring the first column
        #df.iloc[:, 1:] = df.iloc[:, 1:].div(df.iloc[:, 1:].sum(axis=1), axis=0) * 100

        # Definir los colores de las barras
        #colors = ['#FF4136' , '#FFDC00', '#2ECC40' ]#ryb
        colors = ['#2ECC40' , '#FFDC00',  '#FF4136' ]#GYR
        #colors = ['#2ECC40' , '#0074D9' ]#gb

        # Crear el gráfico de barras apiladas horizontal con los colores definidos
        ax = df.plot(kind='barh', stacked=True, width=0.8, figsize=(12,10), color=colors)

        ax.set_title('Owner History')
        ax.set_xlabel('Percent (%)')
        ax.set_ylabel('Databases ' + group_name)
        ax.set_xlim([0, 100])
        ax.tick_params(axis='y', labelsize=8)
        ax.tick_params(axis='x', labelsize=8)
        # Agregar etiquetas en el centro de cada barra
        for container in ax.containers:
            ax.bar_label(container, label_type='center', labels=[f'{x:.2f}%' for x in container.datavalues], fontsize=6)

            #ax.bar_label(container, label_type='center', labels=[f'{x:.0f}' for x in container.datavalues], fontsize=10)

        # Ajustar el tamaño de la figura para que quepan todas las etiquetas del eje Y
        #plt.subplots_adjust(left=0.4, right=0.9, top=0.9, bottom=0.1)
        plt.subplots_adjust(left=0.3)

        out__ = cfg_.get_par('out_path') #+ "reports"

        plt.savefig(f'{out__}{self.report_name__}/{self.report_name__}img_002_owner_history_{group_name}.png')

