import os
import warnings
import random
import sqlalchemy
from sqlalchemy import create_engine
import pandas as pd
import matplotlib.pyplot as plt
from class_config import Config
from class_base_class import BaseClass

#cfg = Config("NO_INICIALIZADO",'NO_INICIALIZADO')

class HistoryCharts(BaseClass):
    """HistoryCharts"""
    __cfg__ = None
    """Generate the history charts of the database"""
    def __init__(self,cfg__,__target_url__):
        """__init__"""
        print(f"cfg__.parameters:{cfg__.get_parameter_dict()}")
        super().__init__( cfg__.get_cfg('log_active') )
        warnings.warn("DEPRECATED:al recibir cfg__ ya no requiere (__target_url__,__flavor__)")
        self.report_name__ = cfg__.get_cfg('report_name')# report_name__
        self.__flavor__ = cfg__.get_cfg('sql_flavor')
        self.__target_url__ = __target_url__
        self.__cfg__ = cfg__

        html_folder = f"{cfg__.get_cfg('out_path')}{self.report_name__}"

        try:
            os.mkdir(html_folder)
            self._log(f"Folder: '{html_folder}'-->created successfully.")
        except FileExistsError:
            self._log(f"Folder: '{html_folder}'-->already exists.")
        except Exception as e:
            self._log(f"An error occurred: {e}")

    def get_engine_target(self):
        """method get_engine_target"""
        engine = create_engine(self.__target_url__)
        return engine

    def read_sql_query(self,sql_):
        """method read_sql_query"""
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
            #self._log(f"target_execute:finally:{sql_}")
            cnx.close()
            engine.dispose()
        #self._log(f"target_execute:END:{sql_}")
        return data

    def img_001_table_vs_index(self, group_desc, group_name):
        """method img_001_table_vs_index"""
        owners_ = f"""
        SELECT  owner as Grupo, total_mb_table as mb_table, total_mb_index as mb_index

        FROM v_METADATA_OWNER_SPACE A
        WHERE

        CASE 
        WHEN total_mb between 0 and 1000 THEN 'LITE' 
        WHEN total_mb between 1000 and 100000 THEN 'MED' 
        WHEN total_mb between 100000 and 999999999 THEN 'BIG' 
        END 
        = '{group_name}'
        ORDER BY total_mb DESC

        """
        df_tb_vs_idx = self.read_sql_query(owners_)

        #df = pd.DataFrame(data).set_index('Grupo')
        df_tb_vs_idx = df_tb_vs_idx.set_index('Grupo')

        # Normalizar los datos
        df_tb_vs_idx = df_tb_vs_idx.div(df_tb_vs_idx.sum(axis=1), axis=0) * 100

        # Normalize the data while ignoring the first column
        #df.iloc[:, 1:] = df.iloc[:, 1:].div(df.iloc[:, 1:].sum(axis=1), axis=0) * 100

        # Definir los colores de las barras
        #colors = ['#FF4136' , '#FFDC00', '#2ECC40' ]#ryb
        colors = ['#2ECC40' , '#0074D9' ]#gb

        # Crear el gráfico de barras apiladas horizontal con los colores definidos
        ax = df_tb_vs_idx.plot(kind='barh', stacked=True, width=0.8, figsize=(12,10), color=colors)

        ax.set_title('MB in tables vs MB in indexes')
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

        out__ = self.__cfg__.get_cfg('out_path') #+ "reports"

        plt.savefig(f'{out__}{self.report_name__}/{self.report_name__}img_001_table_vs_index_{group_name}.png')

    def img_002_owner_history(self, group_desc, group_name):
        
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

        out__ = self.__cfg__.get_cfg('out_path') #+ "reports"

        plt.savefig(f'{out__}{self.report_name__}/{self.report_name__}img_002_owner_history_{group_name}.png')


    def get_html_sql(self, sql_):#TODO: ESTO NO FUNCIONA

        #owners_ = "SELECT * FROM v_METADATA_OWNER_SPACE ORDER BY total_mb_table desc"
        df_metadata = self.read_sql_query(sql_)
        df_metadata['total_mb_table'] = df_metadata['total_mb_table'].round(0).astype(int)
        df_metadata['mb_BIG'] = df_metadata['total_mb_table'].round(0).astype(int)

        # Set the 'text-align' CSS style for the 'Age' column to 'right'
        styles = [{'selector': '.dataframe td', 'props': [('text-align', 'right')]}]
        df_metadata.style.set_table_styles(styles)

        html = df_metadata.to_html()

    def report_v1_issues(self):
        """report_v1_issues"""
        pass

    def report_v1_space(self):
        """report_v1_space"""
        pass

    def report_v1_history(self):
        """report_v1_history"""
        name = self.report_name__

        ########################## INICIO LITE
        owners_ = """
        SELECT 
        a.owner as Grupo,
        total_mb_owner,
        total_LAST_4,
        total_LAST_8,
        total_LAST_12
        FROM v_HISTORY_YEARS_MB_OWNER A WHERE 
        CASE 
        WHEN total_mb_owner between 0 and 1000 THEN 'LITE' 
        WHEN total_mb_owner between 1000 and 100000 THEN 'MED' 
        WHEN total_mb_owner between 100000 and 999999999 THEN 'BIG' 
        END 
        ='LITE'

        """
        df_metadata = self.read_sql_query(owners_)
        df_metadata['total_mb_owner'] = df_metadata['total_mb_owner'].round(0).astype(int)
        df_metadata['total_LAST_4'] = df_metadata['total_LAST_4'].round(0).astype(int)
        df_metadata['total_LAST_8'] = df_metadata['total_LAST_8'].round(0).astype(int)
        df_metadata['total_LAST_12'] = df_metadata['total_LAST_12'].round(0).astype(int)

        # Set the 'text-align' CSS style for the 'Age' column to 'right'
        styles = [{'selector': '.dataframe td', 'props': [('text-align', 'right')]}]
        df_metadata.style.set_table_styles(styles)

        html_lite = df_metadata.to_html()
        ##########################33 FIN LITE


        ########################## INICIO MED
        owners_ = """
        SELECT 
        a.owner as Grupo,
        total_mb_owner,
        total_LAST_4,
        total_LAST_8,
        total_LAST_12
        FROM v_HISTORY_YEARS_MB_OWNER A WHERE 
        CASE 
        WHEN total_mb_owner between 0 and 1000 THEN 'LITE' 
        WHEN total_mb_owner between 1000 and 100000 THEN 'MED' 
        WHEN total_mb_owner between 100000 and 999999999 THEN 'BIG' 
        END 
        ='MED'

        """
        df_metadata = self.read_sql_query(owners_)
        df_metadata['total_mb_owner'] = df_metadata['total_mb_owner'].round(0).astype(int)
        df_metadata['total_LAST_4'] = df_metadata['total_LAST_4'].round(0).astype(int)
        df_metadata['total_LAST_8'] = df_metadata['total_LAST_8'].round(0).astype(int)
        df_metadata['total_LAST_12'] = df_metadata['total_LAST_12'].round(0).astype(int)
        #df_metadata['mb_BIG'] = df_metadata['total_mb_table'].round(0).astype(int)

        # Set the 'text-align' CSS style for the 'Age' column to 'right'
        styles = [{'selector': '.dataframe td', 'props': [('text-align', 'right')]}]
        df_metadata.style.set_table_styles(styles)

        html_med = df_metadata.to_html()
        ##########################33 FIN MED

        ########################## INICIO BIG
        owners_ = """
        SELECT 
        a.owner as Grupo,
        total_mb_owner,
        total_LAST_4,
        total_LAST_8,
        total_LAST_12
        FROM v_HISTORY_YEARS_MB_OWNER A WHERE 
        CASE 
        WHEN total_mb_owner between 0 and 1000 THEN 'LITE' 
        WHEN total_mb_owner between 1000 and 100000 THEN 'MED' 
        WHEN total_mb_owner between 100000 and 999999999 THEN 'BIG' 
        END 
        ='BIG'

        """
        df_metadata = self.read_sql_query(owners_)
        df_metadata['total_mb_owner'] = df_metadata['total_mb_owner'].round(0).astype(int)
        df_metadata['total_LAST_4'] = df_metadata['total_LAST_4'].round(0).astype(int)
        df_metadata['total_LAST_8'] = df_metadata['total_LAST_8'].round(0).astype(int)
        df_metadata['total_LAST_12'] = df_metadata['total_LAST_12'].round(0).astype(int)
       
        # Set the 'text-align' CSS style for the 'Age' column to 'right'
        styles = [{'selector': '.dataframe td', 'props': [('text-align', 'right')]}]
        df_metadata.style.set_table_styles(styles)

        html_big = df_metadata.to_html()
        ##########################33 FIN MED

        # Save the HTML code to a file

        # HistoryCharts (report_name__,__target_url__,__flavor__,__log_active__)
        #history_charts= HistoryCharts(self.report_name__,self.__target_url__,self.__flavor__,self.__log_active__)

        cen = 'style="text-align:center"'

        header_000 = f'<div {cen}><h1>Database History Report:{name}</h1></div>\n'

        #header_01_ = f'<div {cen}><h2>MB in Tables vs MB in Indexes</h2></div>\n'

        header_01_lite = f'<div {cen}><h2>Databases LITE: LITE Group - less than 1GB</h2></div>\n'
        self.img_002_owner_history ('LITE Group - less 1GB','LITE')
        img_002_owner_history_lite = f'<img src="{name}img_002_owner_history_LITE.png"></img>'

        header_01_med = f'<div {cen}><h2>Databases MEDIUM: Medium Group - less than 100GB</h2></div>\n'
        self.img_002_owner_history('MEDIUM Group - less 100GB','MED')
        img_002_owner_history_medium = f'<img src="{name}img_002_owner_history_MED.png"></img>\n'

        header_01_big = f'<div {cen}><h2>Databases BIG: Big Group - more than 100GB</h2></div>\n'
        self.img_002_owner_history('BIG Group - more tha 100GB','BIG')
        img_002_owner_history_big = f'<img src="{name}img_002_owner_history_BIG.png"></img>\n'


        header_002 = '<div style="text-align:center"><h2>Owners Detail - MB by Table sizes</h2></div>\n'

        out__ = f"{self.__cfg__.get_cfg('out_path')}{self.report_name__}/{self.report_name__}_HISTORY.html" #+ "reports"
        with open(out__, 'w') as f:
            f.write(header_000)
            #f.write(header_01_)

            f.write(header_01_lite)
            f.write(html_lite)
            f.write(img_002_owner_history_lite)

            f.write(header_01_med)
            f.write(html_med)
            f.write(img_002_owner_history_medium)

            f.write(header_01_big)
            f.write(html_big)
            f.write(img_002_owner_history_big)

            #f.write(header_01B)
            #f.write(img_001_table_vs_index_big)

            #f.write(header_002)

    def report_v1_indexes(self):
        """report_v1_indexes"""
        name = self.report_name__

        ########################## INICIO LITE
        owners_ = """
        SELECT  A.*,
        CASE 
        WHEN total_mb between 0 and 1000 THEN 'LITE' 
        WHEN total_mb between 1000 and 100000 THEN 'MED' 
        WHEN total_mb between 100000 and 999999999 THEN 'BIG' 
        END AS GROUP_SIZE
        FROM v_METADATA_OWNER_SPACE A
        WHERE GROUP_SIZE = 'LITE'
        ORDER BY total_mb DESC

        """
        df_metadata = self.read_sql_query(owners_)
        df_metadata['total_mb_table'] = df_metadata['total_mb_table'].round(0).astype(int)
        df_metadata['total_mb_index'] = df_metadata['total_mb_index'].round(0).astype(int)
        df_metadata['total_mb'] = df_metadata['total_mb'].round(0).astype(int)
        df_metadata['mb_BIN'] = df_metadata['mb_BIN'].round(0).astype(int)
        df_metadata['mb_LITE'] = df_metadata['mb_LITE'].round(0).astype(int)
        df_metadata['mb_MED'] = df_metadata['mb_MED'].round(0).astype(int)
        df_metadata['mb_BIG'] = df_metadata['mb_BIG'].round(0).astype(int)

        # Set the 'text-align' CSS style for the 'Age' column to 'right'
        styles = [{'selector': '.dataframe td', 'props': [('text-align', 'right')]}]
        df_metadata.style.set_table_styles(styles)

        html_lite = df_metadata.to_html()
        ##########################33 FIN LITE


        ########################## INICIO MED
        owners_ = """
        SELECT  A.*,
        CASE 
        WHEN total_mb between 0 and 1000 THEN 'LITE' 
        WHEN total_mb between 1000 and 100000 THEN 'MED' 
        WHEN total_mb between 100000 and 999999999 THEN 'BIG' 
        END AS GROUP_SIZE
        FROM v_METADATA_OWNER_SPACE A
        WHERE GROUP_SIZE = 'MED'
        ORDER BY total_mb DESC

        """
        df_metadata = self.read_sql_query(owners_)
        df_metadata['total_mb_table'] = df_metadata['total_mb_table'].round(0).astype(int)
        df_metadata['total_mb_index'] = df_metadata['total_mb_index'].round(0).astype(int)
        df_metadata['total_mb'] = df_metadata['total_mb'].round(0).astype(int)

        df_metadata['mb_BIN'] = df_metadata['mb_BIN'].round(0).astype(int)
        df_metadata['mb_LITE'] = df_metadata['mb_LITE'].round(0).astype(int)
        df_metadata['mb_MED'] = df_metadata['mb_MED'].round(0).astype(int)
        df_metadata['mb_BIG'] = df_metadata['mb_BIG'].round(0).astype(int)

        # Set the 'text-align' CSS style for the 'Age' column to 'right'
        styles = [{'selector': '.dataframe td', 'props': [('text-align', 'right')]}]
        df_metadata.style.set_table_styles(styles)

        html_med = df_metadata.to_html()
        ##########################33 FIN MED

        ########################## INICIO BIG
        owners_ = """
        SELECT  A.*,
        CASE 
        WHEN total_mb between 0 and 1000 THEN 'LITE' 
        WHEN total_mb between 1000 and 100000 THEN 'MED' 
        WHEN total_mb between 100000 and 999999999 THEN 'BIG' 
        END AS GROUP_SIZE
        FROM v_METADATA_OWNER_SPACE A
        WHERE GROUP_SIZE = 'BIG'
        ORDER BY total_mb DESC

        """
        df_metadata = self.read_sql_query(owners_)
        df_metadata['total_mb_table'] = df_metadata['total_mb_table'].round(0).astype(int)
        df_metadata['total_mb_index'] = df_metadata['total_mb_index'].round(0).astype(int)
        df_metadata['total_mb'] = df_metadata['total_mb'].round(0).astype(int)

        df_metadata['mb_BIN'] = df_metadata['mb_BIN'].round(0).astype(int)
        df_metadata['mb_LITE'] = df_metadata['mb_LITE'].round(0).astype(int)
        df_metadata['mb_MED'] = df_metadata['mb_MED'].round(0).astype(int)
        df_metadata['mb_BIG'] = df_metadata['mb_BIG'].round(0).astype(int)
        #df_metadata['mb_BIG'] = df_metadata['total_mb_table'].round(0).astype(int)
        
        # Set the 'text-align' CSS style for the 'Age' column to 'right'
        styles = [{'selector': '.dataframe td', 'props': [('text-align', 'right')]}]
        df_metadata.style.set_table_styles(styles)

        html_big = df_metadata.to_html()
        ##########################33 FIN MED

        # Save the HTML code to a file

        cen = 'style="text-align:center"'

        header_000 = f'<div {cen}><h1>Database History Report:{name}</h1></div>\n'

        #header_01_ = f'<div {cen}><h2>MB in Tables vs MB in Indexes</h2></div>\n'

        header_01L = f'<div {cen}><h2>Databases LITE: LITE Group - less 1GB</h2></div>\n'
        self.img_001_table_vs_index('LITE Group - less 1GB','LITE')
        img_001_table_vs_index_lite = f'<img src="{name}img_001_table_vs_index_LITE.png"></img>'

        header_01M = f'<div {cen}><h2>Databases MEDIUM: Medium Group - less 100GB</h2></div>\n'
        self.img_001_table_vs_index('MEDIUM Group - less 100GB','MED')
        img_001_table_vs_index_medium = f'<img src="{name}img_001_table_vs_index_MED.png"></img>\n'

        header_01B = f'<div {cen}><h2>Databases BIG: Big Group - more than 100GB</h2></div>\n'
        self.img_001_table_vs_index('BIG Group - more tha 100GB','BIG')
        img_001_table_vs_index_big = f'<img src="{name}img_001_table_vs_index_BIG.png"></img>\n'


        header_002 = '<div style="text-align:center"><h2>Owners Detail - History Resport</h2></div>\n'

        out__ = f"{self.__cfg__.get_cfg('out_path')}{self.report_name__}/{self.report_name__}_INDEXES.html" #+ "reports"
        with open(out__, 'w') as f:
            f.write(header_000)
            #f.write(header_01_)

            f.write(header_01L)
            f.write(html_lite)
            f.write(img_001_table_vs_index_lite)

            f.write(header_01M)
            f.write(html_med)
            f.write(img_001_table_vs_index_medium)

            f.write(header_01B)
            f.write(html_big)
            f.write(img_001_table_vs_index_big)

    def generate_pie_from_sql(self, title, sql_):#TODO: ESTO NO FUNCIONA
        # Connect to the database and execute a query
        df = self.read_sql_query(sql_)
        print(df.index)
        #df = pd.read_sql(query, self.get_engine_target(), index_col='label_')

        # Create a pie chart
        fig, ax = plt.subplots()
#        ax.pie(df['CNT_TABLES'], labels=df['LABEL_'], autopct='%1.1f%%')
        ax.pie(df['CNT'], labels=df['LABEL_'], autopct='%1.1f%%')

        # Set the title
        ax.set_title(title)

        # Show the chart
        plt.show()

    def generate_excel_from_sql(self, title, sql_):#TODO: ESTO NO FUNCIONA
        # Connect to the database and execute a query
        df = self.read_sql_query(sql_)
        # Create an Excel file from the DataFrame
        writer = pd.ExcelWriter('output.xlsx', engine='xlsxwriter')
        df.to_excel(writer, sheet_name='Sheet1', index=False)
        writer.save()

    def dev_pie(self):
        """dev_pie"""
        self.generate_pie_from_sql('PIE0:Tablas con fecha',"""
        SELECT 'SIN FECHA' AS LABEL_ , COUNT(CASE WHEN METADATA.OWNER IS NULL THEN 1 ELSE NULL END) AS CNT FROM v_METADATA_TABLE_SPACE TABLAS
        left join METADATA_TABLE_DATE METADATA ON TABLAS.owner = METADATA.owner AND TABLAS.table_name = METADATA.table_name
        UNION ALL
        SELECT 'CON FECHA' AS LABEL_ , COUNT(CASE WHEN METADATA.OWNER IS NOT NULL THEN 1 ELSE NULL END) AS CNT FROM v_METADATA_TABLE_SPACE TABLAS
        left join METADATA_TABLE_DATE METADATA ON TABLAS.owner = METADATA.owner AND TABLAS.table_name = METADATA.table_name
        """)

    def generate_report_start_step_3(self):
        """generate_report_start_step_3"""
        sql_ = """
        SELECT A.*,case when mb_bk>0 then 1 else 0 end as FLAG_POSIBLE_BK from v_HISTORY_YEARS_POSIBLE_BK A
        """

        datos = self.read_sql_query(sql_)

        datos = datos.replace(0, None)
        out__ = f"{self.__cfg__.get_cfg('out_path')}{self.report_name__}/{self.report_name__}_POSIBLE_BK.xlsx"

        # Create an Excel file from the DataFrame
        writer = pd.ExcelWriter(out__, engine='xlsxwriter')
        datos.to_excel(writer, sheet_name='Sheet1', index=False)
        writer.save()


    def generate_report_start_step_3_2(self):
        """generate_report_start_step_4"""
        sql_ = """
        select * from v_RPT_POSIBLE_BK
        """
        datos = self.read_sql_query(sql_)
        datos = datos.replace(0, None)
        out__ = f"{self.__cfg__.get_cfg('out_path')}{self.report_name__}/{self.report_name__}_RESUMEN_POSIBLE_BK.xlsx"
        # Create an Excel file from the DataFrame
        writer = pd.ExcelWriter(out__, engine='xlsxwriter')
        datos.to_excel(writer, sheet_name='RESUMEN_POSIBLE_BK', index=False)
        writer.save()

    def dev_rpt_evolutivo(self):
        """dev_rpt_evolutivo"""
        sql_ = """
        SELECT mes_id, 
        case when Y2019 = 0 then NULL else Y2019 end as Y2019,
        case when Y2020 = 0 then NULL else Y2020 end as Y2020,
        case when Y2021 = 0 then NULL else Y2021 end as Y2021,
        case when Y2022 = 0 then NULL else Y2022 end as Y2022,
        case when Y2023 = 0 then NULL else Y2023 end as Y2023
        from (
            SELECT 
            mes_id, 
            sum(case when year_=2019 then mb_estimado else 0 end) as Y2019,
            sum(case when year_=2020 then mb_estimado else 0 end) as Y2020,
            sum(case when year_=2021 then mb_estimado else 0 end) as Y2021,
            sum(case when year_=2022 then mb_estimado else 0 end) as Y2022,
            sum(case when year_=2023 then mb_estimado else 0 end) as Y2023
            FROM 
                v_HISTORY_MES_MB
            WHERE mes_id>0
                GROUP BY mes_id
            ) A
        """

        datos = self.read_sql_query(sql_)
        datos = datos.replace(0, None)

        # Colores aleatorios para cada año
        colores = {}
        for columna in datos.columns:
            color = tuple(random.uniform(0, 1) for _ in range(3))
            colores[columna] = color

        # Gráfico evolutivo
        for columna in datos.columns:
            #print( f"{columna}")
            if f"{columna}"=='mes_id':
                self._log( f"ommiting column in graph:{columna}")
                continue
            plt.plot(datos.index, datos[columna], color=colores[columna], label=columna)

        # Configuración del gráfico
        plt.title('Gráfico evolutivo por mes')
        plt.xlabel('Mes')
        plt.ylabel('Valor')
        plt.xticks(range(1, 13))
        plt.legend()

        # Mostrar el gráfico
        plt.show()

        #self.generate_pie_from_sql('PIE0:Tablas con fecha',

"""
report_name__ = 'BRAHMS1P_stable_002'
target_db_name = f'{report_name__}_EXPORT_HISTORY.db'
__target_url__ = f"sqlite:///{self.__cfg__.get_cfg('out_path')}/{target_db_name}"

history_charts= HistoryCharts(report_name__,  __target_url__ ,'ORACLE',True)
history_charts.dev_EVOLUTIVO()

"""