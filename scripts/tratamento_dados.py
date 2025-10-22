import pandas as pd
import mysql.connector
from mysql.connector import Error

# --- CONECTANDO O BANCO DE DADOS ---

db_password = 'Ibmecjr@123'

try:
    conexao = mysql.connector.connect(
        host='34.39.234.170',
        port=3306,
        user='root',
        password=db_password,
        database='aviacao_db_backup'
    )

    if conexao.is_connected():
        print("Conexão com o banco de dados estabelecida com sucesso!")

        # --- CONSULTA SQL COM PANDAS ---

        sql_query = "SELECT * FROM voo"

        df_voos = pd.read_sql(sql_query, conexao)

        print("\nDados carregados! As 5 primeiras linhas são:")
        print(df_voos.head())

        # --- TRATAMENTO DE DATA E HORA ---

        print("\n--- Iniciando tratamento de data e hora ---")

        df_voos['data_hora_chegada'] = pd.to_datetime(df_voos['data_chegada']) + df_voos['h_chegada']

        print("\nVerificando os tipos de dados das colunas:")
        print(df_voos.info())

        print("\nVisualizando a coluna de data e hora final:")
        print(df_voos[['data_chegada', 'h_chegada', 'data_hora_chegada']].head())

        # --- EXTRAIR INFORMAÇÕES DA DATA ---

        print("\n--- Extraindo dia da semana e hora ---")

        df_voos['dia_da_semana'] = df_voos['data_hora_chegada'].dt.day_name()

        df_voos['hora_da_chegada'] = df_voos['data_hora_chegada'].dt.hour

        print("\nDataFrame final com colunas de análise:")
        print(df_voos[['data_hora_chegada', 'dia_da_semana', 'hora_da_chegada']].head())

        # --- ANÁLISE 1 - VOOS DA MADRUGADA POR DIA DA SEMANA ---

        print("\n--- ANÁLISE: Voos chegando de madrugada (00:00 às 06:00) ---")

        voos_madrugada = df_voos[df_voos['hora_da_chegada'] < 6].copy()

        contagem_por_dia = voos_madrugada.groupby('dia_da_semana').size().sort_values(ascending=False)

        print("\nResultado: Contagem de voos por dia da semana na madrugada:")
        print(contagem_por_dia)

        # --- ANÁLISE 2 - PICO DE VOOS (MOTORISTAS) ---

        print("\n--- ANÁLISE 2: Concentração de voos por dia e hora ---")

        pico_voos = df_voos.groupby(['dia_da_semana', 'hora_da_chegada']).size()

        print("\nTop 10 horários de pico (Dia/Hora):")
        print(pico_voos.sort_values(ascending=False).head(10))

        heatmap_data = pico_voos.unstack(fill_value=0)

        print("\nTabela de concentração de voos:")
        print(heatmap_data)

        # --- PREPARAR DADOS PARA OS GRÁFICOS DO DASHBOARD ---

        print("\n--- Preparando dados para os gráficos do dashboard ---")

        # Gráfico 1: Total de voos por dia da semana
        dias_ordenados_en = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
        total_por_dia = df_voos['dia_da_semana'].value_counts().reindex(dias_ordenados_en, fill_value=0)
        print("\nDados para Gráfico 1 (Total por Dia):")
        print(total_por_dia)


        # Gráfico 2: Total de voos por hora do dia
        total_por_hora = df_voos.groupby('hora_da_chegada').size()
        print("\nDados para Gráfico 2 (Total por Hora):")
        print(total_por_hora)


        # Gráfico 3: Total de voos por Companhia Aérea
        print("\nBuscando nomes das companhias aéreas...")
        sql_companhias = "SELECT icao, nome FROM companhia"
        df_companhias = pd.read_sql(sql_companhias, conexao)

        df_completo = pd.merge(df_voos, df_companhias, left_on='companhia_icao', right_on='icao', how='left')

        total_por_companhia = df_completo.groupby('nome').size().sort_values(ascending=False)
        print("\nDados para Gráfico 3 (Top 10 Companhias):")
        print(total_por_companhia.head(10))

        # --- EXPORTAR RESULTADOS PARA EXCEL ---

        print("\n--- Exportando dados para o arquivo Excel... ---")

        # ExcelWriter salva várias tabelas em abas diferentes de um mesmo arquivo
        with pd.ExcelWriter('dados_para_dashboard.xlsx') as writer:
            heatmap_data.to_excel(writer, sheet_name='Pico_Dia_Hora')
            total_por_dia.to_excel(writer, sheet_name='Total_por_Dia')
            total_por_hora.to_excel(writer, sheet_name='Total_por_Hora')
            total_por_companhia.to_excel(writer, sheet_name='Total_por_Companhia')
            df_completo.to_excel(writer, sheet_name='Dados_Completos_Tratados', index=False)

        print("\nArquivo 'dados_para_dashboard.xlsx' criado com sucesso na pasta do projeto!")

except Error as e:
    print(f"Ocorreu um erro: {e}")

finally:
        conexao.close()
        print("\nConexão com o banco de dados encerrada.")