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

except Error as e:
    print(f"Ocorreu um erro: {e}")

finally:
        conexao.close()
        print("\nConexão com o banco de dados encerrada.")