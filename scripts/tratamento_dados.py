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
        database='aviacao_db'
    )

    if conexao.is_connected():
        print("Conexão com o banco de dados estabelecida com sucesso!")

        # --- CONSULTA SQL COM PANDAS ---

        sql_query = "SELECT * FROM voo"

        df_voos = pd.read_sql(sql_query, conexao)

        print("\nDados carregados! As 5 primeiras linhas são:")
        print(df_voos.head())

except Error as e:
    print(f"Ocorreu um erro: {e}")

finally:
        conexao.close()
        print("\nConexão com o banco de dados encerrada.")