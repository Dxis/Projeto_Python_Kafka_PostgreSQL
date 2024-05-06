import psycopg2
import os
from dotenv import load_dotenv
# Importe o módulo json
import json

# Carrega as variáveis de ambiente do arquivo .env
load_dotenv()

# Obtém a string de conexão do banco de dados do arquivo .env
CONN_POSTGRESQL = os.getenv("CONN_POSTGRESQL")

class BD:
    def __init__(self):
        # Carrega as variáveis de ambiente do arquivo .env
        load_dotenv()

        # Obtém a string de conexão do banco de dados do arquivo .env
        self.CONN_POSTGRESQL = os.getenv("CONN_POSTGRESQL")
        
        # Conecte-se ao banco de dados
        self.conn = psycopg2.connect(self.CONN_POSTGRESQL)
        # Crie um cursor para executar consultas
        self.cursor = self.conn.cursor()

    def inserir_mensagem(self, mensagem):
        # Execute a consulta de inserção com o parâmetro de mensagem
        self.cursor.execute("INSERT INTO tblValidaKafka (mensagem) VALUES (%s)", (mensagem,))
        # Faça o commit da transação
        self.conn.commit()

    def fechar_conexao(self):
        # Feche o cursor e a conexão
        self.cursor.close()
        self.conn.close()

    def select(self): 
        # Conecte-se ao banco de dados
        conn = psycopg2.connect(CONN_POSTGRESQL)

        # Crie um cursor para executar consultas
        cursor = conn.cursor()

        # Execute uma consulta de exemplo
        cursor.execute("SELECT * FROM tblValidaKafka")

        # Recupere os resultados da consulta
        rows = cursor.fetchall()

        # Imprima os resultados
        for row in rows:
            print(row)

        # Feche o cursor e a conexão
        cursor.close()
# Exemplo de uso
if __name__ == "__main__":
    bd = BD()
    bd.inserir_mensagem("Exemplo de mensagem")
    bd.fechar_conexao()
    #bd.select()
 