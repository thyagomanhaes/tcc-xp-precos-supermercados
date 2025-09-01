import pandas as pd
from pymongo import MongoClient
import psycopg2
from psycopg2.extras import execute_values

from dotenv import load_dotenv
import os
import psycopg2
from sqlalchemy import create_engine
from datetime import datetime
from airflow.providers.postgres.hooks.postgres import PostgresHook

load_dotenv()

# Configurações MongoDB
MONGO_URI = "mongodb+srv://admin_projeto_aplicado_xp:nwgmKZ5Mx4tjqj6c@cluster-projeto-aplicad.n0gyhjp.mongodb.net/?retryWrites=true&w=majority&appName=Cluster-Projeto-Aplicado-XP"
MONGO_DB = "supermercados"
COLLECTIONS = ["produtos_atacadao", "megga_atacadista"]

# Configurações PostgreSQL
PG_HOST = "postgres_local"
PG_DB = "supermercados_db"
PG_USER = "admin"
PG_PASS = "admin123"
PG_PORT = "5433"

def extract_from_mongo(collection_name):
    client = MongoClient(MONGO_URI)
    db = client[MONGO_DB]
    collection = db[collection_name]
    data_list = list(collection.find({}))
    
    return data_list

def atacadao_transform_data(data_list):
    atacadao_products = []

    for data in data_list:
        atacadao_product = {
            "product_id": data['id'],
            "product_name": data['name'],
            "product_brand": data['brandName'],
            "price": data['offers'][0]['price'],
            "scraped_at": data['scraped_at']
        }
        atacadao_products.append(atacadao_product)              

    
    df = pd.DataFrame(atacadao_products)
    return df

def megga_transform_data(data_list):
    megga_products = []

    for data in data_list:
        megga_product = {
            "data-id-produto": data['data-id-produto'],
            "data-nome": data['data-nome'],
            "data-marca": data['data-marca'],
            "data-preco": data['data-preco'],
            "scraped_at": data['scraped_at']
        }
        megga_products.append(megga_product)              

    
    df = pd.DataFrame(megga_products)
    return df
    
def load_to_postgres(df, table_name,schema_name):
    
    # conn = psycopg2.connect(
    #     host=PG_HOST, dbname=PG_DB, user=PG_USER, password=PG_PASS, port=PG_PORT
    # )

    hook = PostgresHook(postgres_conn_id="postgres_local")
    conn = hook.get_conn()

    cur = conn.cursor()

    # Criação da tabela (colunas dinâmicas conforme DataFrame)
    # cols = ", ".join([f'"{col}" TEXT' for col in df.columns])
    # cols += ', "data_inclusao_analitico" TIMESTAMP DEFAULT NOW()'

    # cur.execute(f'DROP TABLE IF EXISTS "{schema_name}"."{table_name}";')
    # cur.execute(f'CREATE TABLE "{schema_name}"."{table_name}" ({cols});')

    df_str = df.astype(str)

    # Inserindo os dados
    records = [tuple(x) for x in df_str.to_numpy()]
    
    sql = f'INSERT INTO "{schema_name}"."{table_name}" VALUES %s'

    execute_values(cur, sql, records)

    conn.commit()
    cur.close()
    conn.close()
    print(f"Tabela '{schema_name}.{table_name}' criada e {len(df)} registros inseridos.")


# DAG Airflow
# Scraping dados -> Transformação (Salvar no Postgre -> Tranformar as tabelas Silver (DBT) -> Camada Gold -> automação para Telegram
def run_etl():
    for collection in COLLECTIONS:
        if collection == "produtos_atacadao":
            print(f"Processando dados da coleção '{collection}'...")   
            data_db = extract_from_mongo(collection)
            df_atacadao = atacadao_transform_data(data_db)
            if not df_atacadao.empty:
                print("Iniciando carga do MongoDB para PostgreSQL...")
                load_to_postgres(df_atacadao, collection, 'atacadao_bronze')
                # df_atacadao.to_csv(f"{collection}.csv", index=False)
                print(f"Dados da coleção '{collection}' processados e armazenados com sucesso!")   
            else:
                print(f"A coleção '{collection}' está vazia.")
        else:
            print(f"Processando dados da coleção '{collection}'...")   
            data_db = extract_from_mongo(collection)
            df_megga = megga_transform_data(data_db)
            if not df_megga.empty:
                load_to_postgres(df_megga, collection, 'megga_atacadista_bronze')
                # df_atacadao.to_csv(f"{collection}.csv", index=False)
                print(f"Dados da coleção '{collection}' processados e armazenados com sucesso!")   
            else:
                print(f"A coleção '{collection}' está vazia.")
