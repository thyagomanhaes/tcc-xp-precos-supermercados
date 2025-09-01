from airflow.decorators import dag, task
from pendulum import datetime
import json
from datetime import datetime
from zoneinfo import ZoneInfo
from include.scraping_utils import scrape_megga_atacadista
from include.mongo_utils import bulk_upsert_price_changed_megga_atacadista
from include.ingestao_mongo_to_postgres import run_etl
from include.telegram_utils import run
from include.postgres_utils import query_postgres_to_dataframe

import subprocess
import asyncio
from telegram import Bot

from airflow.providers.postgres.hooks.postgres import PostgresHook


POSTGRES_CONN_ID = "postgres_local"
TELEGRAM_TOKEN = "8345402256:AAEmBvVqg_9oJuj8q4WQ7CVsHYIMvxlR09Y"
TELEGRAM_CHAT_ID = "-1002808832579"


# Define the basic parameters of the DAG, like schedule and start_date
@dag(
    dag_id="scrape_supermercados",
    start_date=datetime(2025, 8, 23),
    schedule="*/10 * * * *", # minuto, hora, dia do mês, mês, dia da semana
    catchup=False,    
    default_args={"owner": "Thyago Manhaes", "retries": 3},
    description="DAG to check data",
    tags=["data_engineering"],
)
def scrape_supermercados_dag():

    @task
    def scraping_megga_atacadista():
        """Executa o scraping e retorna lista de produtos"""
        urls_cerveja = [
            "https://meggaatacadista.com.br/bebidas/cerveja",
            "https://meggaatacadista.com.br/bebidas/cerveja?page=2"
        ]

        produtos = []
        
        for url_cerveja in urls_cerveja:
            produtos += scrape_megga_atacadista(url_cerveja)

        
        return produtos  # O retorno já vai para XCom automaticamente, esse valor é serializado como XCom

    @task
    def save_products_mongo(produtos_megga_atacadista):
        bulk_upsert_price_changed_megga_atacadista(collection_name="megga_atacadista", products=produtos_megga_atacadista) # bulk_salvar_mongo(dados)

    
    @task
    def ingestao_mongo_to_postgres():
        run_etl()
    
    @task
    def run_dbt_model():
        """
        Executa o modelo my_first_dbt_model usando o CLI do dbt
        """
        dbt_dir = "/usr/local/airflow/dbt/dbt_supermercados"
        result = subprocess.run(
            ["dbt", "run", "--select", "my_first_dbt_model", "--profiles-dir", "/home/astro/.dbt"],
            cwd=dbt_dir,
            capture_output=True,
            text=True
        )
        print(result.stdout)
        if result.returncode != 0:
            print(result.stderr)
            raise Exception("Erro ao rodar dbt")
    
    @task
    def send_postgres_report_to_telegram():
        sql = "select marca, nome, preco from silver.produtos_atacadao_gold;"
        df = query_postgres_to_dataframe(POSTGRES_CONN_ID, sql)
        message = f"Total de produtos cadastrados: {df.shape[0]} \n"

        for index, row in df.iterrows():
            message += f"\nMarca: {row['marca']}, Nome: {row['nome']}, Preço: {row['preco']}"

        print(message)
        # Função async interna
        async def send_message():
            bot = Bot(token=TELEGRAM_TOKEN)
            await bot.send_message(chat_id=TELEGRAM_CHAT_ID, text=message)
        
        asyncio.run(send_message())

    # Encadeamento das tasks
    produtos_megga_atacadista = scraping_megga_atacadista()
    salvar_mongodb = save_products_mongo(produtos_megga_atacadista)
    ingestao_mongo_to_postgres = ingestao_mongo_to_postgres()
    
    report_task = send_postgres_report_to_telegram()
    
    # Define a ordem das tasks
    produtos_megga_atacadista >> salvar_mongodb >> ingestao_mongo_to_postgres >> report_task 

scrape_supermercados_dag()        