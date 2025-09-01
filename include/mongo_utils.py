from pymongo import MongoClient
import os
from dotenv import load_dotenv

CONNECTION_STRING = "mongodb+srv://admin_projeto_aplicado_xp:nwgmKZ5Mx4tjqj6c@cluster-projeto-aplicad.n0gyhjp.mongodb.net/?retryWrites=true&w=majority&appName=Cluster-Projeto-Aplicado-XP"

# Nome do seu banco de dados e da coleção
DATABASE_NAME = "supermercados"  # Pode ser qualquer nome que você queira
NOME_COLECAO = "atacadao"        # O nome que você especificou

load_dotenv()

def get_mongo_client():
    """Retorna a conexão com o MongoDB."""
    # mongo_uri = os.getenv("MONGO_URI", "mongodb://localhost:27017/")

    return MongoClient(CONNECTION_STRING)

def insert_if_price_changed(collection_name, product_data):
    """Insere o produto no MongoDB apenas se o preço tiver mudado."""
    client = get_mongo_client()
    # db = client[os.getenv("MONGO_DB", NOME_BANCO_DE_DADOS)]
    # collection = db[collection_name]
    db = client[DATABASE_NAME]
    
    # Seleciona a coleção
    collection = db[collection_name]

    ultimo = collection.find_one(
        {"id": product_data["id"]},
        sort=[("scraped_at", -1)]
    )

    if not ultimo or ultimo["offers"] != product_data["offers"]:
        collection.insert_one(product_data)
        print(f"[MongoDB] Novo registro inserido para ID {product_data['id']}")
    else:
        print(f"[MongoDB] Nenhuma alteração de preço para ID {product_data['id']}")

from pymongo import UpdateOne
from datetime import datetime

def bulk_upsert_price_changed_atacadao(collection_name, products):
    client = get_mongo_client()
    # db = client[os.getenv("MONGO_DB", NOME_BANCO_DE_DADOS)]
    # collection = db[collection_name]
    db = client[DATABASE_NAME]
    
    # Seleciona a coleção
    collection = db[collection_name]

    requests = []

    for product in products:
        filtro = {
            "id": product["id"],
            # "$or": [
            #    {"price": {"$exists": False}},
            #    {"price": {"$ne": produto["price"]}}
            #]
        }
        update_product = {
            # "$set": {
            #     "name": produto["name"],
            #     "price": produto["price"],
                # outros campos que você deseja atualizar
            # }
            "$set": product
        }
        
        requests.append(UpdateOne(filtro, update_product, upsert=True))

    if requests:
        result = collection.bulk_write(requests)
        print(f"Matched: {result.matched_count}, Modified: {result.modified_count}, Upserted: {len(result.upserted_ids)}")


def bulk_upsert_price_changed_megga_atacadista(collection_name, products):
    client = get_mongo_client()
    # db = client[os.getenv("MONGO_DB", NOME_BANCO_DE_DADOS)]
    # collection = db[collection_name]
    db = client[DATABASE_NAME]
    
    # Seleciona a coleção
    collection = db[collection_name]

    requests = []

    for produto in products:
        filtro = {
            "data-id-produto": produto["data-id-produto"],
        }
        atualizacao = {
            "$set": produto
        }
        requests.append(UpdateOne(filtro, atualizacao, upsert=True))

    if requests:
        resultado = collection.bulk_write(requests)
        print(f"Matched: {resultado.matched_count}, Modified: {resultado.modified_count}, Upserted: {len(resultado.upserted_ids)}")
