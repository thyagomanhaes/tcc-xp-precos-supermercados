import requests
import json
from datetime import datetime
from zoneinfo import ZoneInfo
from bs4 import BeautifulSoup


def scrape_megga_atacadista(url=None):
    """Coleta dados de produtos do site Megga Atacadista."""
    # uri_base = "https://meggaatacadista.com.br/bebidas/cerveja"
    # uri_base2 = "https://meggaatacadista.com.br/bebidas/cerveja?page=2"

    response = requests.get(url)

    # Verifica se deu certo
    if response.status_code == 200:
        html_content = response.text

        # Criar um objeto BeautifulSoup
        soup = BeautifulSoup(html_content, 'html.parser')

        # Criar uma lista vazia para armazenar os objetos JSON
        lista_produtos = []

        # Encontrar todos os elementos com a classe "product-card__info"
        products_view = soup.find_all('div', class_='products-view__list products-list products-view__layout-list')

        for produto_view in products_view:
            for produto in produto_view.find_all('div', class_='products-list__item'):
                # Extrair os campos desejados
                product_card_info = produto.find('div', class_='product-card__info')

                data_id_produto = product_card_info.get('data-id-produto')
                data_nome = product_card_info.get('data-nome')
                data_preco = product_card_info.get('data-preco')

                # Criar um objeto JSON com os campos desejados
                produto_json = {
                    'data-id-produto': data_id_produto,
                    'data-nome': data_nome,
                    'data-preco': data_preco,
                    'data-id-marca': product_card_info.get('data-id-marca'),
                    'data-marca': product_card_info.get('data-marca'),
                    'scraped_at': datetime.now(ZoneInfo("America/Sao_Paulo")).isoformat()
                }

                # Adicionar o objeto JSON à lista
                lista_produtos.append(produto_json)

        print(f"{len(lista_produtos)} produtos coletados com sucesso do Megga Atacadista.")

        return lista_produtos