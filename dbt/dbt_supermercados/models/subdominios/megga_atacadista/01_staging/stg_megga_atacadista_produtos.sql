select 
    "data-id-produto" as id_produto,
    "data-nome"  as nome_origem,
    initcap(regexp_replace(trim("data-nome"), '\s+', ' ', 'g')) as nome,
    "data-marca" as marca_origem,
    initcap(regexp_replace(trim("data-marca"), '\s+', ' ', 'g')) as marca,
    cast("data-preco" as decimal(10,2)) as preco,
    cast(scraped_at as timestamp) as data_coleta,
    cast(data_inclusao_analitico as timestamp) as data_inclusao_analitico,
    'Megga Atacadista' as nome_supermercado
from {{ source('megga_atacadista', 'megga_atacadista') }}