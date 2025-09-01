with produtos_atacadao_silver as (
    select 
        product_id as id_produto,
        product_name as nome,
        product_brand as marca,
        cast(price as decimal(10,2)) as preco,
        cast(scraped_at as timestamp) as data_coleta,
        cast(data_inclusao_analitico as timestamp) as data_inclusao_analitico,
        'Atacadão' as nome_supermercado    
    from {{ source('atacadao', 'produtos_atacadao') }} -- Aqui pega da camda bronze atacadao_bronze.produtos_atacadao
) 
select * from produtos_atacadao_silver