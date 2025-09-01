with produtos_mais_recente_atacadao as (
	select
		pa.*	 
	from {{ ref('stg_atacadao_bronze__produtos_atacadao') }} AS pa
	where pa.data_inclusao_analitico = (select max(data_inclusao_analitico ) from atacadao_bronze.produtos_atacadao )
	order by data_inclusao_analitico desc
),
classifica_produtos_por_marca_menor_preco as (
	select	
		marca
		, nome
		, preco
		, row_number() over (partition by marca order by preco) as rn_mais_barato
	from produtos_mais_recente_atacadao
)
select *
from classifica_produtos_por_marca_menor_preco
where rn_mais_barato = 1