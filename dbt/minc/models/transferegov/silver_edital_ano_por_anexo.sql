-- Silver transferegov — ano do edital por ANEXO (PNAB): das abas de definição de
-- edital (bronze_planilha_editais_pnab_ciclo_1), que trazem o "número do edital",
-- mais completo que o nome no contemplado. Só mantém anexo com ano ÚNICO;
-- descarta anexo multi-ano (ambíguo). Colunas via coalesce_por_nome sobre a
-- tabela de origem (origem_da_bronze; ver silver_contemplados_lpg).
{% set editais = origem_da_bronze('bronze_planilha_editais_pnab_ciclo_1') if execute else ref('bronze_planilha_editais_pnab_ciclo_1') %}
with fontes as (
    select
        id_anexo                                                                            as anexo_id,
        {{ coalesce_por_nome(editais, ['número do edital', 'número e título do edital']) }}  as num_edital
    from {{ ref('bronze_planilha_editais_pnab_ciclo_1') }}
    where tabela_origem = 'raw_pnab_acoes_gerais'
    union all
    select
        id_anexo,
        {{ coalesce_por_nome(editais, ['número e título do edital', 'número do edital']) }}
    from {{ ref('bronze_planilha_editais_pnab_ciclo_1') }}
    where tabela_origem = 'raw_pnab_acoes_cultura_viva'
),
anos as (
    select anexo_id, {{ ano_edital('num_edital') }} as ano
    from fontes
)
select
    anexo_id,
    min(ano) as ano_edital
from anos
where ano is not null
group by anexo_id
having count(distinct ano) = 1
