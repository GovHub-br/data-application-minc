-- Bronze transferegov — plano_acao_meta_minc.
-- Origem: transferegov.plano_acao_meta_minc, onde tudo chega como text da ingestão via API.
-- Tipar é o trabalho desta camada.
-- 11 colunas: 8 tipadas, 3 mantidas como texto.
-- O cast vem do padrão medido no dado (scripts/perfilar_padroes.py),
-- não do nome da coluna: exige 100% dos valores preenchidos casando.
select
    {{ bronze_inteiro("cod_ibge") }} as cod_ibge,
    {{ bronze_timestamp("dt_ingest") }} as dt_ingest,
    {{ bronze_inteiro("id_programa") }} as id_programa,
    {{ bronze_inteiro("id_plano_acao") }} as id_plano_acao,
    {{ bronze_inteiro("id_meta_plano_acao") }} as id_meta_plano_acao,
    {{ bronze_texto("nome_meta_plano_acao") }} as nome_meta_plano_acao,
    {{ bronze_numerico("valor_meta_plano_acao") }} as valor_meta_plano_acao,
    {{ bronze_texto("numero_meta_plano_acao") }} as numero_meta_plano_acao,
    {{ bronze_inteiro("versao_meta_plano_acao") }} as versao_meta_plano_acao,
    {{ bronze_texto("descricao_meta_plano_acao") }} as descricao_meta_plano_acao,
    {{ bronze_inteiro("sequencial_meta_plano_acao") }} as sequencial_meta_plano_acao
from {{ source("transferegov", "plano_acao_meta_minc") }}
