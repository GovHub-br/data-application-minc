-- Bronze transferegov — relatorios_gestao.
-- Origem: transferegov.relatorios_gestao, onde tudo chega como text da ingestão via API.
-- Tipar é o trabalho desta camada.
-- 14 colunas: 8 tipadas, 6 mantidas como texto.
-- O cast vem do padrão medido no dado (scripts/perfilar_padroes.py),
-- não do nome da coluna: exige 100% dos valores preenchidos casando.
select
    {{ bronze_timestamp("dt_ingest") }} as dt_ingest,
    {{ bronze_inteiro("id_plano_acao") }} as id_plano_acao,
    {{ bronze_inteiro("id_relatorio_gestao") }} as id_relatorio_gestao,
    {{ bronze_data("data_relatorio_gestao") }} as data_relatorio_gestao,
    {{ bronze_texto("tipo_relatorio_gestao") }} as tipo_relatorio_gestao,
    {{ bronze_texto("situacao_relatorio_gestao") }} as situacao_relatorio_gestao,
    {{ bronze_texto("descritivo_relatorio_gestao") }} as descritivo_relatorio_gestao,
    {{ bronze_timestamp("data_e_hora_relatorio_gestao") }} as data_e_hora_relatorio_gestao,
    {{ bronze_texto("contrapartida_relatorio_gestao") }} as contrapartida_relatorio_gestao,
    {{ bronze_numerico("valor_pendente_relatorio_gestao") }} as valor_pendente_relatorio_gestao,
    {{ bronze_numerico("valor_executado_relatorio_gestao") }} as valor_executado_relatorio_gestao,
    {{ bronze_booleano("declaracao_conformidade_relatorio_gestao") }} as declaracao_conformidade_relatorio_gestao,
    {{ bronze_texto("resultados_alcancados_metas_relatorio_gestao") }} as resultados_alcancados_metas_relatorio_gestao,
    {{ bronze_texto("endereco_eletronico_publicidade_acoes_relatorio_gestao") }} as endereco_eletronico_publicidade_acoes_relatorio_gestao
from {{ source("transferegov", "relatorios_gestao") }}
