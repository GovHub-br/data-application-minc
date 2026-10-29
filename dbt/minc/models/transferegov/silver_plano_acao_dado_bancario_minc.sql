-- Silver transferegov — plano_acao_dado_bancario_minc.
-- Origem: bronze_plano_acao_dado_bancario_minc, cópia fiel de transferegov.plano_acao_dado_bancario_minc, onde tudo chega
-- como text da ingestão via API.
-- Tipar é o trabalho desta camada.
-- 17 colunas: 6 tipadas, 11 mantidas como texto.
-- O cast vem do padrão medido no dado (scripts/perfilar_padroes.py),
-- não do nome da coluna: exige 100% dos valores preenchidos casando.
select
    {{ bronze_inteiro("cod_ibge", "bigint") }} as cod_ibge,
    {{ bronze_timestamp("dt_ingest") }} as dt_ingest,
    {{ bronze_inteiro("id_programa", "bigint") }} as id_programa,
    {{ bronze_inteiro("id_plano_acao", "bigint") }} as id_plano_acao,
    {{ bronze_texto("id_agencia_conta") }} as id_agencia_conta,
    {{ bronze_inteiro("id_plano_acao_dado_bancario", "bigint") }} as id_plano_acao_dado_bancario,
    {{ bronze_texto("dv_conta_plano_acao_dado_bancario") }} as dv_conta_plano_acao_dado_bancario,
    {{ bronze_texto("dv_agencia_plano_acao_dado_bancario") }} as dv_agencia_plano_acao_dado_bancario,
    {{ bronze_texto("nome_banco_plano_acao_dado_bancario") }} as nome_banco_plano_acao_dado_bancario,
    {{ bronze_texto("codigo_banco_plano_acao_dado_bancario") }} as codigo_banco_plano_acao_dado_bancario,
    {{ bronze_texto("numero_conta_plano_acao_dado_bancario") }} as numero_conta_plano_acao_dado_bancario,
    {{ bronze_texto("numero_agencia_plano_acao_dado_bancario") }} as numero_agencia_plano_acao_dado_bancario,
    {{ bronze_texto("situacao_conta_plano_acao_dado_bancario") }} as situacao_conta_plano_acao_dado_bancario,
    {{ bronze_data("data_abertura_conta_plano_acao_dado_bancario") }} as data_abertura_conta_plano_acao_dado_bancario,
    {{ bronze_texto("nome_programa_agil_conta_plano_acao_dado_bancario") }} as nome_programa_agil_conta_plano_acao_dado_bancario,
    {{ bronze_texto("saldo_final_dado_bancario__saldo_final_gestao_financeira") }} as saldo_final_dado_bancario__saldo_final_gestao_financeira,
    {{ bronze_texto("saldo_final_dado_bancario") }} as saldo_final_dado_bancario
from {{ ref("bronze_plano_acao_dado_bancario_minc") }}
