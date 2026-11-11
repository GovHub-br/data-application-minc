-- Silver ibge — estadic_cultura_2014_orgao_gestor.
-- Origem: bronze_estadic_cultura_2014_orgao_gestor, cópia fiel de
-- seeds.ibge_estadic_cultura_2014_orgao_gestor, onde tudo chega
-- como text da carga de fonte estática (ibge_perfil_dag, ADR 0012).
-- Tipar é o trabalho desta camada.
-- 5 colunas: 1 tipadas, 4 mantidas como texto.
-- O cast vem do padrão medido no dado, não do nome da coluna: exige 100%
-- dos valores preenchidos casando. Resposta do questionário continua texto,
-- como o IBGE publica ('Sim', 'Não', '-', 'Recusa').
select
    {{ bronze_inteiro("a1", "bigint") }} as a1,
    {{ bronze_texto("a2") }} as a2,
    {{ bronze_texto("a3") }} as a3,
    {{ bronze_texto("edicao") }} as edicao,
    {{ bronze_timestamptz("dt_ingest") }} as dt_ingest
from {{ ref("bronze_estadic_cultura_2014_orgao_gestor") }}
