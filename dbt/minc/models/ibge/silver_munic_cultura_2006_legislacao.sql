-- Silver ibge — munic_cultura_2006_legislacao.
-- Origem: bronze_munic_cultura_2006_legislacao, cópia fiel de
-- seeds.ibge_munic_cultura_2006_legislacao, onde tudo chega
-- como text da carga de fonte estática (ibge_perfil_dag, ADR 0012).
-- Tipar é o trabalho desta camada.
-- 26 colunas: 1 tipadas, 25 mantidas como texto.
-- O cast vem do padrão medido no dado, não do nome da coluna: exige 100%
-- dos valores preenchidos casando. Resposta do questionário continua texto,
-- como o IBGE publica ('Sim', 'Não', '-', 'Recusa').
select
    {{ bronze_inteiro("a1", "bigint") }} as a1,
    {{ bronze_texto("a107") }} as a107,
    {{ bronze_texto("a108") }} as a108,
    {{ bronze_texto("a109") }} as a109,
    {{ bronze_texto("a110") }} as a110,
    {{ bronze_texto("a111") }} as a111,
    {{ bronze_texto("a112") }} as a112,
    {{ bronze_texto("a113") }} as a113,
    {{ bronze_texto("a114") }} as a114,
    {{ bronze_texto("a115") }} as a115,
    {{ bronze_texto("a116") }} as a116,
    {{ bronze_texto("a117") }} as a117,
    {{ bronze_texto("a118") }} as a118,
    {{ bronze_texto("a119") }} as a119,
    {{ bronze_texto("a120") }} as a120,
    {{ bronze_texto("a121") }} as a121,
    {{ bronze_texto("a122") }} as a122,
    {{ bronze_texto("a123") }} as a123,
    {{ bronze_texto("a124") }} as a124,
    {{ bronze_texto("a125") }} as a125,
    {{ bronze_texto("a126") }} as a126,
    {{ bronze_texto("a127") }} as a127,
    {{ bronze_texto("a128") }} as a128,
    {{ bronze_texto("a129") }} as a129,
    {{ bronze_texto("edicao") }} as edicao,
    {{ bronze_timestamptz("dt_ingest") }} as dt_ingest
from {{ ref("bronze_munic_cultura_2006_legislacao") }}
