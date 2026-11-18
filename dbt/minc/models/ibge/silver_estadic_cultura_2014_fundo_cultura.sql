-- Silver ibge — estadic_cultura_2014_fundo_cultura.
-- Origem: bronze_estadic_cultura_2014_fundo_cultura, cópia fiel de
-- seeds.ibge_estadic_cultura_2014_fundo_cultura, onde tudo chega
-- como text da carga de fonte estática (ibge_perfil_dag, ADR 0012).
-- Tipar é o trabalho desta camada.
-- 34 colunas: 1 tipadas, 33 mantidas como texto.
-- O cast vem do padrão medido no dado, não do nome da coluna: exige 100%
-- dos valores preenchidos casando. Resposta do questionário continua texto,
-- como o IBGE publica ('Sim', 'Não', '-', 'Recusa').
select
    {{ bronze_inteiro("codigouf", "bigint") }} as codigouf,
    {{ bronze_texto("a321") }} as a321,
    {{ bronze_texto("a322") }} as a322,
    {{ bronze_texto("a323") }} as a323,
    {{ bronze_texto("a324") }} as a324,
    {{ bronze_texto("a325") }} as a325,
    {{ bronze_texto("a326") }} as a326,
    {{ bronze_texto("a327") }} as a327,
    {{ bronze_texto("a328") }} as a328,
    {{ bronze_texto("a329") }} as a329,
    {{ bronze_texto("a330") }} as a330,
    {{ bronze_texto("a331") }} as a331,
    {{ bronze_texto("a332") }} as a332,
    {{ bronze_texto("a333") }} as a333,
    {{ bronze_texto("a334") }} as a334,
    {{ bronze_texto("a335") }} as a335,
    {{ bronze_texto("a336") }} as a336,
    {{ bronze_texto("a337") }} as a337,
    {{ bronze_texto("a338") }} as a338,
    {{ bronze_texto("a339") }} as a339,
    {{ bronze_texto("a340") }} as a340,
    {{ bronze_texto("a341") }} as a341,
    {{ bronze_texto("a342") }} as a342,
    {{ bronze_texto("a343") }} as a343,
    {{ bronze_texto("a344") }} as a344,
    {{ bronze_texto("a345") }} as a345,
    {{ bronze_texto("a346") }} as a346,
    {{ bronze_texto("a347") }} as a347,
    {{ bronze_texto("a348") }} as a348,
    {{ bronze_texto("a349") }} as a349,
    {{ bronze_texto("a350") }} as a350,
    {{ bronze_texto("a351") }} as a351,
    {{ bronze_texto("edicao") }} as edicao,
    {{ bronze_timestamptz("dt_ingest") }} as dt_ingest
from {{ ref("bronze_estadic_cultura_2014_fundo_cultura") }}
