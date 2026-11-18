-- Silver ibge — munic_cultura_2006_recursos_humanos.
-- Origem: bronze_munic_cultura_2006_recursos_humanos, cópia fiel de
-- seeds.ibge_munic_cultura_2006_recursos_humanos, onde tudo chega
-- como text da carga de fonte estática (ibge_perfil_dag, ADR 0012).
-- Tipar é o trabalho desta camada.
-- 47 colunas: 1 tipadas, 46 mantidas como texto.
-- O cast vem do padrão medido no dado, não do nome da coluna: exige 100%
-- dos valores preenchidos casando. Resposta do questionário continua texto,
-- como o IBGE publica ('Sim', 'Não', '-', 'Recusa').
select
    {{ bronze_inteiro("a1", "bigint") }} as a1,
    {{ bronze_texto("a12") }} as a12,
    {{ bronze_texto("a13") }} as a13,
    {{ bronze_texto("a14") }} as a14,
    {{ bronze_texto("a15") }} as a15,
    {{ bronze_texto("a16") }} as a16,
    {{ bronze_texto("a17") }} as a17,
    {{ bronze_texto("a18") }} as a18,
    {{ bronze_texto("a19") }} as a19,
    {{ bronze_texto("a20") }} as a20,
    {{ bronze_texto("a21") }} as a21,
    {{ bronze_texto("a22") }} as a22,
    {{ bronze_texto("a23") }} as a23,
    {{ bronze_texto("a24") }} as a24,
    {{ bronze_texto("a25") }} as a25,
    {{ bronze_texto("a26") }} as a26,
    {{ bronze_texto("a27") }} as a27,
    {{ bronze_texto("a28") }} as a28,
    {{ bronze_texto("a29") }} as a29,
    {{ bronze_texto("a30") }} as a30,
    {{ bronze_texto("a31") }} as a31,
    {{ bronze_texto("a32") }} as a32,
    {{ bronze_texto("a33") }} as a33,
    {{ bronze_texto("a34") }} as a34,
    {{ bronze_texto("a35") }} as a35,
    {{ bronze_texto("a36") }} as a36,
    {{ bronze_texto("a37") }} as a37,
    {{ bronze_texto("a38") }} as a38,
    {{ bronze_texto("a39") }} as a39,
    {{ bronze_texto("a40") }} as a40,
    {{ bronze_texto("a41") }} as a41,
    {{ bronze_texto("a42") }} as a42,
    {{ bronze_texto("a43") }} as a43,
    {{ bronze_texto("a44") }} as a44,
    {{ bronze_texto("a45") }} as a45,
    {{ bronze_texto("a46") }} as a46,
    {{ bronze_texto("a47") }} as a47,
    {{ bronze_texto("a48") }} as a48,
    {{ bronze_texto("a49") }} as a49,
    {{ bronze_texto("a50") }} as a50,
    {{ bronze_texto("a51") }} as a51,
    {{ bronze_texto("a52") }} as a52,
    {{ bronze_texto("a53") }} as a53,
    {{ bronze_texto("a54") }} as a54,
    {{ bronze_texto("a55") }} as a55,
    {{ bronze_texto("edicao") }} as edicao,
    {{ bronze_timestamptz("dt_ingest") }} as dt_ingest
from {{ ref("bronze_munic_cultura_2006_recursos_humanos") }}
