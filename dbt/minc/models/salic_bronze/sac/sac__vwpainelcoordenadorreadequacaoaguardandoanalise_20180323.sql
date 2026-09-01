-- Bronze SALIC — sac__vwpainelcoordenadorreadequacaoaguardandoanalise_20180323.
-- Origem: salic_bronze.sac__vwpainelcoordenadorreadequacaoaguardandoanalise_20180323,
-- onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 9 colunas: 6 tipadas, 2 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_inteiro("idpronac") }} as idpronac,
    {{ bronze_inteiro("idreadequacao") }} as idreadequacao,
    {{ bronze_inteiro("pronac") }} as pronac,
    {{ bronze_texto("nomeprojeto") }} as nomeprojeto,
    {{ bronze_inteiro("idorgao") }} as idorgao,
    {{ bronze_timestamp("dtsolicitacao") }} as dtsolicitacao,
    {{ bronze_texto("tpreadequacao") }} as tpreadequacao,
    {{ bronze_inteiro("qtaguardandodistribuicao") }} as qtaguardandodistribuicao,
    _fatia
from
    {{
        source(
            "bronze_sac",
            "sac__vwpainelcoordenadorreadequacaoaguardandoanalise_20180323",
        )
    }}
