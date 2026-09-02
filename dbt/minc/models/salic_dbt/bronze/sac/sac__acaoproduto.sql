-- Bronze SALIC — sac__acaoproduto.
-- Origem: salic_bronze.sac__acaoproduto, onde tudo chega como texto da
-- ingestão via Trino (ADR 0005). Tipar é o trabalho desta camada.
-- 11 colunas: 7 tipadas, 3 mantidas como texto.
-- Os casts são guardados por regex (macros bronze_*): valor fora do
-- padrão vira NULL em vez de derrubar o modelo.
select
    {{ bronze_inteiro("entidade") }} as entidade,
    {{ bronze_texto("anoprojeto") }} as anoprojeto,
    {{ bronze_texto("sequencial") }} as sequencial,
    {{ bronze_inteiro("codigoproduto") }} as codigoproduto,
    {{ bronze_booleano("opcao") }} as opcao,
    {{ bronze_texto("nomeproduto") }} as nomeproduto,
    {{ bronze_inteiro("qtdeproduzida") }} as qtdeproduzida,
    {{ bronze_inteiro("qtderecebida") }} as qtderecebida,
    {{ bronze_inteiro("qtdeexistente") }} as qtdeexistente,
    {{ bronze_inteiro("localizacao") }} as localizacao,
    _fatia
from {{ source("bronze_sac", "sac__acaoproduto") }}
