-- Silver ibge_sidra — resultados: os valores das tabelas do SIDRA.
-- Origem: bronze_resultados, cópia fiel de ibge_sidra.resultados, onde tudo
-- chega como text da ingestão via API. Tipar é o trabalho desta camada.
--
-- A tabela já tem chave (agregado, variável, nível, localidade, período e
-- classificação): a DAG regrava a mesma linha em vez de duplicar. Não há o que
-- deduplicar aqui.
--
-- valor: o SIDRA usa símbolos no lugar do número. '-' é zero absoluto e vira
-- 0; '..' e '...' (não se aplica, não disponível) e 'X' (sigilo) viram nulo:
-- o número que falta não é inventado (ADR 0010, regra 1). Quem precisa do
-- símbolo lê valor_original.
-- periodo fica texto: é rótulo ('2022', '202203'), não quantidade.
-- categorias: as categorias da linha só pelos IDs, na sintaxe do SIDRA
-- ('86[2776]' = cor ou raça branca; '1714[60024]|2661[32776]' com duas
-- classificações). É a classificacoes_chave da DAG, com '' virando nulo no
-- agregado sem classificação, e é por ela que se filtra um indicador.
-- classificacoes traz as mesmas categorias com os nomes, em JSON. Fica text:
-- linha gravada antes de 11/2026 tem o str() do Python, que não é JSON, e um
-- cast quebraria a silver.
select
    {{ bronze_inteiro("agregado_id") }} as agregado_id,
    {{ bronze_inteiro("variavel_id") }} as variavel_id,
    {{ bronze_texto("variavel_nome") }} as variavel_nome,
    {{ bronze_texto("unidade") }} as unidade,
    {{ bronze_texto("nivel") }} as nivel,
    {{ bronze_inteiro("localidade_id", "bigint") }} as localidade_id,
    {{ bronze_texto("localidade_nome") }} as localidade_nome,
    {{ bronze_texto("periodo") }} as periodo,
    case when trim(valor) = '-' then 0 else {{ bronze_numerico("valor") }} end as valor,
    {{ bronze_texto("valor") }} as valor_original,
    {{ bronze_texto("classificacoes_chave") }} as categorias,
    {{ bronze_texto("classificacoes") }} as classificacoes
from {{ ref("bronze_resultados") }}
