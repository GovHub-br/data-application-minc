-- Bronze transferegov — anexos_relatorios.
-- Origem: transferegov.anexos_relatorios, onde tudo chega como text da ingestão via API.
-- Tipar é o trabalho desta camada.
-- 23 colunas: 3 tipadas, 20 mantidas como texto.
-- O cast vem do padrão medido no dado (scripts/perfilar_padroes.py),
-- não do nome da coluna: exige 100% dos valores preenchidos casando.
select
    {{ bronze_texto("versao") }} as versao,
    {{ bronze_texto("auditlogin") }} as auditlogin,
    {{ bronze_inteiro("id") }} as id,
    {{ bronze_texto("nome") }} as nome,
    {{ bronze_texto("descricao") }} as descricao,
    {{ bronze_texto("arquivo") }} as arquivo,
    {{ bronze_texto("arquivocontenttype") }} as arquivocontenttype,
    {{ bronze_texto("tamanho") }} as tamanho,
    {{ bronze_texto("situacao") }} as situacao,
    {{ bronze_texto("tipoanexo") }} as tipoanexo,
    {{ bronze_texto("tipoanexoid") }} as tipoanexoid,
    {{ bronze_texto("inadministrativo") }} as inadministrativo,
    {{ bronze_inteiro("id_relatorio_gestao") }} as id_relatorio_gestao,
    {{ bronze_timestamp("dt_ingest") }} as dt_ingest,
    {{ bronze_texto("tipoanexo__versao") }} as tipoanexo__versao,
    {{ bronze_texto("tipoanexo__auditlogin") }} as tipoanexo__auditlogin,
    {{ bronze_texto("tipoanexo__statusedicao") }} as tipoanexo__statusedicao,
    {{ bronze_texto("tipoanexo__id") }} as tipoanexo__id,
    {{ bronze_texto("tipoanexo__nome") }} as tipoanexo__nome,
    {{ bronze_texto("tipoanexo__funcionalidade") }} as tipoanexo__funcionalidade,
    {{ bronze_texto("tipoanexo__indicadorobrigatoriedade") }} as tipoanexo__indicadorobrigatoriedade,
    {{ bronze_texto("tipoanexo__idprograma") }} as tipoanexo__idprograma,
    {{ bronze_texto("tipoanexo__utilizado") }} as tipoanexo__utilizado
from {{ source("transferegov", "anexos_relatorios") }}
