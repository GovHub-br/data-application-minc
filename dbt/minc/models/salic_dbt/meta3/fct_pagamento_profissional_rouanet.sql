-- Silver SALIC / Meta 3 -- pagamentos a fornecedores e profissionais.
--
-- GRAO PROVISORIO: 1 linha por registro devolvido pela view detalhada de
-- pagamentos. A chave candidata combina projeto, item, comprovante, prestador,
-- data e valor. `ocorrencia_chave_candidata` torna duplicidades visiveis sem
-- descartar nenhum registro antes da auditoria live do grao.
--
-- MEDIDA: `valor_pago_reais` e pagamento, nao captacao, aprovacao ou valor
-- comprovado. O ano de referencia e o da data do pagamento, seguindo o recorte
-- de execucao usado pelo estudo FGV para 2024. Estornos e valores nao positivos
-- permanecem: a silver preserva o fato; inclusoes e exclusoes pertencem ao gold.
--
-- IDENTIDADE: documento e nome do prestador sao necessarios para uma futura
-- ligacao ao perfil, por isso o modelo inteiro e restrito e proibido para RAG.
-- Texto livre de justificativa foi excluido para nao ampliar a superficie de
-- PII. O numero do documento bancario continua classificado como sensivel.
--
-- PROJETO: `idpronac` e a fonte preferida para recuperar o PRONAC no mapa
-- conformado. O PRONAC numerico da view e apenas fallback, pois ja perdeu zeros
-- a esquerda. Inversoes ambiguas do mapa nao sao escolhidas silenciosamente.
with

    chave_por_id_pronac as (
        select id_pronac, min(pronac) as pronac
        from {{ ref("map_chave_projeto_rouanet") }}
        where id_pronac is not null
        group by id_pronac
        having count(*) = 1
    ),

    pagamento_bruto as (
        select
            p.idpronac as id_pronac,
            coalesce(m.pronac, {{ pronac_normalizado("p.pronac") }}) as pronac,
            m.pronac is not null as pronac_resolvido_por_mapa,
            {{ normaliza_documento("p.cnpjcpf") }} as documento_prestador,
            {{ tipo_pessoa_documento("p.cnpjcpf") }} as tipo_pessoa_prestador,
            p.fornecedor as nome_prestador,
            p.idplanilhaitem as id_item_planilha,
            p.item as nome_item,
            p.nrcomprovante as numero_comprovante,
            p.dtemissao::date as data_emissao,
            p.tpdocumento as tipo_documento_comprovante,
            p.dtpagamento::date as data_pagamento,
            extract(year from p.dtpagamento)::integer as ano_pagamento,
            p.tpformadepagamento as tipo_forma_pagamento,
            p.nrdocumentodepagamento as numero_documento_pagamento,
            p.vlpago as valor_pago_reais
        from {{ ref("sac__vwpagamentodefornecedordoprojetoporitemdetalhado") }} as p
        left join chave_por_id_pronac as m on p.idpronac = m.id_pronac
    ),

    com_chave as (
        select
            *,
            md5(
                jsonb_build_array(
                    id_pronac,
                    id_item_planilha,
                    numero_comprovante,
                    documento_prestador,
                    data_pagamento,
                    valor_pago_reais
                )::text
            ) as chave_candidata_pagamento,
            row_number() over (
                partition by
                    id_pronac,
                    id_item_planilha,
                    numero_comprovante,
                    documento_prestador,
                    data_pagamento,
                    valor_pago_reais
                order by
                    data_emissao asc nulls last,
                    numero_documento_pagamento asc nulls last,
                    nome_prestador asc nulls last
            ) as ocorrencia_chave_candidata
        from pagamento_bruto
    )

select
    chave_candidata_pagamento,
    ocorrencia_chave_candidata,
    id_pronac,
    pronac,
    pronac_resolvido_por_mapa,
    documento_prestador,
    tipo_pessoa_prestador,
    nome_prestador,
    id_item_planilha,
    nome_item,
    numero_comprovante,
    data_emissao,
    tipo_documento_comprovante,
    data_pagamento,
    ano_pagamento,
    tipo_forma_pagamento,
    numero_documento_pagamento,
    valor_pago_reais
from com_chave
