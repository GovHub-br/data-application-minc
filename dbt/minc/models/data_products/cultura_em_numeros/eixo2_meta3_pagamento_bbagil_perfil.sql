-- Gold — Meta 3, base da medida das cotas pelo valor pago: cada pagamento do BB Ágil
-- da LPG e da PNAB (eixo2_meta3_pagamento_bbagil) com o perfil de quem recebeu
-- (eixo2_meta3_perfil_agente) e o território do município dele
-- (silver_territorio_municipio). As golds eixo2_meta3_distribuicao_cotas_*_bbagil
-- leem daqui.
--
-- Grão: o de eixo2_meta3_pagamento_bbagil. Nenhum pagamento sai (LEFT JOIN): o sem
-- perfil fica, com tem_perfil false, para entrar no denominador.
--
-- QUEM CONTA EM CADA COTA
-- • negra (25%), indígena (10%) e PCD (5%): as flags de eixo2_meta3_perfil_agente.
-- Sem perfil, as três ficam NULL, não false (ADR 0010, regra 1): não ter
-- respondido à planilha de perfil é ausência de evidência.
-- • território vulnerabilizado (20%, só a LPG): a mesma regra de
-- eixo2_meta3_pagamento_elegivel. Verdadeiro quando o município declarado pelo
-- agente aparece no crosswalk de concentrações urbanas do IBGE; NULL quando ele
-- não declarou cidade. Casamento por nome + UF, que perde grafias divergentes.
--
-- COBERTURA: só uma fatia de quem recebeu respondeu à planilha de perfil (cerca de
-- 6% dos documentos da LPG e 4% dos da PNAB). As cotas valem para essa fatia; o
-- valor_total_com_perfil_ano das golds de distribuição mede o tamanho dela.
with
    pagamentos as (select * from {{ ref("eixo2_meta3_pagamento_bbagil") }}),

    perfil as (select * from {{ ref("eixo2_meta3_perfil_agente") }}),

    territorio as (select * from {{ ref("silver_territorio_municipio") }})

select
    p.beneficiario_documento,
    p.programa_fomento,
    p.data_evento,
    p.ano_evento,
    p.valor,
    p.tipo_pessoa,
    pf.raca_normalizada,
    pf.flag_negra,
    pf.flag_indigena,
    pf.is_pcd as flag_pcd,
    pf.cidade as agente_cidade,
    pf.uf as agente_uf,
    (pf.identificador_unico is not null) as tem_perfil,
    case
        when pf.cidade is null or btrim(pf.cidade) = ''
        then null
        else (terr.chave_municipio_uf is not null)
    end as flag_territorio_vulneravel
from pagamentos as p
left join perfil as pf on p.beneficiario_documento = pf.identificador_unico
left join territorio as terr on pf.chave_municipio_uf = terr.chave_municipio_uf
