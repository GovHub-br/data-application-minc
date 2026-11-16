-- Gold — Meta 3: cotas da LPG por ano do pagamento, sobre o valor PAGO pelo BB Ágil.
-- Os mesmos 4 grupos e metas de eixo2_meta3_distribuicao_cotas_lpg (negra 25%,
-- indígena 10%, PCD 5%, território vulnerabilizado 20%) e a mesma macro; muda só a
-- base, eixo2_meta3_pagamento_bbagil_perfil. Complementa a medida pelas listas de
-- contemplados, não a substitui. Cobertura de perfil baixa: ler
-- valor_total_com_perfil_ano antes do veredito.
{{
    distribuicao_cotas(
        "LPG",
        incluir_territorio=true,
        base="eixo2_meta3_pagamento_bbagil_perfil",
        coluna_programa="programa_fomento",
        coluna_ano="ano_evento",
        coluna_valor="valor",
        coluna_documento="beneficiario_documento",
    )
}}
