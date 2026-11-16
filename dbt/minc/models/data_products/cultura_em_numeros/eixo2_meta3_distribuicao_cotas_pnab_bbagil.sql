-- Gold — Meta 3: cotas da PNAB por ano do pagamento, sobre o valor PAGO pelo BB Ágil.
-- Os mesmos 3 grupos e metas de eixo2_meta3_distribuicao_cotas_pnab (negra 25%,
-- indígena 10%, PCD 5%; sem a cota territorial, que só a LPG tem) e a mesma macro;
-- muda só a base, eixo2_meta3_pagamento_bbagil_perfil. Aqui o denominador é o
-- valor pago a pessoas, não o repasse recebido pelos entes que pesa na medida pelas
-- listas. Complementa aquela medida, não a substitui. Cobertura de perfil baixa:
-- ler valor_total_com_perfil_ano antes do veredito.
{{
    distribuicao_cotas(
        "PNAB",
        incluir_territorio=false,
        base="eixo2_meta3_pagamento_bbagil_perfil",
        coluna_programa="programa_fomento",
        coluna_ano="ano_evento",
        coluna_valor="valor",
        coluna_documento="beneficiario_documento",
    )
}}
