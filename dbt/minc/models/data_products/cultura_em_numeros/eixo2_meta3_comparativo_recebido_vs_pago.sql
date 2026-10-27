{{ config(enabled=false) }}
-- Gold — Meta 3, diagnóstico 4. DESABILITADO junto com bronze_fato_bbagil (depende de
-- eixo2_meta3_pagamento_bbagil). enabled=true depois.
--
-- Validação do Diagnóstico 4: confronta os dois lados-valor.
-- • eixo2_meta3_pagamento_elegivel (listas de contemplados) = valor RECEBIDO pelos
-- entes (esperado ~R$2,7bi ≈ painel oficial "recebido").
-- • eixo2_meta3_pagamento_bbagil (extrato bancário)         = valor PAGO ao beneficiário
-- final (esperado ~R$447mi ≈ painel oficial "gasto").
-- Se o bbágil bater com o "gasto" do painel, é o denominador correto p/ cotas.
--
-- NOTA: enquanto a extração bbágil estiver parcial (amostra de 1 programa/ano),
-- o total do bbágil será MENOR que o real — a comparação de proporção só fecha
-- com a extração completa. Este modelo serve p/ acompanhar a convergência.
with
    recebido as (
        select
            'recebido (listas/contemplados)' as fonte,
            count(*) as linhas,
            count(distinct identificador_unico) as docs_distintos,
            round(sum(valor_pago_num)::numeric, 2) as valor_total
        from {{ ref("eixo2_meta3_pagamento_elegivel") }}
        where nome_programa = 'PNAB'
    ),
    pago as (
        select
            'pago (bbágil/extrato bancário)' as fonte,
            count(*) as linhas,
            count(distinct identificador_unico) as docs_distintos,
            round(sum(valor_pago_num)::numeric, 2) as valor_total
        from {{ ref("eixo2_meta3_pagamento_bbagil") }}
    )
select *
from recebido
union all
select *
from pago
