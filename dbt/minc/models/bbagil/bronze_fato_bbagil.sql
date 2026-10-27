{{ config(enabled=false) }}
-- Bronze bbagil — cópia fiel de bbagil.fato_bbagil, o agregado (ente, documento)
-- que a extracao_bbagil_dag gera. Sem cast, filtro nem join (ADR 0010).
-- DESABILITADO: a tabela ainda não existe no banco (a DAG falha na auth SCA).
-- Quando existir, enabled=true aqui, em silver_fato_bbagil, em
-- silver_plano_acao_ente_ano, em int_bbagil_transferegov e nas golds
-- eixo2_meta3_pagamento_bbagil e eixo2_meta3_comparativo_recebido_vs_pago.
select * from {{ source("bbagil", "fato_bbagil") }}
