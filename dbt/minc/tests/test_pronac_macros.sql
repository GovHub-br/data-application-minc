-- Regressao da chave encontrada na auditoria live de 2026-09-03.
-- O SALIC usa ano com 2 posicoes + sequencial sem largura fixa. Preencher o
-- sequencial ate 5 transformava o PRONAC real 087079 em 0807079.
with
    casos as (
        select *
        from
            (
                values
                    ('087079', '08', '7079', '087079'),
                    ('2312345', '23', '12345', '2312345'),
                    ('99001', '99', '001', '99001')
            ) as t(pronac_origem, ano_projeto, sequencial, pronac_esperado)  -- noqa: LT01
    )

select *
from casos
where
    {{ pronac_normalizado("pronac_origem") }} is distinct from pronac_esperado
    or {{ pronac_de_ano_sequencial("ano_projeto", "sequencial") }}
    is distinct from pronac_esperado
