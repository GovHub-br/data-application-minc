-- Na gold-base do orçamento, a esfera confere com o código IBGE: municipal é
-- 7 dígitos (1000000 a 9999999) e estadual, 2 (10 a 99). Na MSC a esfera sai
-- do próprio código; no ramo da LAB 1 ela é a constante 'municipal', e só o DF
-- (53) é trocado, por Brasília (5300108). Um código de 2 dígitos que não seja o
-- 53, um fora da faixa ou um nulo entraria como município sem aviso. (O nulo
-- também cai no not_null de cod_ibge; fica aqui para o teste não depender dele.)
-- Devolve as linhas que não conferem.
select base_origem, esfera_eixo1, cod_ibge, exercicio, count(*) as linhas
from {{ ref("eixo1_orcamento_empenho_cultura") }}
where
    (
        esfera_eixo1 = 'municipal'
        and (cod_ibge is null or cod_ibge not between 1000000 and 9999999)
    )
    or (
        esfera_eixo1 = 'estadual' and (cod_ibge is null or cod_ibge not between 10 and 99)
    )
group by base_origem, esfera_eixo1, cod_ibge, exercicio
