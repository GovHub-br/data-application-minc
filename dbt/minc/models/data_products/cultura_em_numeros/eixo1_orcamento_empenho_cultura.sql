-- Gold-base — o empenho em cultura de estados e municípios, linha a linha da
-- MSC, com a origem do recurso e o valor em reais de 2024. É onde mora a regra
-- de negócio do orçamento do Eixo 1 (ADR 0015, decisões 3 e 4): as golds de
-- consumo eixo1_orcamento_* leem daqui e só agregam.
--
-- Lê int_bacen_siconfi, que só junta a MSC, o ente e o IPCA. Reproduz a regra
-- de gasto_estadual.R e gasto_municipal.R (repositório
-- cultura-em-numeros-eixo-1, commit d28de40).
--
-- O QUE É EMPENHO (as duas esferas)
-- • a MSC de dezembro, classe 6, saldo final, na matriz agregada (MSCC): o
-- saldo acumulado do ano. A MSCE, que a API também tem em 2025, fica fora;
-- • natureza do saldo credora (C) e conta contábil começando com 62213
-- (crédito empenhado: a liquidar, em liquidação, liquidado a pagar e pago);
-- • o valor em módulo, linha a linha, como o R (abs(valor)).
--
-- DE ONDE VEIO O RECURSO (origem), pela fonte sem pontos, nesta ordem:
-- • pnab: fonte começando com 1719, 2719, 1720, 2720, 719 ou 720;
-- • lpg: 1715, 2715, 1716, 2716, 715 ou 716;
-- • lab: só na esfera estadual e só em 2020 e 2021, fonte começando com 17
-- ou 27 (e não com 19 ou 29). Na municipal o R tira a LAB 1 do BB Ágil, e
-- a MSC não a separa;
-- • emenda: o complemento da fonte, só os dígitos e com zero à esquerda até
-- 4, está em 3101, 3110, 3111, 3120, 3121, 3130, 3140, 3201, 3202, 3210,
-- 3211, 3220 ou 3221 (emendas parlamentares);
-- • proprio: o resto (recurso do próprio ente).
--
-- QUEM CONTA COMO CULTURA: a função 13 (Cultura), e, na esfera estadual, a
-- linha da LAB em qualquer função (o R estadual não exige a função 13 da LAB).
--
-- ESFERA: estadual é o ente de código IBGE de 2 dígitos, os 26 estados e o
-- Distrito Federal (no SICONFI, esfera 'E' e 'D'); municipal é o de 7 dígitos.
-- O DF não entra na municipal.
--
-- VALOR REAL: valor nominal vezes o número-índice médio do IPCA do ano-base
-- (var eixo1_orcamento_ano_base_ipca, padrão 2024) dividido pelo do exercício.
-- O índice vem de int_bacen_siconfi; o do ano-base é lido das próprias linhas
-- daquele ano. Sem IPCA para o exercício, o valor real fica nulo.
--
-- GRÃO: uma linha por item da MSC que conta, (_pagina_id, _item).
{% set ano_base = var("eixo1_orcamento_ano_base_ipca", 2024) %}

with
    empenho as (
        select
            *,
            replace(fonte_recursos, '.', '') as fonte_sem_ponto,
            regexp_replace(complemento_fonte, '[^0-9]', '', 'g') as complemento_digitos,
            substring(funcao from '[0-9]+')::integer as funcao_numero,
            case
                when cod_ibge between 10 and 99
                then 'estadual'
                when cod_ibge between 1000000 and 9999999
                then 'municipal'
            end as esfera_eixo1
        from {{ ref("int_bacen_siconfi") }}
        where
            tipo_matriz = 'MSCC'
            and mes_referencia = 12
            and classe_conta = 6
            and tipo_valor = 'ending_balance'
            and natureza_conta = 'C'
            and replace(conta_contabil, '.', '') like '62213%'
    ),

    classificado as (
        select
            *,
            case
                when fonte_sem_ponto ~ '^(1719|2719|1720|2720|719|720)'
                then 'pnab'
                when fonte_sem_ponto ~ '^(1715|2715|1716|2716|715|716)'
                then 'lpg'
                when
                    esfera_eixo1 = 'estadual'
                    and exercicio in (2020, 2021)
                    and fonte_sem_ponto !~ '^(19|29)'
                    and fonte_sem_ponto ~ '^(17|27)'
                then 'lab'
                -- str_pad do R completa com zero à esquerda até 4 e não corta.
                when
                    case
                        when length(complemento_digitos) >= 4
                        then complemento_digitos
                        else lpad(complemento_digitos, 4, '0')
                    end in (
                        '3101',
                        '3110',
                        '3111',
                        '3120',
                        '3121',
                        '3130',
                        '3140',
                        '3201',
                        '3202',
                        '3210',
                        '3211',
                        '3220',
                        '3221'
                    )
                then 'emenda'
                else 'proprio'
            end as origem
        from empenho
        where
            esfera_eixo1 = 'estadual'
            or (esfera_eixo1 = 'municipal' and uf is distinct from 'DF')
    ),

    cultura as (
        select
            *,
            max(ipca_indice_medio_ano) filter (where exercicio = {{ ano_base }}) over (
            ) as ipca_indice_medio_base
        from classificado
        where funcao_numero = 13 or (esfera_eixo1 = 'estadual' and origem = 'lab')
    )

select
    esfera_eixo1,
    cod_ibge,
    ente,
    uf,
    regiao,
    exercicio,
    origem,
    funcao,
    subfuncao,
    poder_orgao,
    natureza_despesa,
    fonte_recursos,
    complemento_fonte,
    conta_contabil,
    abs(valor) as valor_nominal,
    ipca_indice_medio_base / ipca_indice_medio_ano as fator_ipca,
    abs(valor) * ipca_indice_medio_base / ipca_indice_medio_ano as valor_real,
    {{ ano_base }} as ano_base_ipca,
    ipca_meses_ano,
    _pagina_id,
    _item
from cultura
