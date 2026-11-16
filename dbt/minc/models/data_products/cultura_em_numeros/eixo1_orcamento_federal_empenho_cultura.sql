-- Gold-base do Eixo 1, frente orçamento, esfera federal: as linhas da execução
-- da LOA (SIOP) que o gasto_federal.R conta como gasto direto da União em
-- cultura, cada uma com a origem do recurso. Reproduz o filtro_fases_cultura e
-- o categoria_origem do R (repositório SGE cultura-em-numeros-eixo-1); as golds
-- federais leem daqui.
--
-- As três fases do R, porque o Ministério da Cultura (órgão 42000) não existiu
-- em todos os exercícios:
-- 1. 2019: o órgão 42000, ou a função 13 (Cultura) do órgão 55000 (Cidadania),
-- ou a função 28 dele fora da UO 55901 (Fundo Nacional de Assistência Social);
-- 2. exercício com o órgão 42000: só ele;
-- 3. exercício sem o órgão 42000: a função 13, ou a 28 fora da UO 55901, dos
-- órgãos 54000 (Turismo) e 55000 (Cidadania).
-- Em todas as fases entram as UOs 73120 (PNAB), 74912 (Fundo Setorial do
-- Audiovisual) e 73117 (LAB até 2021, LPG a partir de 2022), de qualquer órgão.
--
-- O R tira o exercício de 2026, ainda em execução quando a planilha foi baixada;
-- aqui o último exercício é a var eixo1_orcamento_federal_ultimo_ano (2025).
with

    execucao as (
        select *
        from {{ ref("silver_execucao_orcamentaria") }}
        where ano <= {{ var("eixo1_orcamento_federal_ultimo_ano", 2025) }}
    ),

    anos_com_minc as (select distinct ano from execucao where orgao_cod = '42000'),

    marcada as (
        select e.*, m.ano is not null as ano_com_minc
        from execucao as e
        left join anos_com_minc as m on e.ano = m.ano
    )

select
    ano,
    orgao_cod,
    orgao_nome,
    uo_cod,
    uo_nome,
    funcao_cod,
    funcao_nome,
    rp_cod,
    rp_nome,
    empenhado,
    liquidado,
    pago,
    case
        when uo_cod = '73120'
        then 'Política Nacional Aldir Blanc (PNAB)'
        when uo_cod = '74912'
        then 'Fundo Setorial do Audiovisual'
        when uo_cod = '73117' and ano >= 2022
        then 'Lei Paulo Gustavo (LPG)'
        when uo_cod = '73117' and ano < 2022
        then 'Lei Aldir Blanc (LAB)'
        when orgao_cod = '42000'
        then 'Execução direta da União (MinC)'
        else 'Execução direta da União (Cidadania/Turismo)'
    end as categoria_origem
from marcada
where
    (
        ano = 2019
        and (
            orgao_cod = '42000'
            or (
                orgao_cod = '55000'
                and (funcao_cod = '13' or (funcao_cod = '28' and uo_cod <> '55901'))
            )
            or uo_cod in ('73120', '74912', '73117')
        )
    )
    or (
        ano <> 2019
        and ano_com_minc
        and (orgao_cod = '42000' or uo_cod in ('73120', '74912', '73117'))
    )
    or (
        ano <> 2019
        and not ano_com_minc
        and (
            (
                orgao_cod in ('54000', '55000')
                and (funcao_cod = '13' or (funcao_cod = '28' and uo_cod <> '55901'))
            )
            or uo_cod in ('73120', '74912', '73117')
        )
    )
