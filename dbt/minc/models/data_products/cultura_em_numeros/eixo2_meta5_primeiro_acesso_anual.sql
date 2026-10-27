{{ config(materialized="table") }}

-- Gold — a série que responde às perguntas 1, 2.1 e 2.2 da Meta 5:
--
-- 1.   percentual ANUAL de agentes de primeiro acesso, considerando
-- LAB, LPG, PNAB e Lei Rouanet  -> filtre programa_fomento = 'TODOS'
-- 2.1  novos entrantes da Rouanet ao longo dos anos
-- -> filtre programa_fomento = 'ROUANET'
-- 2.2  novos entrantes do fomento direto
-- -> filtre programa_fomento IN ('LAB','LPG','PNAB')
--
-- Grão: 1 linha por (ano × mecanismo), mais a linha de roll-up
-- programa_fomento = 'TODOS'.
--
-- COMO LER CADA COLUNA
-- agentes_atendidos  quantos agentes distintos receberam recurso daquele
-- mecanismo naquele ano (denominador)
-- novos_entrantes    destes, quantos tiveram ali o primeiro acesso a
-- QUALQUER fomento federal observável (numerador)
-- percentual_primeiro_acesso  = novos / atendidos
--
-- No roll-up 'TODOS' um agente atendido por dois mecanismos no mesmo ano
-- conta UMA vez (COUNT DISTINCT sobre o documento), então a linha 'TODOS'
-- não é a soma das linhas por mecanismo — é a população real do ano.
--
-- ANO CENSURADO: marca o ano em que a JANELA DE EXTRACAO cortou passado que
-- existia na origem — nao o primeiro ano de um programa novo. Depois da
-- decisao de 20/08/2026 de deixar a Aldir Blanc emergencial fora do escopo,
-- sobrou pouco: a Rouanet em 1993 (a lei e de 1991) e o primeiro ano de cada
-- trilha da Ancine, cujo piso em 2005-01-03 tem cara de extrato cortado.
-- NENHUMA linha de 2023 e censurada: LPG e PNAB nasceram naquele ano, nenhuma
-- conta deles e anterior, e a comparacao contra a Rouanet (1993+) rodou de
-- verdade.
--
-- O NÚMERO É TETO, NÃO ESTIMATIVA PONTUAL: falta FSA (planilha Ancine, fora
-- do banco) e falta a LAB I emergencial de 2020-2021 (não existe em nenhuma
-- fonte). Toda fonte ausente só pode transformar veterano em falso novo
-- entrante, nunca o contrário, então o percentual real é <= o publicado.
with
    atendidos_ano_mecanismo as (
        select distinct ano_evento as ano, programa_fomento, beneficiario_documento
        from {{ ref("eixo2_meta5_evento_fomento") }}
    ),

    -- porta de entrada: uma linha por agente, o mecanismo e o ano em que ele
    -- acessou o fomento federal pela primeira vez
    entradas as (
        select beneficiario_documento, programa_fomento, ano_primeiro_acesso_geral as ano
        from {{ ref("eixo2_meta5_primeiro_acesso_agente") }}
        where eh_porta_de_entrada
    ),

    por_mecanismo as (
        select
            a.ano,
            a.programa_fomento,
            count(distinct a.beneficiario_documento) as agentes_atendidos,
            count(distinct e.beneficiario_documento) as novos_entrantes
        from atendidos_ano_mecanismo a
        left join
            entradas e
            on a.beneficiario_documento = e.beneficiario_documento
            and a.programa_fomento = e.programa_fomento
            and a.ano = e.ano
        group by a.ano, a.programa_fomento
    ),

    -- roll-up: o agente conta uma vez no ano, independente de quantos
    -- mecanismos o atenderam
    total_geral as (
        select
            a.ano,
            'TODOS' as programa_fomento,
            count(distinct a.beneficiario_documento) as agentes_atendidos,
            count(distinct e.beneficiario_documento) as novos_entrantes
        from atendidos_ano_mecanismo a
        left join
            entradas e
            on a.beneficiario_documento = e.beneficiario_documento
            and a.ano = e.ano
        group by a.ano
    ),

    consolidado as (
        select *
        from por_mecanismo
        union all
        select *
        from total_geral
    ),

    -- último ano com dado: serve para marcar o ano corrente como parcial. Um ano
    -- que ainda não terminou tem menos gente nova acumulada e por isso puxa o
    -- percentual para baixo — a queda não é achado, é calendário.
    limite as (
        select max(ano_evento) as ano_max from {{ ref("eixo2_meta5_evento_fomento") }}
    )

select
    c.ano,
    c.programa_fomento,
    c.agentes_atendidos,
    c.novos_entrantes,
    c.agentes_atendidos - c.novos_entrantes as ja_contemplados_antes,
    round(
        c.novos_entrantes::numeric / nullif(c.agentes_atendidos, 0) * 100, 2
    ) as percentual_primeiro_acesso,
    case
        -- ROUANET: 1993 é o primeiro ano da fonte, mas a lei é de 1991 — os
        -- dois primeiros anos podem existir e não estar aqui.
        when c.programa_fomento = 'ROUANET'
        then c.ano <= 1993
        when c.programa_fomento = 'TODOS'
        then c.ano <= 1993
        -- A LAB NAO e censurada. As contas dos programas 7/8/9/15 movimentam
        -- desde 2020 e a extracao so pede meses a partir de 2023-01, mas o que
        -- ficou de fora e a Aldir Blanc EMERGENCIAL — decidido em 20/08/2026, em
        -- reuniao, que a transferencia emergencial da pandemia nao conta como
        -- fomento para esta meta. Nao sendo fomento, sua ausencia nao e lacuna:
        -- nao ha passado a revelar, e nada aqui e artefato de janela.
        -- Ancine: a tabela tem piso duro em 2005-01-03 para TODOS os
        -- mecanismos ao mesmo tempo, o que tem cara de extrato cortado e não
        -- de início de política — a Lei do Audiovisual é de 1993. Marcamos o
        -- primeiro ano de cada um por precaução, na mesma regra de sempre:
        -- censurado é onde a janela pode ter cortado passado que existe.
        when c.programa_fomento = 'AUDIOVISUAL'
        then c.ano <= 2005
        when c.programa_fomento = 'FSA'
        then c.ano <= 2011
        -- LPG e PNAB NÃO são censurados em 2023. Verificado: nenhuma das
        -- 22.371 contas desses programas foi aberta antes de 2023, então a
        -- janela não cortou nada deles — 2023 é o ano em que nasceram. E a
        -- comparação cross-mecanismo de fato rodou contra a Rouanet, que cobre
        -- 1993 em diante: dos 32.002 agentes da LPG em 2023, 254 foram pegos
        -- como veteranos por ela. O que 2023 tem de particular não é ausência
        -- de comparação, é baixa potência dela — em 2024, 68% das correções da
        -- LPG vêm da coorte de 2023 do próprio programa, corretor que ainda
        -- não existia. Isso é ressalva de leitura, não artefato de janela.
        else false
    end as ano_censurado,
    (c.ano = l.ano_max) as ano_parcial
from consolidado c
cross join limite l
order by c.programa_fomento, c.ano
