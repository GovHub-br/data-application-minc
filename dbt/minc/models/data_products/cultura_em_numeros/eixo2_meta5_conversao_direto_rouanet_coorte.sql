{{ config(materialized="table") }}

-- Gold — a resposta da pergunta 2.3 da Meta 5 com o tempo de exposição
-- controlado: "há novos entrantes pelo fomento direto que depois acessaram a
-- Lei Rouanet?"
--
-- Grão: 1 linha por (coorte de entrada × porta de entrada × tipo de pessoa).
--
-- POR QUE ESTA TABELA EXISTE SEPARADA DA eixo2_meta5_trajetoria_*: a contagem bruta de
-- quem fez o percurso é baixa e seria lida como "quase ninguém migra". Só que
-- o fomento direto começou em 2023: quem entrou em 2026 teve meses para
-- captar Rouanet, quem entrou em 2023 teve três anos. Comparar as coortes sem
-- descontar essa diferença de exposição confunde "converte pouco" com
-- "entrou ontem". A coluna converteram_ate_12m nivela isso — é a mesma
-- janela de oportunidade para todas as coortes fechadas.
--
-- QUEM ENTRA NO DENOMINADOR: só agente cuja PORTA DE ENTRADA no fomento
-- federal foi LAB, LPG ou PNAB. Não basta ter passado pelo fomento direto
-- antes da Rouanet — quem já tinha aparecido na trilha da Ancine
-- (AUDIOVISUAL/FSA) antes de 2023 não é novo entrante pelo fomento direto, e
-- a pergunta 2.3 é sobre novo entrante. São 21 agentes nessa situação; eles
-- contam em DIRETO_DEPOIS_ROUANET na eixo2_meta5_trajetoria_* (que classifica percurso,
-- não novidade) e ficam de fora daqui.
--
-- LEITURA DO NUMERADOR: 'acessou a Rouanet' aqui é captação real de recurso,
-- a mesma definição de evento usada no resto da meta (ver
-- eixo2_meta5_evento_fomento) — não é projeto protocolado nem aprovado. Um
-- agente que submeteu projeto e não captou não conta como convertido.
with
    agente as (
        select
            beneficiario_documento,
            tipo_pessoa,
            max(programa_fomento) filter (where eh_porta_de_entrada) as porta_de_entrada,
            min(data_primeiro_acesso_geral) as data_entrada,
            min(data_primeiro_acesso_mecanismo) filter (
                where programa_fomento = 'ROUANET'
            ) as data_rouanet
        from {{ ref("eixo2_meta5_primeiro_acesso_agente") }}
        group by beneficiario_documento, tipo_pessoa
    ),

    novos_entrantes_direto as (
        select
            *,
            -- a entrada é a porta, então data_entrada É a entrada no direto:
            -- basta a Rouanet vir estritamente depois
            (data_rouanet is not null and data_rouanet > data_entrada) as converteu,
            data_rouanet - data_entrada as dias_ate_rouanet
        from agente
        where porta_de_entrada in ('LAB', 'LPG', 'PNAB')
    )

select
    extract(year from data_entrada)::int as coorte_entrada,
    porta_de_entrada,
    tipo_pessoa,
    count(*) as entrantes,
    count(*) filter (where converteu) as converteram_rouanet,
    -- janela fixa: comparável entre coortes. A coorte mais recente ainda não
    -- completou 12 meses, então o valor dela é piso, não taxa final.
    count(*) filter (where converteu and dias_ate_rouanet <= 365) as converteram_ate_12m,
    round(100.0 * count(*) filter (where converteu) / count(*), 3) as pct_converteram,
    round(
        100.0 * count(*) filter (where converteu and dias_ate_rouanet <= 365) / count(*),
        3
    ) as pct_converteram_ate_12m,
    percentile_disc(0.5) within group (order by dias_ate_rouanet) filter (
        where converteu
    ) as mediana_dias_ate_rouanet,
    -- a coorte só está madura quando já teve 12 meses inteiros de exposição;
    -- sem esta marca a última linha da série parece uma queda de conversão
    (max(data_entrada) + 365 <= current_date) as coorte_com_12m_completos
from novos_entrantes_direto
group by coorte_entrada, porta_de_entrada, tipo_pessoa
order by coorte_entrada, porta_de_entrada, tipo_pessoa
