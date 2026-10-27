{{ config(materialized="table") }}

-- Gold — responde à pergunta 2.3 da Meta 5: "há novos entrantes pelo fomento
-- direto que depois acessaram a Lei Rouanet?"
--
-- Grão: 1 linha por trajetória (categoria de percurso do agente entre o
-- fomento direto e a Rouanet), com a contagem de agentes em cada uma.
--
-- 'fomento direto' aqui é LAB/LPG/PNAB. A trilha da Ancine (FSA e
-- AUDIOVISUAL) entrou na espinha em 20/08/2026 e NÃO é fomento direto: quem
-- só aparece nela sai na categoria SO_AUDIOVISUAL, fora da pergunta 2.3.
--
-- POR QUE ESTA TABELA É O TESTE DO INDICADOR INTEIRO: a Rouanet enxerga
-- desde 1993, o fomento direto só desde 2023. Então o cruzamento mede duas
-- coisas de uma vez —
-- (a) a resposta da pergunta 2.3, na direção "entrou pelo direto, depois
-- captou Rouanet" (categoria DIRETO_DEPOIS_ROUANET); e
-- (b) quantos supostos "novos entrantes" do fomento direto a Rouanet
-- DESMENTE, na direção oposta (ROUANET_ANTES_DIRETO) — gente que já
-- acessava recurso público federal muito antes de 2023 e que, sem a
-- fonte da Rouanet, seria contada como agente novo.
-- (b) é a única medição direta que temos do tamanho do viés de fonte que
-- afeta todo o resto da meta.
--
-- TRAJETÓRIA ≠ NOVIDADE: DIRETO_DEPOIS_ROUANET diz que o agente tocou o
-- fomento direto antes da Rouanet, não que ele era novo no fomento federal.
-- Quem já aparecia na trilha da Ancine antes de 2023 e só depois pegou LPG e
-- Rouanet faz esse percurso sem ser novo entrante. Por isso a porta de
-- entrada é dimensão desta tabela e não um detalhe: a resposta estrita da
-- 2.3 são as linhas de DIRETO_DEPOIS_ROUANET com porta em LAB/LPG/PNAB. A
-- série com exposição controlada vive em
-- eixo2_meta5_conversao_direto_rouanet_coorte.
with
    agentes as (
        select
            beneficiario_documento,
            tipo_pessoa,
            min(data_primeiro_acesso_mecanismo) filter (
                where programa_fomento in ('LAB', 'LPG', 'PNAB')
            ) as entrada_direto,
            min(data_primeiro_acesso_mecanismo) filter (
                where programa_fomento = 'ROUANET'
            ) as entrada_rouanet,
            max(programa_fomento) filter (
                where eh_porta_de_entrada
            ) as mecanismo_porta_entrada
        from {{ ref("eixo2_meta5_primeiro_acesso_agente") }}
        group by beneficiario_documento, tipo_pessoa
    ),

    classificado as (
        select
            beneficiario_documento,
            tipo_pessoa,
            entrada_direto,
            entrada_rouanet,
            mecanismo_porta_entrada,
            case
                -- agente que só aparece na trilha da Ancine (FSA / audiovisual):
                -- não tem fomento direto nem Rouanet, então não participa da
                -- pergunta 2.3. Precisa de balde próprio — sem isto ele cairia no
                -- ELSE e seria contado como 'MESMO_DIA', que é o que aconteceu na
                -- primeira execução depois que a Ancine entrou (7 -> 1.197).
                when entrada_direto is null and entrada_rouanet is null
                then 'SO_AUDIOVISUAL'
                when entrada_direto is not null and entrada_rouanet is null
                then 'SO_DIRETO'
                when entrada_direto is null and entrada_rouanet is not null
                then 'SO_ROUANET'
                -- resposta afirmativa à 2.3: entrou pelo fomento direto e só
                -- depois chegou à Rouanet
                when entrada_direto < entrada_rouanet
                then 'DIRETO_DEPOIS_ROUANET'
                -- o contrário: já era da Rouanet antes de aparecer no fomento
                -- direto. Sem SALIC, cada um destes seria um falso novo entrante.
                when entrada_rouanet < entrada_direto
                then 'ROUANET_ANTES_DIRETO'
                else 'MESMO_DIA'
            end as trajetoria
        from agentes
    )

select
    trajetoria,
    tipo_pessoa,
    mecanismo_porta_entrada,
    count(*) as agentes,
    min(entrada_direto) as primeira_entrada_direto,
    max(entrada_direto) as ultima_entrada_direto,
    min(entrada_rouanet) as primeira_entrada_rouanet,
    max(entrada_rouanet) as ultima_entrada_rouanet,
    -- só faz sentido para quem passou pelos dois lados
    round(
        avg(
            case
                when entrada_direto is not null and entrada_rouanet is not null
                then abs(entrada_rouanet - entrada_direto)
            end
        ),
        1
    ) as media_dias_entre_mecanismos
from classificado
group by trajetoria, tipo_pessoa, mecanismo_porta_entrada
order by trajetoria, tipo_pessoa, mecanismo_porta_entrada
