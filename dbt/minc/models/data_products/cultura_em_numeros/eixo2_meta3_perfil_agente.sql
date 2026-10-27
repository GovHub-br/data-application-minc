-- Gold — Meta 3, gold-base: perfil unificado LPG + PNAB, 1 linha por documento, com as
-- flags de quem conta em cada cota (negra, indígena, PCD). A união e o dedup das cinco
-- silvers de perfil estão aqui junto com a regra; separar a limpeza numa silver é
-- dívida registrada no PR da migração.
-- Dedup por identificador (não por tipo) p/ join 1:1 com pagamentos.
-- cidade/uf só existem nos silvers LPG (têm CEP/cidade/UF); os silvers PNAB não trazem
-- localização do agente -> NULL. Propagados até aqui p/ a cota territorial (LPG).
with
    todos as (
        select
            identificador_unico,
            tipo_proponente,
            origem,
            raca_bruto,
            pcd_bruto,
            indigena_bruto,
            quilombola_bruto,
            cidade,
            uf
        from {{ ref("silver_agentes_pf") }}
        union all
        select
            identificador_unico,
            tipo_proponente,
            origem,
            raca_bruto,
            pcd_bruto,
            indigena_bruto,
            quilombola_bruto,
            cidade,
            uf
        from {{ ref("silver_agentes_pj") }}
        union all
        select
            identificador_unico,
            tipo_proponente,
            origem,
            raca_bruto,
            pcd_bruto,
            indigena_bruto,
            quilombola_bruto,
            cidade,
            uf
        from {{ ref("silver_agentes_coletivos") }}
        union all
        select
            identificador_unico,
            tipo_proponente,
            origem,
            raca_bruto,
            pcd_bruto,
            indigena_bruto,
            quilombola_bruto,
            cast(null as text) as cidade,
            cast(null as text) as uf
        from {{ ref("silver_agentes_pnab_pf") }}
        union all
        select
            identificador_unico,
            tipo_proponente,
            origem,
            raca_bruto,
            pcd_bruto,
            indigena_bruto,
            quilombola_bruto,
            cast(null as text) as cidade,
            cast(null as text) as uf
        from {{ ref("silver_agentes_pnab_pj") }}
    ),
    dedup as (
        select
            *,
            row_number() over (
                partition by identificador_unico
                order by
                    case
                        when
                            raca_bruto is not null
                            and {{ sem_acento("raca_bruto") }} not in ('', 'nan')
                        then 0
                        else 1
                    end,
                    origem
            ) as rn
        from todos
    ),
    norm as (
        select
            identificador_unico,
            tipo_proponente,
            origem,
            raca_bruto,
            pcd_bruto,
            indigena_bruto,
            cidade,
            uf,
            case
                when {{ sem_acento("raca_bruto") }} ~ 'pret|pard|negr'
                then 'negra'
                when {{ sem_acento("raca_bruto") }} ~ 'indigen'
                then 'indigena'
                when {{ sem_acento("raca_bruto") }} ~ 'branc'
                then 'branca'
                when {{ sem_acento("raca_bruto") }} ~ 'amarel'
                then 'amarela'
                else 'nao_declarada'
            end as raca_normalizada
        from dedup
        where rn = 1
    )
select
    identificador_unico,
    tipo_proponente,
    origem,
    raca_bruto,
    raca_normalizada,
    cidade,
    uf,
    -- chave p/ casar com silver_territorio_municipio (mesma normalização: sem
    -- acento+lower).
    -- NULL quando cidade/uf ausentes (PNAB) -> não casa território, fica sem classificar.
    case
        when cidade is not null and uf is not null
        then {{ sem_acento("cidade") }} || '|' || {{ sem_acento("uf") }}
    end as chave_municipio_uf,
    (raca_normalizada = 'negra') as flag_negra,
    (
        raca_normalizada = 'indigena'
        or {{ sem_acento("indigena_bruto") }} in ('sim', 's', '1', 'true')
    ) as flag_indigena,
    ({{ sem_acento("pcd_bruto") }} in ('sim', 's', '1', 'true')) as is_pcd
from norm
