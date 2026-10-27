{{ config(materialized="table") }}
-- Silver transferegov — perfil PNAB Pessoa Física (aba pnab_pessoas de
-- bronze_planilha_dados_pnab_ciclo_1): CPF real + raça/PCD + indígena/quilombola.
-- Corta linha sem CPF e normaliza a chave. `table` pelo mesmo motivo de
-- silver_agentes_pf.
with
    base as (
        select
            "nº do cpf" as documento,
            "raça, cor ou etnia" as raca_bruto,
            "é pessoa com deficiência –pcd?" as pcd_bruto,
            "indígena?" as indigena_bruto,
            "quilombola?" as quilombola_bruto,
            cep,
            "cidade / uf" as cidade,
            nome_programa
        from {{ ref("bronze_planilha_dados_pnab_ciclo_1") }}
        where tabela_origem = 'pnab_pessoas'
    )
select
    {{ normaliza_documento("documento") }} as identificador_unico,
    'pf' as tipo_proponente,
    'pnab' as origem,
    raca_bruto,
    pcd_bruto,
    indigena_bruto,
    quilombola_bruto,
    cep,
    cidade,
    cast(null as text) as uf,
    'pnab_pessoas' as origem_tabela,
    nome_programa
from base
where {{ normaliza_documento("documento") }} is not null
