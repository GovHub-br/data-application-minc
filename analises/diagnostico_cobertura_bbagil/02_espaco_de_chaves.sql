-- =============================================================================
-- Q2 — Espaço de chaves: o denominador (82.307 / 12.085) está certo?
-- =============================================================================
-- SUSPEITA 1 — a coluna `cnpj` órfã.
-- views/identificadores_contemplados.sql:29 resolve a coluna de documento com
--     column_name ILIKE '%cpf%cnpj%'
-- Esse padrão exige "cpf" ANTES de "cnpj" no nome. Ele NÃO casa uma coluna
-- chamada literalmente `cnpj`.
-- Mas cotas_dbt/bronze/stg_contemplados_lpg.sql:7 faz
--     coalesce_por_nome(src, ['cpf ou cnpj', 'cnpj'])
-- ou seja, outro modelo do mesmo repo afirma que lpg_contemplados.cnpj carrega
-- dado. Se carregar, esses contemplados estão fora do numerador E do
-- denominador — não "deixaram de aparecer", nunca foram perguntados.
--
-- SUSPEITA 2 — LENGTH sem LPAD.
-- identificadores_contemplados.sql:54 usa LENGTH >= 11 e não faz LPAD.
-- O lado bancário SEMPRE faz LPAD para 11/14 (bbagil_extrato_filtrado.sql:30-35).
-- Corta nos dois sentidos:
--   - doc de 10 dígitos (CPF truncado): excluído do denominador, mas o LPAD
--     recupera do lado bancário -> vira pagamento sem par possível (infla a Q5)
--   - doc de 12/13 (CNPJ truncado): ENTRA no denominador e nunca casa
--     -> deprime a cobertura para sempre
-- =============================================================================


-- -----------------------------------------------------------------------------
-- 2a — Que colunas de documento existem de fato em lpg_contemplados?
-- Rode isto primeiro: o resto do arquivo depende do que aparecer aqui.
-- -----------------------------------------------------------------------------
select
    column_name,
    data_type,
    -- torna visível espaço/NBSP invisível no fim do nome
    '[' || column_name || ']' as nome_delimitado,
    length(column_name)       as tam_nome
from information_schema.columns
where table_schema = 'transferegov_fundo_a_fundo'
  and table_name   = 'lpg_contemplados'
  and (column_name ilike '%cpf%' or column_name ilike '%cnpj%')
order by column_name;

-- Compare o resultado com o que o ILIKE '%cpf%cnpj%' captura:
-- select column_name from information_schema.columns
-- where table_schema='transferegov_fundo_a_fundo' and table_name='lpg_contemplados'
--   and column_name ilike '%cpf%cnpj%';
-- A DIFERENÇA entre as duas listas é o que identificadores_contemplados perde.


-- -----------------------------------------------------------------------------
-- 2b — Volume perdido pela coluna órfã
-- AJUSTE os nomes de coluna conforme o resultado de 2a. O exemplo abaixo assume
-- que existem "cpf ou cnpj" (capturada) e "cnpj" (NÃO capturada).
-- -----------------------------------------------------------------------------
-- select
--     count(*)                                                          as linhas_total,
--     count(nullif(lower(trim("cpf ou cnpj")), 'nan'))                  as tem_cpf_ou_cnpj,
--     count(nullif(lower(trim("cnpj")), 'nan'))                         as tem_cnpj,
--     count(*) filter (
--         where nullif(lower(trim("cpf ou cnpj")), 'nan') is null
--           and nullif(lower(trim("cnpj")), 'nan') is not null
--     )                                                                 as so_na_coluna_orfa
-- from transferegov_fundo_a_fundo.lpg_contemplados;
--
-- `so_na_coluna_orfa` é o volume de contemplados LPG invisíveis ao indicador.


-- -----------------------------------------------------------------------------
-- 2c — Distribuição de comprimento do documento (a assimetria LENGTH × LPAD)
-- -----------------------------------------------------------------------------
select
    programa_fomento,
    case
        when length(id_normalizado) = 11 then '11_cpf_ok'
        when length(id_normalizado) = 14 then '14_cnpj_ok'
        when length(id_normalizado) in (12, 13) then '12_13_cnpj_truncado_nunca_casa'
        when length(id_normalizado) < 11 then '<11_excluido_do_denominador'
        else 'outro_lixo'
    end as classe_comprimento,
    count(*) as docs
from agentes.identificadores_contemplados
group by 1, 2
order by 1, 2;

-- '12_13_cnpj_truncado_nunca_casa' é dano puro: entra no denominador e é
-- não-match garantido. Um LPAD dos dois lados recuperaria esses.


-- -----------------------------------------------------------------------------
-- 2d — Quanto o LPAD recuperaria?
-- Reconta o match aplicando LPAD ao lado do contemplado.
-- -----------------------------------------------------------------------------
with contemplados_lpad as (
    select
        programa_fomento,
        id_normalizado,
        case
            when length(id_normalizado) between 12 and 14 then lpad(id_normalizado, 14, '0')
            when length(id_normalizado) <= 11             then lpad(id_normalizado, 11, '0')
            else id_normalizado
        end as id_lpad
    from agentes.identificadores_contemplados
)
select
    c.programa_fomento,
    count(*)                                                          as contemplados,
    count(*) filter (where atual.beneficiario_documento is not null)   as casam_hoje,
    count(*) filter (where atual.beneficiario_documento is null
                       and com_lpad.beneficiario_documento is not null) as recuperados_pelo_lpad
from contemplados_lpad c
left join agentes.primeiro_pagamento_bancario atual
    on  atual.beneficiario_documento = c.id_normalizado
    and atual.programa_fomento       = c.programa_fomento
left join agentes.primeiro_pagamento_bancario com_lpad
    on  com_lpad.beneficiario_documento = c.id_lpad
    and com_lpad.programa_fomento       = c.programa_fomento
group by 1
order by 1;


-- -----------------------------------------------------------------------------
-- 2e — Comparar com o modelo "bom" de cotas_dbt
-- stg_contemplados_lpg/_pnab já resolvem coalesce multi-coluna, limpam
-- linha-fantasma ('TOTAL DE RECURSOS%', 'insira informa%' — contemplados_unif.sql:26-27)
-- e têm a flag explícita chave_anonimizada (separa "sem chave" de "não apareceu").
-- A diferença entre os dois counts é o erro do denominador, MEDIDO em vez de suposto.
-- -----------------------------------------------------------------------------
select
    'agentes.identificadores_contemplados' as fonte,
    programa_fomento,
    count(distinct id_normalizado) as docs
from agentes.identificadores_contemplados
group by 1, 2

union all

select
    'cotas.stg_contemplados_lpg' as fonte,
    'LPG'                        as programa_fomento,
    count(distinct identificador_unico) as docs
from cotas.stg_contemplados_lpg
where identificador_unico is not null

union all

select
    'cotas.stg_contemplados_pnab' as fonte,
    'PNAB'                        as programa_fomento,
    count(distinct identificador_unico) as docs
from cotas.stg_contemplados_pnab
where identificador_unico is not null

order by 2, 1;

-- Se cotas.* > agentes.* de forma material, 82.307 não é o denominador
-- e 46% não é a cobertura.
