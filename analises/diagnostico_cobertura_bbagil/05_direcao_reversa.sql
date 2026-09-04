-- =============================================================================
-- Q5 — DIREÇÃO REVERSA: quem recebeu dinheiro sem estar na lista?
-- =============================================================================
-- O espelho da Q3. Pagamentos em pagamentos_bbagil_beneficiarios que não casam
-- com nenhum contemplado.
--
-- ANTES DE INTERPRETAR COMO ACHADO, DESCONTE A RESPOSTA CHATA:
-- planos cujo anexo de lista de contemplados nunca foi ingerido/parseado têm
-- 100% dos pagamentos "sem contemplado" — e isso é história de INGESTÃO DE
-- ANEXO, não de pagamento irregular. Só 41,3% dos entes LPG enviaram lista de
-- contemplados (data/transferegov/lpg_entes_por_tipo_anexo.csv), então esse
-- desconto é grande.
--
-- O resíduo depois do desconto é o achado interessante:
--   - fornecedores/contratados (o PNAB paga serviços, não só prêmios)
--   - pagamento ao CPF do representante quando a lista registra o CNPJ do coletivo
--   - CPF de 10 dígitos que o LENGTH>=11 excluiu do denominador mas o LPAD do
--     lado bancário recuperou (ver Q2c) -> pagamento sem par POSSÍVEL
-- =============================================================================

with contemplados as (
    select id_normalizado, programa_fomento
    from agentes.identificadores_contemplados
),

-- este plano tem alguma linha de contemplado parseada?
planos_com_lista as (
    select distinct rg.id_plano_acao::text as id_plano_acao
    from transferegov_fundo_a_fundo.lpg_contemplados lc
    join transferegov_fundo_a_fundo.anexos_relatorios ar
        on ar.id::text = substring(lc.id_anexo from 'anexo_([0-9]+)')
    join transferegov_fundo_a_fundo.relatorios_gestao rg
        on rg.id_relatorio_gestao = ar.id_relatorio_gestao
    where lc.id_anexo ~ '^anexo_[0-9]+'
),

pagamentos as (
    select
        pb.id_plano_acao::text    as id_plano_acao,
        pb.beneficiario_documento,
        pb.programa_fomento,
        sum(pb.valor)             as valor_total
    from agentes.pagamentos_bbagil_beneficiarios pb
    group by 1, 2, 3
),

classificado as (
    select
        p.programa_fomento,
        p.id_plano_acao,
        p.beneficiario_documento,
        p.valor_total,
        case
            when c.id_normalizado is not null           then '0_casou'
            when pcl.id_plano_acao is null              then '1_plano_sem_lista_parseada'
            when length(p.beneficiario_documento) = 14  then '2_pj_fora_da_lista'
            else                                             '3_pf_fora_da_lista'
        end as situacao
    from pagamentos p
    left join contemplados c
        on  c.id_normalizado  = p.beneficiario_documento
        and c.programa_fomento = p.programa_fomento
    left join planos_com_lista pcl on pcl.id_plano_acao = p.id_plano_acao
)

-- -----------------------------------------------------------------------------
-- 5a — Quanto do "pagou sem estar na lista" é só anexo faltando
-- -----------------------------------------------------------------------------
select
    programa_fomento,
    situacao,
    count(distinct beneficiario_documento) as beneficiarios,
    sum(valor_total)                       as valor,
    round(100.0 * sum(valor_total) / sum(sum(valor_total)) over (partition by programa_fomento), 2) as pct_valor
from classificado
group by 1, 2
order by 1, 2;


-- -----------------------------------------------------------------------------
-- 5b — O resíduo interessante: plano TEM lista parseada e mesmo assim o
-- beneficiário não está nela. Amostra para inspeção manual.
-- Reusa o CTE `classificado`: mantenha o WITH acima e substitua o SELECT 5a.
-- -----------------------------------------------------------------------------
-- select c.programa_fomento, c.id_plano_acao, c.beneficiario_documento,
--        c.valor_total, ef.beneficiario_nome
-- from classificado c
-- left join lateral (
--     select beneficiario_nome from agentes.bbagil_extrato_filtrado ef
--     where ef.beneficiario_documento = c.beneficiario_documento
--     limit 1
-- ) ef on true
-- where c.situacao in ('2_pj_fora_da_lista', '3_pf_fora_da_lista')
-- order by c.valor_total desc
-- limit 100;
--
-- Ler os nomes: se aparecerem muitas razões sociais de serviço (gráfica,
-- produtora, locação de som), é contratação, não contemplação — e o
-- denominador "contemplados" nunca deveria tentar cobrir isso.


-- -----------------------------------------------------------------------------
-- 5c — Assimetria entre as duas pernas do UNION
-- bbagil_subtransacoes_filtrado.sql aplica SÓ o filtro de nome de ente público.
-- NÃO aplica: anti-join de estorno, lista descriptionname, exclusão do '191',
-- nem o CNPJ do FNC. Dinheiro que chega por subtransação é sistematicamente
-- MENOS filtrado — é a única coisa no pipeline que INFLA a cobertura.
-- -----------------------------------------------------------------------------
select
    'extrato'       as perna,
    count(*)        as linhas,
    count(distinct beneficiario_documento) as docs,
    sum(valor)      as valor
from agentes.bbagil_extrato_filtrado

union all

select
    'subtransacoes' as perna,
    count(*)        as linhas,
    count(distinct beneficiario_documento) as docs,
    sum(valor)      as valor
from agentes.bbagil_subtransacoes_filtrado;

-- Depois: quantas linhas de subtransação morreriam sob as regras do extrato?
-- select count(*) as morreriam
-- from agentes.bbagil_subtransacoes_filtrado s
-- where s.beneficiario_documento in ('0', '191')
--    or s.beneficiario_documento = '3793086100189';
