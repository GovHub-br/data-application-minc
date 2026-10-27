{{ config(materialized="table") }}

-- Silver — sublançamentos do BB Gestão Ágil, usados quando a transação-mãe
-- do extrato tem subtransactionquantity > 0 (o beneficiário real do
-- pagamento só aparece aqui, não na linha agregada do extrato). Lê as
-- silvers tipadas e só limpa:
-- 1. descarta beneficiário ausente (documento nulo ou '0').
-- 2. valor em módulo (subtransação pode vir negativa na fonte).
--
-- Todos os status ficam, em status_prestacao_contas
-- (subtransactionaccountabilityname): recortar o 'Pago', o sublançamento que
-- chegou ao beneficiário, é regra de elegibilidade e mora na Gold
-- (eixo2_meta5_evento_fomento, ADR 0010).
--
-- Repasse a ente público continua aqui, com beneficiario_nome, para a Gold
-- decidir (ADR 0010, regra de negócio só na Gold).
--
-- Contexto de programa (governmentprogramname) vem herdado da
-- transação-mãe via id_plano_acao + id_plano_acao_dado_bancario +
-- id_transacao_pai = id. A CONTA (id_plano_acao_dado_bancario) faz parte do
-- join de propósito: um plano de ação tem mais de uma conta (os da LPG têm
-- duas) e o `id` da transação não é único entre elas — sem a conta, o join
-- vira 1→N e cada sublançamento é contado duas vezes.
--
-- As colunas da silver_subtransacao_bbagil são todas texto (a tabela não pôde
-- ser perfilada), então as chaves do join e o valor ganham cast aqui, com a
-- mesma guarda das macros bronze_*.
select
    {{ bronze_inteiro("s.id_plano_acao") }} as id_plano_acao,
    {{ bronze_inteiro("s.id_plano_acao_dado_bancario") }} as id_plano_acao_dado_bancario,
    e.governmentprogramname as programa_curto,
    s.beneficiarydocumentid as beneficiario_documento_bruto,
    case
        when s.beneficiarypersontype = '1'
        then lpad(regexp_replace(s.beneficiarydocumentid, '[^0-9]', '', 'g'), 11, '0')
        when s.beneficiarypersontype = '2'
        then lpad(regexp_replace(s.beneficiarydocumentid, '[^0-9]', '', 'g'), 14, '0')
    end as beneficiario_documento,
    s.beneficiaryname as beneficiario_nome,
    abs({{ bronze_numerico("s.value") }}) as valor,
    {{ data_texto("s.paymentdate") }} as data_pagamento,
    s.subtransactionaccountabilityname as status_prestacao_contas
from {{ ref("silver_subtransacao_bbagil") }} s
join
    {{ ref("silver_extrato_bbagil") }} e
    on {{ bronze_inteiro("s.id_plano_acao") }} = e.id_plano_acao
    and {{ bronze_inteiro("s.id_plano_acao_dado_bancario") }}
    = e.id_plano_acao_dado_bancario
    and {{ bronze_inteiro("s.id_transacao_pai", "bigint") }} = e.id
where s.beneficiarydocumentid is not null and s.beneficiarydocumentid != '0'
