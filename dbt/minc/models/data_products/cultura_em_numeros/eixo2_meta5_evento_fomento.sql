{{ config(materialized="table") }}

-- Gold — a espinha da Meta 5: um evento por vez que um agente cultural
-- recebeu recurso de uma política federal de fomento, já com a política
-- classificada. É a tabela que permite responder "este agente já havia sido
-- contemplado antes?" olhando ACIMA do mecanismo — que é exatamente o que a
-- pergunta pede ("novos agentes que não haviam sido anteriormente
-- contemplados", sem qualificar por programa).
--
-- Lê int_ancine_bbagil_salic, que só junta as fontes, e aplica aqui TODA a
-- regra de negócio — o que é pagamento efetivo e quem conta (ADR 0010,
-- regra de negócio só na Gold). As demais golds da meta derivam desta e de
-- eixo2_meta5_primeiro_acesso_agente.
--
-- Grão: 1 linha por (documento, mecanismo, data do evento, valor).
--
-- QUEM CONTA, POR FONTE
--
-- BB Ágil — pagamento a agente cultural. Primeiro, o que é pagamento
-- efetivo:
-- • no extrato, só o débito (a saída de dinheiro da conta), e fora os dois
-- lados de um estorno: débito e crédito da MESMA CONTA com o mesmo
-- documento e o mesmo valor. A chave é a conta, não o plano de ação: um
-- plano tem mais de uma conta (os da LPG têm duas) e estorno acontece
-- dentro de uma conta só. Casando no nível do plano, um débito da conta A
-- e um crédito da conta B viram "par" e os DOIS lados são descartados —
-- perda silenciosa de pagamento real. O pareamento roda antes de
-- descartar linha sem documento normalizado ou sem data, como rodava
-- quando morava na silver: uma perna sem data ainda anula a outra.
-- • no sublançamento, só o status 'Pago' (status_prestacao_contas): os
-- outros não chegaram ao beneficiário.
-- Depois, quem conta, portando os critérios de
-- plugins/regras_negocio_bbagil.py (módulo órfão, mantido como referência):
-- • fora repasse entre entes públicos: nome do beneficiário começando com
-- MUNICIPIO/ESTADO/FUNDO/SECRETARIA/SEFAZ é repasse administrativo, não
-- pagamento a agente. Vale para extrato e sublançamento.
-- • no extrato, fora a transferência para o próprio Banco do Brasil
-- (documento '191'), a devolução de saldo ao Fundo Nacional de Cultura
-- (CNPJ 3793086100189) e os lançamentos internos da conta (aplicação,
-- resgate, imposto, ordem cancelada — pela descriptionname).
-- • a política sai do código curto do programa (programa_bruto). Os códigos
-- que o extrato traz:
-- MINC-LPG-{MUNI,EST}-{AUD,OUTRAS}  -> LPG   (desde 2023-06)
-- MINC-PNAB-2023, MINC_PNAB2025_*   -> PNAB  (desde 2023-11)
-- SECULT-A BLANC-{MUN,EST-D,EST-R}  -> LAB   (desde 2023-01)
-- O nome não segue convenção (hífen, underscore, espaço), então separador
-- e caixa são normalizados antes. Sem isso os 95.658 pagamentos do ciclo 2
-- da PNAB caem fora em silêncio. Código que não casa sai: é movimento de
-- conta que não é repasse de política do MinC.
-- • LAB: os códigos SECULT-A BLANC são a Lei Aldir Blanc II (Lei
-- 14.399/2022). A LAB I emergencial (2020-2021) está FORA DO ESCOPO por
-- decisão de negócio de 20/08/2026: foi resposta emergencial de pandemia,
-- não fomento. Sua ausência não é lacuna.
-- • PISO DE VALOR: o extrato carrega resíduo que não é repasse de fomento
-- (tarifas, ajustes). O piso de {{ var('limiar_valor_fomento', 375) }}
-- reais é aplicado sobre o TOTAL do agente naquele mecanismo, não por
-- transação, para que "foi atendido por este mecanismo" seja uma decisão
-- só. Avaliar por transação removeria parcelas pequenas de um repasse
-- legítimo e poderia adiantar a data de entrada do agente.
--
-- SALIC — recibo de captação da Rouanet com valor positivo. O evento é a
-- captação, não a aprovação: o fomento direto mede dinheiro que saiu da conta
-- para o agente, e o equivalente na Rouanet é o recurso captado junto ao
-- incentivador. Projeto aprovado que nunca captou não deu acesso a recurso.
--
-- ANCINE — decisão de negócio de 20/08/2026: entram o FSA e o fomento federal
-- ao audiovisual com repasse identificável ao proponente, com valor positivo.
-- Ficam de fora:
-- • ART25 e ART18: são Rouanet e já entram pelo SALIC, por fonte melhor;
-- incluí-los duplicaria evento.
-- • RENDIMENTOS (aplicação financeira), CONTRAPARTIDA (dinheiro do próprio
-- proponente) e OUTRAS FONTES (origem não identificada).
-- • LEI ESTADUAL e LEI MUNICIPAL: não são política federal.
-- • ART39 CONDECINE: tributo sobre o mercado, não fomento direto ao agente.
-- Medido: acrescentaria 46 proponentes e 2 correções.
-- São dois mecanismos, não um: 'FSA' responde ao que o documento da meta
-- nomeia; 'AUDIOVISUAL' carrega Lei do Audiovisual (ART1/ART3/1A/3A),
-- FUNCINES e os editais da Ancine. Rouanet e Ancine não passam pelo piso: o
-- evento já é uma captação formal.
--
-- JANELAS DE OBSERVAÇÃO — a assimetria é a limitação mais importante do
-- indicador, e fica em ano_inicio_observacao para a gold anual marcar os
-- anos censurados:
-- ROUANET      1993+  captação real (SALIC)         — praticamente completa
-- AUDIOVISUAL  2005+  Lei do Audiovisual, FUNCINES e editais Ancine
-- FSA          2011+  Fundo Setorial do Audiovisual
-- LAB          2023+  Aldir Blanc II
-- LPG          2023+  Lei Paulo Gustavo             — nasceu em 2023
-- PNAB         2023+  Política Nacional Aldir Blanc — nasceu em 2023
--
-- "Novo entrante" significa "nenhuma evidência de acesso anterior nas fontes
-- disponíveis", não "verificamos que nunca acessou". Fonte que falta só
-- transforma veterano em falso novo entrante, nunca o contrário — então os
-- percentuais desta série são TETO, e cada fonte nova só pode derrubá-los.
with
    eventos as (select * from {{ ref("int_ancine_bbagil_salic") }}),

    -- chaves (conta + documento bruto + valor) que aparecem no extrato como
    -- débito E crédito: estorno/devolução. INTERSECT, e não join, para que valor
    -- nulo também case com valor nulo, como casava na silver
    chaves_estorno as (
        select
            id_plano_acao,
            id_plano_acao_dado_bancario,
            beneficiario_documento_bruto,
            valor
        from eventos
        where
            fonte = 'BB_AGIL'
            and tipo_lancamento = 'extrato'
            and creditdebitindicator = 'D'
        intersect
        select
            id_plano_acao,
            id_plano_acao_dado_bancario,
            beneficiario_documento_bruto,
            valor
        from eventos
        where
            fonte = 'BB_AGIL'
            and tipo_lancamento = 'extrato'
            and creditdebitindicator = 'C'
    ),

    pagamento_efetivo as (
        select e.*
        from eventos e
        left join
            chaves_estorno ce
            on e.tipo_lancamento = 'extrato'
            and e.id_plano_acao = ce.id_plano_acao
            and e.id_plano_acao_dado_bancario = ce.id_plano_acao_dado_bancario
            and e.beneficiario_documento_bruto = ce.beneficiario_documento_bruto
            and e.valor = ce.valor
        where
            (
                e.fonte <> 'BB_AGIL'
                or (
                    e.tipo_lancamento = 'extrato'
                    and e.creditdebitindicator = 'D'
                    and ce.id_plano_acao is null
                )
                or (
                    e.tipo_lancamento = 'subtransacao'
                    and e.status_prestacao_contas = 'Pago'
                )
            )
            -- só depois de parear: sem documento ou data o evento não é
            -- atribuível a um agente nem a um ano. Valor nulo NÃO sai aqui: no
            -- BB Ágil o pagamento com value fora do formato numérico sempre contou
            -- (o piso soma ignorando NULL) e pode ser a data de entrada do agente;
            -- em SALIC e Ancine o valor > 0 abaixo já o descarta
            and e.beneficiario_documento is not null
            and e.data_evento is not null
    ),

    elegivel as (
        select
            beneficiario_documento,
            tipo_pessoa,
            data_evento,
            ano_evento,
            valor,
            fonte,
            case
                when fonte = 'SALIC'
                then 'ROUANET'
                when fonte = 'ANCINE' and programa_bruto = 'FSA'
                then 'FSA'
                when
                    fonte = 'ANCINE'
                    and programa_bruto in (
                        'ART1',
                        'ART3',
                        'ART 1A',
                        'ART 3A',
                        'ART41 (FUNCINES)',
                        'EDITAL ANCINE',
                        'EDITAL ANCINE (PAR)',
                        'OUTROS EDITAIS'
                    )
                then 'AUDIOVISUAL'
                when
                    fonte = 'BB_AGIL'
                    and upper(translate(programa_bruto, '_ ', '--')) like 'MINC-LPG%'
                then 'LPG'
                when
                    fonte = 'BB_AGIL'
                    and upper(translate(programa_bruto, '_ ', '--')) like '%PNAB%'
                then 'PNAB'
                when
                    fonte = 'BB_AGIL'
                    and upper(translate(programa_bruto, '_ ', '--')) like '%A-BLANC%'
                then 'LAB'
            end as programa_fomento
        from pagamento_efetivo
        where
            (fonte in ('SALIC', 'ANCINE') and valor > 0)
            or (
                fonte = 'BB_AGIL'
                and not (
                    upper(coalesce(beneficiario_nome, '')) like 'MUNICIPIO%'
                    or upper(coalesce(beneficiario_nome, '')) like 'ESTADO%'
                    or upper(coalesce(beneficiario_nome, '')) like 'FUNDO%'
                    or upper(coalesce(beneficiario_nome, '')) like 'SECRETARIA%'
                    or upper(coalesce(beneficiario_nome, '')) like 'SEFAZ%'
                )
                and (
                    tipo_lancamento = 'subtransacao'
                    -- descriptionname nula no extrato também sai, como na
                    -- regra original (NOT IN com NULL não é verdadeiro)
                    or (
                        beneficiario_documento_bruto not in ('191', '3793086100189')
                        and descriptionname not in (
                            'BB-APLIC C.PRZ-APL.AUT',
                            'Resgate Automatico',
                            'Impostos',
                            'ORDEM BANC CANCELADA',
                            'Ordem Bancaria',
                            'Resgate BB Fix',
                            'CREDITO CONVENIO',
                            'Estorno Resgate Automatico'
                        )
                    )
                )
            )
    ),

    classificado as (select * from elegivel where programa_fomento is not null),

    bbagil_acima_piso as (
        select beneficiario_documento, programa_fomento
        from classificado
        where fonte = 'BB_AGIL'
        group by beneficiario_documento, programa_fomento
        having sum(valor) >= {{ var("limiar_valor_fomento", 375) }}
    )

select
    c.beneficiario_documento,
    c.programa_fomento,
    c.data_evento,
    c.ano_evento,
    c.valor,
    c.fonte,
    -- primeiro ano em que a fonte daquele mecanismo consegue enxergar
    -- qualquer coisa; a gold anual usa isto para marcar o ano censurado,
    -- onde todo mundo parece estreante porque não existe passado observável
    case
        when c.programa_fomento = 'ROUANET'
        then 1993
        when c.programa_fomento = 'AUDIOVISUAL'
        then 2005
        when c.programa_fomento = 'FSA'
        then 2011
        when c.programa_fomento in ('LAB', 'LPG', 'PNAB')
        then 2023
    end as ano_inicio_observacao,
    c.tipo_pessoa
from classificado c
left join
    bbagil_acima_piso p
    on c.beneficiario_documento = p.beneficiario_documento
    and c.programa_fomento = p.programa_fomento
where c.fonte <> 'BB_AGIL' or p.beneficiario_documento is not null
