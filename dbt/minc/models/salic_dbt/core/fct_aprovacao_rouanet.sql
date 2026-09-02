-- Silver SALIC / core -- fato da aprovacao de projeto na Lei Rouanet.
--
-- GRAO: 1 linha por registro de aprovacao. A chave e `id_aprovacao`.
--
-- POR QUE O HISTORICO INTEIRO. Um projeto tem mais de uma aprovacao ao longo
-- da vida: aprovacao inicial, readequacao, prorrogacao. A
-- `dim_projeto_rouanet` guarda so a mais recente, que e o que um painel le;
-- aqui nenhuma linha se perde, porque a serie temporal de valor autorizado e a
-- janela de captacao dependem do historico. `tipoaprovacao`, `idreadequacao` e
-- `idprorrogacao` sao o que distingue os registros entre si.
--
-- APROVACAO NAO E CAPTACAO. O valor autorizado e um teto: e quanto o projeto
-- pode captar, nao quanto captou. Usa-lo como se fosse recurso realizado
-- superestima qualquer indicador de execucao. Ver `fct_captacao_rouanet`.
--
-- DUAS COLUNAS DA ORIGEM FICAM DE FORA, e o motivo importa:
-- * `logon` identifica o servidor que registrou a aprovacao. E dado de
-- pessoa em coluna tecnica -- exatamente o caso que o plano manda
-- classificar como PII e nao arrastar para camada publicavel;
-- * `resumoaprovacao` e texto livre digitado por analista. Texto livre e o
-- vetor por onde nome, endereco e documento entram numa tabela que nenhuma
-- classificacao automatica marca. A politica de RAG do plano falha o export
-- quando uma descricao carrega exemplo real; a forma de nao chegar la e nao
-- trazer o campo.
--
-- LIMITE CONHECIDO: `tipoaprovacao` e um codigo cujo dominio de valores nao foi
-- levantado contra o banco. Ele fica exposto como codigo, sem descricao
-- inventada, ate a tabela de dominio ser identificada (gate G1).
select
    idaprovacao as id_aprovacao,
    {{ pronac_de_ano_sequencial("anoprojeto", "sequencial") }} as pronac,
    idpronac as id_pronac,
    idparecer as id_parecer,
    tipoaprovacao as codigo_tipo_aprovacao,
    dtaprovacao::date as data_aprovacao,
    extract(year from dtaprovacao)::integer as ano_aprovacao,
    portariaaprovacao as numero_portaria,
    dtportariaaprovacao::date as data_portaria,
    dtpublicacaoaprovacao::date as data_publicacao,
    dtiniciocaptacao::date as data_inicio_captacao,
    dtfimcaptacao::date as data_fim_captacao,
    aprovadoreal as valor_aprovado_reais,
    autorizadoreal as valor_autorizado_reais,
    concedidocusteioreal as valor_concedido_custeio_reais,
    concedidocapitalreal as valor_concedido_capital_reais,
    contrapartidareal as valor_contrapartida_reais,
    aprovadoufir as valor_aprovado_ufir,
    autorizadoufir as valor_autorizado_ufir,
    idprorrogacao as id_prorrogacao,
    idreadequacao as id_readequacao
from {{ ref("sac__aprovacao") }}
where
    idaprovacao is not null
    and {{ pronac_de_ano_sequencial("anoprojeto", "sequencial") }} is not null
