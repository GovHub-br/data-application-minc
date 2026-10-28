-- Gerado por scripts/gerar_mover_bronze_salic.py em 2026-10-28.
-- Roda uma vez, no dbanalytics (psql), como o DONO das tabelas
-- (dl_airflow), com CREATE em salic_bronze (o RENAME exige) e em
-- salic (o SET SCHEMA exige); os dois schemas são de dl_analytics.
-- Idempotente: o que já foi movido não existe mais em salic_bronze e o
-- IF EXISTS pula.
-- 672 tabelas.
\set ON_ERROR_STOP on
begin;

-- 1. colisão: nada em salic pode ter o nome de destino, salvo a
--    própria tabela já movida.
do $$
declare r record;
begin
  for r in
    select 'agentes__agentes' as origem, 'bronze_agentes_agentes' as destino
    union all select 'agentes__endereconacional' as origem, 'bronze_agentes_endereconacional' as destino
    union all select 'agentes__municipios' as origem, 'bronze_agentes_municipios' as destino
    union all select 'agentes__perfil' as origem, 'bronze_agentes_perfil' as destino
    union all select 'agentes__populacaomunicipio' as origem, 'bronze_agentes_populacaomunicipio' as destino
    union all select 'agentes__tbagentefisico' as origem, 'bronze_agentes_tbagentefisico' as destino
    union all select 'agentes__uf' as origem, 'bronze_agentes_uf' as destino
    union all select 'agentes__verificacao' as origem, 'bronze_agentes_verificacao' as destino
    union all select 'agentes__vpais' as origem, 'bronze_agentes_vpais' as destino
    union all select 'agentes__vperfil' as origem, 'bronze_agentes_vperfil' as destino
    union all select 'agentes__vprefeiturasregiaouf' as origem, 'bronze_agentes_vprefeiturasregiaouf' as destino
    union all select 'agentes__vramais' as origem, 'bronze_agentes_vramais' as destino
    union all select 'agentes__vsistema' as origem, 'bronze_agentes_vsistema' as destino
    union all select 'agentes__vtelefones' as origem, 'bronze_agentes_vtelefones' as destino
    union all select 'agentes__vuf' as origem, 'bronze_agentes_vuf' as destino
    union all select 'agentes__vufmunicipio' as origem, 'bronze_agentes_vufmunicipio' as destino
    union all select 'agentes__vverificacao' as origem, 'bronze_agentes_vverificacao' as destino
    union all select 'agentes__vvinculacao' as origem, 'bronze_agentes_vvinculacao' as destino
    union all select 'agentes__vvisao' as origem, 'bronze_agentes_vvisao' as destino
    union all select 'agentes__vwcadastrarparecerista' as origem, 'bronze_agentes_vwcadastrarparecerista' as destino
    union all select 'agentes__vwempresajuridica' as origem, 'bronze_agentes_vwempresajuridica' as destino
    union all select 'agentes__vwempresajuridicasocios' as origem, 'bronze_agentes_vwempresajuridicasocios' as destino
    union all select 'agentes__vwportifolio' as origem, 'bronze_agentes_vwportifolio' as destino
    union all select 'agentes__vwpropostasemavaliacao' as origem, 'bronze_agentes_vwpropostasemavaliacao' as destino
    union all select 'bdcorporativo__sysdiagrams' as origem, 'bronze_bdcorporativo_sysdiagrams' as destino
    union all select 'bdcorporativo__tbalteracaonomeproponente' as origem, 'bronze_bdcorporativo_tbalteracaonomeproponente' as destino
    union all select 'bdcorporativo__tbcomprovantepagamento' as origem, 'bronze_bdcorporativo_tbcomprovantepagamento' as destino
    union all select 'bdcorporativo__tbencaminhamentoprestacaocontas' as origem, 'bronze_bdcorporativo_tbencaminhamentoprestacaocontas' as destino
    union all select 'bdcorporativo__tbsituacaoencaminhamentoprestacaocontas' as origem, 'bronze_bdcorporativo_tbsituacaoencaminhamentoprestacaocontas' as destino
    union all select 'controledeacesso__dtproperties' as origem, 'bronze_controledeacesso_dtproperties' as destino
    union all select 'controledeacesso__sgcacesso' as origem, 'bronze_controledeacesso_sgcacesso' as destino
    union all select 'controledeacesso__sgcsistema' as origem, 'bronze_controledeacesso_sgcsistema' as destino
    union all select 'controledeacesso__sgcusuarioxsistema' as origem, 'bronze_controledeacesso_sgcusuarioxsistema' as destino
    union all select 'controledeacesso__sysdiagrams' as origem, 'bronze_controledeacesso_sysdiagrams' as destino
    union all select 'sac__aberturadecontabancaria' as origem, 'bronze_sac_aberturadecontabancaria' as destino
    union all select 'sac__aberturadecontasav39215' as origem, 'bronze_sac_aberturadecontasav39215' as destino
    union all select 'sac__aberturadecontasav39224' as origem, 'bronze_sac_aberturadecontasav39224' as destino
    union all select 'sac__aberturadecontasav39225' as origem, 'bronze_sac_aberturadecontasav39225' as destino
    union all select 'sac__aberturadecontasav39227' as origem, 'bronze_sac_aberturadecontasav39227' as destino
    union all select 'sac__aberturadecontaseficpf40979' as origem, 'bronze_sac_aberturadecontaseficpf40979' as destino
    union all select 'sac__aberturadecontaseficpf40980' as origem, 'bronze_sac_aberturadecontaseficpf40980' as destino
    union all select 'sac__aberturadecontaseficpj40975' as origem, 'bronze_sac_aberturadecontaseficpj40975' as destino
    union all select 'sac__aberturadecontaseficpj40981' as origem, 'bronze_sac_aberturadecontaseficpj40981' as destino
    union all select 'sac__abrangencia' as origem, 'bronze_sac_abrangencia' as destino
    union all select 'sac__acaoproduto' as origem, 'bronze_sac_acaoproduto' as destino
    union all select 'sac__acaoprojeto' as origem, 'bronze_sac_acaoprojeto' as destino
    union all select 'sac__acompanhamentobandasx' as origem, 'bronze_sac_acompanhamentobandasx' as destino
    union all select 'sac__agenciasbb' as origem, 'bronze_sac_agenciasbb' as destino
    union all select 'sac__aprovacao' as origem, 'bronze_sac_aprovacao' as destino
    union all select 'sac__area' as origem, 'bronze_sac_area' as destino
    union all select 'sac__aspectosfinanceirosx' as origem, 'bronze_sac_aspectosfinanceirosx' as destino
    union all select 'sac__aspectosfisicosx' as origem, 'bronze_sac_aspectosfisicosx' as destino
    union all select 'sac__aspectosresultadosx' as origem, 'bronze_sac_aspectosresultadosx' as destino
    union all select 'sac__atualizaorgaoprojetosx' as origem, 'bronze_sac_atualizaorgaoprojetosx' as destino
    union all select 'sac__auditoria' as origem, 'bronze_sac_auditoria' as destino
    union all select 'sac__autuacaox' as origem, 'bronze_sac_autuacaox' as destino
    union all select 'sac__avaliacaopc' as origem, 'bronze_sac_avaliacaopc' as destino
    union all select 'sac__avaliacaoprojeto' as origem, 'bronze_sac_avaliacaoprojeto' as destino
    union all select 'sac__avaliacaoproponente' as origem, 'bronze_sac_avaliacaoproponente' as destino
    union all select 'sac__bancoagencia' as origem, 'bronze_sac_bancoagencia' as destino
    union all select 'sac__bancoagencia20100824' as origem, 'bronze_sac_bancoagencia20100824' as destino
    union all select 'sac__bancoagencia_08' as origem, 'bronze_sac_bancoagencia_08' as destino
    union all select 'sac__bancoagencia_bkp_04112009x' as origem, 'bronze_sac_bancoagencia_bkp_04112009x' as destino
    union all select 'sac__bancoagencianovo' as origem, 'bronze_sac_bancoagencianovo' as destino
    union all select 'sac__bancoagencianovo1' as origem, 'bronze_sac_bancoagencianovo1' as destino
    union all select 'sac__bandas' as origem, 'bronze_sac_bandas' as destino
    union all select 'sac__bcoagmar2011$' as origem, 'bronze_sac_bcoagmar2011$' as destino
    union all select 'sac__bolsa' as origem, 'bronze_sac_bolsa' as destino
    union all select 'sac__captacao' as origem, 'bronze_sac_captacao' as destino
    union all select 'sac__captacaoconversao' as origem, 'bronze_sac_captacaoconversao' as destino
    union all select 'sac__captacaoguia' as origem, 'bronze_sac_captacaoguia' as destino
    union all select 'sac__captacaoold' as origem, 'bronze_sac_captacaoold' as destino
    union all select 'sac__captacaoquotas' as origem, 'bronze_sac_captacaoquotas' as destino
    union all select 'sac__carregarecibocaptacaodanca' as origem, 'bronze_sac_carregarecibocaptacaodanca' as destino
    union all select 'sac__carregarecibocaptacaodanca2010' as origem, 'bronze_sac_carregarecibocaptacaodanca2010' as destino
    union all select 'sac__carregarecibocaptacaodanca2012' as origem, 'bronze_sac_carregarecibocaptacaodanca2012' as destino
    union all select 'sac__carregarecibocaptacaodanca2013' as origem, 'bronze_sac_carregarecibocaptacaodanca2013' as destino
    union all select 'sac__carta' as origem, 'bronze_sac_carta' as destino
    union all select 'sac__cartaeventual' as origem, 'bronze_sac_cartaeventual' as destino
    union all select 'sac__certidoesnegativas' as origem, 'bronze_sac_certidoesnegativas' as destino
    union all select 'sac__cnic' as origem, 'bronze_sac_cnic' as destino
    union all select 'sac__complementacao' as origem, 'bronze_sac_complementacao' as destino
    union all select 'sac__conselheiro' as origem, 'bronze_sac_conselheiro' as destino
    union all select 'sac__consultor' as origem, 'bronze_sac_consultor' as destino
    union all select 'sac__consultorespc' as origem, 'bronze_sac_consultorespc' as destino
    union all select 'sac__contabancaria' as origem, 'bronze_sac_contabancaria' as destino
    union all select 'sac__convenio' as origem, 'bronze_sac_convenio' as destino
    union all select 'sac__cpbx' as origem, 'bronze_sac_cpbx' as destino
    union all select 'sac__criteriosx' as origem, 'bronze_sac_criteriosx' as destino
    union all select 'sac__curriculo' as origem, 'bronze_sac_curriculo' as destino
    union all select 'sac__dba_captacaoanoufmunicipio' as origem, 'bronze_sac_dba_captacaoanoufmunicipio' as destino
    union all select 'sac__dba_geral' as origem, 'bronze_sac_dba_geral' as destino
    union all select 'sac__dba_geral_2008' as origem, 'bronze_sac_dba_geral_2008' as destino
    union all select 'sac__dba_geral_2009' as origem, 'bronze_sac_dba_geral_2009' as destino
    union all select 'sac__dba_geral_2010' as origem, 'bronze_sac_dba_geral_2010' as destino
    union all select 'sac__dba_geral_2011' as origem, 'bronze_sac_dba_geral_2011' as destino
    union all select 'sac__dba_geral_2012' as origem, 'bronze_sac_dba_geral_2012' as destino
    union all select 'sac__dba_geral_2013' as origem, 'bronze_sac_dba_geral_2013' as destino
    union all select 'sac__dba_geral_2014' as origem, 'bronze_sac_dba_geral_2014' as destino
    union all select 'sac__dba_geral_fred' as origem, 'bronze_sac_dba_geral_fred' as destino
    union all select 'sac__dba_geral_sem_abrangencia' as origem, 'bronze_sac_dba_geral_sem_abrangencia' as destino
    union all select 'sac__dba_qtd_apresentado_aprovado_ano_mes' as origem, 'bronze_sac_dba_qtd_apresentado_aprovado_ano_mes' as destino
    union all select 'sac__dba_qtd_apresentado_aprovado_captado' as origem, 'bronze_sac_dba_qtd_apresentado_aprovado_captado' as destino
    union all select 'sac__dba_qtd_proponentes_inabilitados' as origem, 'bronze_sac_dba_qtd_proponentes_inabilitados' as destino
    union all select 'sac__dba_relacao_projetos' as origem, 'bronze_sac_dba_relacao_projetos' as destino
    union all select 'sac__dba_relacao_projetos_fred' as origem, 'bronze_sac_dba_relacao_projetos_fred' as destino
    union all select 'sac__dba_relacao_projetos_fred_agentes' as origem, 'bronze_sac_dba_relacao_projetos_fred_agentes' as destino
    union all select 'sac__dba_relacao_projetos_fred_tcu' as origem, 'bronze_sac_dba_relacao_projetos_fred_tcu' as destino
    union all select 'sac__dba_relacao_projetos_fred_tcu_aprovados' as origem, 'bronze_sac_dba_relacao_projetos_fred_tcu_aprovados' as destino
    union all select 'sac__dba_relacao_projetos_geral' as origem, 'bronze_sac_dba_relacao_projetos_geral' as destino
    union all select 'sac__dba_tcu_captacao_sav' as origem, 'bronze_sac_dba_tcu_captacao_sav' as destino
    union all select 'sac__dba_tcu_captacao_sefic' as origem, 'bronze_sac_dba_tcu_captacao_sefic' as destino
    union all select 'sac__dba_total_investido_investidor_ano' as origem, 'bronze_sac_dba_total_investido_investidor_ano' as destino
    union all select 'sac__dba_valor_solicitado_captado_projeto' as origem, 'bronze_sac_dba_valor_solicitado_captado_projeto' as destino
    union all select 'sac__dba_valor_solicitado_captado_projeto_sav' as origem, 'bronze_sac_dba_valor_solicitado_captado_projeto_sav' as destino
    union all select 'sac__dbf' as origem, 'bronze_sac_dbf' as destino
    union all select 'sac__dbf_agosto' as origem, 'bronze_sac_dbf_agosto' as destino
    union all select 'sac__dbf_dezembro' as origem, 'bronze_sac_dbf_dezembro' as destino
    union all select 'sac__dbf_maio' as origem, 'bronze_sac_dbf_maio' as destino
    union all select 'sac__dbresult' as origem, 'bronze_sac_dbresult' as destino
    union all select 'sac__deorgaoparaorgao' as origem, 'bronze_sac_deorgaoparaorgao' as destino
    union all select 'sac__distribuicao_avaliacao_proposta' as origem, 'bronze_sac_distribuicao_avaliacao_proposta' as destino
    union all select 'sac__distribuicao_avaliacao_proposta_bkp' as origem, 'bronze_sac_distribuicao_avaliacao_proposta_bkp' as destino
    union all select 'sac__distribuicaopassagem' as origem, 'bronze_sac_distribuicaopassagem' as destino
    union all select 'sac__documentopessoal' as origem, 'bronze_sac_documentopessoal' as destino
    union all select 'sac__documentosexigidos' as origem, 'bronze_sac_documentosexigidos' as destino
    union all select 'sac__documentosprojeto' as origem, 'bronze_sac_documentosprojeto' as destino
    union all select 'sac__documentosproponente' as origem, 'bronze_sac_documentosproponente' as destino
    union all select 'sac__dtproperties' as origem, 'bronze_sac_dtproperties' as destino
    union all select 'sac__edital' as origem, 'bronze_sac_edital' as destino
    union all select 'sac__editaldesembolso' as origem, 'bronze_sac_editaldesembolso' as destino
    union all select 'sac__editalparcelas' as origem, 'bronze_sac_editalparcelas' as destino
    union all select 'sac__editalprojeto' as origem, 'bronze_sac_editalprojeto' as destino
    union all select 'sac__editalrelatorio' as origem, 'bronze_sac_editalrelatorio' as destino
    union all select 'sac__efeitoseconomicos' as origem, 'bronze_sac_efeitoseconomicos' as destino
    union all select 'sac__efeitosresultado' as origem, 'bronze_sac_efeitosresultado' as destino
    union all select 'sac__efeitossociais' as origem, 'bronze_sac_efeitossociais' as destino
    union all select 'sac__empenho' as origem, 'bronze_sac_empenho' as destino
    union all select 'sac__empenhoprojeto' as origem, 'bronze_sac_empenhoprojeto' as destino
    union all select 'sac__enquadramento' as origem, 'bronze_sac_enquadramento' as destino
    union all select 'sac__entregaproduto' as origem, 'bronze_sac_entregaproduto' as destino
    union all select 'sac__estados' as origem, 'bronze_sac_estados' as destino
    union all select 'sac__etiqueta' as origem, 'bronze_sac_etiqueta' as destino
    union all select 'sac__evolucao' as origem, 'bronze_sac_evolucao' as destino
    union all select 'sac__fase' as origem, 'bronze_sac_fase' as destino
    union all select 'sac__fluxos' as origem, 'bronze_sac_fluxos' as destino
    union all select 'sac__fluxosprojeto' as origem, 'bronze_sac_fluxosprojeto' as destino
    union all select 'sac__fonte' as origem, 'bronze_sac_fonte' as destino
    union all select 'sac__genero' as origem, 'bronze_sac_genero' as destino
    union all select 'sac__geralext' as origem, 'bronze_sac_geralext' as destino
    union all select 'sac__grupoempresarial' as origem, 'bronze_sac_grupoempresarial' as destino
    union all select 'sac__guiarecolhimento' as origem, 'bronze_sac_guiarecolhimento' as destino
    union all select 'sac__historicoaspectosfinanceirosx' as origem, 'bronze_sac_historicoaspectosfinanceirosx' as destino
    union all select 'sac__historicoaspectosfisicosx' as origem, 'bronze_sac_historicoaspectosfisicosx' as destino
    union all select 'sac__historicoaspectosresultado' as origem, 'bronze_sac_historicoaspectosresultado' as destino
    union all select 'sac__historicoautuacaox' as origem, 'bronze_sac_historicoautuacaox' as destino
    union all select 'sac__historicocarta' as origem, 'bronze_sac_historicocarta' as destino
    union all select 'sac__historicodelecaoaprovacao' as origem, 'bronze_sac_historicodelecaoaprovacao' as destino
    union all select 'sac__historicoinabilitado' as origem, 'bronze_sac_historicoinabilitado' as destino
    union all select 'sac__historicosituacao' as origem, 'bronze_sac_historicosituacao' as destino
    union all select 'sac__historicounidade' as origem, 'bronze_sac_historicounidade' as destino
    union all select 'sac__humanidades' as origem, 'bronze_sac_humanidades' as destino
    union all select 'sac__impactox' as origem, 'bronze_sac_impactox' as destino
    union all select 'sac__inabilitado' as origem, 'bronze_sac_inabilitado' as destino
    union all select 'sac__incentivoporcgccpf' as origem, 'bronze_sac_incentivoporcgccpf' as destino
    union all select 'sac__informacaotrimestralx' as origem, 'bronze_sac_informacaotrimestralx' as destino
    union all select 'sac__instrumentobanda' as origem, 'bronze_sac_instrumentobanda' as destino
    union all select 'sac__interessado' as origem, 'bronze_sac_interessado' as destino
    union all select 'sac__internet' as origem, 'bronze_sac_internet' as destino
    union all select 'sac__intranet' as origem, 'bronze_sac_intranet' as destino
    union all select 'sac__kitbanda' as origem, 'bronze_sac_kitbanda' as destino
    union all select 'sac__kitentregue' as origem, 'bronze_sac_kitentregue' as destino
    union all select 'sac__preprojeto' as origem, 'bronze_sac_preprojeto' as destino
    union all select 'sac__projetos' as origem, 'bronze_sac_projetos' as destino
    union all select 'sac__situacao' as origem, 'bronze_sac_situacao' as destino
    union all select 'sac__tbapiaprovacoes' as origem, 'bronze_sac_tbapiaprovacoes' as destino
    union all select 'sac__tbapicomprovacoes' as origem, 'bronze_sac_tbapicomprovacoes' as destino
    union all select 'sac__tbapiincentivos' as origem, 'bronze_sac_tbapiincentivos' as destino
    union all select 'sac__tbcomplianceitemcomprovado' as origem, 'bronze_sac_tbcomplianceitemcomprovado' as destino
    union all select 'sac__tbcomprovacaopassivo' as origem, 'bronze_sac_tbcomprovacaopassivo' as destino
    union all select 'sac__tbcomprovantebeneficiario' as origem, 'bronze_sac_tbcomprovantebeneficiario' as destino
    union all select 'sac__tbcomprovanteexecucao' as origem, 'bronze_sac_tbcomprovanteexecucao' as destino
    union all select 'sac__tbcomprovantetrimestral' as origem, 'bronze_sac_tbcomprovantetrimestral' as destino
    union all select 'sac__tbcomunicados' as origem, 'bronze_sac_tbcomunicados' as destino
    union all select 'sac__tbconfigurarpagamento' as origem, 'bronze_sac_tbconfigurarpagamento' as destino
    union all select 'sac__tbconfigurarpagamentoxtbassinantes' as origem, 'bronze_sac_tbconfigurarpagamentoxtbassinantes' as destino
    union all select 'sac__tbcontabancariabloqueada' as origem, 'bronze_sac_tbcontabancariabloqueada' as destino
    union all select 'sac__tbcontacartao' as origem, 'bronze_sac_tbcontacartao' as destino
    union all select 'sac__tbcontacartaovip036' as origem, 'bronze_sac_tbcontacartaovip036' as destino
    union all select 'sac__tbcovid' as origem, 'bronze_sac_tbcovid' as destino
    union all select 'sac__tbcredenciaparecerista_2020' as origem, 'bronze_sac_tbcredenciaparecerista_2020' as destino
    union all select 'sac__tbcumprimentoobjeto' as origem, 'bronze_sac_tbcumprimentoobjeto' as destino
    union all select 'sac__tbcumprimentoobjetoxarquivo' as origem, 'bronze_sac_tbcumprimentoobjetoxarquivo' as destino
    union all select 'sac__tbcustosvinculados' as origem, 'bronze_sac_tbcustosvinculados' as destino
    union all select 'sac__tbdeclaracaobeneficiosfiscais' as origem, 'bronze_sac_tbdeclaracaobeneficiosfiscais' as destino
    union all select 'sac__tbdemandasdeorgaosdecontrole' as origem, 'bronze_sac_tbdemandasdeorgaosdecontrole' as destino
    union all select 'sac__tbdemandasdeorgaosdecontrolexprojetos' as origem, 'bronze_sac_tbdemandasdeorgaosdecontrolexprojetos' as destino
    union all select 'sac__tbdeparaplanilhaaprovacao' as origem, 'bronze_sac_tbdeparaplanilhaaprovacao' as destino
    union all select 'sac__tbdepositoidentificadocaptacao' as origem, 'bronze_sac_tbdepositoidentificadocaptacao' as destino
    union all select 'sac__tbdepositoidentificadocaptacaoold' as origem, 'bronze_sac_tbdepositoidentificadocaptacaoold' as destino
    union all select 'sac__tbdepositoidentificadomovimentacao' as origem, 'bronze_sac_tbdepositoidentificadomovimentacao' as destino
    union all select 'sac__tbdescricaorelatorioconsolidado' as origem, 'bronze_sac_tbdescricaorelatorioconsolidado' as destino
    union all select 'sac__tbdeslocamento' as origem, 'bronze_sac_tbdeslocamento' as destino
    union all select 'sac__tbdespacho' as origem, 'bronze_sac_tbdespacho' as destino
    union all select 'sac__tbdetalhaplanodistribuicao' as origem, 'bronze_sac_tbdetalhaplanodistribuicao' as destino
    union all select 'sac__tbdetalhaplanodistribuicaoreadequacao' as origem, 'bronze_sac_tbdetalhaplanodistribuicaoreadequacao' as destino
    union all select 'sac__tbdevolucaosecretaria' as origem, 'bronze_sac_tbdevolucaosecretaria' as destino
    union all select 'sac__tbdiligencia' as origem, 'bronze_sac_tbdiligencia' as destino
    union all select 'sac__tbdiligenciaxarquivo' as origem, 'bronze_sac_tbdiligenciaxarquivo' as destino
    union all select 'sac__tbdispositivomovel' as origem, 'bronze_sac_tbdispositivomovel' as destino
    union all select 'sac__tbdistribuicaoproduto' as origem, 'bronze_sac_tbdistribuicaoproduto' as destino
    union all select 'sac__tbdistribuirdemandasdeorgaosdecontrole' as origem, 'bronze_sac_tbdistribuirdemandasdeorgaosdecontrole' as destino
    union all select 'sac__tbdistribuirparecer' as origem, 'bronze_sac_tbdistribuirparecer' as destino
    union all select 'sac__tbdistribuirprojeto' as origem, 'bronze_sac_tbdistribuirprojeto' as destino
    union all select 'sac__tbdistribuirprojetopassivo' as origem, 'bronze_sac_tbdistribuirprojetopassivo' as destino
    union all select 'sac__tbdistribuirreadequacao' as origem, 'bronze_sac_tbdistribuirreadequacao' as destino
    union all select 'sac__tbdocumento20100107_1x' as origem, 'bronze_sac_tbdocumento20100107_1x' as destino
    union all select 'sac__tbdocumento20100107x' as origem, 'bronze_sac_tbdocumento20100107x' as destino
    union all select 'sac__tbdocumentoaceitacao' as origem, 'bronze_sac_tbdocumentoaceitacao' as destino
    union all select 'sac__tbdocumentoassinatura' as origem, 'bronze_sac_tbdocumentoassinatura' as destino
    union all select 'sac__tbdocumentodespachoassinatura' as origem, 'bronze_sac_tbdocumentodespachoassinatura' as destino
    union all select 'sac__tbdocumentosagentes20100107x' as origem, 'bronze_sac_tbdocumentosagentes20100107x' as destino
    union all select 'sac__tbdocumentospreprojeto20100107x' as origem, 'bronze_sac_tbdocumentospreprojeto20100107x' as destino
    union all select 'sac__tbfiscalizacao' as origem, 'bronze_sac_tbfiscalizacao' as destino
    union all select 'sac__tbfundopatrimonial' as origem, 'bronze_sac_tbfundopatrimonial' as destino
    union all select 'sac__tbfundopatrimonialusuario' as origem, 'bronze_sac_tbfundopatrimonialusuario' as destino
    union all select 'sac__tbgerarpagamentoparecerista' as origem, 'bronze_sac_tbgerarpagamentoparecerista' as destino
    union all select 'sac__tbhistoricoalteracaodocumento' as origem, 'bronze_sac_tbhistoricoalteracaodocumento' as destino
    union all select 'sac__tbhistoricoalteracaoprojeto' as origem, 'bronze_sac_tbhistoricoalteracaoprojeto' as destino
    union all select 'sac__tbhistoricocontabloqueada' as origem, 'bronze_sac_tbhistoricocontabloqueada' as destino
    union all select 'sac__tbhistoricodevolucaofiscalizacao' as origem, 'bronze_sac_tbhistoricodevolucaofiscalizacao' as destino
    union all select 'sac__tbhistoricodiligencia' as origem, 'bronze_sac_tbhistoricodiligencia' as destino
    union all select 'sac__tbhistoricodocumento' as origem, 'bronze_sac_tbhistoricodocumento' as destino
    union all select 'sac__tbhistoricoemail' as origem, 'bronze_sac_tbhistoricoemail' as destino
    union all select 'sac__tbhistoricoexclusaoconta' as origem, 'bronze_sac_tbhistoricoexclusaoconta' as destino
    union all select 'sac__tbhomologacao' as origem, 'bronze_sac_tbhomologacao' as destino
    union all select 'sac__tbimovel' as origem, 'bronze_sac_tbimovel' as destino
    union all select 'sac__tbinadimplente' as origem, 'bronze_sac_tbinadimplente' as destino
    union all select 'sac__tbitensplanilhaproduto' as origem, 'bronze_sac_tbitensplanilhaproduto' as destino
    union all select 'sac__tblancamentobancario' as origem, 'bronze_sac_tblancamentobancario' as destino
    union all select 'sac__tblancamentobancarioxcomprovantepagamento' as origem, 'bronze_sac_tblancamentobancarioxcomprovantepagamento' as destino
    union all select 'sac__tblaudofinal' as origem, 'bronze_sac_tblaudofinal' as destino
    union all select 'sac__tblogintentativasacesso' as origem, 'bronze_sac_tblogintentativasacesso' as destino
    union all select 'sac__tblogomarca' as origem, 'bronze_sac_tblogomarca' as destino
    union all select 'sac__tblote' as origem, 'bronze_sac_tblote' as destino
    union all select 'sac__tbmanterportaria' as origem, 'bronze_sac_tbmanterportaria' as destino
    union all select 'sac__tbmensagem' as origem, 'bronze_sac_tbmensagem' as destino
    union all select 'sac__tbmensagemdispositivomovel' as origem, 'bronze_sac_tbmensagemdispositivomovel' as destino
    union all select 'sac__tbmodelotermodecisao' as origem, 'bronze_sac_tbmodelotermodecisao' as destino
    union all select 'sac__tbmonitoramentoadistancia' as origem, 'bronze_sac_tbmonitoramentoadistancia' as destino
    union all select 'sac__tbmonitoramentoprojeto' as origem, 'bronze_sac_tbmonitoramentoprojeto' as destino
    union all select 'sac__tbmotivodevolucao' as origem, 'bronze_sac_tbmotivodevolucao' as destino
    union all select 'sac__tbmovimentacao' as origem, 'bronze_sac_tbmovimentacao' as destino
    union all select 'sac__tbmovimentacaobancaria' as origem, 'bronze_sac_tbmovimentacaobancaria' as destino
    union all select 'sac__tbmovimentacaobancariaitem' as origem, 'bronze_sac_tbmovimentacaobancariaitem' as destino
    union all select 'sac__tbmovimentacaobancariaitemxtipoinconsistencia' as origem, 'bronze_sac_tbmovimentacaobancariaitemxtipoinconsistencia' as destino
    union all select 'sac__tbnormativos' as origem, 'bronze_sac_tbnormativos' as destino
    union all select 'sac__tbnotafiscaleletronica' as origem, 'bronze_sac_tbnotafiscaleletronica' as destino
    union all select 'sac__tbnotificacao' as origem, 'bronze_sac_tbnotificacao' as destino
    union all select 'sac__tbopinarprojeto' as origem, 'bronze_sac_tbopinarprojeto' as destino
    union all select 'sac__tborgaofiscalizador' as origem, 'bronze_sac_tborgaofiscalizador' as destino
    union all select 'sac__tbpagamentopareceristaxarquivo' as origem, 'bronze_sac_tbpagamentopareceristaxarquivo' as destino
    union all select 'sac__tbpagarparecerista' as origem, 'bronze_sac_tbpagarparecerista' as destino
    union all select 'sac__tbpagarpareceristareadequacao' as origem, 'bronze_sac_tbpagarpareceristareadequacao' as destino
    union all select 'sac__tbpagarpareceristarecurso' as origem, 'bronze_sac_tbpagarpareceristarecurso' as destino
    union all select 'sac__tbparecerconsolidado' as origem, 'bronze_sac_tbparecerconsolidado' as destino
    union all select 'sac__tbpareceristaafastamento' as origem, 'bronze_sac_tbpareceristaafastamento' as destino
    union all select 'sac__tbpareceristaedital' as origem, 'bronze_sac_tbpareceristaedital' as destino
    union all select 'sac__tbpareceristaempenho' as origem, 'bronze_sac_tbpareceristaempenho' as destino
    union all select 'sac__tbpareceristainexigibilidade' as origem, 'bronze_sac_tbpareceristainexigibilidade' as destino
    union all select 'sac__tbpareceristalotepagamento' as origem, 'bronze_sac_tbpareceristalotepagamento' as destino
    union all select 'sac__tbpareceristapagamento' as origem, 'bronze_sac_tbpareceristapagamento' as destino
    union all select 'sac__tbpareceristaprojetoredistribuido' as origem, 'bronze_sac_tbpareceristaprojetoredistribuido' as destino
    union all select 'sac__tbpareceristaprorrogarprazo' as origem, 'bronze_sac_tbpareceristaprorrogarprazo' as destino
    union all select 'sac__tbpareceristaprorrogarprazoxdistribuicao' as origem, 'bronze_sac_tbpareceristaprorrogarprazoxdistribuicao' as destino
    union all select 'sac__tbpareceristaquestionamento' as origem, 'bronze_sac_tbpareceristaquestionamento' as destino
    union all select 'sac__tbpareceristavaloranalisado' as origem, 'bronze_sac_tbpareceristavaloranalisado' as destino
    union all select 'sac__tbplanilhaaprovacao' as origem, 'bronze_sac_tbplanilhaaprovacao' as destino
    union all select 'sac__tbplanilhadesembolso' as origem, 'bronze_sac_tbplanilhadesembolso' as destino
    union all select 'sac__tbplanilhaetapa' as origem, 'bronze_sac_tbplanilhaetapa' as destino
    union all select 'sac__tbplanilhaitens' as origem, 'bronze_sac_tbplanilhaitens' as destino
    union all select 'sac__tbplanilhajustificativa' as origem, 'bronze_sac_tbplanilhajustificativa' as destino
    union all select 'sac__tbplanilhaprojeto' as origem, 'bronze_sac_tbplanilhaprojeto' as destino
    union all select 'sac__tbplanilhaproposta' as origem, 'bronze_sac_tbplanilhaproposta' as destino
    union all select 'sac__tbplanilhaunidade' as origem, 'bronze_sac_tbplanilhaunidade' as destino
    union all select 'sac__tbplanodistribuicao' as origem, 'bronze_sac_tbplanodistribuicao' as destino
    union all select 'sac__tbplanodivulgacao' as origem, 'bronze_sac_tbplanodivulgacao' as destino
    union all select 'sac__tbpreprojetometa' as origem, 'bronze_sac_tbpreprojetometa' as destino
    union all select 'sac__tbpreprojetoxenquadramento' as origem, 'bronze_sac_tbpreprojetoxenquadramento' as destino
    union all select 'sac__tbprioridadedeanalise' as origem, 'bronze_sac_tbprioridadedeanalise' as destino
    union all select 'sac__tbprodutoxtipicidade' as origem, 'bronze_sac_tbprodutoxtipicidade' as destino
    union all select 'sac__tbprogramarouanet' as origem, 'bronze_sac_tbprogramarouanet' as destino
    union all select 'sac__tbprogramarouanetareasegmento' as origem, 'bronze_sac_tbprogramarouanetareasegmento' as destino
    union all select 'sac__tbprogramarouanetavaliacao' as origem, 'bronze_sac_tbprogramarouanetavaliacao' as destino
    union all select 'sac__tbprogramarouanetavaliacaorecurso' as origem, 'bronze_sac_tbprogramarouanetavaliacaorecurso' as destino
    union all select 'sac__tbprogramarouanetavaliador' as origem, 'bronze_sac_tbprogramarouanetavaliador' as destino
    union all select 'sac__tbprogramarouanetdistribuirproposta' as origem, 'bronze_sac_tbprogramarouanetdistribuirproposta' as destino
    union all select 'sac__tbprogramarouanethabilitacao' as origem, 'bronze_sac_tbprogramarouanethabilitacao' as destino
    union all select 'sac__tbprogramarouanetlocalidade' as origem, 'bronze_sac_tbprogramarouanetlocalidade' as destino
    union all select 'sac__tbprogramarouanetpergunta' as origem, 'bronze_sac_tbprogramarouanetpergunta' as destino
    union all select 'sac__tbprogramarouanetpropostaselecionada' as origem, 'bronze_sac_tbprogramarouanetpropostaselecionada' as destino
    union all select 'sac__tbprogramarouanetrecurso' as origem, 'bronze_sac_tbprogramarouanetrecurso' as destino
    union all select 'sac__tbprogramarouanetresultadofinal' as origem, 'bronze_sac_tbprogramarouanetresultadofinal' as destino
    union all select 'sac__tbprogramarouanetresultadopreliminar' as origem, 'bronze_sac_tbprogramarouanetresultadopreliminar' as destino
    union all select 'sac__tbprogramarouanetselecaopatrocinador' as origem, 'bronze_sac_tbprogramarouanetselecaopatrocinador' as destino
    union all select 'sac__tbprogramarouanetxproposta' as origem, 'bronze_sac_tbprogramarouanetxproposta' as destino
    union all select 'sac__tbprojetofase' as origem, 'bronze_sac_tbprojetofase' as destino
    union all select 'sac__tbprojetoreadequacao' as origem, 'bronze_sac_tbprojetoreadequacao' as destino
    union all select 'sac__tbprojetorecebedorrecurso' as origem, 'bronze_sac_tbprojetorecebedorrecurso' as destino
    union all select 'sac__tbprojetosclonados' as origem, 'bronze_sac_tbprojetosclonados' as destino
    union all select 'sac__tbprojetosituacaoanterior' as origem, 'bronze_sac_tbprojetosituacaoanterior' as destino
    union all select 'sac__tbproposta' as origem, 'bronze_sac_tbproposta' as destino
    union all select 'sac__tbreadequacao' as origem, 'bronze_sac_tbreadequacao' as destino
    union all select 'sac__tbreadequacaoxparecer' as origem, 'bronze_sac_tbreadequacaoxparecer' as destino
    union all select 'sac__tbreadequacaoxtbdiligencia' as origem, 'bronze_sac_tbreadequacaoxtbdiligencia' as destino
    union all select 'sac__tbrecurso' as origem, 'bronze_sac_tbrecurso' as destino
    union all select 'sac__tbrecursoproposta' as origem, 'bronze_sac_tbrecursoproposta' as destino
    union all select 'sac__tbrecursoxplanilhaaprovacao' as origem, 'bronze_sac_tbrecursoxplanilhaaprovacao' as destino
    union all select 'sac__tbrelatorio' as origem, 'bronze_sac_tbrelatorio' as destino
    union all select 'sac__vcapmecaudioanoprojeto' as origem, 'bronze_sac_vcapmecaudioanoprojeto' as destino
    union all select 'sac__vcapmecaudioprojeto' as origem, 'bronze_sac_vcapmecaudioprojeto' as destino
    union all select 'sac__vcaptacaoanoufareasegmentosemestre' as origem, 'bronze_sac_vcaptacaoanoufareasegmentosemestre' as destino
    union all select 'sac__vcaptacaopormptipoapoiomecena' as origem, 'bronze_sac_vcaptacaopormptipoapoiomecena' as destino
    union all select 'sac__vcaptado' as origem, 'bronze_sac_vcaptado' as destino
    union all select 'sac__vcaptadoano' as origem, 'bronze_sac_vcaptadoano' as destino
    union all select 'sac__vcaptadoanomesdiaprojeto' as origem, 'bronze_sac_vcaptadoanomesdiaprojeto' as destino
    union all select 'sac__vcaptadoanoprojeto' as origem, 'bronze_sac_vcaptadoanoprojeto' as destino
    union all select 'sac__vcartaaudio' as origem, 'bronze_sac_vcartaaudio' as destino
    union all select 'sac__vcartapropaudio' as origem, 'bronze_sac_vcartapropaudio' as destino
    union all select 'sac__vcartas' as origem, 'bronze_sac_vcartas' as destino
    union all select 'sac__vchecaproponente' as origem, 'bronze_sac_vchecaproponente' as destino
    union all select 'sac__vchecaqteinabilitacao' as origem, 'bronze_sac_vchecaqteinabilitacao' as destino
    union all select 'sac__vci' as origem, 'bronze_sac_vci' as destino
    union all select 'sac__vciset' as origem, 'bronze_sac_vciset' as destino
    union all select 'sac__vcnpjcpf' as origem, 'bronze_sac_vcnpjcpf' as destino
    union all select 'sac__vcnpjvalido' as origem, 'bronze_sac_vcnpjvalido' as destino
    union all select 'sac__vcomparativoano' as origem, 'bronze_sac_vcomparativoano' as destino
    union all select 'sac__vcomparativoanomec' as origem, 'bronze_sac_vcomparativoanomec' as destino
    union all select 'sac__vcontabancaria' as origem, 'bronze_sac_vcontabancaria' as destino
    union all select 'sac__vcontacorrente' as origem, 'bronze_sac_vcontacorrente' as destino
    union all select 'sac__vconvenioanoarea' as origem, 'bronze_sac_vconvenioanoarea' as destino
    union all select 'sac__vconvenioanouf' as origem, 'bronze_sac_vconvenioanouf' as destino
    union all select 'sac__vcustodoprojeto' as origem, 'bronze_sac_vcustodoprojeto' as destino
    union all select 'sac__vdadoscomplementaresdecartas' as origem, 'bronze_sac_vdadoscomplementaresdecartas' as destino
    union all select 'sac__vdadosconvenio' as origem, 'bronze_sac_vdadosconvenio' as destino
    union all select 'sac__vdadosportariaaprovacao' as origem, 'bronze_sac_vdadosportariaaprovacao' as destino
    union all select 'sac__vdiligencias' as origem, 'bronze_sac_vdiligencias' as destino
    union all select 'sac__vdocumentoexigido' as origem, 'bronze_sac_vdocumentoexigido' as destino
    union all select 'sac__vdtprimeiraultimacaptacao' as origem, 'bronze_sac_vdtprimeiraultimacaptacao' as destino
    union all select 'sac__veiculacao' as origem, 'bronze_sac_veiculacao' as destino
    union all select 'sac__vempresasincentivadorasano' as origem, 'bronze_sac_vempresasincentivadorasano' as destino
    union all select 'sac__verificacao' as origem, 'bronze_sac_verificacao' as destino
    union all select 'sac__verificacaopecaxveiculo' as origem, 'bronze_sac_verificacaopecaxveiculo' as destino
    union all select 'sac__vetiquetaeneida' as origem, 'bronze_sac_vetiquetaeneida' as destino
    union all select 'sac__vfnaprocapproj' as origem, 'bronze_sac_vfnaprocapproj' as destino
    union all select 'sac__vfnaprocapprojmec' as origem, 'bronze_sac_vfnaprocapprojmec' as destino
    union all select 'sac__vfninccaprenuncia' as origem, 'bronze_sac_vfninccaprenuncia' as destino
    union all select 'sac__vfnorcdetalhado' as origem, 'bronze_sac_vfnorcdetalhado' as destino
    union all select 'sac__vfntotalaprocapproj' as origem, 'bronze_sac_vfntotalaprocapproj' as destino
    union all select 'sac__vforadaportaria' as origem, 'bronze_sac_vforadaportaria' as destino
    union all select 'sac__vfsac3485' as origem, 'bronze_sac_vfsac3485' as destino
    union all select 'sac__vfunarte' as origem, 'bronze_sac_vfunarte' as destino
    union all select 'sac__vgerentes' as origem, 'bronze_sac_vgerentes' as destino
    union all select 'sac__vguianaovinculada' as origem, 'bronze_sac_vguianaovinculada' as destino
    union all select 'sac__vguiasderecolhimento' as origem, 'bronze_sac_vguiasderecolhimento' as destino
    union all select 'sac__vincentivador' as origem, 'bronze_sac_vincentivador' as destino
    union all select 'sac__vincentivadorano' as origem, 'bronze_sac_vincentivadorano' as destino
    union all select 'sac__vincentivadoraudioano' as origem, 'bronze_sac_vincentivadoraudioano' as destino
    union all select 'sac__vincentivadoresma' as origem, 'bronze_sac_vincentivadoresma' as destino
    union all select 'sac__vincentivadorpessoaano' as origem, 'bronze_sac_vincentivadorpessoaano' as destino
    union all select 'sac__vincentivadorporprojeto' as origem, 'bronze_sac_vincentivadorporprojeto' as destino
    union all select 'sac__vinvestimentominc' as origem, 'bronze_sac_vinvestimentominc' as destino
    union all select 'sac__vliberado' as origem, 'bronze_sac_vliberado' as destino
    union all select 'sac__vmaioresincentivadoresprojetos' as origem, 'bronze_sac_vmaioresincentivadoresprojetos' as destino
    union all select 'sac__vmostraconvenio' as origem, 'bronze_sac_vmostraconvenio' as destino
    union all select 'sac__vmostraempenho' as origem, 'bronze_sac_vmostraempenho' as destino
    union all select 'sac__vmostraordembancaria' as origem, 'bronze_sac_vmostraordembancaria' as destino
    union all select 'sac__vnacional' as origem, 'bronze_sac_vnacional' as destino
    union all select 'sac__vnegativainabilitado' as origem, 'bronze_sac_vnegativainabilitado' as destino
    union all select 'sac__vnegativainabilitadoetiqueta' as origem, 'bronze_sac_vnegativainabilitadoetiqueta' as destino
    union all select 'sac__vnuncacaptou' as origem, 'bronze_sac_vnuncacaptou' as destino
    union all select 'sac__voutrasinformacoesaudio' as origem, 'bronze_sac_voutrasinformacoesaudio' as destino
    union all select 'sac__vparecerinicial' as origem, 'bronze_sac_vparecerinicial' as destino
    union all select 'sac__vpautadereuniaocnic' as origem, 'bronze_sac_vpautadereuniaocnic' as destino
    union all select 'sac__vpautadereuniaofncintercambio' as origem, 'bronze_sac_vpautadereuniaofncintercambio' as destino
    union all select 'sac__vportariaaprovado' as origem, 'bronze_sac_vportariaaprovado' as destino
    union all select 'sac__vportariacomplementacao' as origem, 'bronze_sac_vportariacomplementacao' as destino
    union all select 'sac__vportarianormal' as origem, 'bronze_sac_vportarianormal' as destino
    union all select 'sac__vportariaprorrogacao' as origem, 'bronze_sac_vportariaprorrogacao' as destino
    union all select 'sac__vportariareducao' as origem, 'bronze_sac_vportariareducao' as destino
    union all select 'sac__vprazocaptacao' as origem, 'bronze_sac_vprazocaptacao' as destino
    union all select 'sac__vprestacao' as origem, 'bronze_sac_vprestacao' as destino
    union all select 'sac__vprojetoavaliado' as origem, 'bronze_sac_vprojetoavaliado' as destino
    union all select 'sac__vprojetodiligencia' as origem, 'bronze_sac_vprojetodiligencia' as destino
    union all select 'sac__vprojetosemanalise' as origem, 'bronze_sac_vprojetosemanalise' as destino
    union all select 'sac__vprojetosemexecucao' as origem, 'bronze_sac_vprojetosemexecucao' as destino
    union all select 'sac__vprojetosexecutados' as origem, 'bronze_sac_vprojetosexecutados' as destino
    union all select 'sac__vprojprazodecaptvencnoexerc' as origem, 'bronze_sac_vprojprazodecaptvencnoexerc' as destino
    union all select 'sac__vpropapreaproexec' as origem, 'bronze_sac_vpropapreaproexec' as destino
    union all select 'sac__vproponenteanouf' as origem, 'bronze_sac_vproponenteanouf' as destino
    union all select 'sac__vproponenteavaliado' as origem, 'bronze_sac_vproponenteavaliado' as destino
    union all select 'sac__vproponentecaptacaoanouf' as origem, 'bronze_sac_vproponentecaptacaoanouf' as destino
    union all select 'sac__vproponentecommaisdecincoproj' as origem, 'bronze_sac_vproponentecommaisdecincoproj' as destino
    union all select 'sac__vproponentemecenato' as origem, 'bronze_sac_vproponentemecenato' as destino
    union all select 'sac__vproponenteporarea' as origem, 'bronze_sac_vproponenteporarea' as destino
    union all select 'sac__vproponenteprojetos' as origem, 'bronze_sac_vproponenteprojetos' as destino
    union all select 'sac__vproponenteprojetosongs' as origem, 'bronze_sac_vproponenteprojetosongs' as destino
    union all select 'sac__vproponentetipopessoa' as origem, 'bronze_sac_vproponentetipopessoa' as destino
    union all select 'sac__vpropprojinabilitados' as origem, 'bronze_sac_vpropprojinabilitados' as destino
    union all select 'sac__vpropquereceberbandadenovo' as origem, 'bronze_sac_vpropquereceberbandadenovo' as destino
    union all select 'sac__vproprecebeubanda' as origem, 'bronze_sac_vproprecebeubanda' as destino
    union all select 'sac__vpropufareaseg' as origem, 'bronze_sac_vpropufareaseg' as destino
    union all select 'sac__vprorrogacao' as origem, 'bronze_sac_vprorrogacao' as destino
    union all select 'sac__vqtdeprojaproanouf' as origem, 'bronze_sac_vqtdeprojaproanouf' as destino
    union all select 'sac__vrankbanda' as origem, 'bronze_sac_vrankbanda' as destino
    union all select 'sac__vregularidade' as origem, 'bronze_sac_vregularidade' as destino
    union all select 'sac__vrenunciaanoareageral' as origem, 'bronze_sac_vrenunciaanoareageral' as destino
    union all select 'sac__vrenunciaanoareaufgeral' as origem, 'bronze_sac_vrenunciaanoareaufgeral' as destino
    union all select 'sac__vrenunciafiscal' as origem, 'bronze_sac_vrenunciafiscal' as destino
    union all select 'sac__vrenunciafiscalanoarea' as origem, 'bronze_sac_vrenunciafiscalanoarea' as destino
    union all select 'sac__vrenunciafiscalgeral' as origem, 'bronze_sac_vrenunciafiscalgeral' as destino
    union all select 'sac__vrenunciageralporanoprojeto' as origem, 'bronze_sac_vrenunciageralporanoprojeto' as destino
    union all select 'sac__vrenunciaporano' as origem, 'bronze_sac_vrenunciaporano' as destino
    union all select 'sac__vrenunciaporincentivador' as origem, 'bronze_sac_vrenunciaporincentivador' as destino
    union all select 'sac__vricardoreis' as origem, 'bronze_sac_vricardoreis' as destino
    union all select 'sac__vsaldoaprovado' as origem, 'bronze_sac_vsaldoaprovado' as destino
    union all select 'sac__vsaldoaprovadoporano' as origem, 'bronze_sac_vsaldoaprovadoporano' as destino
    union all select 'sac__vsaldoquotascav' as origem, 'bronze_sac_vsaldoquotascav' as destino
    union all select 'sac__vsegmento' as origem, 'bronze_sac_vsegmento' as destino
    union all select 'sac__vtemposemcaptar' as origem, 'bronze_sac_vtemposemcaptar' as destino
    union all select 'sac__vtermoaditivo' as origem, 'bronze_sac_vtermoaditivo' as destino
    union all select 'sac__vtermodecompromisso' as origem, 'bronze_sac_vtermodecompromisso' as destino
    union all select 'sac__vtodaaprovacao' as origem, 'bronze_sac_vtodaaprovacao' as destino
    union all select 'sac__vtotalaprovado' as origem, 'bronze_sac_vtotalaprovado' as destino
    union all select 'sac__vtotalcaptado' as origem, 'bronze_sac_vtotalcaptado' as destino
    union all select 'sac__vultimafase' as origem, 'bronze_sac_vultimafase' as destino
    union all select 'sac__vusuarios' as origem, 'bronze_sac_vusuarios' as destino
    union all select 'sac__vvaloresprojeto' as origem, 'bronze_sac_vvaloresprojeto' as destino
    union all select 'sac__vversepodeprorrogar' as origem, 'bronze_sac_vversepodeprorrogar' as destino
    union all select 'sac__vw_captacao_ano_uf' as origem, 'bronze_sac_vw_captacao_ano_uf' as destino
    union all select 'sac__vw_dba_captacao_2007_2017' as origem, 'bronze_sac_vw_dba_captacao_2007_2017' as destino
    union all select 'sac__vw_pc_digital_avaliados_emavaliacao' as origem, 'bronze_sac_vw_pc_digital_avaliados_emavaliacao' as destino
    union all select 'sac__vw_prol_vinc_agu_dis' as origem, 'bronze_sac_vw_prol_vinc_agu_dis' as destino
    union all select 'sac__vwagentesseusprojetos' as origem, 'bronze_sac_vwagentesseusprojetos' as destino
    union all select 'sac__vwalteraranalistaproposta' as origem, 'bronze_sac_vwalteraranalistaproposta' as destino
    union all select 'sac__vwalterarorgao' as origem, 'bronze_sac_vwalterarorgao' as destino
    union all select 'sac__vwalterarprojeto' as origem, 'bronze_sac_vwalterarprojeto' as destino
    union all select 'sac__vwalterarsituacao' as origem, 'bronze_sac_vwalterarsituacao' as destino
    union all select 'sac__vwanalisedeconteudo' as origem, 'bronze_sac_vwanalisedeconteudo' as destino
    union all select 'sac__vwanalisedecusto' as origem, 'bronze_sac_vwanalisedecusto' as destino
    union all select 'sac__vwanalisedocumentalportecnico' as origem, 'bronze_sac_vwanalisedocumentalportecnico' as destino
    union all select 'sac__vwanalisefinanceiravirtual' as origem, 'bronze_sac_vwanalisefinanceiravirtual' as destino
    union all select 'sac__vwanalisevisualportecnico' as origem, 'bronze_sac_vwanalisevisualportecnico' as destino
    union all select 'sac__vwanexarcomprovantes' as origem, 'bronze_sac_vwanexarcomprovantes' as destino
    union all select 'sac__vwanexardocumentoagente' as origem, 'bronze_sac_vwanexardocumentoagente' as destino
    union all select 'sac__vwinformacoes_tcu_etce' as origem, 'bronze_sac_vwinformacoes_tcu_etce' as destino
    union all select 'sac__vwinformacoesconsolidadasparaavaliacaofinanceira' as origem, 'bronze_sac_vwinformacoesconsolidadasparaavaliacaofinanceira' as destino
    union all select 'sac__vwinformacoesdoatoadministrativo' as origem, 'bronze_sac_vwinformacoesdoatoadministrativo' as destino
    union all select 'sac__vwitemduplicado' as origem, 'bronze_sac_vwitemduplicado' as destino
    union all select 'sac__vwitemorcamentarioimpugnado' as origem, 'bronze_sac_vwitemorcamentarioimpugnado' as destino
    union all select 'sac__vwitenscomprovados_fonte_produto_etapa_uf_municipio' as origem, 'bronze_sac_vwitenscomprovados_fonte_produto_etapa_uf_municipio' as destino
    union all select 'sac__vwitensorcamentarioscomprovados' as origem, 'bronze_sac_vwitensorcamentarioscomprovados' as destino
    union all select 'sac__vwlancamentobancariosemcomprovacao' as origem, 'bronze_sac_vwlancamentobancariosemcomprovacao' as destino
    union all select 'sac__vwlaudofinalavaliacaoderesultadodoprojeto' as origem, 'bronze_sac_vwlaudofinalavaliacaoderesultadodoprojeto' as destino
    union all select 'sac__vwlaudofinalderesultadopassivo' as origem, 'bronze_sac_vwlaudofinalderesultadopassivo' as destino
    union all select 'sac__vwliberacao' as origem, 'bronze_sac_vwliberacao' as destino
    union all select 'sac__vwlistarcaptacoes' as origem, 'bronze_sac_vwlistarcaptacoes' as destino
    union all select 'sac__vwlistarpropostas' as origem, 'bronze_sac_vwlistarpropostas' as destino
    union all select 'sac__vwlotedocumento' as origem, 'bronze_sac_vwlotedocumento' as destino
    union all select 'sac__vwmaioresincentivadoresporanouf' as origem, 'bronze_sac_vwmaioresincentivadoresporanouf' as destino
    union all select 'sac__vwmaioresproponentesporanouf' as origem, 'bronze_sac_vwmaioresproponentesporanouf' as destino
    union all select 'sac__vwmediana' as origem, 'bronze_sac_vwmediana' as destino
    union all select 'sac__vwmemoriadecalculo' as origem, 'bronze_sac_vwmemoriadecalculo' as destino
    union all select 'sac__vwmetascomprovadas' as origem, 'bronze_sac_vwmetascomprovadas' as destino
    union all select 'sac__vwnatureza' as origem, 'bronze_sac_vwnatureza' as destino
    union all select 'sac__vwnotificacaoaoproponente' as origem, 'bronze_sac_vwnotificacaoaoproponente' as destino
    union all select 'sac__vworcamentosolicitado' as origem, 'bronze_sac_vworcamentosolicitado' as destino
    union all select 'sac__vwpagamentodefornecedordoprojetoporitem' as origem, 'bronze_sac_vwpagamentodefornecedordoprojetoporitem' as destino
    union all select 'sac__vwpagamentodefornecedordoprojetoporitemdetalhado' as origem, 'bronze_sac_vwpagamentodefornecedordoprojetoporitemdetalhado' as destino
    union all select 'sac__vwpagamentodespesaforaperiodoexecucao' as origem, 'bronze_sac_vwpagamentodespesaforaperiodoexecucao' as destino
    union all select 'sac__vwpagamentofinalizadoparecerista' as origem, 'bronze_sac_vwpagamentofinalizadoparecerista' as destino
    union all select 'sac__vwpagamentofornecedorirregularreceitafederal' as origem, 'bronze_sac_vwpagamentofornecedorirregularreceitafederal' as destino
    union all select 'sac__vwpagamentoparecerista' as origem, 'bronze_sac_vwpagamentoparecerista' as destino
    union all select 'sac__vwpaineladequacaorealidadeexecucao' as origem, 'bronze_sac_vwpaineladequacaorealidadeexecucao' as destino
    union all select 'sac__vwpaineladmissibilidade' as origem, 'bronze_sac_vwpaineladmissibilidade' as destino
    union all select 'sac__vwpainelaguardandodistribuicaoanalisetecnica' as origem, 'bronze_sac_vwpainelaguardandodistribuicaoanalisetecnica' as destino
    union all select 'sac__vwpainelanalisadosuperintendencia' as origem, 'bronze_sac_vwpainelanalisadosuperintendencia' as destino
    union all select 'sac__vwpainelanalisdadosuperintendencia' as origem, 'bronze_sac_vwpainelanalisdadosuperintendencia' as destino
    union all select 'sac__vwpainelavaliarpropostas' as origem, 'bronze_sac_vwpainelavaliarpropostas' as destino
    union all select 'sac__vwpainelcoordenadoravaliacaotrimestral' as origem, 'bronze_sac_vwpainelcoordenadoravaliacaotrimestral' as destino
    union all select 'sac__vwpainelcoordenadorimpedimentoparecerista' as origem, 'bronze_sac_vwpainelcoordenadorimpedimentoparecerista' as destino
    union all select 'sac__vwpainelcoordenadorreadequacaoaguardandoanalise' as origem, 'bronze_sac_vwpainelcoordenadorreadequacaoaguardandoanalise' as destino
    union all select 'sac__vwpainelcoordenadorreadequacaoaguardandoanalise_20180323' as origem, 'bronze_sac_vwpainelcoordenadorreadequacaoaguardandoanalise_20180323' as destino
    union all select 'sac__vwpainelcoordenadorreadequacaoanalisados' as origem, 'bronze_sac_vwpainelcoordenadorreadequacaoanalisados' as destino
    union all select 'sac__vwpainelcoordenadorreadequacaoanalisados_20180323' as origem, 'bronze_sac_vwpainelcoordenadorreadequacaoanalisados_20180323' as destino
    union all select 'sac__vwpainelcoordenadorreadequacaoemanalise' as origem, 'bronze_sac_vwpainelcoordenadorreadequacaoemanalise' as destino
    union all select 'sac__vwpainelcoordenadorreadequacaoemanalise_20180323' as origem, 'bronze_sac_vwpainelcoordenadorreadequacaoemanalise_20180323' as destino
    union all select 'sac__vwpainelcoordenadorvinculadasaguardandoanalise' as origem, 'bronze_sac_vwpainelcoordenadorvinculadasaguardandoanalise' as destino
    union all select 'sac__vwpainelcoordenadorvinculadasemanalise' as origem, 'bronze_sac_vwpainelcoordenadorvinculadasemanalise' as destino
    union all select 'sac__vwpainelcoordenadorvinculadasemvalidacao' as origem, 'bronze_sac_vwpainelcoordenadorvinculadasemvalidacao' as destino
    union all select 'sac__vwpainelcoordenadorvinculadasreanalisar' as origem, 'bronze_sac_vwpainelcoordenadorvinculadasreanalisar' as destino
    union all select 'sac__vwpainelcoordenadorvinculadasvalidados' as origem, 'bronze_sac_vwpainelcoordenadorvinculadasvalidados' as destino
    union all select 'sac__vwpaineldedocumentosaguardandoassinatura' as origem, 'bronze_sac_vwpaineldedocumentosaguardandoassinatura' as destino
    union all select 'sac__vwpaineldedocumentosdereadequacaoaguardandoassinatura' as origem, 'bronze_sac_vwpaineldedocumentosdereadequacaoaguardandoassinatura' as destino
    union all select 'sac__vwpaineldeemissaodelaudofinalderesultado' as origem, 'bronze_sac_vwpaineldeemissaodelaudofinalderesultado' as destino
    union all select 'sac__vwpaineldeemissaodelaudofinalderesultadopassivo' as origem, 'bronze_sac_vwpaineldeemissaodelaudofinalderesultadopassivo' as destino
    union all select 'sac__vwpaineldehomologacaodeprojetos' as origem, 'bronze_sac_vwpaineldehomologacaodeprojetos' as destino
    union all select 'sac__vwpaineldeliberacao' as origem, 'bronze_sac_vwpaineldeliberacao' as destino
    union all select 'sac__vwpaineldemensagens' as origem, 'bronze_sac_vwpaineldemensagens' as destino
    union all select 'sac__vwpaineldetransferenciaderecursos' as origem, 'bronze_sac_vwpaineldetransferenciaderecursos' as destino
    union all select 'sac__vwpaineldocomponentedacomissao' as origem, 'bronze_sac_vwpaineldocomponentedacomissao' as destino
    union all select 'sac__vwpaineldocoordenador' as origem, 'bronze_sac_vwpaineldocoordenador' as destino
    union all select 'sac__vwpaineldocoordenadorgeral' as origem, 'bronze_sac_vwpaineldocoordenadorgeral' as destino
    union all select 'sac__vwpaineldotecnicodeadmissibilidade' as origem, 'bronze_sac_vwpaineldotecnicodeadmissibilidade' as destino
    union all select 'sac__vwpainelemanalisetecnica' as origem, 'bronze_sac_vwpainelemanalisetecnica' as destino
    union all select 'sac__vwpainelemvalidacao' as origem, 'bronze_sac_vwpainelemvalidacao' as destino
    union all select 'sac__vwpainelenquadramentopropostas' as origem, 'bronze_sac_vwpainelenquadramentopropostas' as destino
    union all select 'sac__vwpainelparecerista' as origem, 'bronze_sac_vwpainelparecerista' as destino
    union all select 'sac__vwpainelpareceristaunificado' as origem, 'bronze_sac_vwpainelpareceristaunificado' as destino
    union all select 'sac__vwpainelpresidentevinculadas' as origem, 'bronze_sac_vwpainelpresidentevinculadas' as destino
    union all select 'sac__vwpainelprojetossgptc' as origem, 'bronze_sac_vwpainelprojetossgptc' as destino
    union all select 'sac__vwpainelreadequacaoaguardandopublicacao' as origem, 'bronze_sac_vwpainelreadequacaoaguardandopublicacao' as destino
    union all select 'sac__vwpainelreadequacaocoordenadorpareceraguardandoanalise' as origem, 'bronze_sac_vwpainelreadequacaocoordenadorpareceraguardandoanalise' as destino
    union all select 'sac__vwpainelreadequacaocoordenadorpareceranalisados' as origem, 'bronze_sac_vwpainelreadequacaocoordenadorpareceranalisados' as destino
    union all select 'sac__vwpainelreadequacaocoordenadorpareceremanalise' as origem, 'bronze_sac_vwpainelreadequacaocoordenadorpareceremanalise' as destino
    union all select 'sac__vwpainelreadequacaotecnico' as origem, 'bronze_sac_vwpainelreadequacaotecnico' as destino
    union all select 'sac__vwpainelsuperintendentevinculadas' as origem, 'bronze_sac_vwpainelsuperintendentevinculadas' as destino
    union all select 'sac__vwpaineltecnicoavaliacaotrimestral' as origem, 'bronze_sac_vwpaineltecnicoavaliacaotrimestral' as destino
    union all select 'sac__vwpaineltransferenciarecursoscaptacaomovimento' as origem, 'bronze_sac_vwpaineltransferenciarecursoscaptacaomovimento' as destino
    union all select 'sac__vwpainelvalidados' as origem, 'bronze_sac_vwpainelvalidados' as destino
    union all select 'sac__vwpainelvalidadoscomdevolucao' as origem, 'bronze_sac_vwpainelvalidadoscomdevolucao' as destino
    union all select 'sac__vwpareceranalisedeconteudo' as origem, 'bronze_sac_vwpareceranalisedeconteudo' as destino
    union all select 'sac__vwpareceresdereadequacao' as origem, 'bronze_sac_vwpareceresdereadequacao' as destino
    union all select 'sac__vwparecerfechadoorgao' as origem, 'bronze_sac_vwparecerfechadoorgao' as destino
    union all select 'sac__vwpareceristascredenciados' as origem, 'bronze_sac_vwpareceristascredenciados' as destino
    union all select 'sac__vwpareceristasorgao' as origem, 'bronze_sac_vwpareceristasorgao' as destino
    union all select 'sac__vwpautados_cnic_mais_trinta_dias' as origem, 'bronze_sac_vwpautados_cnic_mais_trinta_dias' as destino
    union all select 'sac__vwpecadedivulgacao' as origem, 'bronze_sac_vwpecadedivulgacao' as destino
    union all select 'sac__vwpedidodeprorrogacao' as origem, 'bronze_sac_vwpedidodeprorrogacao' as destino
    union all select 'sac__vwplanilhaaprovada' as origem, 'bronze_sac_vwplanilhaaprovada' as destino
    union all select 'sac__vwplanilhasugerida' as origem, 'bronze_sac_vwplanilhasugerida' as destino
    union all select 'sac__vwplanilhasugeridacompleta' as origem, 'bronze_sac_vwplanilhasugeridacompleta' as destino
    union all select 'sac__vwplanodedistribuicaoproduto' as origem, 'bronze_sac_vwplanodedistribuicaoproduto' as destino
    union all select 'sac__vwportariasgeradas' as origem, 'bronze_sac_vwportariasgeradas' as destino
    union all select 'sac__vwprecomedio_produto_item_unidade_uf_municipio' as origem, 'bronze_sac_vwprecomedio_produto_item_unidade_uf_municipio' as destino
    union all select 'sac__vwprocuradordeprojeto' as origem, 'bronze_sac_vwprocuradordeprojeto' as destino
    union all select 'sac__vwprodutos' as origem, 'bronze_sac_vwprodutos' as destino
    union all select 'sac__vwprodutosemanalisetecnicanasvinculadas' as origem, 'bronze_sac_vwprodutosemanalisetecnicanasvinculadas' as destino
    union all select 'sac__vwprogramarouanet' as origem, 'bronze_sac_vwprogramarouanet' as destino
    union all select 'sac__vwprogramarouanetavaliacaofinalizada' as origem, 'bronze_sac_vwprogramarouanetavaliacaofinalizada' as destino
    union all select 'sac__vwprogramarouanetavaliacaoinicialmediasnotasproposta' as origem, 'bronze_sac_vwprogramarouanetavaliacaoinicialmediasnotasproposta' as destino
    union all select 'sac__vwprogramarouanetavaliacaoinicialnotasporproposta' as origem, 'bronze_sac_vwprogramarouanetavaliacaoinicialnotasporproposta' as destino
    union all select 'sac__vwprogramarouanetavaliadores' as origem, 'bronze_sac_vwprogramarouanetavaliadores' as destino
    union all select 'sac__vwprogramarouanetdistribuirproposta' as origem, 'bronze_sac_vwprogramarouanetdistribuirproposta' as destino
    union all select 'sac__vwprogramarouanetdistribuirpropostarecurso' as origem, 'bronze_sac_vwprogramarouanetdistribuirpropostarecurso' as destino
    union all select 'sac__vwprogramarouaneteditais' as origem, 'bronze_sac_vwprogramarouaneteditais' as destino
    union all select 'sac__vwprogramarouanethabilitarproposta' as origem, 'bronze_sac_vwprogramarouanethabilitarproposta' as destino
    union all select 'sac__vwprogramarouanetincentivadorprojetosapoiados' as origem, 'bronze_sac_vwprogramarouanetincentivadorprojetosapoiados' as destino
    union all select 'sac__vwprogramarouanetnotasporproposta' as origem, 'bronze_sac_vwprogramarouanetnotasporproposta' as destino
    union all select 'sac__vwprogramarouanetpainelresultadofinal' as origem, 'bronze_sac_vwprogramarouanetpainelresultadofinal' as destino
    union all select 'sac__vwprogramarouanetpainelsolicitarrecurso' as origem, 'bronze_sac_vwprogramarouanetpainelsolicitarrecurso' as destino
    union all select 'sac__vwprogramarouanetprojetosselecionados' as origem, 'bronze_sac_vwprogramarouanetprojetosselecionados' as destino
    union all select 'sac__vwprogramarouanetpropostasavaliadas' as origem, 'bronze_sac_vwprogramarouanetpropostasavaliadas' as destino
    union all select 'sac__vwprogramarouanetpropostasnaoavaliadas' as origem, 'bronze_sac_vwprogramarouanetpropostasnaoavaliadas' as destino
    union all select 'sac__vwprogramarouanetpropostasrecebidas' as origem, 'bronze_sac_vwprogramarouanetpropostasrecebidas' as destino
    union all select 'sac__vwprogramarouanetrecursopropostaavaliadas' as origem, 'bronze_sac_vwprogramarouanetrecursopropostaavaliadas' as destino
    union all select 'sac__vwprogramarouanetrecursospropostasnaoavaliadas' as origem, 'bronze_sac_vwprogramarouanetrecursospropostasnaoavaliadas' as destino
    union all select 'sac__vwprogramarouanetresultadoavaliacaoinicial' as origem, 'bronze_sac_vwprogramarouanetresultadoavaliacaoinicial' as destino
    union all select 'sac__vwprogramarouanettipologia' as origem, 'bronze_sac_vwprogramarouanettipologia' as destino
    union all select 'sac__vwprojetoaenviar' as origem, 'bronze_sac_vwprojetoaenviar' as destino
    union all select 'sac__vwprojetoareasegmentoproduto' as origem, 'bronze_sac_vwprojetoareasegmentoproduto' as destino
    union all select 'sac__vwprojetobeneficiarioprodutocultural' as origem, 'bronze_sac_vwprojetobeneficiarioprodutocultural' as destino
    union all select 'sac__vwprojetodistribuidovinculada' as origem, 'bronze_sac_vwprojetodistribuidovinculada' as destino
    union all select 'sac__vwprojetoorgao' as origem, 'bronze_sac_vwprojetoorgao' as destino
    union all select 'sac__vwprojetos_na_pauta_cnic' as origem, 'bronze_sac_vwprojetos_na_pauta_cnic' as destino
    union all select 'sac__vwprojetosadequadosrealidadeexecucao' as origem, 'bronze_sac_vwprojetosadequadosrealidadeexecucao' as destino
    union all select 'sac__vwprojetosapreciadosaguardandohomologacao' as origem, 'bronze_sac_vwprojetosapreciadosaguardandohomologacao' as destino
    union all select 'sac__vwprojetosaprovadossemhomologacao' as origem, 'bronze_sac_vwprojetosaprovadossemhomologacao' as destino
    union all select 'sac__vwprojetosareceber' as origem, 'bronze_sac_vwprojetosareceber' as destino
    union all select 'sac__vwprojetoscominconsistencianacaptacao' as origem, 'bronze_sac_vwprojetoscominconsistencianacaptacao' as destino
    union all select 'sac__vwprojetosconsolidados' as origem, 'bronze_sac_vwprojetosconsolidados' as destino
    union all select 'sac__vwprojetosdespachados' as origem, 'bronze_sac_vwprojetosdespachados' as destino
    union all select 'sac__vwprojetosemanalisecomparecerista' as origem, 'bronze_sac_vwprojetosemanalisecomparecerista' as destino
    union all select 'sac__vwprojetosenviados' as origem, 'bronze_sac_vwprojetosenviados' as destino
    union all select 'sac__vwprojetoseusdocumentos' as origem, 'bronze_sac_vwprojetoseusdocumentos' as destino
    union all select 'sac__vwprojetosgratuitopago' as origem, 'bronze_sac_vwprojetosgratuitopago' as destino
    union all select 'sac__vwprojetositinerantes' as origem, 'bronze_sac_vwprojetositinerantes' as destino
    union all select 'sac__vwprojetosliberadosparaadequacaorealidadeexecucao' as origem, 'bronze_sac_vwprojetosliberadosparaadequacaorealidadeexecucao' as destino
    union all select 'sac__vwprojetosporfase' as origem, 'bronze_sac_vwprojetosporfase' as destino
    union all select 'tabelas__autorizados' as origem, 'bronze_tabelas_autorizados' as destino
    union all select 'tabelas__avisos' as origem, 'bronze_tabelas_avisos' as destino
    union all select 'tabelas__bairros' as origem, 'bronze_tabelas_bairros' as destino
    union all select 'tabelas__cadastro' as origem, 'bronze_tabelas_cadastro' as destino
    union all select 'tabelas__categorias_pessoa' as origem, 'bronze_tabelas_categorias_pessoa' as destino
    union all select 'tabelas__cdpe' as origem, 'bronze_tabelas_cdpe' as destino
    union all select 'tabelas__dtproperties' as origem, 'bronze_tabelas_dtproperties' as destino
    union all select 'tabelas__ect_bairros' as origem, 'bronze_tabelas_ect_bairros' as destino
    union all select 'tabelas__ect_localidades' as origem, 'bronze_tabelas_ect_localidades' as destino
    union all select 'tabelas__ect_recusados' as origem, 'bronze_tabelas_ect_recusados' as destino
    union all select 'tabelas__emails' as origem, 'bronze_tabelas_emails' as destino
    union all select 'tabelas__estacoes' as origem, 'bronze_tabelas_estacoes' as destino
    union all select 'tabelas__eventos' as origem, 'bronze_tabelas_eventos' as destino
    union all select 'tabelas__funcoes' as origem, 'bronze_tabelas_funcoes' as destino
    union all select 'tabelas__grupos' as origem, 'bronze_tabelas_grupos' as destino
    union all select 'tabelas__grupos_ares' as origem, 'bronze_tabelas_grupos_ares' as destino
    union all select 'tabelas__grupos_minc11' as origem, 'bronze_tabelas_grupos_minc11' as destino
    union all select 'tabelas__gruposxservicos' as origem, 'bronze_tabelas_gruposxservicos' as destino
    union all select 'tabelas__headers' as origem, 'bronze_tabelas_headers' as destino
    union all select 'tabelas__indice_palavras' as origem, 'bronze_tabelas_indice_palavras' as destino
    union all select 'tabelas__listainstitucionais' as origem, 'bronze_tabelas_listainstitucionais' as destino
    union all select 'tabelas__listapessoais' as origem, 'bronze_tabelas_listapessoais' as destino
    union all select 'tabelas__localidades' as origem, 'bronze_tabelas_localidades' as destino
    union all select 'tabelas__logonnt' as origem, 'bronze_tabelas_logonnt' as destino
    union all select 'tabelas__logradouros' as origem, 'bronze_tabelas_logradouros' as destino
    union all select 'tabelas__mei_grupos_intranet' as origem, 'bronze_tabelas_mei_grupos_intranet' as destino
    union all select 'tabelas__mei_gruposxservicos_intranet' as origem, 'bronze_tabelas_mei_gruposxservicos_intranet' as destino
    union all select 'tabelas__mei_servicos_intranet' as origem, 'bronze_tabelas_mei_servicos_intranet' as destino
    union all select 'tabelas__mei_usuariosxservicos_intranet' as origem, 'bronze_tabelas_mei_usuariosxservicos_intranet' as destino
    union all select 'tabelas__mensagens' as origem, 'bronze_tabelas_mensagens' as destino
    union all select 'tabelas__menus' as origem, 'bronze_tabelas_menus' as destino
    union all select 'tabelas__menus_ares' as origem, 'bronze_tabelas_menus_ares' as destino
    union all select 'tabelas__menus_minc11' as origem, 'bronze_tabelas_menus_minc11' as destino
    union all select 'tabelas__menusxgrupos' as origem, 'bronze_tabelas_menusxgrupos' as destino
    union all select 'tabelas__menusxgrupos_ares' as origem, 'bronze_tabelas_menusxgrupos_ares' as destino
    union all select 'tabelas__menusxgrupos_minc11' as origem, 'bronze_tabelas_menusxgrupos_minc11' as destino
    union all select 'tabelas__meta_dados' as origem, 'bronze_tabelas_meta_dados' as destino
    union all select 'tabelas__orgaos' as origem, 'bronze_tabelas_orgaos' as destino
    union all select 'tabelas__orgaos_ares' as origem, 'bronze_tabelas_orgaos_ares' as destino
    union all select 'tabelas__orgaos_subordinados' as origem, 'bronze_tabelas_orgaos_subordinados' as destino
    union all select 'tabelas__orgaosxusuarios' as origem, 'bronze_tabelas_orgaosxusuarios' as destino
    union all select 'tabelas__palavras' as origem, 'bronze_tabelas_palavras' as destino
    union all select 'tabelas__palavras2' as origem, 'bronze_tabelas_palavras2' as destino
    union all select 'tabelas__pessoa_dados' as origem, 'bronze_tabelas_pessoa_dados' as destino
    union all select 'tabelas__pessoa_dados_bkp' as origem, 'bronze_tabelas_pessoa_dados_bkp' as destino
    union all select 'tabelas__pessoa_enderecos' as origem, 'bronze_tabelas_pessoa_enderecos' as destino
    union all select 'tabelas__pessoa_identificacoes' as origem, 'bronze_tabelas_pessoa_identificacoes' as destino
    union all select 'tabelas__pessoa_identificacoes_alteradas' as origem, 'bronze_tabelas_pessoa_identificacoes_alteradas' as destino
    union all select 'tabelas__pessoa_identificacoes_bkp' as origem, 'bronze_tabelas_pessoa_identificacoes_bkp' as destino
    union all select 'tabelas__pessoas' as origem, 'bronze_tabelas_pessoas' as destino
    union all select 'tabelas__pessoas_bkp' as origem, 'bronze_tabelas_pessoas_bkp' as destino
    union all select 'tabelas__pessoasxfuncoes' as origem, 'bronze_tabelas_pessoasxfuncoes' as destino
    union all select 'tabelas__plan1$' as origem, 'bronze_tabelas_plan1$' as destino
    union all select 'tabelas__servicos_intranet' as origem, 'bronze_tabelas_servicos_intranet' as destino
    union all select 'tabelas__sistemas' as origem, 'bronze_tabelas_sistemas' as destino
    union all select 'tabelas__sistemas_ares' as origem, 'bronze_tabelas_sistemas_ares' as destino
    union all select 'tabelas__sysdiagrams' as origem, 'bronze_tabelas_sysdiagrams' as destino
    union all select 'tabelas__tabelas' as origem, 'bronze_tabelas_tabelas' as destino
    union all select 'tabelas__table1' as origem, 'bronze_tabelas_table1' as destino
    union all select 'tabelas__temp_tbl' as origem, 'bronze_tabelas_temp_tbl' as destino
    union all select 'tabelas__tipos_logradouro' as origem, 'bronze_tabelas_tipos_logradouro' as destino
    union all select 'tabelas__tipos_pessoa' as origem, 'bronze_tabelas_tipos_pessoa' as destino
    union all select 'tabelas__unidadesorcamentarias' as origem, 'bronze_tabelas_unidadesorcamentarias' as destino
    union all select 'tabelas__usuarios' as origem, 'bronze_tabelas_usuarios' as destino
    union all select 'tabelas__usuarios_ares' as origem, 'bronze_tabelas_usuarios_ares' as destino
    union all select 'tabelas__usuariosxgrupos_intranet' as origem, 'bronze_tabelas_usuariosxgrupos_intranet' as destino
    union all select 'tabelas__usuariosxorgaosxgrupos' as origem, 'bronze_tabelas_usuariosxorgaosxgrupos' as destino
    union all select 'tabelas__usuariosxorgaosxgrupos_ares' as origem, 'bronze_tabelas_usuariosxorgaosxgrupos_ares' as destino
    union all select 'tabelas__usuexternosistemas' as origem, 'bronze_tabelas_usuexternosistemas' as destino
    union all select 'tabelas__v_distritos_por_palavra' as origem, 'bronze_tabelas_v_distritos_por_palavra' as destino
    union all select 'tabelas__v_estados_por_palavra' as origem, 'bronze_tabelas_v_estados_por_palavra' as destino
    union all select 'tabelas__v_hierarquia_orgaos' as origem, 'bronze_tabelas_v_hierarquia_orgaos' as destino
    union all select 'tabelas__v_localidade_completa' as origem, 'bronze_tabelas_v_localidade_completa' as destino
    union all select 'tabelas__v_logradouro_completo' as origem, 'bronze_tabelas_v_logradouro_completo' as destino
    union all select 'tabelas__v_municipios_por_palavra' as origem, 'bronze_tabelas_v_municipios_por_palavra' as destino
    union all select 'tabelas__v_paises_por_palavra' as origem, 'bronze_tabelas_v_paises_por_palavra' as destino
    union all select 'tabelas__v_pessoas_nome_completo' as origem, 'bronze_tabelas_v_pessoas_nome_completo' as destino
    union all select 'tabelas__v_usuario_completo' as origem, 'bronze_tabelas_v_usuario_completo' as destino
    union all select 'tabelas__v_usuario_orgaos_grupos' as origem, 'bronze_tabelas_v_usuario_orgaos_grupos' as destino
    union all select 'tabelas__v_usuarios' as origem, 'bronze_tabelas_v_usuarios' as destino
    union all select 'tabelas__vwrelservidorhpimpressao' as origem, 'bronze_tabelas_vwrelservidorhpimpressao' as destino
    union all select 'tabelas__vwusuarios' as origem, 'bronze_tabelas_vwusuarios' as destino
    union all select 'tabelas__vwusuariosorgaosgrupos' as origem, 'bronze_tabelas_vwusuariosorgaosgrupos' as destino
  loop
    if to_regclass('salic.' || quote_ident(r.destino)) is not null
       and to_regclass('salic_bronze.' || quote_ident(r.origem)) is not null then
      raise exception 'salic.% já existe e salic_bronze.% também; resolva antes do rename', r.destino, r.origem;
    end if;
  end loop;
end $$;

-- 2. renomeia dentro da origem e só então move. Na ordem inversa a tabela
--    colidiria com a view legada do dbt em salic, que tem o mesmo
--    nome <banco>__<tabela> da origem.
alter table if exists salic_bronze."agentes__agentes" rename to "bronze_agentes_agentes";
alter table if exists salic_bronze."bronze_agentes_agentes" set schema salic;
alter table if exists salic_bronze."agentes__endereconacional" rename to "bronze_agentes_endereconacional";
alter table if exists salic_bronze."bronze_agentes_endereconacional" set schema salic;
alter table if exists salic_bronze."agentes__municipios" rename to "bronze_agentes_municipios";
alter table if exists salic_bronze."bronze_agentes_municipios" set schema salic;
alter table if exists salic_bronze."agentes__perfil" rename to "bronze_agentes_perfil";
alter table if exists salic_bronze."bronze_agentes_perfil" set schema salic;
alter table if exists salic_bronze."agentes__populacaomunicipio" rename to "bronze_agentes_populacaomunicipio";
alter table if exists salic_bronze."bronze_agentes_populacaomunicipio" set schema salic;
alter table if exists salic_bronze."agentes__tbagentefisico" rename to "bronze_agentes_tbagentefisico";
alter table if exists salic_bronze."bronze_agentes_tbagentefisico" set schema salic;
alter table if exists salic_bronze."agentes__uf" rename to "bronze_agentes_uf";
alter table if exists salic_bronze."bronze_agentes_uf" set schema salic;
alter table if exists salic_bronze."agentes__verificacao" rename to "bronze_agentes_verificacao";
alter table if exists salic_bronze."bronze_agentes_verificacao" set schema salic;
alter table if exists salic_bronze."agentes__vpais" rename to "bronze_agentes_vpais";
alter table if exists salic_bronze."bronze_agentes_vpais" set schema salic;
alter table if exists salic_bronze."agentes__vperfil" rename to "bronze_agentes_vperfil";
alter table if exists salic_bronze."bronze_agentes_vperfil" set schema salic;
alter table if exists salic_bronze."agentes__vprefeiturasregiaouf" rename to "bronze_agentes_vprefeiturasregiaouf";
alter table if exists salic_bronze."bronze_agentes_vprefeiturasregiaouf" set schema salic;
alter table if exists salic_bronze."agentes__vramais" rename to "bronze_agentes_vramais";
alter table if exists salic_bronze."bronze_agentes_vramais" set schema salic;
alter table if exists salic_bronze."agentes__vsistema" rename to "bronze_agentes_vsistema";
alter table if exists salic_bronze."bronze_agentes_vsistema" set schema salic;
alter table if exists salic_bronze."agentes__vtelefones" rename to "bronze_agentes_vtelefones";
alter table if exists salic_bronze."bronze_agentes_vtelefones" set schema salic;
alter table if exists salic_bronze."agentes__vuf" rename to "bronze_agentes_vuf";
alter table if exists salic_bronze."bronze_agentes_vuf" set schema salic;
alter table if exists salic_bronze."agentes__vufmunicipio" rename to "bronze_agentes_vufmunicipio";
alter table if exists salic_bronze."bronze_agentes_vufmunicipio" set schema salic;
alter table if exists salic_bronze."agentes__vverificacao" rename to "bronze_agentes_vverificacao";
alter table if exists salic_bronze."bronze_agentes_vverificacao" set schema salic;
alter table if exists salic_bronze."agentes__vvinculacao" rename to "bronze_agentes_vvinculacao";
alter table if exists salic_bronze."bronze_agentes_vvinculacao" set schema salic;
alter table if exists salic_bronze."agentes__vvisao" rename to "bronze_agentes_vvisao";
alter table if exists salic_bronze."bronze_agentes_vvisao" set schema salic;
alter table if exists salic_bronze."agentes__vwcadastrarparecerista" rename to "bronze_agentes_vwcadastrarparecerista";
alter table if exists salic_bronze."bronze_agentes_vwcadastrarparecerista" set schema salic;
alter table if exists salic_bronze."agentes__vwempresajuridica" rename to "bronze_agentes_vwempresajuridica";
alter table if exists salic_bronze."bronze_agentes_vwempresajuridica" set schema salic;
alter table if exists salic_bronze."agentes__vwempresajuridicasocios" rename to "bronze_agentes_vwempresajuridicasocios";
alter table if exists salic_bronze."bronze_agentes_vwempresajuridicasocios" set schema salic;
alter table if exists salic_bronze."agentes__vwportifolio" rename to "bronze_agentes_vwportifolio";
alter table if exists salic_bronze."bronze_agentes_vwportifolio" set schema salic;
alter table if exists salic_bronze."agentes__vwpropostasemavaliacao" rename to "bronze_agentes_vwpropostasemavaliacao";
alter table if exists salic_bronze."bronze_agentes_vwpropostasemavaliacao" set schema salic;
alter table if exists salic_bronze."bdcorporativo__sysdiagrams" rename to "bronze_bdcorporativo_sysdiagrams";
alter table if exists salic_bronze."bronze_bdcorporativo_sysdiagrams" set schema salic;
alter table if exists salic_bronze."bdcorporativo__tbalteracaonomeproponente" rename to "bronze_bdcorporativo_tbalteracaonomeproponente";
alter table if exists salic_bronze."bronze_bdcorporativo_tbalteracaonomeproponente" set schema salic;
alter table if exists salic_bronze."bdcorporativo__tbcomprovantepagamento" rename to "bronze_bdcorporativo_tbcomprovantepagamento";
alter table if exists salic_bronze."bronze_bdcorporativo_tbcomprovantepagamento" set schema salic;
alter table if exists salic_bronze."bdcorporativo__tbencaminhamentoprestacaocontas" rename to "bronze_bdcorporativo_tbencaminhamentoprestacaocontas";
alter table if exists salic_bronze."bronze_bdcorporativo_tbencaminhamentoprestacaocontas" set schema salic;
alter table if exists salic_bronze."bdcorporativo__tbsituacaoencaminhamentoprestacaocontas" rename to "bronze_bdcorporativo_tbsituacaoencaminhamentoprestacaocontas";
alter table if exists salic_bronze."bronze_bdcorporativo_tbsituacaoencaminhamentoprestacaocontas" set schema salic;
alter table if exists salic_bronze."controledeacesso__dtproperties" rename to "bronze_controledeacesso_dtproperties";
alter table if exists salic_bronze."bronze_controledeacesso_dtproperties" set schema salic;
alter table if exists salic_bronze."controledeacesso__sgcacesso" rename to "bronze_controledeacesso_sgcacesso";
alter table if exists salic_bronze."bronze_controledeacesso_sgcacesso" set schema salic;
alter table if exists salic_bronze."controledeacesso__sgcsistema" rename to "bronze_controledeacesso_sgcsistema";
alter table if exists salic_bronze."bronze_controledeacesso_sgcsistema" set schema salic;
alter table if exists salic_bronze."controledeacesso__sgcusuarioxsistema" rename to "bronze_controledeacesso_sgcusuarioxsistema";
alter table if exists salic_bronze."bronze_controledeacesso_sgcusuarioxsistema" set schema salic;
alter table if exists salic_bronze."controledeacesso__sysdiagrams" rename to "bronze_controledeacesso_sysdiagrams";
alter table if exists salic_bronze."bronze_controledeacesso_sysdiagrams" set schema salic;
alter table if exists salic_bronze."sac__aberturadecontabancaria" rename to "bronze_sac_aberturadecontabancaria";
alter table if exists salic_bronze."bronze_sac_aberturadecontabancaria" set schema salic;
alter table if exists salic_bronze."sac__aberturadecontasav39215" rename to "bronze_sac_aberturadecontasav39215";
alter table if exists salic_bronze."bronze_sac_aberturadecontasav39215" set schema salic;
alter table if exists salic_bronze."sac__aberturadecontasav39224" rename to "bronze_sac_aberturadecontasav39224";
alter table if exists salic_bronze."bronze_sac_aberturadecontasav39224" set schema salic;
alter table if exists salic_bronze."sac__aberturadecontasav39225" rename to "bronze_sac_aberturadecontasav39225";
alter table if exists salic_bronze."bronze_sac_aberturadecontasav39225" set schema salic;
alter table if exists salic_bronze."sac__aberturadecontasav39227" rename to "bronze_sac_aberturadecontasav39227";
alter table if exists salic_bronze."bronze_sac_aberturadecontasav39227" set schema salic;
alter table if exists salic_bronze."sac__aberturadecontaseficpf40979" rename to "bronze_sac_aberturadecontaseficpf40979";
alter table if exists salic_bronze."bronze_sac_aberturadecontaseficpf40979" set schema salic;
alter table if exists salic_bronze."sac__aberturadecontaseficpf40980" rename to "bronze_sac_aberturadecontaseficpf40980";
alter table if exists salic_bronze."bronze_sac_aberturadecontaseficpf40980" set schema salic;
alter table if exists salic_bronze."sac__aberturadecontaseficpj40975" rename to "bronze_sac_aberturadecontaseficpj40975";
alter table if exists salic_bronze."bronze_sac_aberturadecontaseficpj40975" set schema salic;
alter table if exists salic_bronze."sac__aberturadecontaseficpj40981" rename to "bronze_sac_aberturadecontaseficpj40981";
alter table if exists salic_bronze."bronze_sac_aberturadecontaseficpj40981" set schema salic;
alter table if exists salic_bronze."sac__abrangencia" rename to "bronze_sac_abrangencia";
alter table if exists salic_bronze."bronze_sac_abrangencia" set schema salic;
alter table if exists salic_bronze."sac__acaoproduto" rename to "bronze_sac_acaoproduto";
alter table if exists salic_bronze."bronze_sac_acaoproduto" set schema salic;
alter table if exists salic_bronze."sac__acaoprojeto" rename to "bronze_sac_acaoprojeto";
alter table if exists salic_bronze."bronze_sac_acaoprojeto" set schema salic;
alter table if exists salic_bronze."sac__acompanhamentobandasx" rename to "bronze_sac_acompanhamentobandasx";
alter table if exists salic_bronze."bronze_sac_acompanhamentobandasx" set schema salic;
alter table if exists salic_bronze."sac__agenciasbb" rename to "bronze_sac_agenciasbb";
alter table if exists salic_bronze."bronze_sac_agenciasbb" set schema salic;
alter table if exists salic_bronze."sac__aprovacao" rename to "bronze_sac_aprovacao";
alter table if exists salic_bronze."bronze_sac_aprovacao" set schema salic;
alter table if exists salic_bronze."sac__area" rename to "bronze_sac_area";
alter table if exists salic_bronze."bronze_sac_area" set schema salic;
alter table if exists salic_bronze."sac__aspectosfinanceirosx" rename to "bronze_sac_aspectosfinanceirosx";
alter table if exists salic_bronze."bronze_sac_aspectosfinanceirosx" set schema salic;
alter table if exists salic_bronze."sac__aspectosfisicosx" rename to "bronze_sac_aspectosfisicosx";
alter table if exists salic_bronze."bronze_sac_aspectosfisicosx" set schema salic;
alter table if exists salic_bronze."sac__aspectosresultadosx" rename to "bronze_sac_aspectosresultadosx";
alter table if exists salic_bronze."bronze_sac_aspectosresultadosx" set schema salic;
alter table if exists salic_bronze."sac__atualizaorgaoprojetosx" rename to "bronze_sac_atualizaorgaoprojetosx";
alter table if exists salic_bronze."bronze_sac_atualizaorgaoprojetosx" set schema salic;
alter table if exists salic_bronze."sac__auditoria" rename to "bronze_sac_auditoria";
alter table if exists salic_bronze."bronze_sac_auditoria" set schema salic;
alter table if exists salic_bronze."sac__autuacaox" rename to "bronze_sac_autuacaox";
alter table if exists salic_bronze."bronze_sac_autuacaox" set schema salic;
alter table if exists salic_bronze."sac__avaliacaopc" rename to "bronze_sac_avaliacaopc";
alter table if exists salic_bronze."bronze_sac_avaliacaopc" set schema salic;
alter table if exists salic_bronze."sac__avaliacaoprojeto" rename to "bronze_sac_avaliacaoprojeto";
alter table if exists salic_bronze."bronze_sac_avaliacaoprojeto" set schema salic;
alter table if exists salic_bronze."sac__avaliacaoproponente" rename to "bronze_sac_avaliacaoproponente";
alter table if exists salic_bronze."bronze_sac_avaliacaoproponente" set schema salic;
alter table if exists salic_bronze."sac__bancoagencia" rename to "bronze_sac_bancoagencia";
alter table if exists salic_bronze."bronze_sac_bancoagencia" set schema salic;
alter table if exists salic_bronze."sac__bancoagencia20100824" rename to "bronze_sac_bancoagencia20100824";
alter table if exists salic_bronze."bronze_sac_bancoagencia20100824" set schema salic;
alter table if exists salic_bronze."sac__bancoagencia_08" rename to "bronze_sac_bancoagencia_08";
alter table if exists salic_bronze."bronze_sac_bancoagencia_08" set schema salic;
alter table if exists salic_bronze."sac__bancoagencia_bkp_04112009x" rename to "bronze_sac_bancoagencia_bkp_04112009x";
alter table if exists salic_bronze."bronze_sac_bancoagencia_bkp_04112009x" set schema salic;
alter table if exists salic_bronze."sac__bancoagencianovo" rename to "bronze_sac_bancoagencianovo";
alter table if exists salic_bronze."bronze_sac_bancoagencianovo" set schema salic;
alter table if exists salic_bronze."sac__bancoagencianovo1" rename to "bronze_sac_bancoagencianovo1";
alter table if exists salic_bronze."bronze_sac_bancoagencianovo1" set schema salic;
alter table if exists salic_bronze."sac__bandas" rename to "bronze_sac_bandas";
alter table if exists salic_bronze."bronze_sac_bandas" set schema salic;
alter table if exists salic_bronze."sac__bcoagmar2011$" rename to "bronze_sac_bcoagmar2011$";
alter table if exists salic_bronze."bronze_sac_bcoagmar2011$" set schema salic;
alter table if exists salic_bronze."sac__bolsa" rename to "bronze_sac_bolsa";
alter table if exists salic_bronze."bronze_sac_bolsa" set schema salic;
alter table if exists salic_bronze."sac__captacao" rename to "bronze_sac_captacao";
alter table if exists salic_bronze."bronze_sac_captacao" set schema salic;
alter table if exists salic_bronze."sac__captacaoconversao" rename to "bronze_sac_captacaoconversao";
alter table if exists salic_bronze."bronze_sac_captacaoconversao" set schema salic;
alter table if exists salic_bronze."sac__captacaoguia" rename to "bronze_sac_captacaoguia";
alter table if exists salic_bronze."bronze_sac_captacaoguia" set schema salic;
alter table if exists salic_bronze."sac__captacaoold" rename to "bronze_sac_captacaoold";
alter table if exists salic_bronze."bronze_sac_captacaoold" set schema salic;
alter table if exists salic_bronze."sac__captacaoquotas" rename to "bronze_sac_captacaoquotas";
alter table if exists salic_bronze."bronze_sac_captacaoquotas" set schema salic;
alter table if exists salic_bronze."sac__carregarecibocaptacaodanca" rename to "bronze_sac_carregarecibocaptacaodanca";
alter table if exists salic_bronze."bronze_sac_carregarecibocaptacaodanca" set schema salic;
alter table if exists salic_bronze."sac__carregarecibocaptacaodanca2010" rename to "bronze_sac_carregarecibocaptacaodanca2010";
alter table if exists salic_bronze."bronze_sac_carregarecibocaptacaodanca2010" set schema salic;
alter table if exists salic_bronze."sac__carregarecibocaptacaodanca2012" rename to "bronze_sac_carregarecibocaptacaodanca2012";
alter table if exists salic_bronze."bronze_sac_carregarecibocaptacaodanca2012" set schema salic;
alter table if exists salic_bronze."sac__carregarecibocaptacaodanca2013" rename to "bronze_sac_carregarecibocaptacaodanca2013";
alter table if exists salic_bronze."bronze_sac_carregarecibocaptacaodanca2013" set schema salic;
alter table if exists salic_bronze."sac__carta" rename to "bronze_sac_carta";
alter table if exists salic_bronze."bronze_sac_carta" set schema salic;
alter table if exists salic_bronze."sac__cartaeventual" rename to "bronze_sac_cartaeventual";
alter table if exists salic_bronze."bronze_sac_cartaeventual" set schema salic;
alter table if exists salic_bronze."sac__certidoesnegativas" rename to "bronze_sac_certidoesnegativas";
alter table if exists salic_bronze."bronze_sac_certidoesnegativas" set schema salic;
alter table if exists salic_bronze."sac__cnic" rename to "bronze_sac_cnic";
alter table if exists salic_bronze."bronze_sac_cnic" set schema salic;
alter table if exists salic_bronze."sac__complementacao" rename to "bronze_sac_complementacao";
alter table if exists salic_bronze."bronze_sac_complementacao" set schema salic;
alter table if exists salic_bronze."sac__conselheiro" rename to "bronze_sac_conselheiro";
alter table if exists salic_bronze."bronze_sac_conselheiro" set schema salic;
alter table if exists salic_bronze."sac__consultor" rename to "bronze_sac_consultor";
alter table if exists salic_bronze."bronze_sac_consultor" set schema salic;
alter table if exists salic_bronze."sac__consultorespc" rename to "bronze_sac_consultorespc";
alter table if exists salic_bronze."bronze_sac_consultorespc" set schema salic;
alter table if exists salic_bronze."sac__contabancaria" rename to "bronze_sac_contabancaria";
alter table if exists salic_bronze."bronze_sac_contabancaria" set schema salic;
alter table if exists salic_bronze."sac__convenio" rename to "bronze_sac_convenio";
alter table if exists salic_bronze."bronze_sac_convenio" set schema salic;
alter table if exists salic_bronze."sac__cpbx" rename to "bronze_sac_cpbx";
alter table if exists salic_bronze."bronze_sac_cpbx" set schema salic;
alter table if exists salic_bronze."sac__criteriosx" rename to "bronze_sac_criteriosx";
alter table if exists salic_bronze."bronze_sac_criteriosx" set schema salic;
alter table if exists salic_bronze."sac__curriculo" rename to "bronze_sac_curriculo";
alter table if exists salic_bronze."bronze_sac_curriculo" set schema salic;
alter table if exists salic_bronze."sac__dba_captacaoanoufmunicipio" rename to "bronze_sac_dba_captacaoanoufmunicipio";
alter table if exists salic_bronze."bronze_sac_dba_captacaoanoufmunicipio" set schema salic;
alter table if exists salic_bronze."sac__dba_geral" rename to "bronze_sac_dba_geral";
alter table if exists salic_bronze."bronze_sac_dba_geral" set schema salic;
alter table if exists salic_bronze."sac__dba_geral_2008" rename to "bronze_sac_dba_geral_2008";
alter table if exists salic_bronze."bronze_sac_dba_geral_2008" set schema salic;
alter table if exists salic_bronze."sac__dba_geral_2009" rename to "bronze_sac_dba_geral_2009";
alter table if exists salic_bronze."bronze_sac_dba_geral_2009" set schema salic;
alter table if exists salic_bronze."sac__dba_geral_2010" rename to "bronze_sac_dba_geral_2010";
alter table if exists salic_bronze."bronze_sac_dba_geral_2010" set schema salic;
alter table if exists salic_bronze."sac__dba_geral_2011" rename to "bronze_sac_dba_geral_2011";
alter table if exists salic_bronze."bronze_sac_dba_geral_2011" set schema salic;
alter table if exists salic_bronze."sac__dba_geral_2012" rename to "bronze_sac_dba_geral_2012";
alter table if exists salic_bronze."bronze_sac_dba_geral_2012" set schema salic;
alter table if exists salic_bronze."sac__dba_geral_2013" rename to "bronze_sac_dba_geral_2013";
alter table if exists salic_bronze."bronze_sac_dba_geral_2013" set schema salic;
alter table if exists salic_bronze."sac__dba_geral_2014" rename to "bronze_sac_dba_geral_2014";
alter table if exists salic_bronze."bronze_sac_dba_geral_2014" set schema salic;
alter table if exists salic_bronze."sac__dba_geral_fred" rename to "bronze_sac_dba_geral_fred";
alter table if exists salic_bronze."bronze_sac_dba_geral_fred" set schema salic;
alter table if exists salic_bronze."sac__dba_geral_sem_abrangencia" rename to "bronze_sac_dba_geral_sem_abrangencia";
alter table if exists salic_bronze."bronze_sac_dba_geral_sem_abrangencia" set schema salic;
alter table if exists salic_bronze."sac__dba_qtd_apresentado_aprovado_ano_mes" rename to "bronze_sac_dba_qtd_apresentado_aprovado_ano_mes";
alter table if exists salic_bronze."bronze_sac_dba_qtd_apresentado_aprovado_ano_mes" set schema salic;
alter table if exists salic_bronze."sac__dba_qtd_apresentado_aprovado_captado" rename to "bronze_sac_dba_qtd_apresentado_aprovado_captado";
alter table if exists salic_bronze."bronze_sac_dba_qtd_apresentado_aprovado_captado" set schema salic;
alter table if exists salic_bronze."sac__dba_qtd_proponentes_inabilitados" rename to "bronze_sac_dba_qtd_proponentes_inabilitados";
alter table if exists salic_bronze."bronze_sac_dba_qtd_proponentes_inabilitados" set schema salic;
alter table if exists salic_bronze."sac__dba_relacao_projetos" rename to "bronze_sac_dba_relacao_projetos";
alter table if exists salic_bronze."bronze_sac_dba_relacao_projetos" set schema salic;
alter table if exists salic_bronze."sac__dba_relacao_projetos_fred" rename to "bronze_sac_dba_relacao_projetos_fred";
alter table if exists salic_bronze."bronze_sac_dba_relacao_projetos_fred" set schema salic;
alter table if exists salic_bronze."sac__dba_relacao_projetos_fred_agentes" rename to "bronze_sac_dba_relacao_projetos_fred_agentes";
alter table if exists salic_bronze."bronze_sac_dba_relacao_projetos_fred_agentes" set schema salic;
alter table if exists salic_bronze."sac__dba_relacao_projetos_fred_tcu" rename to "bronze_sac_dba_relacao_projetos_fred_tcu";
alter table if exists salic_bronze."bronze_sac_dba_relacao_projetos_fred_tcu" set schema salic;
alter table if exists salic_bronze."sac__dba_relacao_projetos_fred_tcu_aprovados" rename to "bronze_sac_dba_relacao_projetos_fred_tcu_aprovados";
alter table if exists salic_bronze."bronze_sac_dba_relacao_projetos_fred_tcu_aprovados" set schema salic;
alter table if exists salic_bronze."sac__dba_relacao_projetos_geral" rename to "bronze_sac_dba_relacao_projetos_geral";
alter table if exists salic_bronze."bronze_sac_dba_relacao_projetos_geral" set schema salic;
alter table if exists salic_bronze."sac__dba_tcu_captacao_sav" rename to "bronze_sac_dba_tcu_captacao_sav";
alter table if exists salic_bronze."bronze_sac_dba_tcu_captacao_sav" set schema salic;
alter table if exists salic_bronze."sac__dba_tcu_captacao_sefic" rename to "bronze_sac_dba_tcu_captacao_sefic";
alter table if exists salic_bronze."bronze_sac_dba_tcu_captacao_sefic" set schema salic;
alter table if exists salic_bronze."sac__dba_total_investido_investidor_ano" rename to "bronze_sac_dba_total_investido_investidor_ano";
alter table if exists salic_bronze."bronze_sac_dba_total_investido_investidor_ano" set schema salic;
alter table if exists salic_bronze."sac__dba_valor_solicitado_captado_projeto" rename to "bronze_sac_dba_valor_solicitado_captado_projeto";
alter table if exists salic_bronze."bronze_sac_dba_valor_solicitado_captado_projeto" set schema salic;
alter table if exists salic_bronze."sac__dba_valor_solicitado_captado_projeto_sav" rename to "bronze_sac_dba_valor_solicitado_captado_projeto_sav";
alter table if exists salic_bronze."bronze_sac_dba_valor_solicitado_captado_projeto_sav" set schema salic;
alter table if exists salic_bronze."sac__dbf" rename to "bronze_sac_dbf";
alter table if exists salic_bronze."bronze_sac_dbf" set schema salic;
alter table if exists salic_bronze."sac__dbf_agosto" rename to "bronze_sac_dbf_agosto";
alter table if exists salic_bronze."bronze_sac_dbf_agosto" set schema salic;
alter table if exists salic_bronze."sac__dbf_dezembro" rename to "bronze_sac_dbf_dezembro";
alter table if exists salic_bronze."bronze_sac_dbf_dezembro" set schema salic;
alter table if exists salic_bronze."sac__dbf_maio" rename to "bronze_sac_dbf_maio";
alter table if exists salic_bronze."bronze_sac_dbf_maio" set schema salic;
alter table if exists salic_bronze."sac__dbresult" rename to "bronze_sac_dbresult";
alter table if exists salic_bronze."bronze_sac_dbresult" set schema salic;
alter table if exists salic_bronze."sac__deorgaoparaorgao" rename to "bronze_sac_deorgaoparaorgao";
alter table if exists salic_bronze."bronze_sac_deorgaoparaorgao" set schema salic;
alter table if exists salic_bronze."sac__distribuicao_avaliacao_proposta" rename to "bronze_sac_distribuicao_avaliacao_proposta";
alter table if exists salic_bronze."bronze_sac_distribuicao_avaliacao_proposta" set schema salic;
alter table if exists salic_bronze."sac__distribuicao_avaliacao_proposta_bkp" rename to "bronze_sac_distribuicao_avaliacao_proposta_bkp";
alter table if exists salic_bronze."bronze_sac_distribuicao_avaliacao_proposta_bkp" set schema salic;
alter table if exists salic_bronze."sac__distribuicaopassagem" rename to "bronze_sac_distribuicaopassagem";
alter table if exists salic_bronze."bronze_sac_distribuicaopassagem" set schema salic;
alter table if exists salic_bronze."sac__documentopessoal" rename to "bronze_sac_documentopessoal";
alter table if exists salic_bronze."bronze_sac_documentopessoal" set schema salic;
alter table if exists salic_bronze."sac__documentosexigidos" rename to "bronze_sac_documentosexigidos";
alter table if exists salic_bronze."bronze_sac_documentosexigidos" set schema salic;
alter table if exists salic_bronze."sac__documentosprojeto" rename to "bronze_sac_documentosprojeto";
alter table if exists salic_bronze."bronze_sac_documentosprojeto" set schema salic;
alter table if exists salic_bronze."sac__documentosproponente" rename to "bronze_sac_documentosproponente";
alter table if exists salic_bronze."bronze_sac_documentosproponente" set schema salic;
alter table if exists salic_bronze."sac__dtproperties" rename to "bronze_sac_dtproperties";
alter table if exists salic_bronze."bronze_sac_dtproperties" set schema salic;
alter table if exists salic_bronze."sac__edital" rename to "bronze_sac_edital";
alter table if exists salic_bronze."bronze_sac_edital" set schema salic;
alter table if exists salic_bronze."sac__editaldesembolso" rename to "bronze_sac_editaldesembolso";
alter table if exists salic_bronze."bronze_sac_editaldesembolso" set schema salic;
alter table if exists salic_bronze."sac__editalparcelas" rename to "bronze_sac_editalparcelas";
alter table if exists salic_bronze."bronze_sac_editalparcelas" set schema salic;
alter table if exists salic_bronze."sac__editalprojeto" rename to "bronze_sac_editalprojeto";
alter table if exists salic_bronze."bronze_sac_editalprojeto" set schema salic;
alter table if exists salic_bronze."sac__editalrelatorio" rename to "bronze_sac_editalrelatorio";
alter table if exists salic_bronze."bronze_sac_editalrelatorio" set schema salic;
alter table if exists salic_bronze."sac__efeitoseconomicos" rename to "bronze_sac_efeitoseconomicos";
alter table if exists salic_bronze."bronze_sac_efeitoseconomicos" set schema salic;
alter table if exists salic_bronze."sac__efeitosresultado" rename to "bronze_sac_efeitosresultado";
alter table if exists salic_bronze."bronze_sac_efeitosresultado" set schema salic;
alter table if exists salic_bronze."sac__efeitossociais" rename to "bronze_sac_efeitossociais";
alter table if exists salic_bronze."bronze_sac_efeitossociais" set schema salic;
alter table if exists salic_bronze."sac__empenho" rename to "bronze_sac_empenho";
alter table if exists salic_bronze."bronze_sac_empenho" set schema salic;
alter table if exists salic_bronze."sac__empenhoprojeto" rename to "bronze_sac_empenhoprojeto";
alter table if exists salic_bronze."bronze_sac_empenhoprojeto" set schema salic;
alter table if exists salic_bronze."sac__enquadramento" rename to "bronze_sac_enquadramento";
alter table if exists salic_bronze."bronze_sac_enquadramento" set schema salic;
alter table if exists salic_bronze."sac__entregaproduto" rename to "bronze_sac_entregaproduto";
alter table if exists salic_bronze."bronze_sac_entregaproduto" set schema salic;
alter table if exists salic_bronze."sac__estados" rename to "bronze_sac_estados";
alter table if exists salic_bronze."bronze_sac_estados" set schema salic;
alter table if exists salic_bronze."sac__etiqueta" rename to "bronze_sac_etiqueta";
alter table if exists salic_bronze."bronze_sac_etiqueta" set schema salic;
alter table if exists salic_bronze."sac__evolucao" rename to "bronze_sac_evolucao";
alter table if exists salic_bronze."bronze_sac_evolucao" set schema salic;
alter table if exists salic_bronze."sac__fase" rename to "bronze_sac_fase";
alter table if exists salic_bronze."bronze_sac_fase" set schema salic;
alter table if exists salic_bronze."sac__fluxos" rename to "bronze_sac_fluxos";
alter table if exists salic_bronze."bronze_sac_fluxos" set schema salic;
alter table if exists salic_bronze."sac__fluxosprojeto" rename to "bronze_sac_fluxosprojeto";
alter table if exists salic_bronze."bronze_sac_fluxosprojeto" set schema salic;
alter table if exists salic_bronze."sac__fonte" rename to "bronze_sac_fonte";
alter table if exists salic_bronze."bronze_sac_fonte" set schema salic;
alter table if exists salic_bronze."sac__genero" rename to "bronze_sac_genero";
alter table if exists salic_bronze."bronze_sac_genero" set schema salic;
alter table if exists salic_bronze."sac__geralext" rename to "bronze_sac_geralext";
alter table if exists salic_bronze."bronze_sac_geralext" set schema salic;
alter table if exists salic_bronze."sac__grupoempresarial" rename to "bronze_sac_grupoempresarial";
alter table if exists salic_bronze."bronze_sac_grupoempresarial" set schema salic;
alter table if exists salic_bronze."sac__guiarecolhimento" rename to "bronze_sac_guiarecolhimento";
alter table if exists salic_bronze."bronze_sac_guiarecolhimento" set schema salic;
alter table if exists salic_bronze."sac__historicoaspectosfinanceirosx" rename to "bronze_sac_historicoaspectosfinanceirosx";
alter table if exists salic_bronze."bronze_sac_historicoaspectosfinanceirosx" set schema salic;
alter table if exists salic_bronze."sac__historicoaspectosfisicosx" rename to "bronze_sac_historicoaspectosfisicosx";
alter table if exists salic_bronze."bronze_sac_historicoaspectosfisicosx" set schema salic;
alter table if exists salic_bronze."sac__historicoaspectosresultado" rename to "bronze_sac_historicoaspectosresultado";
alter table if exists salic_bronze."bronze_sac_historicoaspectosresultado" set schema salic;
alter table if exists salic_bronze."sac__historicoautuacaox" rename to "bronze_sac_historicoautuacaox";
alter table if exists salic_bronze."bronze_sac_historicoautuacaox" set schema salic;
alter table if exists salic_bronze."sac__historicocarta" rename to "bronze_sac_historicocarta";
alter table if exists salic_bronze."bronze_sac_historicocarta" set schema salic;
alter table if exists salic_bronze."sac__historicodelecaoaprovacao" rename to "bronze_sac_historicodelecaoaprovacao";
alter table if exists salic_bronze."bronze_sac_historicodelecaoaprovacao" set schema salic;
alter table if exists salic_bronze."sac__historicoinabilitado" rename to "bronze_sac_historicoinabilitado";
alter table if exists salic_bronze."bronze_sac_historicoinabilitado" set schema salic;
alter table if exists salic_bronze."sac__historicosituacao" rename to "bronze_sac_historicosituacao";
alter table if exists salic_bronze."bronze_sac_historicosituacao" set schema salic;
alter table if exists salic_bronze."sac__historicounidade" rename to "bronze_sac_historicounidade";
alter table if exists salic_bronze."bronze_sac_historicounidade" set schema salic;
alter table if exists salic_bronze."sac__humanidades" rename to "bronze_sac_humanidades";
alter table if exists salic_bronze."bronze_sac_humanidades" set schema salic;
alter table if exists salic_bronze."sac__impactox" rename to "bronze_sac_impactox";
alter table if exists salic_bronze."bronze_sac_impactox" set schema salic;
alter table if exists salic_bronze."sac__inabilitado" rename to "bronze_sac_inabilitado";
alter table if exists salic_bronze."bronze_sac_inabilitado" set schema salic;
alter table if exists salic_bronze."sac__incentivoporcgccpf" rename to "bronze_sac_incentivoporcgccpf";
alter table if exists salic_bronze."bronze_sac_incentivoporcgccpf" set schema salic;
alter table if exists salic_bronze."sac__informacaotrimestralx" rename to "bronze_sac_informacaotrimestralx";
alter table if exists salic_bronze."bronze_sac_informacaotrimestralx" set schema salic;
alter table if exists salic_bronze."sac__instrumentobanda" rename to "bronze_sac_instrumentobanda";
alter table if exists salic_bronze."bronze_sac_instrumentobanda" set schema salic;
alter table if exists salic_bronze."sac__interessado" rename to "bronze_sac_interessado";
alter table if exists salic_bronze."bronze_sac_interessado" set schema salic;
alter table if exists salic_bronze."sac__internet" rename to "bronze_sac_internet";
alter table if exists salic_bronze."bronze_sac_internet" set schema salic;
alter table if exists salic_bronze."sac__intranet" rename to "bronze_sac_intranet";
alter table if exists salic_bronze."bronze_sac_intranet" set schema salic;
alter table if exists salic_bronze."sac__kitbanda" rename to "bronze_sac_kitbanda";
alter table if exists salic_bronze."bronze_sac_kitbanda" set schema salic;
alter table if exists salic_bronze."sac__kitentregue" rename to "bronze_sac_kitentregue";
alter table if exists salic_bronze."bronze_sac_kitentregue" set schema salic;
alter table if exists salic_bronze."sac__preprojeto" rename to "bronze_sac_preprojeto";
alter table if exists salic_bronze."bronze_sac_preprojeto" set schema salic;
alter table if exists salic_bronze."sac__projetos" rename to "bronze_sac_projetos";
alter table if exists salic_bronze."bronze_sac_projetos" set schema salic;
alter table if exists salic_bronze."sac__situacao" rename to "bronze_sac_situacao";
alter table if exists salic_bronze."bronze_sac_situacao" set schema salic;
alter table if exists salic_bronze."sac__tbapiaprovacoes" rename to "bronze_sac_tbapiaprovacoes";
alter table if exists salic_bronze."bronze_sac_tbapiaprovacoes" set schema salic;
alter table if exists salic_bronze."sac__tbapicomprovacoes" rename to "bronze_sac_tbapicomprovacoes";
alter table if exists salic_bronze."bronze_sac_tbapicomprovacoes" set schema salic;
alter table if exists salic_bronze."sac__tbapiincentivos" rename to "bronze_sac_tbapiincentivos";
alter table if exists salic_bronze."bronze_sac_tbapiincentivos" set schema salic;
alter table if exists salic_bronze."sac__tbcomplianceitemcomprovado" rename to "bronze_sac_tbcomplianceitemcomprovado";
alter table if exists salic_bronze."bronze_sac_tbcomplianceitemcomprovado" set schema salic;
alter table if exists salic_bronze."sac__tbcomprovacaopassivo" rename to "bronze_sac_tbcomprovacaopassivo";
alter table if exists salic_bronze."bronze_sac_tbcomprovacaopassivo" set schema salic;
alter table if exists salic_bronze."sac__tbcomprovantebeneficiario" rename to "bronze_sac_tbcomprovantebeneficiario";
alter table if exists salic_bronze."bronze_sac_tbcomprovantebeneficiario" set schema salic;
alter table if exists salic_bronze."sac__tbcomprovanteexecucao" rename to "bronze_sac_tbcomprovanteexecucao";
alter table if exists salic_bronze."bronze_sac_tbcomprovanteexecucao" set schema salic;
alter table if exists salic_bronze."sac__tbcomprovantetrimestral" rename to "bronze_sac_tbcomprovantetrimestral";
alter table if exists salic_bronze."bronze_sac_tbcomprovantetrimestral" set schema salic;
alter table if exists salic_bronze."sac__tbcomunicados" rename to "bronze_sac_tbcomunicados";
alter table if exists salic_bronze."bronze_sac_tbcomunicados" set schema salic;
alter table if exists salic_bronze."sac__tbconfigurarpagamento" rename to "bronze_sac_tbconfigurarpagamento";
alter table if exists salic_bronze."bronze_sac_tbconfigurarpagamento" set schema salic;
alter table if exists salic_bronze."sac__tbconfigurarpagamentoxtbassinantes" rename to "bronze_sac_tbconfigurarpagamentoxtbassinantes";
alter table if exists salic_bronze."bronze_sac_tbconfigurarpagamentoxtbassinantes" set schema salic;
alter table if exists salic_bronze."sac__tbcontabancariabloqueada" rename to "bronze_sac_tbcontabancariabloqueada";
alter table if exists salic_bronze."bronze_sac_tbcontabancariabloqueada" set schema salic;
alter table if exists salic_bronze."sac__tbcontacartao" rename to "bronze_sac_tbcontacartao";
alter table if exists salic_bronze."bronze_sac_tbcontacartao" set schema salic;
alter table if exists salic_bronze."sac__tbcontacartaovip036" rename to "bronze_sac_tbcontacartaovip036";
alter table if exists salic_bronze."bronze_sac_tbcontacartaovip036" set schema salic;
alter table if exists salic_bronze."sac__tbcovid" rename to "bronze_sac_tbcovid";
alter table if exists salic_bronze."bronze_sac_tbcovid" set schema salic;
alter table if exists salic_bronze."sac__tbcredenciaparecerista_2020" rename to "bronze_sac_tbcredenciaparecerista_2020";
alter table if exists salic_bronze."bronze_sac_tbcredenciaparecerista_2020" set schema salic;
alter table if exists salic_bronze."sac__tbcumprimentoobjeto" rename to "bronze_sac_tbcumprimentoobjeto";
alter table if exists salic_bronze."bronze_sac_tbcumprimentoobjeto" set schema salic;
alter table if exists salic_bronze."sac__tbcumprimentoobjetoxarquivo" rename to "bronze_sac_tbcumprimentoobjetoxarquivo";
alter table if exists salic_bronze."bronze_sac_tbcumprimentoobjetoxarquivo" set schema salic;
alter table if exists salic_bronze."sac__tbcustosvinculados" rename to "bronze_sac_tbcustosvinculados";
alter table if exists salic_bronze."bronze_sac_tbcustosvinculados" set schema salic;
alter table if exists salic_bronze."sac__tbdeclaracaobeneficiosfiscais" rename to "bronze_sac_tbdeclaracaobeneficiosfiscais";
alter table if exists salic_bronze."bronze_sac_tbdeclaracaobeneficiosfiscais" set schema salic;
alter table if exists salic_bronze."sac__tbdemandasdeorgaosdecontrole" rename to "bronze_sac_tbdemandasdeorgaosdecontrole";
alter table if exists salic_bronze."bronze_sac_tbdemandasdeorgaosdecontrole" set schema salic;
alter table if exists salic_bronze."sac__tbdemandasdeorgaosdecontrolexprojetos" rename to "bronze_sac_tbdemandasdeorgaosdecontrolexprojetos";
alter table if exists salic_bronze."bronze_sac_tbdemandasdeorgaosdecontrolexprojetos" set schema salic;
alter table if exists salic_bronze."sac__tbdeparaplanilhaaprovacao" rename to "bronze_sac_tbdeparaplanilhaaprovacao";
alter table if exists salic_bronze."bronze_sac_tbdeparaplanilhaaprovacao" set schema salic;
alter table if exists salic_bronze."sac__tbdepositoidentificadocaptacao" rename to "bronze_sac_tbdepositoidentificadocaptacao";
alter table if exists salic_bronze."bronze_sac_tbdepositoidentificadocaptacao" set schema salic;
alter table if exists salic_bronze."sac__tbdepositoidentificadocaptacaoold" rename to "bronze_sac_tbdepositoidentificadocaptacaoold";
alter table if exists salic_bronze."bronze_sac_tbdepositoidentificadocaptacaoold" set schema salic;
alter table if exists salic_bronze."sac__tbdepositoidentificadomovimentacao" rename to "bronze_sac_tbdepositoidentificadomovimentacao";
alter table if exists salic_bronze."bronze_sac_tbdepositoidentificadomovimentacao" set schema salic;
alter table if exists salic_bronze."sac__tbdescricaorelatorioconsolidado" rename to "bronze_sac_tbdescricaorelatorioconsolidado";
alter table if exists salic_bronze."bronze_sac_tbdescricaorelatorioconsolidado" set schema salic;
alter table if exists salic_bronze."sac__tbdeslocamento" rename to "bronze_sac_tbdeslocamento";
alter table if exists salic_bronze."bronze_sac_tbdeslocamento" set schema salic;
alter table if exists salic_bronze."sac__tbdespacho" rename to "bronze_sac_tbdespacho";
alter table if exists salic_bronze."bronze_sac_tbdespacho" set schema salic;
alter table if exists salic_bronze."sac__tbdetalhaplanodistribuicao" rename to "bronze_sac_tbdetalhaplanodistribuicao";
alter table if exists salic_bronze."bronze_sac_tbdetalhaplanodistribuicao" set schema salic;
alter table if exists salic_bronze."sac__tbdetalhaplanodistribuicaoreadequacao" rename to "bronze_sac_tbdetalhaplanodistribuicaoreadequacao";
alter table if exists salic_bronze."bronze_sac_tbdetalhaplanodistribuicaoreadequacao" set schema salic;
alter table if exists salic_bronze."sac__tbdevolucaosecretaria" rename to "bronze_sac_tbdevolucaosecretaria";
alter table if exists salic_bronze."bronze_sac_tbdevolucaosecretaria" set schema salic;
alter table if exists salic_bronze."sac__tbdiligencia" rename to "bronze_sac_tbdiligencia";
alter table if exists salic_bronze."bronze_sac_tbdiligencia" set schema salic;
alter table if exists salic_bronze."sac__tbdiligenciaxarquivo" rename to "bronze_sac_tbdiligenciaxarquivo";
alter table if exists salic_bronze."bronze_sac_tbdiligenciaxarquivo" set schema salic;
alter table if exists salic_bronze."sac__tbdispositivomovel" rename to "bronze_sac_tbdispositivomovel";
alter table if exists salic_bronze."bronze_sac_tbdispositivomovel" set schema salic;
alter table if exists salic_bronze."sac__tbdistribuicaoproduto" rename to "bronze_sac_tbdistribuicaoproduto";
alter table if exists salic_bronze."bronze_sac_tbdistribuicaoproduto" set schema salic;
alter table if exists salic_bronze."sac__tbdistribuirdemandasdeorgaosdecontrole" rename to "bronze_sac_tbdistribuirdemandasdeorgaosdecontrole";
alter table if exists salic_bronze."bronze_sac_tbdistribuirdemandasdeorgaosdecontrole" set schema salic;
alter table if exists salic_bronze."sac__tbdistribuirparecer" rename to "bronze_sac_tbdistribuirparecer";
alter table if exists salic_bronze."bronze_sac_tbdistribuirparecer" set schema salic;
alter table if exists salic_bronze."sac__tbdistribuirprojeto" rename to "bronze_sac_tbdistribuirprojeto";
alter table if exists salic_bronze."bronze_sac_tbdistribuirprojeto" set schema salic;
alter table if exists salic_bronze."sac__tbdistribuirprojetopassivo" rename to "bronze_sac_tbdistribuirprojetopassivo";
alter table if exists salic_bronze."bronze_sac_tbdistribuirprojetopassivo" set schema salic;
alter table if exists salic_bronze."sac__tbdistribuirreadequacao" rename to "bronze_sac_tbdistribuirreadequacao";
alter table if exists salic_bronze."bronze_sac_tbdistribuirreadequacao" set schema salic;
alter table if exists salic_bronze."sac__tbdocumento20100107_1x" rename to "bronze_sac_tbdocumento20100107_1x";
alter table if exists salic_bronze."bronze_sac_tbdocumento20100107_1x" set schema salic;
alter table if exists salic_bronze."sac__tbdocumento20100107x" rename to "bronze_sac_tbdocumento20100107x";
alter table if exists salic_bronze."bronze_sac_tbdocumento20100107x" set schema salic;
alter table if exists salic_bronze."sac__tbdocumentoaceitacao" rename to "bronze_sac_tbdocumentoaceitacao";
alter table if exists salic_bronze."bronze_sac_tbdocumentoaceitacao" set schema salic;
alter table if exists salic_bronze."sac__tbdocumentoassinatura" rename to "bronze_sac_tbdocumentoassinatura";
alter table if exists salic_bronze."bronze_sac_tbdocumentoassinatura" set schema salic;
alter table if exists salic_bronze."sac__tbdocumentodespachoassinatura" rename to "bronze_sac_tbdocumentodespachoassinatura";
alter table if exists salic_bronze."bronze_sac_tbdocumentodespachoassinatura" set schema salic;
alter table if exists salic_bronze."sac__tbdocumentosagentes20100107x" rename to "bronze_sac_tbdocumentosagentes20100107x";
alter table if exists salic_bronze."bronze_sac_tbdocumentosagentes20100107x" set schema salic;
alter table if exists salic_bronze."sac__tbdocumentospreprojeto20100107x" rename to "bronze_sac_tbdocumentospreprojeto20100107x";
alter table if exists salic_bronze."bronze_sac_tbdocumentospreprojeto20100107x" set schema salic;
alter table if exists salic_bronze."sac__tbfiscalizacao" rename to "bronze_sac_tbfiscalizacao";
alter table if exists salic_bronze."bronze_sac_tbfiscalizacao" set schema salic;
alter table if exists salic_bronze."sac__tbfundopatrimonial" rename to "bronze_sac_tbfundopatrimonial";
alter table if exists salic_bronze."bronze_sac_tbfundopatrimonial" set schema salic;
alter table if exists salic_bronze."sac__tbfundopatrimonialusuario" rename to "bronze_sac_tbfundopatrimonialusuario";
alter table if exists salic_bronze."bronze_sac_tbfundopatrimonialusuario" set schema salic;
alter table if exists salic_bronze."sac__tbgerarpagamentoparecerista" rename to "bronze_sac_tbgerarpagamentoparecerista";
alter table if exists salic_bronze."bronze_sac_tbgerarpagamentoparecerista" set schema salic;
alter table if exists salic_bronze."sac__tbhistoricoalteracaodocumento" rename to "bronze_sac_tbhistoricoalteracaodocumento";
alter table if exists salic_bronze."bronze_sac_tbhistoricoalteracaodocumento" set schema salic;
alter table if exists salic_bronze."sac__tbhistoricoalteracaoprojeto" rename to "bronze_sac_tbhistoricoalteracaoprojeto";
alter table if exists salic_bronze."bronze_sac_tbhistoricoalteracaoprojeto" set schema salic;
alter table if exists salic_bronze."sac__tbhistoricocontabloqueada" rename to "bronze_sac_tbhistoricocontabloqueada";
alter table if exists salic_bronze."bronze_sac_tbhistoricocontabloqueada" set schema salic;
alter table if exists salic_bronze."sac__tbhistoricodevolucaofiscalizacao" rename to "bronze_sac_tbhistoricodevolucaofiscalizacao";
alter table if exists salic_bronze."bronze_sac_tbhistoricodevolucaofiscalizacao" set schema salic;
alter table if exists salic_bronze."sac__tbhistoricodiligencia" rename to "bronze_sac_tbhistoricodiligencia";
alter table if exists salic_bronze."bronze_sac_tbhistoricodiligencia" set schema salic;
alter table if exists salic_bronze."sac__tbhistoricodocumento" rename to "bronze_sac_tbhistoricodocumento";
alter table if exists salic_bronze."bronze_sac_tbhistoricodocumento" set schema salic;
alter table if exists salic_bronze."sac__tbhistoricoemail" rename to "bronze_sac_tbhistoricoemail";
alter table if exists salic_bronze."bronze_sac_tbhistoricoemail" set schema salic;
alter table if exists salic_bronze."sac__tbhistoricoexclusaoconta" rename to "bronze_sac_tbhistoricoexclusaoconta";
alter table if exists salic_bronze."bronze_sac_tbhistoricoexclusaoconta" set schema salic;
alter table if exists salic_bronze."sac__tbhomologacao" rename to "bronze_sac_tbhomologacao";
alter table if exists salic_bronze."bronze_sac_tbhomologacao" set schema salic;
alter table if exists salic_bronze."sac__tbimovel" rename to "bronze_sac_tbimovel";
alter table if exists salic_bronze."bronze_sac_tbimovel" set schema salic;
alter table if exists salic_bronze."sac__tbinadimplente" rename to "bronze_sac_tbinadimplente";
alter table if exists salic_bronze."bronze_sac_tbinadimplente" set schema salic;
alter table if exists salic_bronze."sac__tbitensplanilhaproduto" rename to "bronze_sac_tbitensplanilhaproduto";
alter table if exists salic_bronze."bronze_sac_tbitensplanilhaproduto" set schema salic;
alter table if exists salic_bronze."sac__tblancamentobancario" rename to "bronze_sac_tblancamentobancario";
alter table if exists salic_bronze."bronze_sac_tblancamentobancario" set schema salic;
alter table if exists salic_bronze."sac__tblancamentobancarioxcomprovantepagamento" rename to "bronze_sac_tblancamentobancarioxcomprovantepagamento";
alter table if exists salic_bronze."bronze_sac_tblancamentobancarioxcomprovantepagamento" set schema salic;
alter table if exists salic_bronze."sac__tblaudofinal" rename to "bronze_sac_tblaudofinal";
alter table if exists salic_bronze."bronze_sac_tblaudofinal" set schema salic;
alter table if exists salic_bronze."sac__tblogintentativasacesso" rename to "bronze_sac_tblogintentativasacesso";
alter table if exists salic_bronze."bronze_sac_tblogintentativasacesso" set schema salic;
alter table if exists salic_bronze."sac__tblogomarca" rename to "bronze_sac_tblogomarca";
alter table if exists salic_bronze."bronze_sac_tblogomarca" set schema salic;
alter table if exists salic_bronze."sac__tblote" rename to "bronze_sac_tblote";
alter table if exists salic_bronze."bronze_sac_tblote" set schema salic;
alter table if exists salic_bronze."sac__tbmanterportaria" rename to "bronze_sac_tbmanterportaria";
alter table if exists salic_bronze."bronze_sac_tbmanterportaria" set schema salic;
alter table if exists salic_bronze."sac__tbmensagem" rename to "bronze_sac_tbmensagem";
alter table if exists salic_bronze."bronze_sac_tbmensagem" set schema salic;
alter table if exists salic_bronze."sac__tbmensagemdispositivomovel" rename to "bronze_sac_tbmensagemdispositivomovel";
alter table if exists salic_bronze."bronze_sac_tbmensagemdispositivomovel" set schema salic;
alter table if exists salic_bronze."sac__tbmodelotermodecisao" rename to "bronze_sac_tbmodelotermodecisao";
alter table if exists salic_bronze."bronze_sac_tbmodelotermodecisao" set schema salic;
alter table if exists salic_bronze."sac__tbmonitoramentoadistancia" rename to "bronze_sac_tbmonitoramentoadistancia";
alter table if exists salic_bronze."bronze_sac_tbmonitoramentoadistancia" set schema salic;
alter table if exists salic_bronze."sac__tbmonitoramentoprojeto" rename to "bronze_sac_tbmonitoramentoprojeto";
alter table if exists salic_bronze."bronze_sac_tbmonitoramentoprojeto" set schema salic;
alter table if exists salic_bronze."sac__tbmotivodevolucao" rename to "bronze_sac_tbmotivodevolucao";
alter table if exists salic_bronze."bronze_sac_tbmotivodevolucao" set schema salic;
alter table if exists salic_bronze."sac__tbmovimentacao" rename to "bronze_sac_tbmovimentacao";
alter table if exists salic_bronze."bronze_sac_tbmovimentacao" set schema salic;
alter table if exists salic_bronze."sac__tbmovimentacaobancaria" rename to "bronze_sac_tbmovimentacaobancaria";
alter table if exists salic_bronze."bronze_sac_tbmovimentacaobancaria" set schema salic;
alter table if exists salic_bronze."sac__tbmovimentacaobancariaitem" rename to "bronze_sac_tbmovimentacaobancariaitem";
alter table if exists salic_bronze."bronze_sac_tbmovimentacaobancariaitem" set schema salic;
alter table if exists salic_bronze."sac__tbmovimentacaobancariaitemxtipoinconsistencia" rename to "bronze_sac_tbmovimentacaobancariaitemxtipoinconsistencia";
alter table if exists salic_bronze."bronze_sac_tbmovimentacaobancariaitemxtipoinconsistencia" set schema salic;
alter table if exists salic_bronze."sac__tbnormativos" rename to "bronze_sac_tbnormativos";
alter table if exists salic_bronze."bronze_sac_tbnormativos" set schema salic;
alter table if exists salic_bronze."sac__tbnotafiscaleletronica" rename to "bronze_sac_tbnotafiscaleletronica";
alter table if exists salic_bronze."bronze_sac_tbnotafiscaleletronica" set schema salic;
alter table if exists salic_bronze."sac__tbnotificacao" rename to "bronze_sac_tbnotificacao";
alter table if exists salic_bronze."bronze_sac_tbnotificacao" set schema salic;
alter table if exists salic_bronze."sac__tbopinarprojeto" rename to "bronze_sac_tbopinarprojeto";
alter table if exists salic_bronze."bronze_sac_tbopinarprojeto" set schema salic;
alter table if exists salic_bronze."sac__tborgaofiscalizador" rename to "bronze_sac_tborgaofiscalizador";
alter table if exists salic_bronze."bronze_sac_tborgaofiscalizador" set schema salic;
alter table if exists salic_bronze."sac__tbpagamentopareceristaxarquivo" rename to "bronze_sac_tbpagamentopareceristaxarquivo";
alter table if exists salic_bronze."bronze_sac_tbpagamentopareceristaxarquivo" set schema salic;
alter table if exists salic_bronze."sac__tbpagarparecerista" rename to "bronze_sac_tbpagarparecerista";
alter table if exists salic_bronze."bronze_sac_tbpagarparecerista" set schema salic;
alter table if exists salic_bronze."sac__tbpagarpareceristareadequacao" rename to "bronze_sac_tbpagarpareceristareadequacao";
alter table if exists salic_bronze."bronze_sac_tbpagarpareceristareadequacao" set schema salic;
alter table if exists salic_bronze."sac__tbpagarpareceristarecurso" rename to "bronze_sac_tbpagarpareceristarecurso";
alter table if exists salic_bronze."bronze_sac_tbpagarpareceristarecurso" set schema salic;
alter table if exists salic_bronze."sac__tbparecerconsolidado" rename to "bronze_sac_tbparecerconsolidado";
alter table if exists salic_bronze."bronze_sac_tbparecerconsolidado" set schema salic;
alter table if exists salic_bronze."sac__tbpareceristaafastamento" rename to "bronze_sac_tbpareceristaafastamento";
alter table if exists salic_bronze."bronze_sac_tbpareceristaafastamento" set schema salic;
alter table if exists salic_bronze."sac__tbpareceristaedital" rename to "bronze_sac_tbpareceristaedital";
alter table if exists salic_bronze."bronze_sac_tbpareceristaedital" set schema salic;
alter table if exists salic_bronze."sac__tbpareceristaempenho" rename to "bronze_sac_tbpareceristaempenho";
alter table if exists salic_bronze."bronze_sac_tbpareceristaempenho" set schema salic;
alter table if exists salic_bronze."sac__tbpareceristainexigibilidade" rename to "bronze_sac_tbpareceristainexigibilidade";
alter table if exists salic_bronze."bronze_sac_tbpareceristainexigibilidade" set schema salic;
alter table if exists salic_bronze."sac__tbpareceristalotepagamento" rename to "bronze_sac_tbpareceristalotepagamento";
alter table if exists salic_bronze."bronze_sac_tbpareceristalotepagamento" set schema salic;
alter table if exists salic_bronze."sac__tbpareceristapagamento" rename to "bronze_sac_tbpareceristapagamento";
alter table if exists salic_bronze."bronze_sac_tbpareceristapagamento" set schema salic;
alter table if exists salic_bronze."sac__tbpareceristaprojetoredistribuido" rename to "bronze_sac_tbpareceristaprojetoredistribuido";
alter table if exists salic_bronze."bronze_sac_tbpareceristaprojetoredistribuido" set schema salic;
alter table if exists salic_bronze."sac__tbpareceristaprorrogarprazo" rename to "bronze_sac_tbpareceristaprorrogarprazo";
alter table if exists salic_bronze."bronze_sac_tbpareceristaprorrogarprazo" set schema salic;
alter table if exists salic_bronze."sac__tbpareceristaprorrogarprazoxdistribuicao" rename to "bronze_sac_tbpareceristaprorrogarprazoxdistribuicao";
alter table if exists salic_bronze."bronze_sac_tbpareceristaprorrogarprazoxdistribuicao" set schema salic;
alter table if exists salic_bronze."sac__tbpareceristaquestionamento" rename to "bronze_sac_tbpareceristaquestionamento";
alter table if exists salic_bronze."bronze_sac_tbpareceristaquestionamento" set schema salic;
alter table if exists salic_bronze."sac__tbpareceristavaloranalisado" rename to "bronze_sac_tbpareceristavaloranalisado";
alter table if exists salic_bronze."bronze_sac_tbpareceristavaloranalisado" set schema salic;
alter table if exists salic_bronze."sac__tbplanilhaaprovacao" rename to "bronze_sac_tbplanilhaaprovacao";
alter table if exists salic_bronze."bronze_sac_tbplanilhaaprovacao" set schema salic;
alter table if exists salic_bronze."sac__tbplanilhadesembolso" rename to "bronze_sac_tbplanilhadesembolso";
alter table if exists salic_bronze."bronze_sac_tbplanilhadesembolso" set schema salic;
alter table if exists salic_bronze."sac__tbplanilhaetapa" rename to "bronze_sac_tbplanilhaetapa";
alter table if exists salic_bronze."bronze_sac_tbplanilhaetapa" set schema salic;
alter table if exists salic_bronze."sac__tbplanilhaitens" rename to "bronze_sac_tbplanilhaitens";
alter table if exists salic_bronze."bronze_sac_tbplanilhaitens" set schema salic;
alter table if exists salic_bronze."sac__tbplanilhajustificativa" rename to "bronze_sac_tbplanilhajustificativa";
alter table if exists salic_bronze."bronze_sac_tbplanilhajustificativa" set schema salic;
alter table if exists salic_bronze."sac__tbplanilhaprojeto" rename to "bronze_sac_tbplanilhaprojeto";
alter table if exists salic_bronze."bronze_sac_tbplanilhaprojeto" set schema salic;
alter table if exists salic_bronze."sac__tbplanilhaproposta" rename to "bronze_sac_tbplanilhaproposta";
alter table if exists salic_bronze."bronze_sac_tbplanilhaproposta" set schema salic;
alter table if exists salic_bronze."sac__tbplanilhaunidade" rename to "bronze_sac_tbplanilhaunidade";
alter table if exists salic_bronze."bronze_sac_tbplanilhaunidade" set schema salic;
alter table if exists salic_bronze."sac__tbplanodistribuicao" rename to "bronze_sac_tbplanodistribuicao";
alter table if exists salic_bronze."bronze_sac_tbplanodistribuicao" set schema salic;
alter table if exists salic_bronze."sac__tbplanodivulgacao" rename to "bronze_sac_tbplanodivulgacao";
alter table if exists salic_bronze."bronze_sac_tbplanodivulgacao" set schema salic;
alter table if exists salic_bronze."sac__tbpreprojetometa" rename to "bronze_sac_tbpreprojetometa";
alter table if exists salic_bronze."bronze_sac_tbpreprojetometa" set schema salic;
alter table if exists salic_bronze."sac__tbpreprojetoxenquadramento" rename to "bronze_sac_tbpreprojetoxenquadramento";
alter table if exists salic_bronze."bronze_sac_tbpreprojetoxenquadramento" set schema salic;
alter table if exists salic_bronze."sac__tbprioridadedeanalise" rename to "bronze_sac_tbprioridadedeanalise";
alter table if exists salic_bronze."bronze_sac_tbprioridadedeanalise" set schema salic;
alter table if exists salic_bronze."sac__tbprodutoxtipicidade" rename to "bronze_sac_tbprodutoxtipicidade";
alter table if exists salic_bronze."bronze_sac_tbprodutoxtipicidade" set schema salic;
alter table if exists salic_bronze."sac__tbprogramarouanet" rename to "bronze_sac_tbprogramarouanet";
alter table if exists salic_bronze."bronze_sac_tbprogramarouanet" set schema salic;
alter table if exists salic_bronze."sac__tbprogramarouanetareasegmento" rename to "bronze_sac_tbprogramarouanetareasegmento";
alter table if exists salic_bronze."bronze_sac_tbprogramarouanetareasegmento" set schema salic;
alter table if exists salic_bronze."sac__tbprogramarouanetavaliacao" rename to "bronze_sac_tbprogramarouanetavaliacao";
alter table if exists salic_bronze."bronze_sac_tbprogramarouanetavaliacao" set schema salic;
alter table if exists salic_bronze."sac__tbprogramarouanetavaliacaorecurso" rename to "bronze_sac_tbprogramarouanetavaliacaorecurso";
alter table if exists salic_bronze."bronze_sac_tbprogramarouanetavaliacaorecurso" set schema salic;
alter table if exists salic_bronze."sac__tbprogramarouanetavaliador" rename to "bronze_sac_tbprogramarouanetavaliador";
alter table if exists salic_bronze."bronze_sac_tbprogramarouanetavaliador" set schema salic;
alter table if exists salic_bronze."sac__tbprogramarouanetdistribuirproposta" rename to "bronze_sac_tbprogramarouanetdistribuirproposta";
alter table if exists salic_bronze."bronze_sac_tbprogramarouanetdistribuirproposta" set schema salic;
alter table if exists salic_bronze."sac__tbprogramarouanethabilitacao" rename to "bronze_sac_tbprogramarouanethabilitacao";
alter table if exists salic_bronze."bronze_sac_tbprogramarouanethabilitacao" set schema salic;
alter table if exists salic_bronze."sac__tbprogramarouanetlocalidade" rename to "bronze_sac_tbprogramarouanetlocalidade";
alter table if exists salic_bronze."bronze_sac_tbprogramarouanetlocalidade" set schema salic;
alter table if exists salic_bronze."sac__tbprogramarouanetpergunta" rename to "bronze_sac_tbprogramarouanetpergunta";
alter table if exists salic_bronze."bronze_sac_tbprogramarouanetpergunta" set schema salic;
alter table if exists salic_bronze."sac__tbprogramarouanetpropostaselecionada" rename to "bronze_sac_tbprogramarouanetpropostaselecionada";
alter table if exists salic_bronze."bronze_sac_tbprogramarouanetpropostaselecionada" set schema salic;
alter table if exists salic_bronze."sac__tbprogramarouanetrecurso" rename to "bronze_sac_tbprogramarouanetrecurso";
alter table if exists salic_bronze."bronze_sac_tbprogramarouanetrecurso" set schema salic;
alter table if exists salic_bronze."sac__tbprogramarouanetresultadofinal" rename to "bronze_sac_tbprogramarouanetresultadofinal";
alter table if exists salic_bronze."bronze_sac_tbprogramarouanetresultadofinal" set schema salic;
alter table if exists salic_bronze."sac__tbprogramarouanetresultadopreliminar" rename to "bronze_sac_tbprogramarouanetresultadopreliminar";
alter table if exists salic_bronze."bronze_sac_tbprogramarouanetresultadopreliminar" set schema salic;
alter table if exists salic_bronze."sac__tbprogramarouanetselecaopatrocinador" rename to "bronze_sac_tbprogramarouanetselecaopatrocinador";
alter table if exists salic_bronze."bronze_sac_tbprogramarouanetselecaopatrocinador" set schema salic;
alter table if exists salic_bronze."sac__tbprogramarouanetxproposta" rename to "bronze_sac_tbprogramarouanetxproposta";
alter table if exists salic_bronze."bronze_sac_tbprogramarouanetxproposta" set schema salic;
alter table if exists salic_bronze."sac__tbprojetofase" rename to "bronze_sac_tbprojetofase";
alter table if exists salic_bronze."bronze_sac_tbprojetofase" set schema salic;
alter table if exists salic_bronze."sac__tbprojetoreadequacao" rename to "bronze_sac_tbprojetoreadequacao";
alter table if exists salic_bronze."bronze_sac_tbprojetoreadequacao" set schema salic;
alter table if exists salic_bronze."sac__tbprojetorecebedorrecurso" rename to "bronze_sac_tbprojetorecebedorrecurso";
alter table if exists salic_bronze."bronze_sac_tbprojetorecebedorrecurso" set schema salic;
alter table if exists salic_bronze."sac__tbprojetosclonados" rename to "bronze_sac_tbprojetosclonados";
alter table if exists salic_bronze."bronze_sac_tbprojetosclonados" set schema salic;
alter table if exists salic_bronze."sac__tbprojetosituacaoanterior" rename to "bronze_sac_tbprojetosituacaoanterior";
alter table if exists salic_bronze."bronze_sac_tbprojetosituacaoanterior" set schema salic;
alter table if exists salic_bronze."sac__tbproposta" rename to "bronze_sac_tbproposta";
alter table if exists salic_bronze."bronze_sac_tbproposta" set schema salic;
alter table if exists salic_bronze."sac__tbreadequacao" rename to "bronze_sac_tbreadequacao";
alter table if exists salic_bronze."bronze_sac_tbreadequacao" set schema salic;
alter table if exists salic_bronze."sac__tbreadequacaoxparecer" rename to "bronze_sac_tbreadequacaoxparecer";
alter table if exists salic_bronze."bronze_sac_tbreadequacaoxparecer" set schema salic;
alter table if exists salic_bronze."sac__tbreadequacaoxtbdiligencia" rename to "bronze_sac_tbreadequacaoxtbdiligencia";
alter table if exists salic_bronze."bronze_sac_tbreadequacaoxtbdiligencia" set schema salic;
alter table if exists salic_bronze."sac__tbrecurso" rename to "bronze_sac_tbrecurso";
alter table if exists salic_bronze."bronze_sac_tbrecurso" set schema salic;
alter table if exists salic_bronze."sac__tbrecursoproposta" rename to "bronze_sac_tbrecursoproposta";
alter table if exists salic_bronze."bronze_sac_tbrecursoproposta" set schema salic;
alter table if exists salic_bronze."sac__tbrecursoxplanilhaaprovacao" rename to "bronze_sac_tbrecursoxplanilhaaprovacao";
alter table if exists salic_bronze."bronze_sac_tbrecursoxplanilhaaprovacao" set schema salic;
alter table if exists salic_bronze."sac__tbrelatorio" rename to "bronze_sac_tbrelatorio";
alter table if exists salic_bronze."bronze_sac_tbrelatorio" set schema salic;
alter table if exists salic_bronze."sac__vcapmecaudioanoprojeto" rename to "bronze_sac_vcapmecaudioanoprojeto";
alter table if exists salic_bronze."bronze_sac_vcapmecaudioanoprojeto" set schema salic;
alter table if exists salic_bronze."sac__vcapmecaudioprojeto" rename to "bronze_sac_vcapmecaudioprojeto";
alter table if exists salic_bronze."bronze_sac_vcapmecaudioprojeto" set schema salic;
alter table if exists salic_bronze."sac__vcaptacaoanoufareasegmentosemestre" rename to "bronze_sac_vcaptacaoanoufareasegmentosemestre";
alter table if exists salic_bronze."bronze_sac_vcaptacaoanoufareasegmentosemestre" set schema salic;
alter table if exists salic_bronze."sac__vcaptacaopormptipoapoiomecena" rename to "bronze_sac_vcaptacaopormptipoapoiomecena";
alter table if exists salic_bronze."bronze_sac_vcaptacaopormptipoapoiomecena" set schema salic;
alter table if exists salic_bronze."sac__vcaptado" rename to "bronze_sac_vcaptado";
alter table if exists salic_bronze."bronze_sac_vcaptado" set schema salic;
alter table if exists salic_bronze."sac__vcaptadoano" rename to "bronze_sac_vcaptadoano";
alter table if exists salic_bronze."bronze_sac_vcaptadoano" set schema salic;
alter table if exists salic_bronze."sac__vcaptadoanomesdiaprojeto" rename to "bronze_sac_vcaptadoanomesdiaprojeto";
alter table if exists salic_bronze."bronze_sac_vcaptadoanomesdiaprojeto" set schema salic;
alter table if exists salic_bronze."sac__vcaptadoanoprojeto" rename to "bronze_sac_vcaptadoanoprojeto";
alter table if exists salic_bronze."bronze_sac_vcaptadoanoprojeto" set schema salic;
alter table if exists salic_bronze."sac__vcartaaudio" rename to "bronze_sac_vcartaaudio";
alter table if exists salic_bronze."bronze_sac_vcartaaudio" set schema salic;
alter table if exists salic_bronze."sac__vcartapropaudio" rename to "bronze_sac_vcartapropaudio";
alter table if exists salic_bronze."bronze_sac_vcartapropaudio" set schema salic;
alter table if exists salic_bronze."sac__vcartas" rename to "bronze_sac_vcartas";
alter table if exists salic_bronze."bronze_sac_vcartas" set schema salic;
alter table if exists salic_bronze."sac__vchecaproponente" rename to "bronze_sac_vchecaproponente";
alter table if exists salic_bronze."bronze_sac_vchecaproponente" set schema salic;
alter table if exists salic_bronze."sac__vchecaqteinabilitacao" rename to "bronze_sac_vchecaqteinabilitacao";
alter table if exists salic_bronze."bronze_sac_vchecaqteinabilitacao" set schema salic;
alter table if exists salic_bronze."sac__vci" rename to "bronze_sac_vci";
alter table if exists salic_bronze."bronze_sac_vci" set schema salic;
alter table if exists salic_bronze."sac__vciset" rename to "bronze_sac_vciset";
alter table if exists salic_bronze."bronze_sac_vciset" set schema salic;
alter table if exists salic_bronze."sac__vcnpjcpf" rename to "bronze_sac_vcnpjcpf";
alter table if exists salic_bronze."bronze_sac_vcnpjcpf" set schema salic;
alter table if exists salic_bronze."sac__vcnpjvalido" rename to "bronze_sac_vcnpjvalido";
alter table if exists salic_bronze."bronze_sac_vcnpjvalido" set schema salic;
alter table if exists salic_bronze."sac__vcomparativoano" rename to "bronze_sac_vcomparativoano";
alter table if exists salic_bronze."bronze_sac_vcomparativoano" set schema salic;
alter table if exists salic_bronze."sac__vcomparativoanomec" rename to "bronze_sac_vcomparativoanomec";
alter table if exists salic_bronze."bronze_sac_vcomparativoanomec" set schema salic;
alter table if exists salic_bronze."sac__vcontabancaria" rename to "bronze_sac_vcontabancaria";
alter table if exists salic_bronze."bronze_sac_vcontabancaria" set schema salic;
alter table if exists salic_bronze."sac__vcontacorrente" rename to "bronze_sac_vcontacorrente";
alter table if exists salic_bronze."bronze_sac_vcontacorrente" set schema salic;
alter table if exists salic_bronze."sac__vconvenioanoarea" rename to "bronze_sac_vconvenioanoarea";
alter table if exists salic_bronze."bronze_sac_vconvenioanoarea" set schema salic;
alter table if exists salic_bronze."sac__vconvenioanouf" rename to "bronze_sac_vconvenioanouf";
alter table if exists salic_bronze."bronze_sac_vconvenioanouf" set schema salic;
alter table if exists salic_bronze."sac__vcustodoprojeto" rename to "bronze_sac_vcustodoprojeto";
alter table if exists salic_bronze."bronze_sac_vcustodoprojeto" set schema salic;
alter table if exists salic_bronze."sac__vdadoscomplementaresdecartas" rename to "bronze_sac_vdadoscomplementaresdecartas";
alter table if exists salic_bronze."bronze_sac_vdadoscomplementaresdecartas" set schema salic;
alter table if exists salic_bronze."sac__vdadosconvenio" rename to "bronze_sac_vdadosconvenio";
alter table if exists salic_bronze."bronze_sac_vdadosconvenio" set schema salic;
alter table if exists salic_bronze."sac__vdadosportariaaprovacao" rename to "bronze_sac_vdadosportariaaprovacao";
alter table if exists salic_bronze."bronze_sac_vdadosportariaaprovacao" set schema salic;
alter table if exists salic_bronze."sac__vdiligencias" rename to "bronze_sac_vdiligencias";
alter table if exists salic_bronze."bronze_sac_vdiligencias" set schema salic;
alter table if exists salic_bronze."sac__vdocumentoexigido" rename to "bronze_sac_vdocumentoexigido";
alter table if exists salic_bronze."bronze_sac_vdocumentoexigido" set schema salic;
alter table if exists salic_bronze."sac__vdtprimeiraultimacaptacao" rename to "bronze_sac_vdtprimeiraultimacaptacao";
alter table if exists salic_bronze."bronze_sac_vdtprimeiraultimacaptacao" set schema salic;
alter table if exists salic_bronze."sac__veiculacao" rename to "bronze_sac_veiculacao";
alter table if exists salic_bronze."bronze_sac_veiculacao" set schema salic;
alter table if exists salic_bronze."sac__vempresasincentivadorasano" rename to "bronze_sac_vempresasincentivadorasano";
alter table if exists salic_bronze."bronze_sac_vempresasincentivadorasano" set schema salic;
alter table if exists salic_bronze."sac__verificacao" rename to "bronze_sac_verificacao";
alter table if exists salic_bronze."bronze_sac_verificacao" set schema salic;
alter table if exists salic_bronze."sac__verificacaopecaxveiculo" rename to "bronze_sac_verificacaopecaxveiculo";
alter table if exists salic_bronze."bronze_sac_verificacaopecaxveiculo" set schema salic;
alter table if exists salic_bronze."sac__vetiquetaeneida" rename to "bronze_sac_vetiquetaeneida";
alter table if exists salic_bronze."bronze_sac_vetiquetaeneida" set schema salic;
alter table if exists salic_bronze."sac__vfnaprocapproj" rename to "bronze_sac_vfnaprocapproj";
alter table if exists salic_bronze."bronze_sac_vfnaprocapproj" set schema salic;
alter table if exists salic_bronze."sac__vfnaprocapprojmec" rename to "bronze_sac_vfnaprocapprojmec";
alter table if exists salic_bronze."bronze_sac_vfnaprocapprojmec" set schema salic;
alter table if exists salic_bronze."sac__vfninccaprenuncia" rename to "bronze_sac_vfninccaprenuncia";
alter table if exists salic_bronze."bronze_sac_vfninccaprenuncia" set schema salic;
alter table if exists salic_bronze."sac__vfnorcdetalhado" rename to "bronze_sac_vfnorcdetalhado";
alter table if exists salic_bronze."bronze_sac_vfnorcdetalhado" set schema salic;
alter table if exists salic_bronze."sac__vfntotalaprocapproj" rename to "bronze_sac_vfntotalaprocapproj";
alter table if exists salic_bronze."bronze_sac_vfntotalaprocapproj" set schema salic;
alter table if exists salic_bronze."sac__vforadaportaria" rename to "bronze_sac_vforadaportaria";
alter table if exists salic_bronze."bronze_sac_vforadaportaria" set schema salic;
alter table if exists salic_bronze."sac__vfsac3485" rename to "bronze_sac_vfsac3485";
alter table if exists salic_bronze."bronze_sac_vfsac3485" set schema salic;
alter table if exists salic_bronze."sac__vfunarte" rename to "bronze_sac_vfunarte";
alter table if exists salic_bronze."bronze_sac_vfunarte" set schema salic;
alter table if exists salic_bronze."sac__vgerentes" rename to "bronze_sac_vgerentes";
alter table if exists salic_bronze."bronze_sac_vgerentes" set schema salic;
alter table if exists salic_bronze."sac__vguianaovinculada" rename to "bronze_sac_vguianaovinculada";
alter table if exists salic_bronze."bronze_sac_vguianaovinculada" set schema salic;
alter table if exists salic_bronze."sac__vguiasderecolhimento" rename to "bronze_sac_vguiasderecolhimento";
alter table if exists salic_bronze."bronze_sac_vguiasderecolhimento" set schema salic;
alter table if exists salic_bronze."sac__vincentivador" rename to "bronze_sac_vincentivador";
alter table if exists salic_bronze."bronze_sac_vincentivador" set schema salic;
alter table if exists salic_bronze."sac__vincentivadorano" rename to "bronze_sac_vincentivadorano";
alter table if exists salic_bronze."bronze_sac_vincentivadorano" set schema salic;
alter table if exists salic_bronze."sac__vincentivadoraudioano" rename to "bronze_sac_vincentivadoraudioano";
alter table if exists salic_bronze."bronze_sac_vincentivadoraudioano" set schema salic;
alter table if exists salic_bronze."sac__vincentivadoresma" rename to "bronze_sac_vincentivadoresma";
alter table if exists salic_bronze."bronze_sac_vincentivadoresma" set schema salic;
alter table if exists salic_bronze."sac__vincentivadorpessoaano" rename to "bronze_sac_vincentivadorpessoaano";
alter table if exists salic_bronze."bronze_sac_vincentivadorpessoaano" set schema salic;
alter table if exists salic_bronze."sac__vincentivadorporprojeto" rename to "bronze_sac_vincentivadorporprojeto";
alter table if exists salic_bronze."bronze_sac_vincentivadorporprojeto" set schema salic;
alter table if exists salic_bronze."sac__vinvestimentominc" rename to "bronze_sac_vinvestimentominc";
alter table if exists salic_bronze."bronze_sac_vinvestimentominc" set schema salic;
alter table if exists salic_bronze."sac__vliberado" rename to "bronze_sac_vliberado";
alter table if exists salic_bronze."bronze_sac_vliberado" set schema salic;
alter table if exists salic_bronze."sac__vmaioresincentivadoresprojetos" rename to "bronze_sac_vmaioresincentivadoresprojetos";
alter table if exists salic_bronze."bronze_sac_vmaioresincentivadoresprojetos" set schema salic;
alter table if exists salic_bronze."sac__vmostraconvenio" rename to "bronze_sac_vmostraconvenio";
alter table if exists salic_bronze."bronze_sac_vmostraconvenio" set schema salic;
alter table if exists salic_bronze."sac__vmostraempenho" rename to "bronze_sac_vmostraempenho";
alter table if exists salic_bronze."bronze_sac_vmostraempenho" set schema salic;
alter table if exists salic_bronze."sac__vmostraordembancaria" rename to "bronze_sac_vmostraordembancaria";
alter table if exists salic_bronze."bronze_sac_vmostraordembancaria" set schema salic;
alter table if exists salic_bronze."sac__vnacional" rename to "bronze_sac_vnacional";
alter table if exists salic_bronze."bronze_sac_vnacional" set schema salic;
alter table if exists salic_bronze."sac__vnegativainabilitado" rename to "bronze_sac_vnegativainabilitado";
alter table if exists salic_bronze."bronze_sac_vnegativainabilitado" set schema salic;
alter table if exists salic_bronze."sac__vnegativainabilitadoetiqueta" rename to "bronze_sac_vnegativainabilitadoetiqueta";
alter table if exists salic_bronze."bronze_sac_vnegativainabilitadoetiqueta" set schema salic;
alter table if exists salic_bronze."sac__vnuncacaptou" rename to "bronze_sac_vnuncacaptou";
alter table if exists salic_bronze."bronze_sac_vnuncacaptou" set schema salic;
alter table if exists salic_bronze."sac__voutrasinformacoesaudio" rename to "bronze_sac_voutrasinformacoesaudio";
alter table if exists salic_bronze."bronze_sac_voutrasinformacoesaudio" set schema salic;
alter table if exists salic_bronze."sac__vparecerinicial" rename to "bronze_sac_vparecerinicial";
alter table if exists salic_bronze."bronze_sac_vparecerinicial" set schema salic;
alter table if exists salic_bronze."sac__vpautadereuniaocnic" rename to "bronze_sac_vpautadereuniaocnic";
alter table if exists salic_bronze."bronze_sac_vpautadereuniaocnic" set schema salic;
alter table if exists salic_bronze."sac__vpautadereuniaofncintercambio" rename to "bronze_sac_vpautadereuniaofncintercambio";
alter table if exists salic_bronze."bronze_sac_vpautadereuniaofncintercambio" set schema salic;
alter table if exists salic_bronze."sac__vportariaaprovado" rename to "bronze_sac_vportariaaprovado";
alter table if exists salic_bronze."bronze_sac_vportariaaprovado" set schema salic;
alter table if exists salic_bronze."sac__vportariacomplementacao" rename to "bronze_sac_vportariacomplementacao";
alter table if exists salic_bronze."bronze_sac_vportariacomplementacao" set schema salic;
alter table if exists salic_bronze."sac__vportarianormal" rename to "bronze_sac_vportarianormal";
alter table if exists salic_bronze."bronze_sac_vportarianormal" set schema salic;
alter table if exists salic_bronze."sac__vportariaprorrogacao" rename to "bronze_sac_vportariaprorrogacao";
alter table if exists salic_bronze."bronze_sac_vportariaprorrogacao" set schema salic;
alter table if exists salic_bronze."sac__vportariareducao" rename to "bronze_sac_vportariareducao";
alter table if exists salic_bronze."bronze_sac_vportariareducao" set schema salic;
alter table if exists salic_bronze."sac__vprazocaptacao" rename to "bronze_sac_vprazocaptacao";
alter table if exists salic_bronze."bronze_sac_vprazocaptacao" set schema salic;
alter table if exists salic_bronze."sac__vprestacao" rename to "bronze_sac_vprestacao";
alter table if exists salic_bronze."bronze_sac_vprestacao" set schema salic;
alter table if exists salic_bronze."sac__vprojetoavaliado" rename to "bronze_sac_vprojetoavaliado";
alter table if exists salic_bronze."bronze_sac_vprojetoavaliado" set schema salic;
alter table if exists salic_bronze."sac__vprojetodiligencia" rename to "bronze_sac_vprojetodiligencia";
alter table if exists salic_bronze."bronze_sac_vprojetodiligencia" set schema salic;
alter table if exists salic_bronze."sac__vprojetosemanalise" rename to "bronze_sac_vprojetosemanalise";
alter table if exists salic_bronze."bronze_sac_vprojetosemanalise" set schema salic;
alter table if exists salic_bronze."sac__vprojetosemexecucao" rename to "bronze_sac_vprojetosemexecucao";
alter table if exists salic_bronze."bronze_sac_vprojetosemexecucao" set schema salic;
alter table if exists salic_bronze."sac__vprojetosexecutados" rename to "bronze_sac_vprojetosexecutados";
alter table if exists salic_bronze."bronze_sac_vprojetosexecutados" set schema salic;
alter table if exists salic_bronze."sac__vprojprazodecaptvencnoexerc" rename to "bronze_sac_vprojprazodecaptvencnoexerc";
alter table if exists salic_bronze."bronze_sac_vprojprazodecaptvencnoexerc" set schema salic;
alter table if exists salic_bronze."sac__vpropapreaproexec" rename to "bronze_sac_vpropapreaproexec";
alter table if exists salic_bronze."bronze_sac_vpropapreaproexec" set schema salic;
alter table if exists salic_bronze."sac__vproponenteanouf" rename to "bronze_sac_vproponenteanouf";
alter table if exists salic_bronze."bronze_sac_vproponenteanouf" set schema salic;
alter table if exists salic_bronze."sac__vproponenteavaliado" rename to "bronze_sac_vproponenteavaliado";
alter table if exists salic_bronze."bronze_sac_vproponenteavaliado" set schema salic;
alter table if exists salic_bronze."sac__vproponentecaptacaoanouf" rename to "bronze_sac_vproponentecaptacaoanouf";
alter table if exists salic_bronze."bronze_sac_vproponentecaptacaoanouf" set schema salic;
alter table if exists salic_bronze."sac__vproponentecommaisdecincoproj" rename to "bronze_sac_vproponentecommaisdecincoproj";
alter table if exists salic_bronze."bronze_sac_vproponentecommaisdecincoproj" set schema salic;
alter table if exists salic_bronze."sac__vproponentemecenato" rename to "bronze_sac_vproponentemecenato";
alter table if exists salic_bronze."bronze_sac_vproponentemecenato" set schema salic;
alter table if exists salic_bronze."sac__vproponenteporarea" rename to "bronze_sac_vproponenteporarea";
alter table if exists salic_bronze."bronze_sac_vproponenteporarea" set schema salic;
alter table if exists salic_bronze."sac__vproponenteprojetos" rename to "bronze_sac_vproponenteprojetos";
alter table if exists salic_bronze."bronze_sac_vproponenteprojetos" set schema salic;
alter table if exists salic_bronze."sac__vproponenteprojetosongs" rename to "bronze_sac_vproponenteprojetosongs";
alter table if exists salic_bronze."bronze_sac_vproponenteprojetosongs" set schema salic;
alter table if exists salic_bronze."sac__vproponentetipopessoa" rename to "bronze_sac_vproponentetipopessoa";
alter table if exists salic_bronze."bronze_sac_vproponentetipopessoa" set schema salic;
alter table if exists salic_bronze."sac__vpropprojinabilitados" rename to "bronze_sac_vpropprojinabilitados";
alter table if exists salic_bronze."bronze_sac_vpropprojinabilitados" set schema salic;
alter table if exists salic_bronze."sac__vpropquereceberbandadenovo" rename to "bronze_sac_vpropquereceberbandadenovo";
alter table if exists salic_bronze."bronze_sac_vpropquereceberbandadenovo" set schema salic;
alter table if exists salic_bronze."sac__vproprecebeubanda" rename to "bronze_sac_vproprecebeubanda";
alter table if exists salic_bronze."bronze_sac_vproprecebeubanda" set schema salic;
alter table if exists salic_bronze."sac__vpropufareaseg" rename to "bronze_sac_vpropufareaseg";
alter table if exists salic_bronze."bronze_sac_vpropufareaseg" set schema salic;
alter table if exists salic_bronze."sac__vprorrogacao" rename to "bronze_sac_vprorrogacao";
alter table if exists salic_bronze."bronze_sac_vprorrogacao" set schema salic;
alter table if exists salic_bronze."sac__vqtdeprojaproanouf" rename to "bronze_sac_vqtdeprojaproanouf";
alter table if exists salic_bronze."bronze_sac_vqtdeprojaproanouf" set schema salic;
alter table if exists salic_bronze."sac__vrankbanda" rename to "bronze_sac_vrankbanda";
alter table if exists salic_bronze."bronze_sac_vrankbanda" set schema salic;
alter table if exists salic_bronze."sac__vregularidade" rename to "bronze_sac_vregularidade";
alter table if exists salic_bronze."bronze_sac_vregularidade" set schema salic;
alter table if exists salic_bronze."sac__vrenunciaanoareageral" rename to "bronze_sac_vrenunciaanoareageral";
alter table if exists salic_bronze."bronze_sac_vrenunciaanoareageral" set schema salic;
alter table if exists salic_bronze."sac__vrenunciaanoareaufgeral" rename to "bronze_sac_vrenunciaanoareaufgeral";
alter table if exists salic_bronze."bronze_sac_vrenunciaanoareaufgeral" set schema salic;
alter table if exists salic_bronze."sac__vrenunciafiscal" rename to "bronze_sac_vrenunciafiscal";
alter table if exists salic_bronze."bronze_sac_vrenunciafiscal" set schema salic;
alter table if exists salic_bronze."sac__vrenunciafiscalanoarea" rename to "bronze_sac_vrenunciafiscalanoarea";
alter table if exists salic_bronze."bronze_sac_vrenunciafiscalanoarea" set schema salic;
alter table if exists salic_bronze."sac__vrenunciafiscalgeral" rename to "bronze_sac_vrenunciafiscalgeral";
alter table if exists salic_bronze."bronze_sac_vrenunciafiscalgeral" set schema salic;
alter table if exists salic_bronze."sac__vrenunciageralporanoprojeto" rename to "bronze_sac_vrenunciageralporanoprojeto";
alter table if exists salic_bronze."bronze_sac_vrenunciageralporanoprojeto" set schema salic;
alter table if exists salic_bronze."sac__vrenunciaporano" rename to "bronze_sac_vrenunciaporano";
alter table if exists salic_bronze."bronze_sac_vrenunciaporano" set schema salic;
alter table if exists salic_bronze."sac__vrenunciaporincentivador" rename to "bronze_sac_vrenunciaporincentivador";
alter table if exists salic_bronze."bronze_sac_vrenunciaporincentivador" set schema salic;
alter table if exists salic_bronze."sac__vricardoreis" rename to "bronze_sac_vricardoreis";
alter table if exists salic_bronze."bronze_sac_vricardoreis" set schema salic;
alter table if exists salic_bronze."sac__vsaldoaprovado" rename to "bronze_sac_vsaldoaprovado";
alter table if exists salic_bronze."bronze_sac_vsaldoaprovado" set schema salic;
alter table if exists salic_bronze."sac__vsaldoaprovadoporano" rename to "bronze_sac_vsaldoaprovadoporano";
alter table if exists salic_bronze."bronze_sac_vsaldoaprovadoporano" set schema salic;
alter table if exists salic_bronze."sac__vsaldoquotascav" rename to "bronze_sac_vsaldoquotascav";
alter table if exists salic_bronze."bronze_sac_vsaldoquotascav" set schema salic;
alter table if exists salic_bronze."sac__vsegmento" rename to "bronze_sac_vsegmento";
alter table if exists salic_bronze."bronze_sac_vsegmento" set schema salic;
alter table if exists salic_bronze."sac__vtemposemcaptar" rename to "bronze_sac_vtemposemcaptar";
alter table if exists salic_bronze."bronze_sac_vtemposemcaptar" set schema salic;
alter table if exists salic_bronze."sac__vtermoaditivo" rename to "bronze_sac_vtermoaditivo";
alter table if exists salic_bronze."bronze_sac_vtermoaditivo" set schema salic;
alter table if exists salic_bronze."sac__vtermodecompromisso" rename to "bronze_sac_vtermodecompromisso";
alter table if exists salic_bronze."bronze_sac_vtermodecompromisso" set schema salic;
alter table if exists salic_bronze."sac__vtodaaprovacao" rename to "bronze_sac_vtodaaprovacao";
alter table if exists salic_bronze."bronze_sac_vtodaaprovacao" set schema salic;
alter table if exists salic_bronze."sac__vtotalaprovado" rename to "bronze_sac_vtotalaprovado";
alter table if exists salic_bronze."bronze_sac_vtotalaprovado" set schema salic;
alter table if exists salic_bronze."sac__vtotalcaptado" rename to "bronze_sac_vtotalcaptado";
alter table if exists salic_bronze."bronze_sac_vtotalcaptado" set schema salic;
alter table if exists salic_bronze."sac__vultimafase" rename to "bronze_sac_vultimafase";
alter table if exists salic_bronze."bronze_sac_vultimafase" set schema salic;
alter table if exists salic_bronze."sac__vusuarios" rename to "bronze_sac_vusuarios";
alter table if exists salic_bronze."bronze_sac_vusuarios" set schema salic;
alter table if exists salic_bronze."sac__vvaloresprojeto" rename to "bronze_sac_vvaloresprojeto";
alter table if exists salic_bronze."bronze_sac_vvaloresprojeto" set schema salic;
alter table if exists salic_bronze."sac__vversepodeprorrogar" rename to "bronze_sac_vversepodeprorrogar";
alter table if exists salic_bronze."bronze_sac_vversepodeprorrogar" set schema salic;
alter table if exists salic_bronze."sac__vw_captacao_ano_uf" rename to "bronze_sac_vw_captacao_ano_uf";
alter table if exists salic_bronze."bronze_sac_vw_captacao_ano_uf" set schema salic;
alter table if exists salic_bronze."sac__vw_dba_captacao_2007_2017" rename to "bronze_sac_vw_dba_captacao_2007_2017";
alter table if exists salic_bronze."bronze_sac_vw_dba_captacao_2007_2017" set schema salic;
alter table if exists salic_bronze."sac__vw_pc_digital_avaliados_emavaliacao" rename to "bronze_sac_vw_pc_digital_avaliados_emavaliacao";
alter table if exists salic_bronze."bronze_sac_vw_pc_digital_avaliados_emavaliacao" set schema salic;
alter table if exists salic_bronze."sac__vw_prol_vinc_agu_dis" rename to "bronze_sac_vw_prol_vinc_agu_dis";
alter table if exists salic_bronze."bronze_sac_vw_prol_vinc_agu_dis" set schema salic;
alter table if exists salic_bronze."sac__vwagentesseusprojetos" rename to "bronze_sac_vwagentesseusprojetos";
alter table if exists salic_bronze."bronze_sac_vwagentesseusprojetos" set schema salic;
alter table if exists salic_bronze."sac__vwalteraranalistaproposta" rename to "bronze_sac_vwalteraranalistaproposta";
alter table if exists salic_bronze."bronze_sac_vwalteraranalistaproposta" set schema salic;
alter table if exists salic_bronze."sac__vwalterarorgao" rename to "bronze_sac_vwalterarorgao";
alter table if exists salic_bronze."bronze_sac_vwalterarorgao" set schema salic;
alter table if exists salic_bronze."sac__vwalterarprojeto" rename to "bronze_sac_vwalterarprojeto";
alter table if exists salic_bronze."bronze_sac_vwalterarprojeto" set schema salic;
alter table if exists salic_bronze."sac__vwalterarsituacao" rename to "bronze_sac_vwalterarsituacao";
alter table if exists salic_bronze."bronze_sac_vwalterarsituacao" set schema salic;
alter table if exists salic_bronze."sac__vwanalisedeconteudo" rename to "bronze_sac_vwanalisedeconteudo";
alter table if exists salic_bronze."bronze_sac_vwanalisedeconteudo" set schema salic;
alter table if exists salic_bronze."sac__vwanalisedecusto" rename to "bronze_sac_vwanalisedecusto";
alter table if exists salic_bronze."bronze_sac_vwanalisedecusto" set schema salic;
alter table if exists salic_bronze."sac__vwanalisedocumentalportecnico" rename to "bronze_sac_vwanalisedocumentalportecnico";
alter table if exists salic_bronze."bronze_sac_vwanalisedocumentalportecnico" set schema salic;
alter table if exists salic_bronze."sac__vwanalisefinanceiravirtual" rename to "bronze_sac_vwanalisefinanceiravirtual";
alter table if exists salic_bronze."bronze_sac_vwanalisefinanceiravirtual" set schema salic;
alter table if exists salic_bronze."sac__vwanalisevisualportecnico" rename to "bronze_sac_vwanalisevisualportecnico";
alter table if exists salic_bronze."bronze_sac_vwanalisevisualportecnico" set schema salic;
alter table if exists salic_bronze."sac__vwanexarcomprovantes" rename to "bronze_sac_vwanexarcomprovantes";
alter table if exists salic_bronze."bronze_sac_vwanexarcomprovantes" set schema salic;
alter table if exists salic_bronze."sac__vwanexardocumentoagente" rename to "bronze_sac_vwanexardocumentoagente";
alter table if exists salic_bronze."bronze_sac_vwanexardocumentoagente" set schema salic;
alter table if exists salic_bronze."sac__vwinformacoes_tcu_etce" rename to "bronze_sac_vwinformacoes_tcu_etce";
alter table if exists salic_bronze."bronze_sac_vwinformacoes_tcu_etce" set schema salic;
alter table if exists salic_bronze."sac__vwinformacoesconsolidadasparaavaliacaofinanceira" rename to "bronze_sac_vwinformacoesconsolidadasparaavaliacaofinanceira";
alter table if exists salic_bronze."bronze_sac_vwinformacoesconsolidadasparaavaliacaofinanceira" set schema salic;
alter table if exists salic_bronze."sac__vwinformacoesdoatoadministrativo" rename to "bronze_sac_vwinformacoesdoatoadministrativo";
alter table if exists salic_bronze."bronze_sac_vwinformacoesdoatoadministrativo" set schema salic;
alter table if exists salic_bronze."sac__vwitemduplicado" rename to "bronze_sac_vwitemduplicado";
alter table if exists salic_bronze."bronze_sac_vwitemduplicado" set schema salic;
alter table if exists salic_bronze."sac__vwitemorcamentarioimpugnado" rename to "bronze_sac_vwitemorcamentarioimpugnado";
alter table if exists salic_bronze."bronze_sac_vwitemorcamentarioimpugnado" set schema salic;
alter table if exists salic_bronze."sac__vwitenscomprovados_fonte_produto_etapa_uf_municipio" rename to "bronze_sac_vwitenscomprovados_fonte_produto_etapa_uf_municipio";
alter table if exists salic_bronze."bronze_sac_vwitenscomprovados_fonte_produto_etapa_uf_municipio" set schema salic;
alter table if exists salic_bronze."sac__vwitensorcamentarioscomprovados" rename to "bronze_sac_vwitensorcamentarioscomprovados";
alter table if exists salic_bronze."bronze_sac_vwitensorcamentarioscomprovados" set schema salic;
alter table if exists salic_bronze."sac__vwlancamentobancariosemcomprovacao" rename to "bronze_sac_vwlancamentobancariosemcomprovacao";
alter table if exists salic_bronze."bronze_sac_vwlancamentobancariosemcomprovacao" set schema salic;
alter table if exists salic_bronze."sac__vwlaudofinalavaliacaoderesultadodoprojeto" rename to "bronze_sac_vwlaudofinalavaliacaoderesultadodoprojeto";
alter table if exists salic_bronze."bronze_sac_vwlaudofinalavaliacaoderesultadodoprojeto" set schema salic;
alter table if exists salic_bronze."sac__vwlaudofinalderesultadopassivo" rename to "bronze_sac_vwlaudofinalderesultadopassivo";
alter table if exists salic_bronze."bronze_sac_vwlaudofinalderesultadopassivo" set schema salic;
alter table if exists salic_bronze."sac__vwliberacao" rename to "bronze_sac_vwliberacao";
alter table if exists salic_bronze."bronze_sac_vwliberacao" set schema salic;
alter table if exists salic_bronze."sac__vwlistarcaptacoes" rename to "bronze_sac_vwlistarcaptacoes";
alter table if exists salic_bronze."bronze_sac_vwlistarcaptacoes" set schema salic;
alter table if exists salic_bronze."sac__vwlistarpropostas" rename to "bronze_sac_vwlistarpropostas";
alter table if exists salic_bronze."bronze_sac_vwlistarpropostas" set schema salic;
alter table if exists salic_bronze."sac__vwlotedocumento" rename to "bronze_sac_vwlotedocumento";
alter table if exists salic_bronze."bronze_sac_vwlotedocumento" set schema salic;
alter table if exists salic_bronze."sac__vwmaioresincentivadoresporanouf" rename to "bronze_sac_vwmaioresincentivadoresporanouf";
alter table if exists salic_bronze."bronze_sac_vwmaioresincentivadoresporanouf" set schema salic;
alter table if exists salic_bronze."sac__vwmaioresproponentesporanouf" rename to "bronze_sac_vwmaioresproponentesporanouf";
alter table if exists salic_bronze."bronze_sac_vwmaioresproponentesporanouf" set schema salic;
alter table if exists salic_bronze."sac__vwmediana" rename to "bronze_sac_vwmediana";
alter table if exists salic_bronze."bronze_sac_vwmediana" set schema salic;
alter table if exists salic_bronze."sac__vwmemoriadecalculo" rename to "bronze_sac_vwmemoriadecalculo";
alter table if exists salic_bronze."bronze_sac_vwmemoriadecalculo" set schema salic;
alter table if exists salic_bronze."sac__vwmetascomprovadas" rename to "bronze_sac_vwmetascomprovadas";
alter table if exists salic_bronze."bronze_sac_vwmetascomprovadas" set schema salic;
alter table if exists salic_bronze."sac__vwnatureza" rename to "bronze_sac_vwnatureza";
alter table if exists salic_bronze."bronze_sac_vwnatureza" set schema salic;
alter table if exists salic_bronze."sac__vwnotificacaoaoproponente" rename to "bronze_sac_vwnotificacaoaoproponente";
alter table if exists salic_bronze."bronze_sac_vwnotificacaoaoproponente" set schema salic;
alter table if exists salic_bronze."sac__vworcamentosolicitado" rename to "bronze_sac_vworcamentosolicitado";
alter table if exists salic_bronze."bronze_sac_vworcamentosolicitado" set schema salic;
alter table if exists salic_bronze."sac__vwpagamentodefornecedordoprojetoporitem" rename to "bronze_sac_vwpagamentodefornecedordoprojetoporitem";
alter table if exists salic_bronze."bronze_sac_vwpagamentodefornecedordoprojetoporitem" set schema salic;
alter table if exists salic_bronze."sac__vwpagamentodefornecedordoprojetoporitemdetalhado" rename to "bronze_sac_vwpagamentodefornecedordoprojetoporitemdetalhado";
alter table if exists salic_bronze."bronze_sac_vwpagamentodefornecedordoprojetoporitemdetalhado" set schema salic;
alter table if exists salic_bronze."sac__vwpagamentodespesaforaperiodoexecucao" rename to "bronze_sac_vwpagamentodespesaforaperiodoexecucao";
alter table if exists salic_bronze."bronze_sac_vwpagamentodespesaforaperiodoexecucao" set schema salic;
alter table if exists salic_bronze."sac__vwpagamentofinalizadoparecerista" rename to "bronze_sac_vwpagamentofinalizadoparecerista";
alter table if exists salic_bronze."bronze_sac_vwpagamentofinalizadoparecerista" set schema salic;
alter table if exists salic_bronze."sac__vwpagamentofornecedorirregularreceitafederal" rename to "bronze_sac_vwpagamentofornecedorirregularreceitafederal";
alter table if exists salic_bronze."bronze_sac_vwpagamentofornecedorirregularreceitafederal" set schema salic;
alter table if exists salic_bronze."sac__vwpagamentoparecerista" rename to "bronze_sac_vwpagamentoparecerista";
alter table if exists salic_bronze."bronze_sac_vwpagamentoparecerista" set schema salic;
alter table if exists salic_bronze."sac__vwpaineladequacaorealidadeexecucao" rename to "bronze_sac_vwpaineladequacaorealidadeexecucao";
alter table if exists salic_bronze."bronze_sac_vwpaineladequacaorealidadeexecucao" set schema salic;
alter table if exists salic_bronze."sac__vwpaineladmissibilidade" rename to "bronze_sac_vwpaineladmissibilidade";
alter table if exists salic_bronze."bronze_sac_vwpaineladmissibilidade" set schema salic;
alter table if exists salic_bronze."sac__vwpainelaguardandodistribuicaoanalisetecnica" rename to "bronze_sac_vwpainelaguardandodistribuicaoanalisetecnica";
alter table if exists salic_bronze."bronze_sac_vwpainelaguardandodistribuicaoanalisetecnica" set schema salic;
alter table if exists salic_bronze."sac__vwpainelanalisadosuperintendencia" rename to "bronze_sac_vwpainelanalisadosuperintendencia";
alter table if exists salic_bronze."bronze_sac_vwpainelanalisadosuperintendencia" set schema salic;
alter table if exists salic_bronze."sac__vwpainelanalisdadosuperintendencia" rename to "bronze_sac_vwpainelanalisdadosuperintendencia";
alter table if exists salic_bronze."bronze_sac_vwpainelanalisdadosuperintendencia" set schema salic;
alter table if exists salic_bronze."sac__vwpainelavaliarpropostas" rename to "bronze_sac_vwpainelavaliarpropostas";
alter table if exists salic_bronze."bronze_sac_vwpainelavaliarpropostas" set schema salic;
alter table if exists salic_bronze."sac__vwpainelcoordenadoravaliacaotrimestral" rename to "bronze_sac_vwpainelcoordenadoravaliacaotrimestral";
alter table if exists salic_bronze."bronze_sac_vwpainelcoordenadoravaliacaotrimestral" set schema salic;
alter table if exists salic_bronze."sac__vwpainelcoordenadorimpedimentoparecerista" rename to "bronze_sac_vwpainelcoordenadorimpedimentoparecerista";
alter table if exists salic_bronze."bronze_sac_vwpainelcoordenadorimpedimentoparecerista" set schema salic;
alter table if exists salic_bronze."sac__vwpainelcoordenadorreadequacaoaguardandoanalise" rename to "bronze_sac_vwpainelcoordenadorreadequacaoaguardandoanalise";
alter table if exists salic_bronze."bronze_sac_vwpainelcoordenadorreadequacaoaguardandoanalise" set schema salic;
alter table if exists salic_bronze."sac__vwpainelcoordenadorreadequacaoaguardandoanalise_20180323" rename to "bronze_sac_vwpainelcoordenadorreadequacaoaguardandoanalise_20180323";
alter table if exists salic_bronze."bronze_sac_vwpainelcoordenadorreadequacaoaguardandoanalise_20180323" set schema salic;
alter table if exists salic_bronze."sac__vwpainelcoordenadorreadequacaoanalisados" rename to "bronze_sac_vwpainelcoordenadorreadequacaoanalisados";
alter table if exists salic_bronze."bronze_sac_vwpainelcoordenadorreadequacaoanalisados" set schema salic;
alter table if exists salic_bronze."sac__vwpainelcoordenadorreadequacaoanalisados_20180323" rename to "bronze_sac_vwpainelcoordenadorreadequacaoanalisados_20180323";
alter table if exists salic_bronze."bronze_sac_vwpainelcoordenadorreadequacaoanalisados_20180323" set schema salic;
alter table if exists salic_bronze."sac__vwpainelcoordenadorreadequacaoemanalise" rename to "bronze_sac_vwpainelcoordenadorreadequacaoemanalise";
alter table if exists salic_bronze."bronze_sac_vwpainelcoordenadorreadequacaoemanalise" set schema salic;
alter table if exists salic_bronze."sac__vwpainelcoordenadorreadequacaoemanalise_20180323" rename to "bronze_sac_vwpainelcoordenadorreadequacaoemanalise_20180323";
alter table if exists salic_bronze."bronze_sac_vwpainelcoordenadorreadequacaoemanalise_20180323" set schema salic;
alter table if exists salic_bronze."sac__vwpainelcoordenadorvinculadasaguardandoanalise" rename to "bronze_sac_vwpainelcoordenadorvinculadasaguardandoanalise";
alter table if exists salic_bronze."bronze_sac_vwpainelcoordenadorvinculadasaguardandoanalise" set schema salic;
alter table if exists salic_bronze."sac__vwpainelcoordenadorvinculadasemanalise" rename to "bronze_sac_vwpainelcoordenadorvinculadasemanalise";
alter table if exists salic_bronze."bronze_sac_vwpainelcoordenadorvinculadasemanalise" set schema salic;
alter table if exists salic_bronze."sac__vwpainelcoordenadorvinculadasemvalidacao" rename to "bronze_sac_vwpainelcoordenadorvinculadasemvalidacao";
alter table if exists salic_bronze."bronze_sac_vwpainelcoordenadorvinculadasemvalidacao" set schema salic;
alter table if exists salic_bronze."sac__vwpainelcoordenadorvinculadasreanalisar" rename to "bronze_sac_vwpainelcoordenadorvinculadasreanalisar";
alter table if exists salic_bronze."bronze_sac_vwpainelcoordenadorvinculadasreanalisar" set schema salic;
alter table if exists salic_bronze."sac__vwpainelcoordenadorvinculadasvalidados" rename to "bronze_sac_vwpainelcoordenadorvinculadasvalidados";
alter table if exists salic_bronze."bronze_sac_vwpainelcoordenadorvinculadasvalidados" set schema salic;
alter table if exists salic_bronze."sac__vwpaineldedocumentosaguardandoassinatura" rename to "bronze_sac_vwpaineldedocumentosaguardandoassinatura";
alter table if exists salic_bronze."bronze_sac_vwpaineldedocumentosaguardandoassinatura" set schema salic;
alter table if exists salic_bronze."sac__vwpaineldedocumentosdereadequacaoaguardandoassinatura" rename to "bronze_sac_vwpaineldedocumentosdereadequacaoaguardandoassinatura";
alter table if exists salic_bronze."bronze_sac_vwpaineldedocumentosdereadequacaoaguardandoassinatura" set schema salic;
alter table if exists salic_bronze."sac__vwpaineldeemissaodelaudofinalderesultado" rename to "bronze_sac_vwpaineldeemissaodelaudofinalderesultado";
alter table if exists salic_bronze."bronze_sac_vwpaineldeemissaodelaudofinalderesultado" set schema salic;
alter table if exists salic_bronze."sac__vwpaineldeemissaodelaudofinalderesultadopassivo" rename to "bronze_sac_vwpaineldeemissaodelaudofinalderesultadopassivo";
alter table if exists salic_bronze."bronze_sac_vwpaineldeemissaodelaudofinalderesultadopassivo" set schema salic;
alter table if exists salic_bronze."sac__vwpaineldehomologacaodeprojetos" rename to "bronze_sac_vwpaineldehomologacaodeprojetos";
alter table if exists salic_bronze."bronze_sac_vwpaineldehomologacaodeprojetos" set schema salic;
alter table if exists salic_bronze."sac__vwpaineldeliberacao" rename to "bronze_sac_vwpaineldeliberacao";
alter table if exists salic_bronze."bronze_sac_vwpaineldeliberacao" set schema salic;
alter table if exists salic_bronze."sac__vwpaineldemensagens" rename to "bronze_sac_vwpaineldemensagens";
alter table if exists salic_bronze."bronze_sac_vwpaineldemensagens" set schema salic;
alter table if exists salic_bronze."sac__vwpaineldetransferenciaderecursos" rename to "bronze_sac_vwpaineldetransferenciaderecursos";
alter table if exists salic_bronze."bronze_sac_vwpaineldetransferenciaderecursos" set schema salic;
alter table if exists salic_bronze."sac__vwpaineldocomponentedacomissao" rename to "bronze_sac_vwpaineldocomponentedacomissao";
alter table if exists salic_bronze."bronze_sac_vwpaineldocomponentedacomissao" set schema salic;
alter table if exists salic_bronze."sac__vwpaineldocoordenador" rename to "bronze_sac_vwpaineldocoordenador";
alter table if exists salic_bronze."bronze_sac_vwpaineldocoordenador" set schema salic;
alter table if exists salic_bronze."sac__vwpaineldocoordenadorgeral" rename to "bronze_sac_vwpaineldocoordenadorgeral";
alter table if exists salic_bronze."bronze_sac_vwpaineldocoordenadorgeral" set schema salic;
alter table if exists salic_bronze."sac__vwpaineldotecnicodeadmissibilidade" rename to "bronze_sac_vwpaineldotecnicodeadmissibilidade";
alter table if exists salic_bronze."bronze_sac_vwpaineldotecnicodeadmissibilidade" set schema salic;
alter table if exists salic_bronze."sac__vwpainelemanalisetecnica" rename to "bronze_sac_vwpainelemanalisetecnica";
alter table if exists salic_bronze."bronze_sac_vwpainelemanalisetecnica" set schema salic;
alter table if exists salic_bronze."sac__vwpainelemvalidacao" rename to "bronze_sac_vwpainelemvalidacao";
alter table if exists salic_bronze."bronze_sac_vwpainelemvalidacao" set schema salic;
alter table if exists salic_bronze."sac__vwpainelenquadramentopropostas" rename to "bronze_sac_vwpainelenquadramentopropostas";
alter table if exists salic_bronze."bronze_sac_vwpainelenquadramentopropostas" set schema salic;
alter table if exists salic_bronze."sac__vwpainelparecerista" rename to "bronze_sac_vwpainelparecerista";
alter table if exists salic_bronze."bronze_sac_vwpainelparecerista" set schema salic;
alter table if exists salic_bronze."sac__vwpainelpareceristaunificado" rename to "bronze_sac_vwpainelpareceristaunificado";
alter table if exists salic_bronze."bronze_sac_vwpainelpareceristaunificado" set schema salic;
alter table if exists salic_bronze."sac__vwpainelpresidentevinculadas" rename to "bronze_sac_vwpainelpresidentevinculadas";
alter table if exists salic_bronze."bronze_sac_vwpainelpresidentevinculadas" set schema salic;
alter table if exists salic_bronze."sac__vwpainelprojetossgptc" rename to "bronze_sac_vwpainelprojetossgptc";
alter table if exists salic_bronze."bronze_sac_vwpainelprojetossgptc" set schema salic;
alter table if exists salic_bronze."sac__vwpainelreadequacaoaguardandopublicacao" rename to "bronze_sac_vwpainelreadequacaoaguardandopublicacao";
alter table if exists salic_bronze."bronze_sac_vwpainelreadequacaoaguardandopublicacao" set schema salic;
alter table if exists salic_bronze."sac__vwpainelreadequacaocoordenadorpareceraguardandoanalise" rename to "bronze_sac_vwpainelreadequacaocoordenadorpareceraguardandoanalise";
alter table if exists salic_bronze."bronze_sac_vwpainelreadequacaocoordenadorpareceraguardandoanalise" set schema salic;
alter table if exists salic_bronze."sac__vwpainelreadequacaocoordenadorpareceranalisados" rename to "bronze_sac_vwpainelreadequacaocoordenadorpareceranalisados";
alter table if exists salic_bronze."bronze_sac_vwpainelreadequacaocoordenadorpareceranalisados" set schema salic;
alter table if exists salic_bronze."sac__vwpainelreadequacaocoordenadorpareceremanalise" rename to "bronze_sac_vwpainelreadequacaocoordenadorpareceremanalise";
alter table if exists salic_bronze."bronze_sac_vwpainelreadequacaocoordenadorpareceremanalise" set schema salic;
alter table if exists salic_bronze."sac__vwpainelreadequacaotecnico" rename to "bronze_sac_vwpainelreadequacaotecnico";
alter table if exists salic_bronze."bronze_sac_vwpainelreadequacaotecnico" set schema salic;
alter table if exists salic_bronze."sac__vwpainelsuperintendentevinculadas" rename to "bronze_sac_vwpainelsuperintendentevinculadas";
alter table if exists salic_bronze."bronze_sac_vwpainelsuperintendentevinculadas" set schema salic;
alter table if exists salic_bronze."sac__vwpaineltecnicoavaliacaotrimestral" rename to "bronze_sac_vwpaineltecnicoavaliacaotrimestral";
alter table if exists salic_bronze."bronze_sac_vwpaineltecnicoavaliacaotrimestral" set schema salic;
alter table if exists salic_bronze."sac__vwpaineltransferenciarecursoscaptacaomovimento" rename to "bronze_sac_vwpaineltransferenciarecursoscaptacaomovimento";
alter table if exists salic_bronze."bronze_sac_vwpaineltransferenciarecursoscaptacaomovimento" set schema salic;
alter table if exists salic_bronze."sac__vwpainelvalidados" rename to "bronze_sac_vwpainelvalidados";
alter table if exists salic_bronze."bronze_sac_vwpainelvalidados" set schema salic;
alter table if exists salic_bronze."sac__vwpainelvalidadoscomdevolucao" rename to "bronze_sac_vwpainelvalidadoscomdevolucao";
alter table if exists salic_bronze."bronze_sac_vwpainelvalidadoscomdevolucao" set schema salic;
alter table if exists salic_bronze."sac__vwpareceranalisedeconteudo" rename to "bronze_sac_vwpareceranalisedeconteudo";
alter table if exists salic_bronze."bronze_sac_vwpareceranalisedeconteudo" set schema salic;
alter table if exists salic_bronze."sac__vwpareceresdereadequacao" rename to "bronze_sac_vwpareceresdereadequacao";
alter table if exists salic_bronze."bronze_sac_vwpareceresdereadequacao" set schema salic;
alter table if exists salic_bronze."sac__vwparecerfechadoorgao" rename to "bronze_sac_vwparecerfechadoorgao";
alter table if exists salic_bronze."bronze_sac_vwparecerfechadoorgao" set schema salic;
alter table if exists salic_bronze."sac__vwpareceristascredenciados" rename to "bronze_sac_vwpareceristascredenciados";
alter table if exists salic_bronze."bronze_sac_vwpareceristascredenciados" set schema salic;
alter table if exists salic_bronze."sac__vwpareceristasorgao" rename to "bronze_sac_vwpareceristasorgao";
alter table if exists salic_bronze."bronze_sac_vwpareceristasorgao" set schema salic;
alter table if exists salic_bronze."sac__vwpautados_cnic_mais_trinta_dias" rename to "bronze_sac_vwpautados_cnic_mais_trinta_dias";
alter table if exists salic_bronze."bronze_sac_vwpautados_cnic_mais_trinta_dias" set schema salic;
alter table if exists salic_bronze."sac__vwpecadedivulgacao" rename to "bronze_sac_vwpecadedivulgacao";
alter table if exists salic_bronze."bronze_sac_vwpecadedivulgacao" set schema salic;
alter table if exists salic_bronze."sac__vwpedidodeprorrogacao" rename to "bronze_sac_vwpedidodeprorrogacao";
alter table if exists salic_bronze."bronze_sac_vwpedidodeprorrogacao" set schema salic;
alter table if exists salic_bronze."sac__vwplanilhaaprovada" rename to "bronze_sac_vwplanilhaaprovada";
alter table if exists salic_bronze."bronze_sac_vwplanilhaaprovada" set schema salic;
alter table if exists salic_bronze."sac__vwplanilhasugerida" rename to "bronze_sac_vwplanilhasugerida";
alter table if exists salic_bronze."bronze_sac_vwplanilhasugerida" set schema salic;
alter table if exists salic_bronze."sac__vwplanilhasugeridacompleta" rename to "bronze_sac_vwplanilhasugeridacompleta";
alter table if exists salic_bronze."bronze_sac_vwplanilhasugeridacompleta" set schema salic;
alter table if exists salic_bronze."sac__vwplanodedistribuicaoproduto" rename to "bronze_sac_vwplanodedistribuicaoproduto";
alter table if exists salic_bronze."bronze_sac_vwplanodedistribuicaoproduto" set schema salic;
alter table if exists salic_bronze."sac__vwportariasgeradas" rename to "bronze_sac_vwportariasgeradas";
alter table if exists salic_bronze."bronze_sac_vwportariasgeradas" set schema salic;
alter table if exists salic_bronze."sac__vwprecomedio_produto_item_unidade_uf_municipio" rename to "bronze_sac_vwprecomedio_produto_item_unidade_uf_municipio";
alter table if exists salic_bronze."bronze_sac_vwprecomedio_produto_item_unidade_uf_municipio" set schema salic;
alter table if exists salic_bronze."sac__vwprocuradordeprojeto" rename to "bronze_sac_vwprocuradordeprojeto";
alter table if exists salic_bronze."bronze_sac_vwprocuradordeprojeto" set schema salic;
alter table if exists salic_bronze."sac__vwprodutos" rename to "bronze_sac_vwprodutos";
alter table if exists salic_bronze."bronze_sac_vwprodutos" set schema salic;
alter table if exists salic_bronze."sac__vwprodutosemanalisetecnicanasvinculadas" rename to "bronze_sac_vwprodutosemanalisetecnicanasvinculadas";
alter table if exists salic_bronze."bronze_sac_vwprodutosemanalisetecnicanasvinculadas" set schema salic;
alter table if exists salic_bronze."sac__vwprogramarouanet" rename to "bronze_sac_vwprogramarouanet";
alter table if exists salic_bronze."bronze_sac_vwprogramarouanet" set schema salic;
alter table if exists salic_bronze."sac__vwprogramarouanetavaliacaofinalizada" rename to "bronze_sac_vwprogramarouanetavaliacaofinalizada";
alter table if exists salic_bronze."bronze_sac_vwprogramarouanetavaliacaofinalizada" set schema salic;
alter table if exists salic_bronze."sac__vwprogramarouanetavaliacaoinicialmediasnotasproposta" rename to "bronze_sac_vwprogramarouanetavaliacaoinicialmediasnotasproposta";
alter table if exists salic_bronze."bronze_sac_vwprogramarouanetavaliacaoinicialmediasnotasproposta" set schema salic;
alter table if exists salic_bronze."sac__vwprogramarouanetavaliacaoinicialnotasporproposta" rename to "bronze_sac_vwprogramarouanetavaliacaoinicialnotasporproposta";
alter table if exists salic_bronze."bronze_sac_vwprogramarouanetavaliacaoinicialnotasporproposta" set schema salic;
alter table if exists salic_bronze."sac__vwprogramarouanetavaliadores" rename to "bronze_sac_vwprogramarouanetavaliadores";
alter table if exists salic_bronze."bronze_sac_vwprogramarouanetavaliadores" set schema salic;
alter table if exists salic_bronze."sac__vwprogramarouanetdistribuirproposta" rename to "bronze_sac_vwprogramarouanetdistribuirproposta";
alter table if exists salic_bronze."bronze_sac_vwprogramarouanetdistribuirproposta" set schema salic;
alter table if exists salic_bronze."sac__vwprogramarouanetdistribuirpropostarecurso" rename to "bronze_sac_vwprogramarouanetdistribuirpropostarecurso";
alter table if exists salic_bronze."bronze_sac_vwprogramarouanetdistribuirpropostarecurso" set schema salic;
alter table if exists salic_bronze."sac__vwprogramarouaneteditais" rename to "bronze_sac_vwprogramarouaneteditais";
alter table if exists salic_bronze."bronze_sac_vwprogramarouaneteditais" set schema salic;
alter table if exists salic_bronze."sac__vwprogramarouanethabilitarproposta" rename to "bronze_sac_vwprogramarouanethabilitarproposta";
alter table if exists salic_bronze."bronze_sac_vwprogramarouanethabilitarproposta" set schema salic;
alter table if exists salic_bronze."sac__vwprogramarouanetincentivadorprojetosapoiados" rename to "bronze_sac_vwprogramarouanetincentivadorprojetosapoiados";
alter table if exists salic_bronze."bronze_sac_vwprogramarouanetincentivadorprojetosapoiados" set schema salic;
alter table if exists salic_bronze."sac__vwprogramarouanetnotasporproposta" rename to "bronze_sac_vwprogramarouanetnotasporproposta";
alter table if exists salic_bronze."bronze_sac_vwprogramarouanetnotasporproposta" set schema salic;
alter table if exists salic_bronze."sac__vwprogramarouanetpainelresultadofinal" rename to "bronze_sac_vwprogramarouanetpainelresultadofinal";
alter table if exists salic_bronze."bronze_sac_vwprogramarouanetpainelresultadofinal" set schema salic;
alter table if exists salic_bronze."sac__vwprogramarouanetpainelsolicitarrecurso" rename to "bronze_sac_vwprogramarouanetpainelsolicitarrecurso";
alter table if exists salic_bronze."bronze_sac_vwprogramarouanetpainelsolicitarrecurso" set schema salic;
alter table if exists salic_bronze."sac__vwprogramarouanetprojetosselecionados" rename to "bronze_sac_vwprogramarouanetprojetosselecionados";
alter table if exists salic_bronze."bronze_sac_vwprogramarouanetprojetosselecionados" set schema salic;
alter table if exists salic_bronze."sac__vwprogramarouanetpropostasavaliadas" rename to "bronze_sac_vwprogramarouanetpropostasavaliadas";
alter table if exists salic_bronze."bronze_sac_vwprogramarouanetpropostasavaliadas" set schema salic;
alter table if exists salic_bronze."sac__vwprogramarouanetpropostasnaoavaliadas" rename to "bronze_sac_vwprogramarouanetpropostasnaoavaliadas";
alter table if exists salic_bronze."bronze_sac_vwprogramarouanetpropostasnaoavaliadas" set schema salic;
alter table if exists salic_bronze."sac__vwprogramarouanetpropostasrecebidas" rename to "bronze_sac_vwprogramarouanetpropostasrecebidas";
alter table if exists salic_bronze."bronze_sac_vwprogramarouanetpropostasrecebidas" set schema salic;
alter table if exists salic_bronze."sac__vwprogramarouanetrecursopropostaavaliadas" rename to "bronze_sac_vwprogramarouanetrecursopropostaavaliadas";
alter table if exists salic_bronze."bronze_sac_vwprogramarouanetrecursopropostaavaliadas" set schema salic;
alter table if exists salic_bronze."sac__vwprogramarouanetrecursospropostasnaoavaliadas" rename to "bronze_sac_vwprogramarouanetrecursospropostasnaoavaliadas";
alter table if exists salic_bronze."bronze_sac_vwprogramarouanetrecursospropostasnaoavaliadas" set schema salic;
alter table if exists salic_bronze."sac__vwprogramarouanetresultadoavaliacaoinicial" rename to "bronze_sac_vwprogramarouanetresultadoavaliacaoinicial";
alter table if exists salic_bronze."bronze_sac_vwprogramarouanetresultadoavaliacaoinicial" set schema salic;
alter table if exists salic_bronze."sac__vwprogramarouanettipologia" rename to "bronze_sac_vwprogramarouanettipologia";
alter table if exists salic_bronze."bronze_sac_vwprogramarouanettipologia" set schema salic;
alter table if exists salic_bronze."sac__vwprojetoaenviar" rename to "bronze_sac_vwprojetoaenviar";
alter table if exists salic_bronze."bronze_sac_vwprojetoaenviar" set schema salic;
alter table if exists salic_bronze."sac__vwprojetoareasegmentoproduto" rename to "bronze_sac_vwprojetoareasegmentoproduto";
alter table if exists salic_bronze."bronze_sac_vwprojetoareasegmentoproduto" set schema salic;
alter table if exists salic_bronze."sac__vwprojetobeneficiarioprodutocultural" rename to "bronze_sac_vwprojetobeneficiarioprodutocultural";
alter table if exists salic_bronze."bronze_sac_vwprojetobeneficiarioprodutocultural" set schema salic;
alter table if exists salic_bronze."sac__vwprojetodistribuidovinculada" rename to "bronze_sac_vwprojetodistribuidovinculada";
alter table if exists salic_bronze."bronze_sac_vwprojetodistribuidovinculada" set schema salic;
alter table if exists salic_bronze."sac__vwprojetoorgao" rename to "bronze_sac_vwprojetoorgao";
alter table if exists salic_bronze."bronze_sac_vwprojetoorgao" set schema salic;
alter table if exists salic_bronze."sac__vwprojetos_na_pauta_cnic" rename to "bronze_sac_vwprojetos_na_pauta_cnic";
alter table if exists salic_bronze."bronze_sac_vwprojetos_na_pauta_cnic" set schema salic;
alter table if exists salic_bronze."sac__vwprojetosadequadosrealidadeexecucao" rename to "bronze_sac_vwprojetosadequadosrealidadeexecucao";
alter table if exists salic_bronze."bronze_sac_vwprojetosadequadosrealidadeexecucao" set schema salic;
alter table if exists salic_bronze."sac__vwprojetosapreciadosaguardandohomologacao" rename to "bronze_sac_vwprojetosapreciadosaguardandohomologacao";
alter table if exists salic_bronze."bronze_sac_vwprojetosapreciadosaguardandohomologacao" set schema salic;
alter table if exists salic_bronze."sac__vwprojetosaprovadossemhomologacao" rename to "bronze_sac_vwprojetosaprovadossemhomologacao";
alter table if exists salic_bronze."bronze_sac_vwprojetosaprovadossemhomologacao" set schema salic;
alter table if exists salic_bronze."sac__vwprojetosareceber" rename to "bronze_sac_vwprojetosareceber";
alter table if exists salic_bronze."bronze_sac_vwprojetosareceber" set schema salic;
alter table if exists salic_bronze."sac__vwprojetoscominconsistencianacaptacao" rename to "bronze_sac_vwprojetoscominconsistencianacaptacao";
alter table if exists salic_bronze."bronze_sac_vwprojetoscominconsistencianacaptacao" set schema salic;
alter table if exists salic_bronze."sac__vwprojetosconsolidados" rename to "bronze_sac_vwprojetosconsolidados";
alter table if exists salic_bronze."bronze_sac_vwprojetosconsolidados" set schema salic;
alter table if exists salic_bronze."sac__vwprojetosdespachados" rename to "bronze_sac_vwprojetosdespachados";
alter table if exists salic_bronze."bronze_sac_vwprojetosdespachados" set schema salic;
alter table if exists salic_bronze."sac__vwprojetosemanalisecomparecerista" rename to "bronze_sac_vwprojetosemanalisecomparecerista";
alter table if exists salic_bronze."bronze_sac_vwprojetosemanalisecomparecerista" set schema salic;
alter table if exists salic_bronze."sac__vwprojetosenviados" rename to "bronze_sac_vwprojetosenviados";
alter table if exists salic_bronze."bronze_sac_vwprojetosenviados" set schema salic;
alter table if exists salic_bronze."sac__vwprojetoseusdocumentos" rename to "bronze_sac_vwprojetoseusdocumentos";
alter table if exists salic_bronze."bronze_sac_vwprojetoseusdocumentos" set schema salic;
alter table if exists salic_bronze."sac__vwprojetosgratuitopago" rename to "bronze_sac_vwprojetosgratuitopago";
alter table if exists salic_bronze."bronze_sac_vwprojetosgratuitopago" set schema salic;
alter table if exists salic_bronze."sac__vwprojetositinerantes" rename to "bronze_sac_vwprojetositinerantes";
alter table if exists salic_bronze."bronze_sac_vwprojetositinerantes" set schema salic;
alter table if exists salic_bronze."sac__vwprojetosliberadosparaadequacaorealidadeexecucao" rename to "bronze_sac_vwprojetosliberadosparaadequacaorealidadeexecucao";
alter table if exists salic_bronze."bronze_sac_vwprojetosliberadosparaadequacaorealidadeexecucao" set schema salic;
alter table if exists salic_bronze."sac__vwprojetosporfase" rename to "bronze_sac_vwprojetosporfase";
alter table if exists salic_bronze."bronze_sac_vwprojetosporfase" set schema salic;
alter table if exists salic_bronze."tabelas__autorizados" rename to "bronze_tabelas_autorizados";
alter table if exists salic_bronze."bronze_tabelas_autorizados" set schema salic;
alter table if exists salic_bronze."tabelas__avisos" rename to "bronze_tabelas_avisos";
alter table if exists salic_bronze."bronze_tabelas_avisos" set schema salic;
alter table if exists salic_bronze."tabelas__bairros" rename to "bronze_tabelas_bairros";
alter table if exists salic_bronze."bronze_tabelas_bairros" set schema salic;
alter table if exists salic_bronze."tabelas__cadastro" rename to "bronze_tabelas_cadastro";
alter table if exists salic_bronze."bronze_tabelas_cadastro" set schema salic;
alter table if exists salic_bronze."tabelas__categorias_pessoa" rename to "bronze_tabelas_categorias_pessoa";
alter table if exists salic_bronze."bronze_tabelas_categorias_pessoa" set schema salic;
alter table if exists salic_bronze."tabelas__cdpe" rename to "bronze_tabelas_cdpe";
alter table if exists salic_bronze."bronze_tabelas_cdpe" set schema salic;
alter table if exists salic_bronze."tabelas__dtproperties" rename to "bronze_tabelas_dtproperties";
alter table if exists salic_bronze."bronze_tabelas_dtproperties" set schema salic;
alter table if exists salic_bronze."tabelas__ect_bairros" rename to "bronze_tabelas_ect_bairros";
alter table if exists salic_bronze."bronze_tabelas_ect_bairros" set schema salic;
alter table if exists salic_bronze."tabelas__ect_localidades" rename to "bronze_tabelas_ect_localidades";
alter table if exists salic_bronze."bronze_tabelas_ect_localidades" set schema salic;
alter table if exists salic_bronze."tabelas__ect_recusados" rename to "bronze_tabelas_ect_recusados";
alter table if exists salic_bronze."bronze_tabelas_ect_recusados" set schema salic;
alter table if exists salic_bronze."tabelas__emails" rename to "bronze_tabelas_emails";
alter table if exists salic_bronze."bronze_tabelas_emails" set schema salic;
alter table if exists salic_bronze."tabelas__estacoes" rename to "bronze_tabelas_estacoes";
alter table if exists salic_bronze."bronze_tabelas_estacoes" set schema salic;
alter table if exists salic_bronze."tabelas__eventos" rename to "bronze_tabelas_eventos";
alter table if exists salic_bronze."bronze_tabelas_eventos" set schema salic;
alter table if exists salic_bronze."tabelas__funcoes" rename to "bronze_tabelas_funcoes";
alter table if exists salic_bronze."bronze_tabelas_funcoes" set schema salic;
alter table if exists salic_bronze."tabelas__grupos" rename to "bronze_tabelas_grupos";
alter table if exists salic_bronze."bronze_tabelas_grupos" set schema salic;
alter table if exists salic_bronze."tabelas__grupos_ares" rename to "bronze_tabelas_grupos_ares";
alter table if exists salic_bronze."bronze_tabelas_grupos_ares" set schema salic;
alter table if exists salic_bronze."tabelas__grupos_minc11" rename to "bronze_tabelas_grupos_minc11";
alter table if exists salic_bronze."bronze_tabelas_grupos_minc11" set schema salic;
alter table if exists salic_bronze."tabelas__gruposxservicos" rename to "bronze_tabelas_gruposxservicos";
alter table if exists salic_bronze."bronze_tabelas_gruposxservicos" set schema salic;
alter table if exists salic_bronze."tabelas__headers" rename to "bronze_tabelas_headers";
alter table if exists salic_bronze."bronze_tabelas_headers" set schema salic;
alter table if exists salic_bronze."tabelas__indice_palavras" rename to "bronze_tabelas_indice_palavras";
alter table if exists salic_bronze."bronze_tabelas_indice_palavras" set schema salic;
alter table if exists salic_bronze."tabelas__listainstitucionais" rename to "bronze_tabelas_listainstitucionais";
alter table if exists salic_bronze."bronze_tabelas_listainstitucionais" set schema salic;
alter table if exists salic_bronze."tabelas__listapessoais" rename to "bronze_tabelas_listapessoais";
alter table if exists salic_bronze."bronze_tabelas_listapessoais" set schema salic;
alter table if exists salic_bronze."tabelas__localidades" rename to "bronze_tabelas_localidades";
alter table if exists salic_bronze."bronze_tabelas_localidades" set schema salic;
alter table if exists salic_bronze."tabelas__logonnt" rename to "bronze_tabelas_logonnt";
alter table if exists salic_bronze."bronze_tabelas_logonnt" set schema salic;
alter table if exists salic_bronze."tabelas__logradouros" rename to "bronze_tabelas_logradouros";
alter table if exists salic_bronze."bronze_tabelas_logradouros" set schema salic;
alter table if exists salic_bronze."tabelas__mei_grupos_intranet" rename to "bronze_tabelas_mei_grupos_intranet";
alter table if exists salic_bronze."bronze_tabelas_mei_grupos_intranet" set schema salic;
alter table if exists salic_bronze."tabelas__mei_gruposxservicos_intranet" rename to "bronze_tabelas_mei_gruposxservicos_intranet";
alter table if exists salic_bronze."bronze_tabelas_mei_gruposxservicos_intranet" set schema salic;
alter table if exists salic_bronze."tabelas__mei_servicos_intranet" rename to "bronze_tabelas_mei_servicos_intranet";
alter table if exists salic_bronze."bronze_tabelas_mei_servicos_intranet" set schema salic;
alter table if exists salic_bronze."tabelas__mei_usuariosxservicos_intranet" rename to "bronze_tabelas_mei_usuariosxservicos_intranet";
alter table if exists salic_bronze."bronze_tabelas_mei_usuariosxservicos_intranet" set schema salic;
alter table if exists salic_bronze."tabelas__mensagens" rename to "bronze_tabelas_mensagens";
alter table if exists salic_bronze."bronze_tabelas_mensagens" set schema salic;
alter table if exists salic_bronze."tabelas__menus" rename to "bronze_tabelas_menus";
alter table if exists salic_bronze."bronze_tabelas_menus" set schema salic;
alter table if exists salic_bronze."tabelas__menus_ares" rename to "bronze_tabelas_menus_ares";
alter table if exists salic_bronze."bronze_tabelas_menus_ares" set schema salic;
alter table if exists salic_bronze."tabelas__menus_minc11" rename to "bronze_tabelas_menus_minc11";
alter table if exists salic_bronze."bronze_tabelas_menus_minc11" set schema salic;
alter table if exists salic_bronze."tabelas__menusxgrupos" rename to "bronze_tabelas_menusxgrupos";
alter table if exists salic_bronze."bronze_tabelas_menusxgrupos" set schema salic;
alter table if exists salic_bronze."tabelas__menusxgrupos_ares" rename to "bronze_tabelas_menusxgrupos_ares";
alter table if exists salic_bronze."bronze_tabelas_menusxgrupos_ares" set schema salic;
alter table if exists salic_bronze."tabelas__menusxgrupos_minc11" rename to "bronze_tabelas_menusxgrupos_minc11";
alter table if exists salic_bronze."bronze_tabelas_menusxgrupos_minc11" set schema salic;
alter table if exists salic_bronze."tabelas__meta_dados" rename to "bronze_tabelas_meta_dados";
alter table if exists salic_bronze."bronze_tabelas_meta_dados" set schema salic;
alter table if exists salic_bronze."tabelas__orgaos" rename to "bronze_tabelas_orgaos";
alter table if exists salic_bronze."bronze_tabelas_orgaos" set schema salic;
alter table if exists salic_bronze."tabelas__orgaos_ares" rename to "bronze_tabelas_orgaos_ares";
alter table if exists salic_bronze."bronze_tabelas_orgaos_ares" set schema salic;
alter table if exists salic_bronze."tabelas__orgaos_subordinados" rename to "bronze_tabelas_orgaos_subordinados";
alter table if exists salic_bronze."bronze_tabelas_orgaos_subordinados" set schema salic;
alter table if exists salic_bronze."tabelas__orgaosxusuarios" rename to "bronze_tabelas_orgaosxusuarios";
alter table if exists salic_bronze."bronze_tabelas_orgaosxusuarios" set schema salic;
alter table if exists salic_bronze."tabelas__palavras" rename to "bronze_tabelas_palavras";
alter table if exists salic_bronze."bronze_tabelas_palavras" set schema salic;
alter table if exists salic_bronze."tabelas__palavras2" rename to "bronze_tabelas_palavras2";
alter table if exists salic_bronze."bronze_tabelas_palavras2" set schema salic;
alter table if exists salic_bronze."tabelas__pessoa_dados" rename to "bronze_tabelas_pessoa_dados";
alter table if exists salic_bronze."bronze_tabelas_pessoa_dados" set schema salic;
alter table if exists salic_bronze."tabelas__pessoa_dados_bkp" rename to "bronze_tabelas_pessoa_dados_bkp";
alter table if exists salic_bronze."bronze_tabelas_pessoa_dados_bkp" set schema salic;
alter table if exists salic_bronze."tabelas__pessoa_enderecos" rename to "bronze_tabelas_pessoa_enderecos";
alter table if exists salic_bronze."bronze_tabelas_pessoa_enderecos" set schema salic;
alter table if exists salic_bronze."tabelas__pessoa_identificacoes" rename to "bronze_tabelas_pessoa_identificacoes";
alter table if exists salic_bronze."bronze_tabelas_pessoa_identificacoes" set schema salic;
alter table if exists salic_bronze."tabelas__pessoa_identificacoes_alteradas" rename to "bronze_tabelas_pessoa_identificacoes_alteradas";
alter table if exists salic_bronze."bronze_tabelas_pessoa_identificacoes_alteradas" set schema salic;
alter table if exists salic_bronze."tabelas__pessoa_identificacoes_bkp" rename to "bronze_tabelas_pessoa_identificacoes_bkp";
alter table if exists salic_bronze."bronze_tabelas_pessoa_identificacoes_bkp" set schema salic;
alter table if exists salic_bronze."tabelas__pessoas" rename to "bronze_tabelas_pessoas";
alter table if exists salic_bronze."bronze_tabelas_pessoas" set schema salic;
alter table if exists salic_bronze."tabelas__pessoas_bkp" rename to "bronze_tabelas_pessoas_bkp";
alter table if exists salic_bronze."bronze_tabelas_pessoas_bkp" set schema salic;
alter table if exists salic_bronze."tabelas__pessoasxfuncoes" rename to "bronze_tabelas_pessoasxfuncoes";
alter table if exists salic_bronze."bronze_tabelas_pessoasxfuncoes" set schema salic;
alter table if exists salic_bronze."tabelas__plan1$" rename to "bronze_tabelas_plan1$";
alter table if exists salic_bronze."bronze_tabelas_plan1$" set schema salic;
alter table if exists salic_bronze."tabelas__servicos_intranet" rename to "bronze_tabelas_servicos_intranet";
alter table if exists salic_bronze."bronze_tabelas_servicos_intranet" set schema salic;
alter table if exists salic_bronze."tabelas__sistemas" rename to "bronze_tabelas_sistemas";
alter table if exists salic_bronze."bronze_tabelas_sistemas" set schema salic;
alter table if exists salic_bronze."tabelas__sistemas_ares" rename to "bronze_tabelas_sistemas_ares";
alter table if exists salic_bronze."bronze_tabelas_sistemas_ares" set schema salic;
alter table if exists salic_bronze."tabelas__sysdiagrams" rename to "bronze_tabelas_sysdiagrams";
alter table if exists salic_bronze."bronze_tabelas_sysdiagrams" set schema salic;
alter table if exists salic_bronze."tabelas__tabelas" rename to "bronze_tabelas_tabelas";
alter table if exists salic_bronze."bronze_tabelas_tabelas" set schema salic;
alter table if exists salic_bronze."tabelas__table1" rename to "bronze_tabelas_table1";
alter table if exists salic_bronze."bronze_tabelas_table1" set schema salic;
alter table if exists salic_bronze."tabelas__temp_tbl" rename to "bronze_tabelas_temp_tbl";
alter table if exists salic_bronze."bronze_tabelas_temp_tbl" set schema salic;
alter table if exists salic_bronze."tabelas__tipos_logradouro" rename to "bronze_tabelas_tipos_logradouro";
alter table if exists salic_bronze."bronze_tabelas_tipos_logradouro" set schema salic;
alter table if exists salic_bronze."tabelas__tipos_pessoa" rename to "bronze_tabelas_tipos_pessoa";
alter table if exists salic_bronze."bronze_tabelas_tipos_pessoa" set schema salic;
alter table if exists salic_bronze."tabelas__unidadesorcamentarias" rename to "bronze_tabelas_unidadesorcamentarias";
alter table if exists salic_bronze."bronze_tabelas_unidadesorcamentarias" set schema salic;
alter table if exists salic_bronze."tabelas__usuarios" rename to "bronze_tabelas_usuarios";
alter table if exists salic_bronze."bronze_tabelas_usuarios" set schema salic;
alter table if exists salic_bronze."tabelas__usuarios_ares" rename to "bronze_tabelas_usuarios_ares";
alter table if exists salic_bronze."bronze_tabelas_usuarios_ares" set schema salic;
alter table if exists salic_bronze."tabelas__usuariosxgrupos_intranet" rename to "bronze_tabelas_usuariosxgrupos_intranet";
alter table if exists salic_bronze."bronze_tabelas_usuariosxgrupos_intranet" set schema salic;
alter table if exists salic_bronze."tabelas__usuariosxorgaosxgrupos" rename to "bronze_tabelas_usuariosxorgaosxgrupos";
alter table if exists salic_bronze."bronze_tabelas_usuariosxorgaosxgrupos" set schema salic;
alter table if exists salic_bronze."tabelas__usuariosxorgaosxgrupos_ares" rename to "bronze_tabelas_usuariosxorgaosxgrupos_ares";
alter table if exists salic_bronze."bronze_tabelas_usuariosxorgaosxgrupos_ares" set schema salic;
alter table if exists salic_bronze."tabelas__usuexternosistemas" rename to "bronze_tabelas_usuexternosistemas";
alter table if exists salic_bronze."bronze_tabelas_usuexternosistemas" set schema salic;
alter table if exists salic_bronze."tabelas__v_distritos_por_palavra" rename to "bronze_tabelas_v_distritos_por_palavra";
alter table if exists salic_bronze."bronze_tabelas_v_distritos_por_palavra" set schema salic;
alter table if exists salic_bronze."tabelas__v_estados_por_palavra" rename to "bronze_tabelas_v_estados_por_palavra";
alter table if exists salic_bronze."bronze_tabelas_v_estados_por_palavra" set schema salic;
alter table if exists salic_bronze."tabelas__v_hierarquia_orgaos" rename to "bronze_tabelas_v_hierarquia_orgaos";
alter table if exists salic_bronze."bronze_tabelas_v_hierarquia_orgaos" set schema salic;
alter table if exists salic_bronze."tabelas__v_localidade_completa" rename to "bronze_tabelas_v_localidade_completa";
alter table if exists salic_bronze."bronze_tabelas_v_localidade_completa" set schema salic;
alter table if exists salic_bronze."tabelas__v_logradouro_completo" rename to "bronze_tabelas_v_logradouro_completo";
alter table if exists salic_bronze."bronze_tabelas_v_logradouro_completo" set schema salic;
alter table if exists salic_bronze."tabelas__v_municipios_por_palavra" rename to "bronze_tabelas_v_municipios_por_palavra";
alter table if exists salic_bronze."bronze_tabelas_v_municipios_por_palavra" set schema salic;
alter table if exists salic_bronze."tabelas__v_paises_por_palavra" rename to "bronze_tabelas_v_paises_por_palavra";
alter table if exists salic_bronze."bronze_tabelas_v_paises_por_palavra" set schema salic;
alter table if exists salic_bronze."tabelas__v_pessoas_nome_completo" rename to "bronze_tabelas_v_pessoas_nome_completo";
alter table if exists salic_bronze."bronze_tabelas_v_pessoas_nome_completo" set schema salic;
alter table if exists salic_bronze."tabelas__v_usuario_completo" rename to "bronze_tabelas_v_usuario_completo";
alter table if exists salic_bronze."bronze_tabelas_v_usuario_completo" set schema salic;
alter table if exists salic_bronze."tabelas__v_usuario_orgaos_grupos" rename to "bronze_tabelas_v_usuario_orgaos_grupos";
alter table if exists salic_bronze."bronze_tabelas_v_usuario_orgaos_grupos" set schema salic;
alter table if exists salic_bronze."tabelas__v_usuarios" rename to "bronze_tabelas_v_usuarios";
alter table if exists salic_bronze."bronze_tabelas_v_usuarios" set schema salic;
alter table if exists salic_bronze."tabelas__vwrelservidorhpimpressao" rename to "bronze_tabelas_vwrelservidorhpimpressao";
alter table if exists salic_bronze."bronze_tabelas_vwrelservidorhpimpressao" set schema salic;
alter table if exists salic_bronze."tabelas__vwusuarios" rename to "bronze_tabelas_vwusuarios";
alter table if exists salic_bronze."bronze_tabelas_vwusuarios" set schema salic;
alter table if exists salic_bronze."tabelas__vwusuariosorgaosgrupos" rename to "bronze_tabelas_vwusuariosorgaosgrupos";
alter table if exists salic_bronze."bronze_tabelas_vwusuariosorgaosgrupos" set schema salic;

commit;

-- 3. conferência
select
    (select count(*) from pg_tables
      where schemaname = 'salic' and tablename like 'bronze\_%') as bronzes_em_destino,
    (select count(*) from pg_tables
      where schemaname = 'salic_bronze') as sobrou_na_origem;
