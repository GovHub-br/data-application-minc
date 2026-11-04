"""Politica de governanca comum as guardas de documentacao dos modelos dbt.

O que esta aqui vale para toda cadeia que segue o padrao das silvers do SALIC
(test_salic_silver_governance, test_meta5_governance): as politicas de RAG que o
exportador entende e o que conta como coluna com cara de dado pessoal. Mudar
uma destas definicoes muda as duas guardas de uma vez -- e e essa a intencao.
"""

import re

POLITICAS_RAG = {
    "prohibited",
    "eligible_after_security_validation",
    "determined_per_model",
}

# Nomes de coluna que nao podem sair sem classificacao de PII. Vem da lista do
# plano: documento, nome, endereco, contato, conta bancaria, logon e IP -- mais
# as chaves tecnicas, que identificam pessoa indiretamente e por isso contam.
PADROES_PII = re.compile(
    r"(documento|cpf|cnpj|nome_proponente|nome_agente|nome_prestador|nome_fornecedor"
    r"|endereco|cep|telefone|email|raca|cor|etnia|indigena|deficiencia|pcd"
    r"|conta|agencia|logon|usuario|_ip$|id_agente|id_solicitante|id_prestador)",
    re.IGNORECASE,
)

TAGS_PII = {"PII.Sensitive", "PII.NonSensitive"}
