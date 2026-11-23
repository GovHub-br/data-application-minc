-- A macro documento_valido contra exemplos de livro, nenhum de pessoa: o CNPJ de
-- exemplo 11.222.333/0001-81 e o CPF de teste 000.000.001-91, cada um com o
-- ultimo digito trocado, sequencias de um digito so e tamanho errado. Uma linha
-- aqui quer dizer que a conta do modulo 11 quebrou.
select doc, esperado
from
    (
        values
            ('11222333000181', true),
            ('11222333000182', false),
            ('00000000191', true),
            ('00000000192', false),
            ('11111111111', false),
            ('00000000000000', false),
            ('123', false)
    ) as exemplo(doc, esperado)  -- noqa: LT01
where {{ documento_valido("doc") }} is distinct from esperado
