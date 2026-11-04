"""``flatten_records`` com a lista nula ou ausente.

Em nov/2026 o BSC respondeu 200 com ``"transactions": null``. O achatamento
quebrava com ``TypeError`` antes de gravar, e a extração do BB Ágil ficou
refazendo as mesmas 2.000 chamadas a cada retry, sem avançar.
"""

from file_io_local import flatten_records


def test_lista_nula_nao_gera_registro() -> None:
    documentos = [{"agencia": 1, "transactions": None}]
    assert flatten_records(documentos, record_key="transactions") == []


def test_lista_ausente_nao_gera_registro() -> None:
    assert flatten_records([{"agencia": 1}], record_key="transactions") == []


def test_campos_da_raiz_vao_para_cada_item() -> None:
    documentos = [
        {"id_plano_acao": "1", "transactions": [{"id": "a"}, {"id": "b"}]},
        {"id_plano_acao": "2", "transactions": None},
    ]
    assert flatten_records(documentos, record_key="transactions") == [
        {"id_plano_acao": "1", "id": "a"},
        {"id_plano_acao": "1", "id": "b"},
    ]


def test_sem_record_key_cada_documento_vira_um_registro() -> None:
    documentos = [{"a": 1}, {"a": 2}]
    assert flatten_records(documentos) == documentos
