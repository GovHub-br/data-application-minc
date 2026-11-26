import pyarrow as pa

from byte_nulo import sem_byte_nulo, tabela_sem_byte_nulo


def test_sem_byte_nulo_tira_o_byte_e_anula_o_que_era_so_ele() -> None:
    assert sem_byte_nulo("ante\x00rior") == "anterior"
    assert sem_byte_nulo("\x00\x00") is None
    assert sem_byte_nulo("ok") == "ok"
    assert sem_byte_nulo(None) is None
    assert sem_byte_nulo(3) == 3


def test_tabela_sem_byte_nulo_so_mexe_no_texto_com_byte_nulo() -> None:
    tabela = pa.table(
        {"a": ["A\x00B", "\x00", None], "b": ["x", "y", "z"], "n": [1, 2, 3]}
    )
    limpa = tabela_sem_byte_nulo(tabela)
    assert limpa.column("a").to_pylist() == ["AB", None, None]
    assert limpa.column("b").to_pylist() == ["x", "y", "z"]
    assert limpa.column("n").to_pylist() == [1, 2, 3]
    assert limpa.schema == tabela.schema
