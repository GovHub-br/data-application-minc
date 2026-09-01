"""O manifest versionado precisa descrever o projeto dbt que existe hoje.

`minc_cosmos_dag` monta a DAG a partir de `dbt/minc/manifest.json` em vez de
rodar `dbt ls` no parse -- com o tamanho atual do projeto o `dbt ls` leva ~35s
e estoura o timeout do dag processor.

O custo dessa escolha e o manifest poder ficar velho em silencio: modelo novo
nao vira task, modelo removido continua aparecendo, e nada falha. Estes testes
transformam esse desencontro em erro de CI.

Depois de mexer no projeto dbt: `make dbt-manifest` e commite o resultado.
"""

import json
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
PROJETO_DBT = REPO_ROOT / "dbt/minc"
MANIFEST = PROJETO_DBT / "manifest.json"


def _manifest() -> dict:
    if not MANIFEST.exists():
        pytest.fail(
            f"{MANIFEST.relative_to(REPO_ROOT)} nao existe. "
            "Rode `make dbt-manifest` e commite o resultado."
        )
    return json.loads(MANIFEST.read_text(encoding="utf-8"))


def _modelos_no_manifest() -> set[str]:
    """Modelos conhecidos pelo manifest, ativos ou nao.

    Modelo com `enabled: false` nao entra em `nodes`: o dbt o registra em
    `disabled`. Ele existe em disco e o manifest sabe dele -- so nao vira task.
    Ignorar `disabled` faria o teste acusar desatualizacao que nao existe.
    """
    manifest = _manifest()

    ativos = {
        node["name"]
        for node in manifest["nodes"].values()
        if node.get("resource_type") == "model"
    }
    desativados = {
        node["name"]
        for lista in manifest.get("disabled", {}).values()
        for node in lista
        if node.get("resource_type") == "model"
    }
    return ativos | desativados


def _modelos_em_disco() -> set[str]:
    return {caminho.stem for caminho in (PROJETO_DBT / "models").rglob("*.sql")}


def test_manifest_cobre_todos_os_modelos_do_disco() -> None:
    """Modelo novo sem regerar o manifest simplesmente nao vira task."""
    faltando = _modelos_em_disco() - _modelos_no_manifest()

    assert not faltando, (
        f"{len(faltando)} modelo(s) existem em dbt/minc/models mas nao no manifest: "
        f"{', '.join(sorted(faltando)[:8])}. Rode `make dbt-manifest`."
    )


def test_manifest_nao_tem_modelo_que_sumiu_do_disco() -> None:
    """Modelo removido continuaria virando task, e falharia so na execucao."""
    sobrando = _modelos_no_manifest() - _modelos_em_disco()

    assert not sobrando, (
        f"{len(sobrando)} modelo(s) estao no manifest mas nao existem mais em disco: "
        f"{', '.join(sorted(sobrando)[:8])}. Rode `make dbt-manifest`."
    )


def test_manifest_foi_gerado_pela_mesma_versao_do_dbt() -> None:
    """Manifest de outra versao do dbt pode ter schema incompativel."""
    metadata = _manifest()["metadata"]
    gerado_com = metadata.get("dbt_version", "")

    assert gerado_com.startswith("1.10."), (
        f"manifest gerado com dbt {gerado_com}; o projeto usa a linha 1.10. "
        "Rode `make dbt-manifest` no ambiente correto."
    )


def test_cosmos_le_o_manifest_em_vez_de_rodar_dbt_ls() -> None:
    """Voltar para DBT_LS derruba a DAG por timeout de parse."""
    dag = (REPO_ROOT / "dags/dbt/minc_cosmos_dag.py").read_text(encoding="utf-8")

    assert "LoadMode.DBT_MANIFEST" in dag
    assert "manifest_path=" in dag
