"""Trava de sincronia entre o ambiente de testes e o de producao.

As dependencias moram em tres arquivos:

- infra/docker/airflow/requirements.lock.txt: o que a imagem instala, ou seja,
  o que roda em producao. E gerado por `make lock`.
- requirements.txt: o que pedimos ao gerar o lock.
- pyproject.toml: o ambiente do Poetry, onde rodam os testes e o CI.

O Dependabot abre um PR por arquivo e nenhum deles regenera o lock. Com os tres
aceitos separadamente, o CI testava cosmos 1.15.1 e dbt 1.12 enquanto a imagem
rodava cosmos 1.14.2 e dbt 1.10. Estes testes reprovam essa divergencia.

A regra: o lock manda. Todo pacote do pyproject pina exatamente a versao do
lock, e todo requisito do requirements.txt tem que aceitar a versao do lock.
Para subir uma versao, edite requirements.txt, rode `make lock` e copie a
versao resolvida para o pyproject.toml, no mesmo PR.
"""

import tomllib
from importlib import metadata
from pathlib import Path

from packaging.requirements import Requirement
from packaging.utils import canonicalize_name

REPO_ROOT = Path(__file__).resolve().parents[1]
LOCK_PATH = REPO_ROOT / "infra/docker/airflow/requirements.lock.txt"
REQUIREMENTS_PATH = REPO_ROOT / "requirements.txt"
PYPROJECT_PATH = REPO_ROOT / "pyproject.toml"


def _versoes_do_lock() -> dict[str, str]:
    versoes = {}
    for linha in LOCK_PATH.read_text().splitlines():
        if not linha or linha[0] in "# ":
            continue
        nome, versao = linha.split("==", 1)
        versoes[canonicalize_name(nome)] = versao.strip()
    return versoes


def _dependencias_do_pyproject() -> dict[str, str]:
    """Dependencias de runtime do Poetry, sem os grupos dev e docs."""
    dados = tomllib.loads(PYPROJECT_PATH.read_text())
    deps = dados["tool"]["poetry"]["dependencies"]
    return {
        canonicalize_name(nome): spec["version"] if isinstance(spec, dict) else spec
        for nome, spec in deps.items()
        if nome != "python"
    }


def _requisitos() -> list[Requirement]:
    return [
        Requirement(linha)
        for linha in REQUIREMENTS_PATH.read_text().splitlines()
        if linha.strip() and not linha.lstrip().startswith("#")
    ]


def test_pyproject_pina_a_versao_do_lock():
    lock = _versoes_do_lock()
    divergentes = [
        f"{nome}: pyproject={spec!r} lock={lock.get(nome, 'ausente')!r}"
        for nome, spec in _dependencias_do_pyproject().items()
        if spec != lock.get(nome)
    ]
    assert not divergentes, (
        "pyproject.toml diverge do lock de producao. Copie a versao do "
        "requirements.lock.txt, ou suba a versao via requirements.txt + "
        "`make lock`:\n" + "\n".join(divergentes)
    )


def test_requirements_aceita_a_versao_do_lock():
    lock = _versoes_do_lock()
    divergentes = [
        f"{req}: lock={lock.get(canonicalize_name(req.name), 'ausente')!r}"
        for req in _requisitos()
        if canonicalize_name(req.name) not in lock
        or not req.specifier.contains(lock[canonicalize_name(req.name)], prereleases=True)
    ]
    assert not divergentes, (
        "requirements.txt pede uma versao que o lock nao tem. Rode `make lock` "
        "e commite o lock junto:\n" + "\n".join(divergentes)
    )


def test_ambiente_instalado_bate_com_o_lock():
    """Vale nos dois ambientes: no Poetry (job Test) e dentro da imagem.

    Pega o caso em que o pyproject esta certo mas o ambiente nao foi
    reinstalado, e o teste passaria contra versoes que nao sao as de producao.
    """
    lock = _versoes_do_lock()
    divergentes = []
    for nome in _dependencias_do_pyproject():
        try:
            instalada = metadata.version(nome)
        except metadata.PackageNotFoundError:
            instalada = "ausente"
        if instalada != lock.get(nome):
            divergentes.append(f"{nome}: instalado={instalada} lock={lock.get(nome)}")
    assert not divergentes, (
        "O ambiente instalado nao e o de producao. Reinstale com "
        "`poetry install`:\n" + "\n".join(divergentes)
    )
