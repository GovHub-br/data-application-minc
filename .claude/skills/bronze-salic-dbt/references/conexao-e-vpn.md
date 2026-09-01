# Conectar no `dbanalytics`

## 1. A VPN precisa estar de pé

Sem VPN o host não responde — e o sintoma é `timeout`, não "senha errada".
**Você não consegue subir a VPN; só o usuário.** Teste antes de qualquer coisa:

```bash
python3 -c "
import socket
env={}
for l in open('.env',encoding='utf-8'):
    l=l.strip()
    if l and not l.startswith('#') and '=' in l:
        k,v=l.split('=',1); env[k.strip()]=v.strip().strip('\"').strip(\"'\")
s=socket.socket(); s.settimeout(6)
try: s.connect((env['IP'],int(env['PORTA']))); print('VPN de pe')
except Exception as e: print('VPN fora:',type(e).__name__)
"
```

Se der `VPN fora`, peça ao usuário para conectar e **não fique tentando em
loop**. Aproveite para adiantar o que não depende do banco.

## 2. As credenciais

Estão no `.env` da raiz do repositório, em cinco chaves:

```
IP  PORTA  USER  PASS  DB
```

O banco é `dbanalytics` (PostgreSQL 15).

**Nunca imprima os valores.** Leia o `.env` de dentro do script, não exporte
para o shell nem passe por linha de comando — evita vazar em log e em histórico.

## 3. O cliente

O `psycopg2` não está no Python do sistema. Existe um venv de trabalho com
`dbt-core 1.10.23` + `dbt-postgres` (que traz o `psycopg2`). Se não existir,
crie:

```bash
python3 -m venv /tmp/dbtvenv
/tmp/dbtvenv/bin/pip install -q "dbt-core>=1.10,<1.11" "dbt-postgres==1.10.0"
```

Serve para as duas coisas: conectar no banco e rodar `dbt parse` sem depender
do container do Airflow (que costuma estar sem o mount do projeto dbt).

## 4. O padrão de conexão

Sempre **read-only** e sempre com **`statement_timeout`**. É banco de produção.

```python
import psycopg2

def conectar():
    env = {}
    for linha in open('.env', encoding='utf-8'):
        linha = linha.strip()
        if linha and not linha.startswith('#') and '=' in linha:
            k, v = linha.split('=', 1)
            env[k.strip()] = v.strip().strip('"').strip("'")
    c = psycopg2.connect(
        host=env['IP'], port=env['PORTA'], user=env['USER'],
        password=env['PASS'], dbname=env['DB'], connect_timeout=20,
    )
    c.set_session(readonly=True)
    c.cursor().execute("set statement_timeout='30s'")
    return c
```

## 5. O que existe no banco

| schema | tabelas | o que é |
|---|---|---|
| `salic_bronze` | 656 | **o raw do SALIC — o alvo deste trabalho** |
| `bronze` | 465 | saída da ingestão v1, legada |
| `transferegov` | 6 | repasse fundo a fundo |
| `ibge_sidra` | 5 | catálogo SIDRA |
| `bbagil` | 2 | extrato bancário |
| `bacen` | 1 | séries do SGS |
| `control` | 2 | log da ingestão — **sem permissão de leitura** |

Números de 2026-09-01. Confira sempre, não assuma.

## 6. Quando a conexão cai no meio

`OperationalError: timeout expired` em consulta que antes funcionava = VPN caiu.

- **Persista resultado parcial em disco assim que obtiver**, antes de seguir
  para a próxima tabela. Perfilar 571 tabelas duas vezes é desperdício caro.
- Ao retomar, **releia o que já está em disco** em vez de refazer.
- Se um processo em segundo plano ficou preso varrendo o banco, **mate-o**
  (`TaskStop`). Não deixe consulta pesada rodando à toa em produção.
