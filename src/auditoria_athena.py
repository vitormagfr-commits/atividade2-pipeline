"""Etapa 3 - Auditoria e validação no Amazon Athena.

1. Cria o database e as tabelas externas (Raw, Quarentena, Silver, Gold) - sql/01_ddl_tabelas.sql
2. Registra as partições Hive-style (MSCK REPAIR TABLE)
3. Executa as queries de auditoria - sql/02_auditoria.sql
   (pseudo-colunas "$path" / "$file_size" e conciliação Raw = Silver + Quarentena)

Os resultados do Athena são gravados em s3://<bucket>/athena-results/.
"""
from __future__ import annotations

import csv
import re
import time
from pathlib import Path
from typing import List, Tuple

from config import Settings, build_parser, load_settings

RAIZ = Path(__file__).resolve().parent.parent
SQL_DIR = RAIZ / "sql"
RESULTADOS_DIR = RAIZ / "docs" / "resultados"


def carregar_sql(arquivo: Path, settings: Settings) -> List[Tuple[str, str]]:
    """Lê um .sql, substitui os placeholders e devolve [(titulo, statement), ...]."""
    texto = arquivo.read_text(encoding="utf-8")
    texto = (texto.replace("{{BUCKET}}", settings.bucket)
                  .replace("{{DATABASE}}", settings.database)
                  .replace("{{INGEST_DATE}}", settings.ingest_date))
    statements = []
    for pedaco in texto.split(";"):
        linhas = pedaco.strip().splitlines()
        comentarios = [l.strip() for l in linhas if l.strip().startswith("--")]
        codigo = "\n".join(l for l in linhas if not l.strip().startswith("--")).strip()
        if not codigo:
            continue
        titulo = comentarios[-1].lstrip("- ").strip() if comentarios else codigo.splitlines()[0][:60]
        statements.append((titulo, codigo))
    return statements


class AthenaRunner:
    def __init__(self, settings: Settings):
        import boto3
        self.client = boto3.client("athena", region_name=settings.region)
        self.output = f"s3://{settings.bucket}/athena-results/"

    def executar(self, sql: str, database: str, timeout_s: int = 300):
        qid = self.client.start_query_execution(
            QueryString=sql,
            QueryExecutionContext={"Database": database},
            ResultConfiguration={"OutputLocation": self.output},
        )["QueryExecutionId"]

        inicio = time.time()
        while True:
            estado = self.client.get_query_execution(QueryExecutionId=qid)["QueryExecution"]["Status"]
            if estado["State"] in ("SUCCEEDED", "FAILED", "CANCELLED"):
                break
            if time.time() - inicio > timeout_s:
                raise TimeoutError(f"Query {qid} excedeu {timeout_s}s")
            time.sleep(1)

        if estado["State"] != "SUCCEEDED":
            raise RuntimeError(f"Athena {estado['State']}: {estado.get('StateChangeReason', '')}\n{sql}")

        if not re.match(r"^\s*(SELECT|WITH)\b", sql, re.IGNORECASE):
            return qid, None, None

        linhas = []
        for pagina in self.client.get_paginator("get_query_results").paginate(QueryExecutionId=qid):
            for row in pagina["ResultSet"]["Rows"]:
                linhas.append([c.get("VarCharValue", "") for c in row["Data"]])
        return qid, linhas[0], linhas[1:]   # cabeçalho, dados


def imprimir_tabela(cabecalho, dados) -> None:
    larguras = [max(len(str(x)) for x in col) for col in zip(cabecalho, *dados)] if dados else \
               [len(c) for c in cabecalho]
    fmt = " | ".join("{:<" + str(w) + "}" for w in larguras)
    print("   " + fmt.format(*cabecalho))
    print("   " + "-+-".join("-" * w for w in larguras))
    for linha in dados:
        print("   " + fmt.format(*linha))


def salvar_csv(titulo: str, cabecalho, dados) -> None:
    RESULTADOS_DIR.mkdir(parents=True, exist_ok=True)
    nome = re.match(r"(Q\d+)", titulo)
    destino = RESULTADOS_DIR / f"{nome.group(1) if nome else 'resultado'}.csv"
    with open(destino, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(cabecalho)
        w.writerows(dados)


def executar(settings: Settings) -> None:
    if settings.modo_local:
        raise SystemExit("A etapa Athena precisa do S3 real: rode sem --local-dir.")

    athena = AthenaRunner(settings)

    print("== Criando database e tabelas externas ==")
    for titulo, sql in carregar_sql(SQL_DIR / "01_ddl_tabelas.sql", settings):
        athena.executar(sql, "default")
        print(f"[DDL ] {titulo}")

    print("\n== Queries de auditoria ==")
    for titulo, sql in carregar_sql(SQL_DIR / "02_auditoria.sql", settings):
        _, cabecalho, dados = athena.executar(sql, settings.database)
        print(f"\n[{titulo}]")
        imprimir_tabela(cabecalho, dados)
        salvar_csv(titulo, cabecalho, dados)

    print(f"\nResultados brutos do Athena: s3://{settings.bucket}/athena-results/")
    print(f"CSVs locais das consultas:   {RESULTADOS_DIR}")


def main() -> None:
    args = build_parser("Etapa 3: cria tabelas no Athena e executa a auditoria").parse_args()
    executar(load_settings(args))


if __name__ == "__main__":
    main()
