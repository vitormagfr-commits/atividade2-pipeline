"""Etapa 2 - Data Quality, Quarentena, Silver e Gold.

Lê a camada Raw e aplica:
  1. Data Quality em pedidos:
       - descarta quantidade <= 0
       - descarta product_id ou cliente_id inexistentes nas dimensões
  2. Quarentena: registros inválidos + motivo, em JSON Lines
       quarantine/pedidos_rejeitados/data=YYYY-MM-DD/rejeitados.json
  3. Silver: JOIN pedidos válidos + clientes + produtos, com valor_total = quantidade * preco
       processed/fato_vendas/ingest_date=YYYY-MM-DD/fato_vendas.parquet
  4. Gold: agregação por uf e categoria
       gold/vendas_uf_categoria/ingest_date=YYYY-MM-DD/vendas_uf_categoria.parquet
"""
from __future__ import annotations

import io
from typing import Tuple

import pandas as pd

from config import Settings, build_parser, load_settings
from storage import Storage


def ler_csv(storage: Storage, key: str) -> pd.DataFrame:
    return pd.read_csv(io.BytesIO(storage.get(key)))


def para_parquet(df: pd.DataFrame) -> bytes:
    """Serializa em Parquet com compressão Snappy."""
    buf = io.BytesIO()
    df.to_parquet(buf, engine="pyarrow", compression="snappy", index=False)
    return buf.getvalue()


# --------------------------------------------------------------------------- #
# Data Quality
# --------------------------------------------------------------------------- #
def aplicar_data_quality(pedidos: pd.DataFrame, clientes: pd.DataFrame,
                         produtos: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Separa pedidos válidos dos rejeitados. Um registro pode ter mais de um motivo."""
    quantidade = pd.to_numeric(pedidos["quantidade"], errors="coerce")

    regras = pd.DataFrame({
        "QUANTIDADE_INVALIDA": ~(quantidade > 0),                          # <= 0 ou nula
        "CLIENTE_INEXISTENTE": ~pedidos["cliente_id"].isin(clientes["cliente_id"]),
        "PRODUTO_INEXISTENTE": ~pedidos["product_id"].isin(produtos["product_id"]),
    })

    motivos = regras.apply(lambda linha: "; ".join(linha.index[linha.to_numpy()]), axis=1)
    invalido = regras.any(axis=1)

    validos = pedidos.loc[~invalido].copy()
    rejeitados = pedidos.loc[invalido].copy()
    rejeitados["motivo_rejeicao"] = motivos.loc[invalido]
    return validos, rejeitados


def gravar_quarentena(storage: Storage, rejeitados: pd.DataFrame, data: str) -> str:
    """JSON Lines (um objeto por linha): formato que o Athena consegue ler."""
    conteudo = rejeitados.to_json(orient="records", lines=True, force_ascii=False)
    if conteudo and not conteudo.endswith("\n"):
        conteudo += "\n"
    key = f"quarantine/pedidos_rejeitados/data={data}/rejeitados.json"
    return storage.put(key, conteudo.encode("utf-8"))


# --------------------------------------------------------------------------- #
# Silver e Gold
# --------------------------------------------------------------------------- #
def construir_silver(validos: pd.DataFrame, clientes: pd.DataFrame,
                     produtos: pd.DataFrame) -> pd.DataFrame:
    if clientes["cliente_id"].duplicated().any() or produtos["product_id"].duplicated().any():
        raise ValueError("Chaves duplicadas nas dimensões: o JOIN multiplicaria linhas.")

    dim_cli = clientes[["cliente_id", "nome", "uf", "cidade"]].rename(columns={"nome": "nome_cliente"})
    dim_prod = produtos[["product_id", "nome_produto", "categoria", "preco"]]

    silver = (validos
              .merge(dim_cli, on="cliente_id", how="inner")
              .merge(dim_prod, on="product_id", how="inner"))

    if len(silver) != len(validos):
        raise ValueError("O JOIN perdeu linhas: pedidos válidos não encontraram dimensão.")

    silver["valor_total"] = (silver["quantidade"] * silver["preco"]).round(2)

    colunas = ["pedido_id", "data_pedido", "cliente_id", "nome_cliente", "uf", "cidade",
               "product_id", "nome_produto", "categoria", "quantidade", "preco", "valor_total"]
    silver = silver[colunas].sort_values("pedido_id").reset_index(drop=True)

    # tipos explícitos: o DDL do Athena espera bigint/double/string
    for c in ["pedido_id", "cliente_id", "product_id", "quantidade"]:
        silver[c] = silver[c].astype("int64")
    for c in ["preco", "valor_total"]:
        silver[c] = silver[c].astype("float64")
    return silver


def construir_gold(silver: pd.DataFrame) -> pd.DataFrame:
    gold = (silver.groupby(["uf", "categoria"], as_index=False)
            .agg(qtd_pedidos=("pedido_id", "nunique"),
                 qtd_itens=("quantidade", "sum"),
                 receita_total=("valor_total", "sum")))
    gold["ticket_medio"] = (gold["receita_total"] / gold["qtd_pedidos"]).round(2)
    gold["receita_total"] = gold["receita_total"].round(2)
    gold["qtd_pedidos"] = gold["qtd_pedidos"].astype("int64")
    gold["qtd_itens"] = gold["qtd_itens"].astype("int64")
    return gold.sort_values("receita_total", ascending=False).reset_index(drop=True)


# --------------------------------------------------------------------------- #
# Orquestração da etapa
# --------------------------------------------------------------------------- #
def executar(settings: Settings, storage: Storage) -> None:
    d = settings.ingest_date

    clientes = ler_csv(storage, f"raw/clientes/ingest_date={d}/clientes.csv")
    produtos = ler_csv(storage, f"raw/produtos/ingest_date={d}/produtos.csv")
    pedidos = ler_csv(storage, f"raw/pedidos/ingest_date={d}/pedidos.csv")
    print(f"[RAW ] lidos: {len(clientes)} clientes, {len(produtos)} produtos, {len(pedidos)} pedidos")

    validos, rejeitados = aplicar_data_quality(pedidos, clientes, produtos)
    print(f"[DQ  ] válidos: {len(validos)} | rejeitados: {len(rejeitados)}")
    if len(rejeitados):
        for motivo, qtd in rejeitados["motivo_rejeicao"].value_counts().items():
            print(f"[DQ  ]   {qtd:>5} x {motivo}")

    destino_q = gravar_quarentena(storage, rejeitados, d)
    print(f"[QUAR] {len(rejeitados):>6} linhas -> {destino_q}")

    silver = construir_silver(validos, clientes, produtos)
    destino_s = storage.put(f"processed/fato_vendas/ingest_date={d}/fato_vendas.parquet",
                            para_parquet(silver))
    print(f"[SILV] {len(silver):>6} linhas -> {destino_s}")

    gold = construir_gold(silver)
    destino_g = storage.put(f"gold/vendas_uf_categoria/ingest_date={d}/vendas_uf_categoria.parquet",
                            para_parquet(gold))
    print(f"[GOLD] {len(gold):>6} linhas -> {destino_g}")

    # Conciliação: Raw = Silver + Quarentena
    if len(pedidos) != len(silver) + len(rejeitados):
        raise RuntimeError(
            f"Falha de integridade: raw={len(pedidos)} != silver={len(silver)} + quarentena={len(rejeitados)}")
    print(f"[OK  ] Conciliação: {len(pedidos)} (raw) = {len(silver)} (silver) + {len(rejeitados)} (quarentena)")


def main() -> None:
    args = build_parser("Etapa 2: Data Quality, quarentena, Silver e Gold").parse_args()
    settings = load_settings(args)
    executar(settings, Storage(settings))


if __name__ == "__main__":
    main()
