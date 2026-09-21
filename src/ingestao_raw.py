"""Etapa 1 - Ingestão (camada Raw).

Gera massas de dados simuladas (clientes, produtos e pedidos) contendo anomalias
propositais e grava CSVs no S3 com partição Hive-style por data de ingestão:

    raw/clientes/ingest_date=YYYY-MM-DD/clientes.csv
    raw/produtos/ingest_date=YYYY-MM-DD/produtos.csv
    raw/pedidos/ingest_date=YYYY-MM-DD/pedidos.csv

Anomalias injetadas em pedidos:
    - quantidade <= 0 (zero ou negativa)
    - cliente_id inexistente em clientes
    - product_id inexistente em produtos
"""
from __future__ import annotations

from typing import Dict, Tuple

import numpy as np
import pandas as pd

from config import Settings, build_parser, load_settings
from storage import Storage

PRIMEIROS_NOMES = ["Ana", "Bruno", "Carla", "Daniel", "Eduarda", "Felipe", "Gabriela", "Henrique",
                   "Isabela", "Joao", "Karina", "Lucas", "Mariana", "Nicolas", "Olivia", "Paulo",
                   "Rafaela", "Samuel", "Tatiana", "Vinicius"]
SOBRENOMES = ["Silva", "Santos", "Oliveira", "Souza", "Pereira", "Lima", "Carvalho", "Ribeiro",
              "Almeida", "Gomes", "Martins", "Rocha", "Barbosa", "Costa", "Fernandes"]
CIDADES_POR_UF = {
    "SP": ["Sao Paulo", "Campinas", "Santos"],
    "RJ": ["Rio de Janeiro", "Niteroi"],
    "MG": ["Belo Horizonte", "Uberlandia"],
    "RS": ["Porto Alegre", "Caxias do Sul"],
    "PR": ["Curitiba", "Londrina"],
    "BA": ["Salvador", "Feira de Santana"],
    "SC": ["Florianopolis", "Joinville"],
    "PE": ["Recife", "Olinda"],
    "CE": ["Fortaleza", "Sobral"],
    "GO": ["Goiania", "Anapolis"],
}
# categoria -> (produtos-base, faixa de preço)
CATALOGO = {
    "Eletronicos": (["Notebook", "Smartphone", "Monitor", "Fone de Ouvido"], (150.0, 4500.0)),
    "Informatica": (["Teclado", "Mouse", "Webcam", "Hub USB"], (30.0, 600.0)),
    "Casa": (["Cafeteira", "Liquidificador", "Aspirador", "Ventilador"], (60.0, 900.0)),
}
LINHAS = ["Basic", "Plus", "Pro", "Max", "Ultra"]


def gerar_clientes(rng: np.random.Generator, n: int, ingest_date: str) -> pd.DataFrame:
    ufs = list(CIDADES_POR_UF)
    uf = rng.choice(ufs, size=n)
    nomes = [f"{rng.choice(PRIMEIROS_NOMES)} {rng.choice(SOBRENOMES)}" for _ in range(n)]
    ids = np.arange(1, n + 1)
    cidades = [rng.choice(CIDADES_POR_UF[u]) for u in uf]
    dias = rng.integers(30, 1500, size=n)
    cadastro = (pd.Timestamp(ingest_date) - pd.to_timedelta(dias, unit="D")).strftime("%Y-%m-%d")
    return pd.DataFrame({
        "cliente_id": ids,
        "nome": nomes,
        "email": [f"{nome.lower().replace(' ', '.')}.{i}@exemplo.com" for nome, i in zip(nomes, ids)],
        "uf": uf,
        "cidade": cidades,
        "data_cadastro": list(cadastro),
    })


def gerar_produtos(rng: np.random.Generator, n: int) -> pd.DataFrame:
    itens = [(cat, base, faixa) for cat, (bases, faixa) in CATALOGO.items() for base in bases]
    linhas = []
    for i in range(n):
        cat, base, (pmin, pmax) = itens[i % len(itens)]
        linha = LINHAS[(i // len(itens)) % len(LINHAS)]
        nome = f"{base} {linha}" if i < len(itens) * len(LINHAS) else f"{base} {linha} #{i}"
        linhas.append({
            "product_id": i + 1,
            "nome_produto": nome,
            "categoria": cat,
            "preco": round(float(rng.uniform(pmin, pmax)), 2),
        })
    return pd.DataFrame(linhas)


def gerar_pedidos(rng: np.random.Generator, n: int, n_clientes: int, n_produtos: int,
                  ingest_date: str, taxa_anomalia: float) -> Tuple[pd.DataFrame, Dict[str, int]]:
    dias = rng.integers(0, 90, size=n)
    data_pedido = (pd.Timestamp(ingest_date) - pd.to_timedelta(dias, unit="D")).strftime("%Y-%m-%d")
    pedidos = pd.DataFrame({
        "pedido_id": np.arange(1, n + 1),
        "cliente_id": rng.integers(1, n_clientes + 1, size=n),
        "product_id": rng.integers(1, n_produtos + 1, size=n),
        "quantidade": rng.integers(1, 11, size=n),
        "data_pedido": list(data_pedido),
    })

    k = max(1, int(n * taxa_anomalia))
    idx_qtd = rng.choice(n, size=k, replace=False)
    idx_cli = rng.choice(n, size=k, replace=False)
    idx_prod = rng.choice(n, size=k, replace=False)

    pedidos.loc[idx_qtd, "quantidade"] = rng.choice([0, -1, -3, -10], size=k)
    pedidos.loc[idx_cli, "cliente_id"] = rng.integers(9000, 9999, size=k)     # chave inexistente
    pedidos.loc[idx_prod, "product_id"] = rng.integers(9000, 9999, size=k)    # chave inexistente

    return pedidos, {"quantidade_invalida": k, "cliente_inexistente": k, "produto_inexistente": k}


def para_csv(df: pd.DataFrame) -> bytes:
    # lineterminator fixo em \n: evita \r\n no Windows, que suja a última coluna no Athena
    return df.to_csv(index=False, lineterminator="\n").encode("utf-8")


def executar(settings: Settings, storage: Storage) -> None:
    rng = np.random.default_rng(settings.seed)
    d = settings.ingest_date

    clientes = gerar_clientes(rng, settings.n_clientes, d)
    produtos = gerar_produtos(rng, settings.n_produtos)
    pedidos, injetadas = gerar_pedidos(rng, settings.n_pedidos, settings.n_clientes,
                                       settings.n_produtos, d, settings.taxa_anomalia)

    for nome, df in [("clientes", clientes), ("produtos", produtos), ("pedidos", pedidos)]:
        key = f"raw/{nome}/ingest_date={d}/{nome}.csv"
        destino = storage.put(key, para_csv(df))
        print(f"[RAW] {len(df):>6} linhas -> {destino}")

    print(f"[RAW] Anomalias injetadas em pedidos (podem se sobrepor): {injetadas}")


def main() -> None:
    args = build_parser("Etapa 1: gera dados simulados e grava a camada Raw no S3").parse_args()
    settings = load_settings(args)
    executar(settings, Storage(settings))


if __name__ == "__main__":
    main()
