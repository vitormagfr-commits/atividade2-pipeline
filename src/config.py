"""Configuração compartilhada pelos scripts do pipeline."""
from __future__ import annotations

import argparse
import os
from dataclasses import dataclass
from datetime import date
from typing import Optional


@dataclass
class Settings:
    bucket: str
    region: str
    ingest_date: str
    local_dir: Optional[str]
    database: str
    seed: int
    n_clientes: int
    n_produtos: int
    n_pedidos: int
    taxa_anomalia: float

    @property
    def modo_local(self) -> bool:
        return self.local_dir is not None


def build_parser(descricao: str) -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description=descricao)
    p.add_argument("--bucket", default=os.getenv("BUCKET_NAME"),
                   help="Nome do bucket S3 (ou variável de ambiente BUCKET_NAME)")
    p.add_argument("--region", default=os.getenv("AWS_REGION", "us-east-1"),
                   help="Região AWS (padrão: us-east-1)")
    p.add_argument("--ingest-date", default=date.today().isoformat(),
                   help="Data de ingestão YYYY-MM-DD (padrão: hoje)")
    p.add_argument("--local-dir", default=None,
                   help="Modo de teste: grava em uma pasta local em vez do S3")
    p.add_argument("--database", default="atividade2",
                   help="Nome do database no Athena (padrão: atividade2)")
    p.add_argument("--seed", type=int, default=42, help="Semente aleatória")
    p.add_argument("--n-clientes", type=int, default=200)
    p.add_argument("--n-produtos", type=int, default=50)
    p.add_argument("--n-pedidos", type=int, default=5000)
    p.add_argument("--taxa-anomalia", type=float, default=0.03,
                   help="Fração de pedidos por tipo de anomalia (padrão: 0.03)")
    return p


def load_settings(args: argparse.Namespace) -> Settings:
    if not args.local_dir and not args.bucket:
        raise SystemExit("Informe --bucket <nome> (ou BUCKET_NAME) ou use --local-dir para teste local.")
    return Settings(
        bucket=args.bucket or "bucket-local",
        region=args.region,
        ingest_date=args.ingest_date,
        local_dir=args.local_dir,
        database=args.database,
        seed=args.seed,
        n_clientes=args.n_clientes,
        n_produtos=args.n_produtos,
        n_pedidos=args.n_pedidos,
        taxa_anomalia=args.taxa_anomalia,
    )
