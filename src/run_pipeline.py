"""Executa o pipeline completo: Raw -> Data Quality/Quarentena -> Silver -> Gold -> Auditoria Athena.

Exemplos:
    python src/run_pipeline.py --bucket meu-bucket-unico
    python src/run_pipeline.py --bucket meu-bucket-unico --ingest-date 2025-06-01
    python src/run_pipeline.py --local-dir s3_local          # teste local, sem AWS
    python src/run_pipeline.py --bucket meu-bucket-unico --skip-athena
"""
from __future__ import annotations

import auditoria_athena
import ingestao_raw
import processamento
from config import build_parser, load_settings
from storage import Storage


def main() -> None:
    parser = build_parser("Pipeline completo: ingestão, qualidade, Silver, Gold e auditoria no Athena")
    parser.add_argument("--skip-athena", action="store_true",
                        help="Não executa a etapa do Athena (útil em modo local)")
    args = parser.parse_args()
    settings = load_settings(args)
    storage = Storage(settings)

    print(f"### Ingest date: {settings.ingest_date} | destino: "
          f"{settings.local_dir if settings.modo_local else 's3://' + settings.bucket}\n")

    print("=== 1/3 Ingestão (Raw) ===")
    ingestao_raw.executar(settings, storage)

    print("\n=== 2/3 Data Quality, Quarentena, Silver e Gold ===")
    processamento.executar(settings, storage)

    if args.skip_athena or settings.modo_local:
        print("\n=== 3/3 Athena: ignorado ===")
        return

    print("\n=== 3/3 Auditoria no Athena ===")
    auditoria_athena.executar(settings)


if __name__ == "__main__":
    main()
