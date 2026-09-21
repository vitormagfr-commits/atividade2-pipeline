"""Abstração de armazenamento: S3 (padrão) ou pasta local (modo de teste)."""
from __future__ import annotations

from pathlib import Path

from config import Settings


class Storage:
    def __init__(self, settings: Settings):
        self.bucket = settings.bucket
        self.local_dir = Path(settings.local_dir) if settings.local_dir else None
        self._s3 = None
        if self.local_dir is None:
            import boto3  # import tardio: o modo local não exige boto3
            self._s3 = boto3.client("s3", region_name=settings.region)

    def uri(self, key: str) -> str:
        if self.local_dir is not None:
            return str(self.local_dir / key)
        return f"s3://{self.bucket}/{key}"

    def put(self, key: str, data: bytes) -> str:
        if self.local_dir is not None:
            destino = self.local_dir / key
            destino.parent.mkdir(parents=True, exist_ok=True)
            destino.write_bytes(data)
        else:
            self._s3.put_object(Bucket=self.bucket, Key=key, Body=data)
        return self.uri(key)

    def get(self, key: str) -> bytes:
        if self.local_dir is not None:
            return (self.local_dir / key).read_bytes()
        return self._s3.get_object(Bucket=self.bucket, Key=key)["Body"].read()
