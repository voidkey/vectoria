from storage.base import ObjectStorage
from storage.s3 import S3ObjectStorage
from config import get_settings


async def get_storage() -> ObjectStorage:
    """Factory: return an ObjectStorage instance based on config."""
    cfg = get_settings()
    if cfg.storage_type == "s3":
        return S3ObjectStorage(
            endpoint=cfg.s3_endpoint,
            region=cfg.s3_region,
            access_key=cfg.s3_access_key,
            secret_key=cfg.s3_secret_key.get_secret_value(),
            bucket=cfg.s3_bucket,
            addressing_style=cfg.s3_addressing_style,
            presign_expires=cfg.s3_presign_expires,
        )
    raise ValueError(f"Unknown storage type: {cfg.storage_type}")
