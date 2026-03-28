from storage.base import ObjectStorage
from storage.s3 import S3ObjectStorage
from config import get_settings

_instance: ObjectStorage | None = None


async def get_storage() -> ObjectStorage:
    """Factory: return a cached ObjectStorage instance based on config."""
    global _instance
    if _instance is not None:
        return _instance
    cfg = get_settings()
    if cfg.storage_type == "s3":
        _instance = S3ObjectStorage(
            endpoint=cfg.s3_endpoint,
            region=cfg.s3_region,
            access_key=cfg.s3_access_key,
            secret_key=cfg.s3_secret_key.get_secret_value(),
            bucket=cfg.s3_bucket,
            addressing_style=cfg.s3_addressing_style,
            presign_expires=cfg.s3_presign_expires,
        )
        return _instance
    raise ValueError(f"Unknown storage type: {cfg.storage_type}")
