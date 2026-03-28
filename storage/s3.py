import aioboto3
from botocore.config import Config as BotoConfig

from storage.base import ObjectStorage


class S3ObjectStorage(ObjectStorage):
    """S3-compatible object storage (MinIO, TOS, AWS S3, etc.)."""

    def __init__(
        self,
        endpoint: str,
        region: str,
        access_key: str,
        secret_key: str,
        bucket: str,
        addressing_style: str = "auto",
        presign_expires: int = 3600,
    ):
        self._endpoint = endpoint
        self._region = region or None
        self._bucket = bucket
        self._presign_expires = presign_expires

        self._session = aioboto3.Session(
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            region_name=self._region,
        )
        self._boto_config = BotoConfig(
            signature_version="s3v4",
            s3={"addressing_style": addressing_style},
        )

    def _client(self):
        return self._session.client(
            "s3",
            endpoint_url=self._endpoint,
            config=self._boto_config,
        )

    async def put(self, key: str, data: bytes, content_type: str = "") -> None:
        kwargs: dict = {"Bucket": self._bucket, "Key": key, "Body": data}
        if content_type:
            kwargs["ContentType"] = content_type
        async with self._client() as client:
            await client.put_object(**kwargs)

    async def get(self, key: str) -> bytes:
        async with self._client() as client:
            resp = await client.get_object(Bucket=self._bucket, Key=key)
            return await resp["Body"].read()

    async def delete(self, key: str) -> None:
        async with self._client() as client:
            await client.delete_object(Bucket=self._bucket, Key=key)

    async def delete_prefix(self, prefix: str) -> None:
        async with self._client() as client:
            paginator = client.get_paginator("list_objects_v2")
            async for page in paginator.paginate(Bucket=self._bucket, Prefix=prefix):
                objects = page.get("Contents", [])
                if not objects:
                    continue
                delete_req = {"Objects": [{"Key": obj["Key"]} for obj in objects]}
                await client.delete_objects(Bucket=self._bucket, Delete=delete_req)

    async def presign_url(self, key: str, expires: int = 0) -> str:
        ttl = expires if expires > 0 else self._presign_expires
        async with self._client() as client:
            return await client.generate_presigned_url(
                "get_object",
                Params={"Bucket": self._bucket, "Key": key},
                ExpiresIn=ttl,
            )

    async def exists(self, key: str) -> bool:
        async with self._client() as client:
            try:
                await client.head_object(Bucket=self._bucket, Key=key)
                return True
            except client.exceptions.ClientError:
                return False
