from abc import ABC, abstractmethod


class ObjectStorage(ABC):
    """Abstract interface for object storage backends."""

    @abstractmethod
    async def put(self, key: str, data: bytes, content_type: str = "") -> None:
        """Upload an object."""

    @abstractmethod
    async def get(self, key: str) -> bytes:
        """Download an object."""

    @abstractmethod
    async def delete(self, key: str) -> None:
        """Delete a single object."""

    @abstractmethod
    async def delete_prefix(self, prefix: str) -> None:
        """Delete all objects under a given prefix."""

    @abstractmethod
    async def presign_url(self, key: str, expires: int = 0) -> str:
        """Generate a presigned download URL. expires=0 uses the configured default."""

    @abstractmethod
    async def exists(self, key: str) -> bool:
        """Check whether an object exists."""
