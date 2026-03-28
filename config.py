from pydantic import SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict
from typing import Literal
from functools import lru_cache


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    # LLM
    openai_base_url: str = "https://api.openai.com/v1"
    openai_api_key: SecretStr = SecretStr("")
    llm_model: str = "gpt-4o"

    # Embedding (falls back to LLM settings if not set)
    embedding_base_url: str = ""
    embedding_api_key: SecretStr = SecretStr("")
    embedding_model: str = "text-embedding-3-small"
    embedding_dimensions: int = 1536

    @property
    def effective_embedding_base_url(self) -> str:
        return self.embedding_base_url or self.openai_base_url

    @property
    def effective_embedding_api_key(self) -> str:
        key = self.embedding_api_key.get_secret_value()
        return key if key else self.openai_api_key.get_secret_value()

    # Vector store
    vector_store: Literal["pgvector", "chroma"] = "pgvector"
    database_url: SecretStr = SecretStr("postgresql+asyncpg://postgres:postgres@localhost/vectoria")

    # File storage (parsed images)
    storage_path: str = "./data/files"

    # Object storage
    storage_type: str = "s3"
    s3_endpoint: str = "http://localhost:9000"
    s3_region: str = ""
    s3_access_key: str = "minioadmin"
    s3_secret_key: SecretStr = SecretStr("minioadmin")
    s3_bucket: str = "vectoria"
    s3_addressing_style: str = "auto"  # auto|virtual|path
    s3_presign_expires: int = 3600

    # Parse engine
    default_parse_engine: str = "auto"

    # MinerU remote API
    mineru_api_url: str = ""
    mineru_backend: str = "pipeline"
    mineru_language: str = "ch"

    # RAG pipeline toggles
    enable_query_rewrite: bool = True
    enable_reranker: bool = False
    reranker_base_url: str = ""


@lru_cache
def get_settings() -> Settings:
    return Settings()
