from pydantic import SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict
from typing import Literal
from functools import lru_cache


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    # LLM / Embedding
    openai_base_url: str = "https://api.openai.com/v1"
    openai_api_key: SecretStr = SecretStr("")
    embedding_model: str = "text-embedding-3-small"
    embedding_dimensions: int = 1536
    llm_model: str = "gpt-4o"

    # Vector store
    vector_store: Literal["pgvector", "chroma"] = "pgvector"
    database_url: SecretStr = SecretStr("postgresql+asyncpg://postgres:postgres@localhost/vectoria")

    # File storage (parsed images)
    storage_path: str = "./data/files"

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
