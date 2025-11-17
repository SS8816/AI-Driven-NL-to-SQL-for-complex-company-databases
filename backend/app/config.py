"""
Application Configuration
Centralized configuration management using Pydantic Settings
"""

from typing import List
from pathlib import Path
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Application settings loaded from environment variables"""

    # Application
    APP_NAME: str = "AI-Driven Violation Detection API"
    ENVIRONMENT: str = "development"
    DEBUG: bool = False
    LOG_LEVEL: str = "INFO"
    API_V1_PREFIX: str = "/api/v1"

    # Azure OpenAI
    AZURE_OPENAI_API_KEY: str
    AZURE_OPENAI_ENDPOINT: str
    AZURE_OPENAI_DEPLOYMENT: str
    AZURE_OPENAI_API_VERSION: str = "2023-12-01-preview"

    # AWS Athena
    AWS_REGION: str = "us-east-1"
    ATHENA_S3_OUTPUT_LOCATION: str
    ATHENA_WORKGROUP: str = "primary"
    ATHENA_TIMEOUT_SECONDS: int = 1800

    # Authentication
    HERE_AUTH_ENDPOINT: str
    JWT_SECRET_KEY: str
    JWT_ALGORITHM: str = "HS256"
    JWT_EXPIRATION_MINUTES: int = 480

    # Database
    DATABASE_URL: str = "sqlite+aiosqlite:///./app_data.db"
    CACHE_DATABASE_URL: str = "sqlite:///./query_cache.db"
    LOGS_DATABASE_URL: str = "sqlite:///./query_logs.db"

    # Schemas & Vector Stores
    SCHEMAS_DIR: str = "schemas"
    DOCS_VECTORSTORE_PATH: str = "athena_docs_vectorstore"
    FUNCTION_VECTORSTORE_PATH: str = "vectorstores"

    # CORS
    CORS_ORIGINS: List[str] = [
        "http://localhost:3000",
        "http://localhost:5173",
    ]

    # Rate Limiting (requests per minute)
    RATE_LIMIT_PER_MINUTE: int = 60

    # Query Execution
    MAX_PREVIEW_ROWS: int = 1000
    CACHE_TTL_DAYS: int = 7
    MAX_RETRY_ATTEMPTS: int = 5

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=True,
        extra="ignore"
    )

    @property
    def schemas_path(self) -> Path:
        """Get schemas directory as Path object"""
        return Path(self.SCHEMAS_DIR)

    @property
    def docs_vectorstore_path(self) -> Path:
        """Get docs vectorstore directory as Path object"""
        return Path(self.DOCS_VECTORSTORE_PATH)

    @property
    def function_vectorstore_path(self) -> Path:
        """Get function vectorstore directory as Path object"""
        return Path(self.FUNCTION_VECTORSTORE_PATH)

    @property
    def DATABASE_PATH(self) -> Path:
        """Get database file path from DATABASE_URL"""
        # Extract path from sqlite URL format: sqlite+aiosqlite:///./app_data.db
        db_path = self.DATABASE_URL.split("///")[-1]
        return Path(db_path)

    def validate_paths(self) -> None:
        """Validate that required paths exist"""
        if not self.schemas_path.exists():
            raise ValueError(f"Schemas directory not found: {self.SCHEMAS_DIR}")

        # Vectorstores are optional (created by setup script)
        if not self.docs_vectorstore_path.exists():
            print(f"Warning: Docs vectorstore not found at {self.DOCS_VECTORSTORE_PATH}")

        if not self.function_vectorstore_path.exists():
            print(f"Warning: Function vectorstore not found at {self.FUNCTION_VECTORSTORE_PATH}")


# Global settings instance
settings = Settings()
