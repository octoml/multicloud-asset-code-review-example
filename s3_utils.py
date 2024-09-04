import asyncio
import structlog
import contextlib
from pydantic_settings import BaseSettings

_LOG = structlog.stdlib.get_logger(__name__)

"""Basic settings so that tests can run -- feel free to ignore this portion"""
class S3Settings(BaseSettings):
    s3_upload_chunk_size_bytes: int = 50 * 1024**2
    """Size of each part of a multipart upload in bytes."""

    max_url_upload_size: int = 100 * 1024**3  # 100 gb for now
    """Maximum size of an asset that can be uploaded via a URL."""

    insecure_sts_bypass: bool = False

    s3_large_object_copy_parallelism: int = 10
    """Max number of parallel CopyPart operations for large objects."""

    s3_aws_prefix_copy_parallelism: int = 3
    """Max mumber of parallel object copy."""


s3_settings = S3Settings()
S3_ENDPOINT_URL = "url-to-s3-endpoint"

@contextlib.asynccontextmanager
async def s3_client(region: str | None = None):
    session = get_boto_session(region) # this method is omitted
    async with session.client(
        "s3",
        endpoint_url=S3_ENDPOINT_URL,
    ) as client:
        yield client
