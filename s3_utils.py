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

"""Please review starting here!"""
def _get_byte_offsets_and_parts(
    object_size: int,
    task_count: int,
    part_size: int,
):
    """Partitions an object into groups of parts based on the task count and part size.

    Note: the returned start and end positions for all the parts governed by the entire
    task and thus will typically be larger than part_size.

    :param task_count: The number of tasks to partition the object into
    :param part_size: The size of each part in bytes

    :return: A tuple of lists containing the byte start positions, byte end positions,
                and starting part numbers for each task
    """

    # Total parts is the ceiling division of the object size divided by the part size
    total_parts = -(-object_size // part_size)

    # Parts per-task is the floor division of the total parts by the task count
    # The last task being an exception which will have the remaining parts
    parts_per_task = total_parts // task_count

    byte_start_positions = [
        part_size * parts_per_task * task_num for task_num in range(task_count)
    ]

    # end byte is inclusive
    byte_end_positions = [
        byte_start_positions[task_num + 1] - 1 for task_num in range(task_count - 1)
    ] + [object_size - 1]

    starting_part_nums = [
        parts_per_task * task_num + 1 for task_num in range(task_count)
    ]

    return byte_start_positions, byte_end_positions, starting_part_nums


async def _large_copy_object_to_aws(
    source_bucket: str,
    source_key: str,
    destination_bucket: str,
    destination_key: str,
    destination_region: str,
    object_size: int,
):
    _LOG.info("using multipart copy for object")
    task_count = s3_settings.s3_large_object_copy_parallelism
    async with contextlib.AsyncExitStack() as es:
        s3_dest = await es.enter_async_context(s3_client(region=destination_region))
        default_part_size = s3_settings.s3_upload_chunk_size_bytes
        part_size = (
            default_part_size
            if 9000 * default_part_size > object_size
            else int(object_size / 9000)
        )

        total_parts = -(-object_size // part_size)

        assert total_parts > 0

        task_count = min(task_count, total_parts)

        initiate_multipart = await s3_dest.create_multipart_upload(
            Bucket=destination_bucket, Key=destination_key
        )
        upload_id = initiate_multipart["UploadId"]
        (
            byte_start_positions,
            byte_end_positions,
            starting_part_nums,
        ) = _get_byte_offsets_and_parts(
            object_size,
            task_count,
            part_size,
        )
        tasks = []
        queue: asyncio.Queue = asyncio.Queue(maxsize=task_count)
        for task_num in range(task_count):
            task = asyncio.create_task(
                _upload_part_to_aws(
                    byte_start_positions[task_num],
                    byte_end_positions[task_num],
                    starting_part_nums[task_num],
                    part_size,
                    queue,
                    object_size,
                    destination_region,
                    destination_bucket,
                    destination_key,
                    source_bucket,
                    source_key,
                    upload_id,
                ),
            )
            tasks.append(task)
        await asyncio.gather(*tasks)
        _LOG.info(f"all {task_count} tasks finished")
        etag_lists = []
        while not queue.empty():
            etag_lists.append(queue.get_nowait())
        parts_etags = [etag for etag_sublist in etag_lists for etag in etag_sublist]
        assert parts_etags != []
        parts_etags.sort(key=lambda etag: etag["PartNumber"])
        try:
            await s3_dest.complete_multipart_upload(
                Bucket=destination_bucket,
                Key=destination_key,
                MultipartUpload={"Parts": parts_etags},
                UploadId=upload_id,
            )
            _LOG.info(f"upload complete for {upload_id}")
        except Exception as ex:
            raise ex


async def _upload_part_to_aws(
    byte_start_position: int,
    byte_end_position: int,
    start_part_num: int,
    part_size: int,
    queue: asyncio.Queue,
    object_size: int,
    destination_region: str,
    destination_bucket: str,
    destination_prefix: str,
    source_bucket: str,
    source_prefix: str,
    upload_id: str,
):
    _LOG.info(
        f"starting task for parts {start_part_num}, spanning bytes "
        f"{byte_start_position} to {byte_end_position}"
    )
    try:
        async with contextlib.AsyncExitStack() as es:
            parts_etags = []
            s3_dest = await es.enter_async_context(s3_client(region=destination_region))
            byte_position = byte_start_position
            part_num = start_part_num
            if parts_etags is not None:
                while byte_position < byte_end_position:
                    #  The last part could be smaller than part_size
                    last_byte = min(byte_position + part_size - 1, object_size - 1)
                    copy_source_range = f"bytes={byte_position}-{last_byte}"
                    try:
                        response = await s3_dest.upload_part_copy(
                            Bucket=destination_bucket,
                            CopySource={"Bucket": source_bucket, "Key": source_prefix},
                            CopySourceRange=copy_source_range,
                            Key=destination_prefix,
                            PartNumber=part_num,
                            UploadId=upload_id,
                        )
                    except Exception:
                        _LOG.exception("Failed to copy part")
                        raise
                    parts_etags.append(
                        {"ETag": response["CopyPartResult"]["ETag"], "PartNumber": part_num}
                    )
                    part_num += 1
                    byte_position += part_size
    except Exception:
        _LOG.exception("_upload_part_to_aws worker failed to copy part")
        raise
    await queue.put(parts_etags)
    _LOG.info(
        f"task for part {start_part_num}, spanning bytes "
        f"{byte_start_position} to {byte_end_position} finished"
    )
