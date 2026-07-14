"""
Utility functions for the bioinformatics workflow.
Handles logging, retries, file operations, and common helpers.
"""

import sys
import io
import logging
import hashlib
import time
import functools
import zipfile
from pathlib import Path
from typing import Any, Callable, Optional
import json


# Logging Setup

def setup_logger(logger_name: str, log_file: Optional[str] = None, level=logging.INFO) -> logging.Logger:
    """
    Configure a logger with optional file output.

    Args:
        name: Logger name
        log_file: Optional path to log file
        level: Logging level (default: INFO)

    Returns:
        Configured logger instance
    """
    logger = logging.getLogger(logger_name)
    logger.setLevel(level)

    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    if log_file:
        log_path = Path(log_file)
        (log_path.parent if log_path.parent != Path("") else Path(".")).mkdir(parents=True, exist_ok=True)
        file_handler = logging.FileHandler(log_path)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

    return logger


# Retry Decorator

def retry(max_attempts: int = 3, delay: float = 2.0, backoff: float = 2.0, exceptions: tuple = (Exception,)):
    """
    Retry decorator with exponential backoff.

    Args:
        max_attempts: Maximum number of retry attempts
        delay: Initial delay in seconds
        backoff: Multiplier for exponential backoff
        exceptions: Tuple of exceptions to catch and retry on

    Returns:
        Decorated function
    """
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(*wrapped_args, **wrapped_kwargs):
            logger = logging.getLogger(__name__)
            current_delay = delay

            for attempt in range(1, max_attempts + 1):
                try:
                    return func(*wrapped_args, **wrapped_kwargs)
                except exceptions as exception:
                    if attempt == max_attempts:
                        logger.error(f"Failed after {max_attempts} attempts: {exception}")
                        raise
                    logger.warning(f"Attempt {attempt} failed: {exception}. Retrying in {current_delay}s...")
                    time.sleep(current_delay)
                    current_delay *= backoff

        return wrapper
    return decorator


# File Operations

def compute_md5(file_path: str, chunk_size: int = 8192) -> str:
    """
    Compute MD5 checksum of a file.

    Args:
        file_path: Path to file
        chunk_size: Chunk size for reading (default: 8KB)

    Returns:
        MD5 hexdigest
    """
    md5_hash = hashlib.md5()
    with Path(file_path).open('rb') as file_handle:
        for chunk in iter(lambda: file_handle.read(chunk_size), b''):
            md5_hash.update(chunk)
    return md5_hash.hexdigest()


def verify_md5(file_path: str, expected_md5: str) -> bool:
    """
    Verify file MD5 checksum against expected value.

    Args:
        file_path: Path to file
        expected_md5: Expected MD5 hexdigest

    Returns:
        True if checksums match, False otherwise
    """
    computed = compute_md5(file_path)
    return computed.lower() == expected_md5.lower()


def ensure_dir(file_path_value: str) -> str:
    """
    Ensure directory exists and return path.

    Args:
        path: Directory path

    Returns:
        Absolute path to directory
    """
    json_path = Path(file_path_value)
    json_path.mkdir(parents=True, exist_ok=True)
    return str(json_path.resolve())


def safe_symlink(source_path: str, dst: str, logger=None) -> bool:
    """
    Safely create symbolic link, removing existing link if needed.

    Args:
        src: Source path
        dst: Destination link path
        logger: Optional logger instance

    Returns:
        True if successful, False otherwise
    """
    dst_path = Path(dst)
    try:
        if dst_path.is_symlink():
            dst_path.unlink()
        elif dst_path.exists():
            if logger:
                logger.warning(f"Destination {dst} exists but is not a symlink. Skipping.")
            return False

        dst_path.symlink_to(source_path)
        if logger:
            logger.info(f"Created symlink: {dst} -> {source_path}")
        return True
    except Exception as exception:
        if logger:
            logger.error(f"Failed to create symlink: {exception}")
        return False


# Archiving

def zip_directory(directory: str, arc_root: str) -> io.BytesIO:
    """
    Zip a directory's files into an in-memory buffer.

    Args:
        directory: Directory to zip
        arc_root: Path prefix given to each file's entry inside the archive

    Returns:
        BytesIO of the zip data, seeked to the start
    """
    buffer = io.BytesIO()
    directory = Path(directory)
    with zipfile.ZipFile(buffer, 'w', zipfile.ZIP_DEFLATED) as archive:
        for file_path in directory.rglob('*'):
            if file_path.is_file():
                archive.write(file_path, Path(arc_root) / file_path.relative_to(directory))
    buffer.seek(0)
    return buffer


class _ChunkSink:
    """Write target for ZipFile that hands each compressed chunk to the caller
    instead of accumulating an archive.

    It deliberately has no tell() and no seek(). That absence is load-bearing:
    it is how zipfile detects a non-seekable stream and switches to emitting
    data descriptors after each entry, which is what allows the archive to be
    produced in a single forward pass with nothing held back.
    """

    def __init__(self):
        self._chunks = []

    def write(self, data):
        self._chunks.append(bytes(data))
        return len(data)

    def flush(self):
        pass

    def drain(self):
        chunks, self._chunks = self._chunks, []
        return chunks


def stream_directory_zip(directory: str, arc_root: str, chunk_size: int = 1 << 20):
    """
    Zip a directory's files, yielding the archive in pieces as it is built.

    zip_directory's peak memory is the whole finished archive. That is survivable
    for one isolate and fatal for a whole job, where the archive is every isolate
    at once -- large enough to get the (single) web worker OOM-killed, which takes
    the site down and fails whatever runs were in flight. Here memory stays bounded
    by chunk_size no matter how large the job is.

    Args:
        directory: Directory to zip
        arc_root: Path prefix given to each file's entry inside the archive
        chunk_size: Bytes read from each file at a time

    Yields:
        Chunks of zip data, in order
    """
    directory = Path(directory)
    sink = _ChunkSink()
    with zipfile.ZipFile(sink, 'w', zipfile.ZIP_DEFLATED) as archive:
        for file_path in sorted(directory.rglob('*')):
            if not file_path.is_file():
                continue
            arcname = Path(arc_root) / file_path.relative_to(directory)
            entry_info = zipfile.ZipInfo.from_file(file_path, arcname)
            entry_info.compress_type = zipfile.ZIP_DEFLATED
            # force_zip64: the entry's size is not known to the header when we
            # stream it, so assume it may be over the 2GB non-zip64 ceiling. The
            # cost on small entries is a few bytes; the cost of guessing wrong is
            # a RuntimeError partway through the download.
            with archive.open(entry_info, 'w', force_zip64=True) as entry:
                with file_path.open('rb') as source:
                    while chunk := source.read(chunk_size):
                        entry.write(chunk)
                        yield from sink.drain()
            yield from sink.drain()
    yield from sink.drain()


# Data Parsing

def load_json_safe(file_path: str, logger=None) -> dict:
    """
    Safely load JSON file with error handling.

    Args:
        file_path: Path to JSON file
        logger: Optional logger instance

    Returns:
        Parsed JSON dict, or empty dict if error
    """
    try:
        with Path(file_path).open('r') as file_handle:
            return json.load(file_handle)
    except FileNotFoundError:
        if logger:
            logger.error(f"JSON file not found: {file_path}")
        return {}
    except json.JSONDecodeError as exception:
        if logger:
            logger.error(f"Failed to parse JSON file {file_path}: {exception}")
        return {}


def save_json(json_data: dict, file_path: str, logger=None) -> bool:
    """
    Save dictionary to JSON file with error handling.

    Args:
        data: Dictionary to save
        file_path: Output file path
        logger: Optional logger instance

    Returns:
        True if successful, False otherwise
    """
    try:
        json_path = Path(file_path)
        (json_path.parent if json_path.parent != Path("") else Path(".")).mkdir(parents=True, exist_ok=True)
        with json_path.open('w') as file_handle:
            json.dump(json_data, file_handle, indent=2)
        if logger:
            logger.info(f"Saved JSON to {file_path}")
        return True
    except Exception as exception:
        if logger:
            logger.error(f"Failed to save JSON to {file_path}: {exception}")
        return False


# Polling / Async Helpers

def wait_for_condition(
    condition_fn: Callable,
    max_wait_seconds: int = 600,
    poll_interval: int = 10,
    logger=None
) -> bool:
    """
    Poll a condition function until True or timeout.

    Args:
        condition_fn: Function that returns True when condition met
        max_wait_seconds: Maximum time to wait
        poll_interval: Seconds between polls
        logger: Optional logger instance

    Returns:
        True if condition met, False if timeout
    """
    start_time = time.time()

    while True:
        elapsed = time.time() - start_time

        if elapsed > max_wait_seconds:
            if logger:
                logger.warning(f"Condition not met after {max_wait_seconds}s")
            return False

        if condition_fn():
            if logger:
                logger.info(f"Condition met after {elapsed:.1f}s")
            return True

        if logger:
            logger.debug(f"Waiting... ({elapsed:.1f}s / {max_wait_seconds}s)")

        time.sleep(poll_interval)


if __name__ == "__main__":
    logger = setup_logger("utils_test", log_file="logs/utils_test.log")
    logger.info("Logging setup successful")
