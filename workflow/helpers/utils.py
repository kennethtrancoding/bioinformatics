"""
Cross-cutting helpers for the bioinformatics workflow: logging, retries,
checksums, and streaming archives.

Everything here is used by more than one caller. Single-use helpers belong with
their caller, and anything that only reads or writes job state belongs in
job_store.py, whose writes are atomic.
"""

import functools
import hashlib
import logging
import re
import sys
import time
import zipfile
from pathlib import Path
from typing import Callable, Optional


# Logging Setup

# A BV-BRC remote path embeds the account name and the app directory it writes
# into: /kenneth@bvbrc/home/bioinformatics_analysis/reads/X.fastq.gz. Job logs
# are downloadable from the results page, so neither belongs in them — keep the
# part below the app directory, which is what's actually useful when reading a
# log, and drop the prefix.
_REMOTE_WORKSPACE_PATH = re.compile(r"/[^/\s'\"]+@[^/\s'\"]+/home/[^/\s'\"]+")


class _RedactWorkspacePaths(logging.Filter):
    """Strip the account/app-directory prefix out of every record, including the
    server-supplied text in RPC errors and tracebacks. This sits on the logger
    rather than at the call sites so a new log line can't reintroduce the leak."""

    def filter(self, record: logging.LogRecord) -> bool:
        if isinstance(record.msg, str):
            record.msg = _REMOTE_WORKSPACE_PATH.sub("<workspace>", record.msg)
        if record.args:
            record.args = tuple(
                _REMOTE_WORKSPACE_PATH.sub("<workspace>", arg) if isinstance(arg, str) else arg
                for arg in record.args
            )
        return True


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

    if not any(isinstance(existing, _RedactWorkspacePaths) for existing in logger.filters):
        logger.addFilter(_RedactWorkspacePaths())

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


# Archiving

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

    Buffering a finished archive in memory is survivable for one isolate and fatal
    for a whole job, where the archive is every isolate at once -- large enough to
    get the (single) web worker OOM-killed, which takes the site down and fails
    whatever runs were in flight. Here memory stays bounded by chunk_size no matter
    how large the job is. This is the only archiver: the download routes stream it
    straight to the client, and s3_storage spools it to a temp file.

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


