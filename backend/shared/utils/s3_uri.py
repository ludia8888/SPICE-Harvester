"""
Helpers for working with s3://bucket/key style URIs.
"""

from __future__ import annotations

from typing import Optional, Tuple
from urllib.parse import urlparse


def is_s3_uri(value: str) -> bool:
    return isinstance(value, str) and value.startswith("s3://")


def build_s3_uri(bucket: str, key: str) -> str:
    bucket = (bucket or "").strip()
    key = (key or "").lstrip("/")
    if not bucket:
        raise ValueError("bucket is required")
    if not key:
        raise ValueError("key is required")
    return f"s3://{bucket}/{key}"


def parse_s3_uri(uri: str) -> Optional[Tuple[str, str]]:
    if not is_s3_uri(uri):
        return None
    parsed = urlparse(uri)
    bucket = parsed.netloc
    key = parsed.path.lstrip("/")
    if not bucket or not key:
        return None
    return bucket, key


def normalize_s3_uri(value: str, *, bucket: Optional[str] = None) -> Optional[str]:
    if is_s3_uri(value):
        return value
    if bucket and value:
        return build_s3_uri(bucket, value)
    return None
