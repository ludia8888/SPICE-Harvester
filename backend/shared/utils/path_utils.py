from __future__ import annotations


def safe_lakefs_ref(value: str) -> str:
    raw = str(value or "").strip()
    if not raw:
        return "main"
    allowed: list[str] = []
    for ch in raw:
        # lakeFS S3-gateway addressing uses `s3://<repo>/<ref>/<path>`; refs must be a single path segment.
        if ch.isalnum() or ch in {"-", "_", "."}:
            allowed.append(ch)
        else:
            allowed.append("-")
    cleaned = "".join(allowed).strip("-")
    while "--" in cleaned:
        cleaned = cleaned.replace("--", "-")
    return cleaned or "main"


def safe_path_segment(value: str) -> str:
    raw = str(value or "").strip()
    if not raw:
        return "untitled"
    allowed: list[str] = []
    for ch in raw:
        if ch.isalnum() or ch in {"-", "_", "."}:
            allowed.append(ch)
        elif ch.isspace():
            allowed.append("_")
        else:
            allowed.append("_")
    cleaned = "".join(allowed)
    while "__" in cleaned:
        cleaned = cleaned.replace("__", "_")
    return cleaned.strip("_") or "untitled"
