from __future__ import annotations

import asyncio
import csv
import io
import json
import logging
import os
import queue as queue_module
import tempfile
import threading
from typing import Any, AsyncIterator, Dict, List, Optional, Tuple

from shared.models.objectify_job import ObjectifyJob
from shared.services.pipeline.objectify_delta_utils import create_delta_computer_for_mapping_spec
from shared.utils.s3_uri import parse_s3_uri

logger = logging.getLogger(__name__)


async def iter_dataset_batches(
    worker: Any,
    *,
    job: ObjectifyJob,
    options: Dict[str, Any],
    row_batch_size: int,
    max_rows: Optional[int],
):
    if not worker.storage:
        raise RuntimeError("Storage service not initialized")
    parsed = parse_s3_uri(job.artifact_key)
    if not parsed:
        raise ValueError(f"Invalid artifact_key: {job.artifact_key}")
    bucket, key = parsed
    has_header = options.get("source_has_header", True)
    delimiter = options.get("delimiter") or options.get("csv_delimiter") or ","

    if key.endswith(".csv") or key.endswith(".tsv") or key.endswith(".txt"):
        async for columns, rows, row_offset in iter_csv_batches(
            worker,
            bucket=bucket,
            key=key,
            delimiter=delimiter,
            has_header=bool(has_header),
            row_batch_size=row_batch_size,
            max_rows=max_rows,
        ):
            yield columns, rows, row_offset
        return

    if key.endswith(".xlsx") or key.endswith(".xlsm") or key.endswith(".xls"):
        raise RuntimeError("Excel artifacts are not supported for objectify; convert via pipeline first.")

    parquet_keys: List[str] = []
    try:
        if key.endswith(".parquet"):
            parquet_keys = [key]
        else:
            async for obj in worker.storage.iter_objects(bucket=bucket, prefix=key, max_keys=worker.list_page_size):
                obj_key = obj.get("Key")
                if not obj_key or obj_key.endswith("/"):
                    continue
                if not obj_key.startswith(key):
                    continue
                if str(obj_key).endswith(".parquet"):
                    parquet_keys.append(str(obj_key))
    except Exception as exc:
        logger.warning(
            "Failed to detect parquet artifacts for objectify sidecar output (%s/%s): %s",
            bucket,
            key,
            exc,
            exc_info=True,
        )
        parquet_keys = []

    if parquet_keys:
        async for columns, rows, row_offset in iter_parquet_object_batches(
            worker,
            bucket=bucket,
            parquet_keys=parquet_keys,
            row_batch_size=row_batch_size,
            max_rows=max_rows,
        ):
            yield columns, rows, row_offset
        return

    async for columns, rows, row_offset in iter_json_part_batches(
        worker,
        bucket=bucket,
        prefix=key,
        row_batch_size=row_batch_size,
        max_rows=max_rows,
    ):
        yield columns, rows, row_offset


async def download_object_to_file(
    worker: Any,
    *,
    bucket: str,
    key: str,
    dest_path: str,
) -> None:
    if not worker.storage:
        raise RuntimeError("Storage service not initialized")

    def _download() -> None:
        body = None
        try:
            resp = worker.storage.client.get_object(Bucket=bucket, Key=key)
            body = resp.get("Body")
            if body is None:
                raise RuntimeError(f"Storage returned empty body for {bucket}/{key}")

            with open(dest_path, "wb") as out:
                for chunk in iter(lambda: body.read(1024 * 1024), b""):
                    out.write(chunk)
        finally:
            try:
                if body:
                    body.close()
            except Exception as exc:
                logger.warning(
                    "Failed to close storage stream for parquet download %s/%s: %s",
                    bucket,
                    key,
                    exc,
                    exc_info=True,
                )

    await asyncio.to_thread(_download)


async def iter_parquet_object_batches(
    worker: Any,
    *,
    bucket: str,
    parquet_keys: List[str],
    row_batch_size: int,
    max_rows: Optional[int],
):
    if not worker.storage:
        raise RuntimeError("Storage service not initialized")
    try:
        import duckdb  # type: ignore
    except ImportError as exc:
        raise RuntimeError("DuckDB is required to objectify Parquet artifacts (pip install duckdb)") from exc

    resolved_keys = sorted({str(k) for k in parquet_keys if str(k).endswith(".parquet")})
    if not resolved_keys:
        return

    with tempfile.TemporaryDirectory(prefix="spice-objectify-parquet-") as tmpdir:
        for idx, obj_key in enumerate(resolved_keys):
            base = os.path.basename(obj_key) or f"part-{idx}.parquet"
            base = base.replace(os.sep, "_").replace("..", "_")
            local_path = os.path.join(tmpdir, f"{idx:05d}_{base}")
            await download_object_to_file(worker, bucket=bucket, key=obj_key, dest_path=local_path)

        q: queue_module.Queue = queue_module.Queue(maxsize=2)
        stop_flag = threading.Event()

        def _safe_put(item: Tuple[str, Any, Any, Any]) -> None:
            while not stop_flag.is_set():
                try:
                    q.put(item, timeout=0.5)
                    return
                except queue_module.Full:
                    continue

        def _reader() -> None:
            conn = None
            try:
                conn = duckdb.connect(":memory:")
                pattern = os.path.join(tmpdir, "*.parquet")
                cursor = conn.execute("SELECT * FROM read_parquet(?)", [pattern])
                columns = [desc[0] for desc in cursor.description] if cursor.description else []

                row_offset = 0
                seen_rows = 0
                while not stop_flag.is_set():
                    batch = cursor.fetchmany(row_batch_size)
                    if not batch:
                        break
                    rows = [list(row) for row in batch]
                    if max_rows is not None and (seen_rows + len(rows)) > max_rows:
                        rows = rows[: max_rows - seen_rows]
                    if rows:
                        _safe_put(("batch", columns, rows, row_offset))
                        row_offset += len(rows)
                        seen_rows += len(rows)
                    if max_rows is not None and seen_rows >= max_rows:
                        break

                _safe_put(("done", columns, [], row_offset))
            except Exception as exc:
                logger.error("Objectify parquet reader failed: %s", exc, exc_info=True)
                _safe_put(("error", exc, None, None))
            finally:
                try:
                    if conn:
                        conn.close()
                except Exception as exc:
                    logger.warning("Failed to close DuckDB connection: %s", exc, exc_info=True)

        thread = threading.Thread(target=_reader, daemon=True)
        thread.start()

        try:
            while True:
                kind, columns, rows, row_offset = await asyncio.to_thread(q.get)
                if kind == "error":
                    raise columns
                if kind == "done":
                    break
                if kind == "batch":
                    yield columns, rows, row_offset
        finally:
            stop_flag.set()


async def iter_csv_batches(
    worker: Any,
    *,
    bucket: str,
    key: str,
    delimiter: str,
    has_header: bool,
    row_batch_size: int,
    max_rows: Optional[int],
):
    if not worker.storage:
        raise RuntimeError("Storage service not initialized")

    q: queue_module.Queue = queue_module.Queue(maxsize=2)
    stop_flag = threading.Event()

    def _safe_put(item: Tuple[str, Any, Any, Any]) -> None:
        while not stop_flag.is_set():
            try:
                q.put(item, timeout=0.5)
                return
            except queue_module.Full:
                continue

    def _reader() -> None:
        body = None
        try:
            resp = worker.storage.client.get_object(Bucket=bucket, Key=key)
            body = resp.get("Body")
            text_stream = io.TextIOWrapper(body, encoding="utf-8", errors="replace")
            reader = csv.reader(text_stream, delimiter=delimiter)

            columns: List[str] = []
            rows: List[List[Any]] = []
            row_offset = 0
            seen_rows = 0

            if has_header:
                header = next(reader, None)
                if header:
                    columns = [str(c).strip() for c in header]
            else:
                first = next(reader, None)
                if first is None:
                    _safe_put(("done", columns, [], row_offset))
                    return
                columns = [f"col_{i}" for i in range(len(first))]
                rows.append(first)
                seen_rows += 1

            for row in reader:
                if stop_flag.is_set():
                    break
                if max_rows is not None and seen_rows >= max_rows:
                    break
                if not columns:
                    columns = [f"col_{i}" for i in range(len(row))]
                rows.append(row)
                seen_rows += 1
                if len(rows) >= row_batch_size:
                    _safe_put(("batch", columns, rows, row_offset))
                    row_offset += len(rows)
                    rows = []

            if rows and not stop_flag.is_set():
                _safe_put(("batch", columns, rows, row_offset))
                row_offset += len(rows)
            _safe_put(("done", columns, [], row_offset))
        except Exception as exc:
            logger.warning("Objectify sidecar stream reader failed: %s", exc, exc_info=True)
            _safe_put(("error", exc, None, None))
        finally:
            try:
                if body:
                    body.close()
            except Exception as exc:
                logger.warning("Failed to close objectify sidecar stream body: %s", exc, exc_info=True)

    thread = threading.Thread(target=_reader, daemon=True)
    thread.start()

    try:
        while True:
            kind, columns, rows, row_offset = await asyncio.to_thread(q.get)
            if kind == "error":
                raise columns
            if kind == "done":
                break
            if kind == "batch":
                yield columns, rows, row_offset
    finally:
        stop_flag.set()


async def iter_json_part_batches(
    worker: Any,
    *,
    bucket: str,
    prefix: str,
    row_batch_size: int,
    max_rows: Optional[int],
):
    if not worker.storage:
        raise RuntimeError("Storage service not initialized")
    rows: List[List[Any]] = []
    columns: List[str] = []
    row_offset = 0
    total_rows = 0

    async for obj in worker.storage.iter_objects(bucket=bucket, prefix=prefix, max_keys=worker.list_page_size):
        obj_key = obj.get("Key")
        if not obj_key or obj_key.endswith("/"):
            continue
        if not obj_key.startswith(prefix):
            continue
        raw = await worker.storage.load_bytes(bucket, obj_key)
        for line in raw.splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                payload = json.loads(line)
            except (json.JSONDecodeError, UnicodeDecodeError) as exc:
                logger.warning("Invalid JSON line from objectify sidecar output: %s", exc, exc_info=True)
                continue
            if isinstance(payload, dict):
                if not columns:
                    columns = list(payload.keys())
                row = [payload.get(col) for col in columns]
            elif isinstance(payload, list):
                if not columns:
                    columns = [f"col_{i}" for i in range(len(payload))]
                row = payload
            else:
                continue

            rows.append(row)
            total_rows += 1
            if max_rows is not None and total_rows >= max_rows:
                break
            if len(rows) >= row_batch_size:
                yield columns, rows, row_offset
                row_offset += len(rows)
                rows = []

        if max_rows is not None and total_rows >= max_rows:
            break

    if rows:
        yield columns, rows, row_offset


async def iter_dataset_batches_incremental(
    worker: Any,
    *,
    job: ObjectifyJob,
    options: Dict[str, Any],
    row_batch_size: int,
    max_rows: Optional[int],
    mapping_spec: Any,
) -> AsyncIterator[Tuple[List[str], List[List[Any]], int, Optional[str]]]:
    execution_mode = str(options.get("execution_mode") or job.execution_mode or "full").strip().lower() or "full"
    if execution_mode not in {"full", "incremental", "delta"}:
        execution_mode = "full"
    watermark_column = job.watermark_column or options.get("watermark_column")
    previous_watermark = job.previous_watermark

    if execution_mode == "full" or not watermark_column:
        async for columns, rows, row_offset in iter_dataset_batches(
            worker,
            job=job,
            options=options,
            row_batch_size=row_batch_size,
            max_rows=max_rows,
        ):
            yield columns, rows, row_offset, None
        return

    delta_computer = create_delta_computer_for_mapping_spec(
        vars(mapping_spec) if hasattr(mapping_spec, "__dict__") else dict(mapping_spec or {})
    )

    max_watermark: Optional[Any] = previous_watermark
    total_yielded = 0

    async for columns, rows, row_offset in iter_dataset_batches(
        worker,
        job=job,
        options=options,
        row_batch_size=row_batch_size,
        max_rows=None,
    ):
        if not rows:
            continue

        col_map = {col: idx for idx, col in enumerate(columns)}
        wm_col_idx = col_map.get(watermark_column)

        if wm_col_idx is None:
            logger.warning(
                "Watermark column %s not found in dataset columns; falling back to full mode",
                watermark_column,
            )
            yield columns, rows, row_offset, None
            total_yielded += len(rows)
            if max_rows and total_yielded >= max_rows:
                break
            continue

        filtered_rows: List[List[Any]] = []
        for row in rows:
            row_wm = row[wm_col_idx] if wm_col_idx < len(row) else None
            if row_wm is None:
                continue
            if previous_watermark is not None:
                cmp = delta_computer._compare_watermarks(row_wm, previous_watermark)
                if cmp <= 0:
                    continue
            filtered_rows.append(row)
            if max_watermark is None:
                max_watermark = row_wm
            elif delta_computer._compare_watermarks(row_wm, max_watermark) > 0:
                max_watermark = row_wm

        if filtered_rows:
            if max_rows and total_yielded + len(filtered_rows) > max_rows:
                remaining = max_rows - total_yielded
                filtered_rows = filtered_rows[:remaining]

            yield columns, filtered_rows, row_offset, str(max_watermark) if max_watermark else None
            total_yielded += len(filtered_rows)

            if max_rows and total_yielded >= max_rows:
                break

    if total_yielded == 0 and max_watermark and max_watermark != previous_watermark:
        logger.info(
            "Incremental objectify: no new rows found (max_watermark=%s, previous=%s)",
            max_watermark, previous_watermark,
        )


async def update_watermark_after_job(
    worker: Any,
    *,
    job: ObjectifyJob,
    new_watermark: Optional[str],
) -> None:
    if not worker.objectify_registry or not new_watermark:
        return

    watermark_column = job.watermark_column or job.options.get("watermark_column")
    if not watermark_column:
        return

    try:
        lakefs_ref = None
        if job.artifact_key:
            parts = job.artifact_key.replace("s3://", "").split("/", 2)
            if len(parts) >= 2:
                lakefs_ref = parts[1]
        await worker.objectify_registry.update_watermark(
            mapping_spec_id=job.mapping_spec_id,
            dataset_branch=job.dataset_branch,
            watermark_column=watermark_column,
            watermark_value=new_watermark,
            dataset_version_id=job.dataset_version_id,
            lakefs_commit_id=lakefs_ref,
        )
        logger.info(
            "Updated watermark for mapping_spec %s: %s=%s",
            job.mapping_spec_id,
            watermark_column,
            new_watermark,
        )
    except Exception as exc:
        logger.error("Failed to update watermark: %s", exc)
