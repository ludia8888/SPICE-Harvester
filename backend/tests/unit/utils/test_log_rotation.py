from __future__ import annotations

import os
from pathlib import Path

from shared.utils.log_rotation import LogRotationManager


def _touch_file(path: Path, *, size: int = 0) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "wb") as handle:
        if size:
            handle.write(b"a" * size)
    os.utime(path, None)


def test_rotate_and_limit_logs(tmp_path: Path) -> None:
    log_dir = tmp_path / "logs"
    manager = LogRotationManager(log_dir=str(log_dir), max_size_mb=1, max_files=2)

    log_file = log_dir / "service.log"
    _touch_file(log_file, size=2 * 1024 * 1024)

    assert manager.should_rotate(log_file) is True
    assert manager.rotate_log_file(log_file, "service") is True
    assert log_file.exists()

    rotated_files = list(log_dir.glob("service_rotated_*.log"))
    assert len(rotated_files) == 1

    # Create extra rotated files to trigger limit
    for idx in range(3):
        extra = log_dir / f"service_rotated_extra_{idx}.log"
        _touch_file(extra, size=10)

    removed = manager.limit_rotated_files("service")
    assert removed >= 1
    remaining = list(log_dir.glob("service_rotated_*.log"))
    assert len(remaining) <= manager.max_files


def test_compress_and_cleanup(tmp_path: Path) -> None:
    log_dir = tmp_path / "logs"
    manager = LogRotationManager(log_dir=str(log_dir), compress_after_days=0, delete_after_days=0)

    rotated = log_dir / "service_rotated_20240101_000000.log"
    _touch_file(rotated, size=128)

    compressed_count = manager.compress_old_logs()
    assert compressed_count == 1
    assert not rotated.exists()

    gz_file = log_dir / "service_rotated_20240101_000000.log.gz"
    assert gz_file.exists()

    deleted_count, freed_space = manager.cleanup_old_logs()
    assert deleted_count == 1
    assert freed_space > 0
    assert not gz_file.exists()
