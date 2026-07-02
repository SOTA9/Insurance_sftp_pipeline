from __future__ import annotations

import hashlib
import logging
from pathlib import Path
from typing import Dict

logger = logging.getLogger(__name__)

CHUNK = 65_536  # 64 KB read chunks


def compute_sha256(file_path: Path) -> str:
    h = hashlib.sha256()
    with open(file_path, "rb") as fh:
        while chunk := fh.read(CHUNK):
            h.update(chunk)
    return h.hexdigest()


def read_sidecar_checksum(sidecar_path: Path) -> str:
    raw = sidecar_path.read_text().strip()
    return raw.split()[0].lower()


def validate_checksum(data_file: Path, sidecar_file: Path) -> bool:
    expected = read_sidecar_checksum(sidecar_file)
    actual   = compute_sha256(data_file)
    if expected != actual:
        raise ValueError(
            f"Checksum MISMATCH for {data_file.name}: "
            f"expected={expected} actual={actual}"
        )
    logger.info("Checksum OK: %s (%s...)", data_file.name, actual[:16])
    return True


def validate_directory(
    data_dir: Path,
    use_checksum: bool,
    checksum_ext: str = ".sha256",
) -> Dict[str, bool]:
    """
    Main entry point used by the DAG.

    use_checksum=True  → verify every CSV against its .sha256 sidecar.
                         Raises RuntimeError if any checksum fails.
    use_checksum=False → skip validation entirely, return empty dict.
                         Used for /reports/ directory which has no sidecars.
    """
    if not use_checksum:
        logger.info("Checksum validation skipped (use_checksum=False for this directory).")
        return {}

    sidecars = list(data_dir.glob(f"*{checksum_ext}"))
    if not sidecars:
        logger.warning("No sidecar files found in %s — skipping checksum validation.", data_dir)
        return {}

    results: Dict[str, bool] = {}
    for sidecar in sidecars:
        data_file = data_dir / sidecar.stem
        if not data_file.exists():
            logger.error("Data file missing for sidecar: %s", sidecar.name)
            results[sidecar.stem] = False
            continue
        try:
            validate_checksum(data_file, sidecar)
            results[sidecar.stem] = True
        except ValueError as exc:
            logger.error(exc)
            results[sidecar.stem] = False

    failed = [k for k, v in results.items() if not v]
    if failed:
        raise RuntimeError(f"Checksum validation FAILED for: {failed}")

    return results