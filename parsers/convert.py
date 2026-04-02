"""LibreOffice-based legacy Office format conversion (.doc → .docx, .ppt → .pptx)."""

import logging
import shutil
import subprocess
from pathlib import Path

logger = logging.getLogger(__name__)

LEGACY_FORMAT_MAP = {".doc": ".docx", ".ppt": ".pptx"}


def libreoffice_available() -> bool:
    return shutil.which("libreoffice") is not None


def convert_legacy_format(src_path: str, suffix: str) -> str:
    """Convert .doc/.ppt to .docx/.pptx via LibreOffice.

    Returns the path to the converted file.
    Raises RuntimeError if conversion fails.
    """
    target_suffix = LEGACY_FORMAT_MAP[suffix]
    target_fmt = target_suffix.lstrip(".")
    out_dir = Path(src_path).parent

    result = subprocess.run(
        ["libreoffice", "--headless", "--norestore", "--convert-to",
         target_fmt, "--outdir", str(out_dir), src_path],
        check=True, timeout=120, capture_output=True,
    )

    expected = Path(src_path).with_suffix(target_suffix)
    if not expected.exists():
        raise RuntimeError(
            f"LibreOffice conversion produced no output: {expected}\n"
            f"stderr: {result.stderr.decode(errors='replace')}"
        )

    return str(expected)
