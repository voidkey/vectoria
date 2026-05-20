"""Repair malformed OOXML packages so mainstream parsers can ingest them.

Problem: WPS Office (and occasionally older Word versions) writes
**dangling image relationships** when an image is removed from a
document. The rel table still references something like
``Target="../NULL"`` or points at a media file that was deleted from
the zip, but the matching ``<a:blip r:embed="...">`` survives in
``document.xml``.

Microsoft Word and WPS open these files cleanly — they silently skip
the missing image — but every Python OOXML library we use crashes on
the dangling lookup:

  * mammoth raises ``KeyError`` from ``zipfile.read("word/../NULL")``
  * markitdown wraps mammoth, so it crashes the same way
  * python-docx raises ``KeyError`` from the same zip path

The full fallback chain therefore returns empty content and the
document gets misclassified as ``empty_content`` even though all the
text is right there.

The fix mirrors what Word does when re-saving:

  1. Scan every ``*.rels`` file in the package
  2. Drop image-type ``Relationship`` entries whose target doesn't
     resolve to an actual archive member
  3. Strip the now-orphan ``<a:blip>`` / ``<v:imagedata>`` references
     from the part XMLs that pointed at them — paragraphs survive,
     only the broken image node disappears

Returns the original bytes when no repairs are needed (the cheap
common path) and fails open — any internal exception logs and
returns the input unchanged so the parser still tries its normal
path on the original bytes.

Image rels are the only kind we touch in v1: that's the one crash
pattern we have evidence for. Hyperlinks, footnotes, etc. degrade
silently in mammoth (rel just doesn't resolve, no exception). If we
ever see one crash, extend the scanner.
"""
from __future__ import annotations

import io
import logging
import posixpath
import zipfile
from dataclasses import dataclass

from lxml import etree

logger = logging.getLogger(__name__)

_NS_PKGREL = "http://schemas.openxmlformats.org/package/2006/relationships"
_NS_R      = "http://schemas.openxmlformats.org/officeDocument/2006/relationships"
_NS_A      = "http://schemas.openxmlformats.org/drawingml/2006/main"
_NS_V      = "urn:schemas-microsoft-com:vml"

_IMAGE_REL_TYPE = _NS_R + "/image"

_REL_TAG       = f"{{{_NS_PKGREL}}}Relationship"
_BLIP_TAG      = f"{{{_NS_A}}}blip"
_IMAGEDATA_TAG = f"{{{_NS_V}}}imagedata"
_R_EMBED_ATTR  = f"{{{_NS_R}}}embed"
_R_LINK_ATTR   = f"{{{_NS_R}}}link"
_R_ID_ATTR     = f"{{{_NS_R}}}id"


@dataclass(frozen=True)
class RepairAction:
    kind: str        # "dangling_image_rel" — only kind today
    rels_file: str   # e.g. "word/_rels/document.xml.rels"
    rel_id: str      # e.g. "rId4"
    target: str      # raw target string, kept for forensics in logs


def sanitize_ooxml_package(raw: bytes) -> tuple[bytes, list[RepairAction]]:
    """Return ``(patched_bytes, actions)``.

    ``actions`` is empty when no repair was needed; the bytes returned
    in that case are the input bytes object unchanged. On any internal
    error we log and return ``(raw, [])`` — we never substitute a
    worse failure than the one the parser would have hit on its own.
    """
    try:
        return _sanitize(raw)
    except Exception:
        logger.exception("docx_repair: sanitize failed; returning original bytes")
        return raw, []


def _sanitize(raw: bytes) -> tuple[bytes, list[RepairAction]]:
    bio = io.BytesIO(raw)
    if not zipfile.is_zipfile(bio):
        return raw, []
    bio.seek(0)

    with zipfile.ZipFile(bio) as zin:
        names = set(zin.namelist())

        # Pass 1: find dangling image rels per rels file.
        dangling_by_rels: dict[str, dict[str, str]] = {}
        for name in names:
            if not name.endswith(".rels"):
                continue
            found = _find_dangling_image_rels(zin, name, names)
            if found:
                dangling_by_rels[name] = found

        if not dangling_by_rels:
            return raw, []

        actions = [
            RepairAction("dangling_image_rel", rels_path, rid, target)
            for rels_path, rels in dangling_by_rels.items()
            for rid, target in rels.items()
        ]

        # Pass 2: figure out which part XMLs need <a:blip>/<v:imagedata>
        # references stripped. The mapping is structural — a rels file
        # at ``word/_rels/document.xml.rels`` describes the part at
        # ``word/document.xml``; same shape for header/footer/comments.
        dropped_rids_by_part: dict[str, set[str]] = {}
        for rels_path, rels in dangling_by_rels.items():
            part_path = _rels_path_to_part_path(rels_path)
            if part_path is None or part_path not in names:
                continue
            dropped_rids_by_part.setdefault(part_path, set()).update(rels.keys())

        # Pass 3: write the patched zip out.
        bio_out = io.BytesIO()
        with zipfile.ZipFile(bio_out, "w", zipfile.ZIP_DEFLATED) as zout:
            for item in zin.infolist():
                data = zin.read(item.filename)
                if item.filename in dangling_by_rels:
                    data = _strip_rels(
                        data, set(dangling_by_rels[item.filename].keys()),
                    )
                if item.filename in dropped_rids_by_part:
                    data = _strip_blip_refs(
                        data, dropped_rids_by_part[item.filename],
                    )
                zout.writestr(item, data)

        return bio_out.getvalue(), actions


def _find_dangling_image_rels(
    zin: zipfile.ZipFile, rels_path: str, names: set[str],
) -> dict[str, str]:
    """Return ``{rel_id: target_str}`` for image rels whose target
    isn't an archive member. External-target rels are skipped — they
    legitimately point outside the package and aren't expected in the
    zip."""
    try:
        rels_xml = zin.read(rels_path)
    except KeyError:
        return {}
    try:
        root = etree.fromstring(rels_xml)
    except etree.XMLSyntaxError:
        return {}

    # rels Targets are relative to the directory holding the resource
    # the rels file describes: ``word/_rels/document.xml.rels`` resolves
    # Targets against ``word/`` (the directory of document.xml).
    resource_dir = ""
    if "/" in rels_path:
        resource_dir = posixpath.dirname(posixpath.dirname(rels_path))

    dangling: dict[str, str] = {}
    for rel in root.findall(_REL_TAG):
        if rel.get("Type") != _IMAGE_REL_TYPE:
            continue
        if rel.get("TargetMode") == "External":
            continue
        target = rel.get("Target", "")
        if not target:
            continue
        if resource_dir:
            resolved = posixpath.normpath(posixpath.join(resource_dir, target))
        else:
            resolved = posixpath.normpath(target)
        # Accept either the resolved-relative path or the raw target —
        # different OOXML producers normalize differently.
        if resolved in names or target in names:
            continue
        rid = rel.get("Id")
        if rid:
            dangling[rid] = target
    return dangling


def _rels_path_to_part_path(rels_path: str) -> str | None:
    """``word/_rels/document.xml.rels`` → ``word/document.xml``.

    Package-level rels (``_rels/.rels``) describe the package itself,
    not a part with image refs — return None to skip the blip scrub
    in that case."""
    head, tail = posixpath.split(rels_path)
    if not tail.endswith(".rels"):
        return None
    if posixpath.basename(head) != "_rels":
        return None
    part_name = tail[: -len(".rels")]
    if not part_name:
        return None
    part_dir = posixpath.dirname(head)
    return posixpath.join(part_dir, part_name) if part_dir else part_name


def _strip_rels(data: bytes, drop_ids: set[str]) -> bytes:
    try:
        root = etree.fromstring(data)
    except etree.XMLSyntaxError:
        return data
    for rel in list(root):
        if rel.tag == _REL_TAG and rel.get("Id") in drop_ids:
            root.remove(rel)
    return etree.tostring(
        root, xml_declaration=True, encoding="UTF-8", standalone=True,
    )


def _strip_blip_refs(data: bytes, drop_rids: set[str]) -> bytes:
    """Remove DrawingML ``<a:blip>`` and VML ``<v:imagedata>`` nodes
    whose r:embed / r:link / r:id is in ``drop_rids``. The surrounding
    ``<w:drawing>`` / ``<w:pict>`` survives — it just renders empty,
    which mammoth accepts. Paragraph text is never touched."""
    try:
        root = etree.fromstring(data)
    except etree.XMLSyntaxError:
        return data

    to_remove: list[etree._Element] = []
    for blip in root.iter(_BLIP_TAG):
        if blip.get(_R_EMBED_ATTR) in drop_rids or blip.get(_R_LINK_ATTR) in drop_rids:
            to_remove.append(blip)
    for img in root.iter(_IMAGEDATA_TAG):
        if img.get(_R_ID_ATTR) in drop_rids:
            to_remove.append(img)

    if not to_remove:
        return data
    for el in to_remove:
        parent = el.getparent()
        if parent is not None:
            parent.remove(el)
    return etree.tostring(
        root, xml_declaration=True, encoding="UTF-8", standalone=True,
    )
