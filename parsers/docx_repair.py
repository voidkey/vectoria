"""Repair malformed OOXML packages so mainstream parsers can ingest them.

Problem: WPS Office (and occasionally older Word versions) writes
**broken image relationships** that crash every Python OOXML library
we use while Word and WPS themselves open the file cleanly. Two
distinct patterns we've hit in production:

  * **dangling_image_rel** — rel references ``Target="../NULL"`` or a
    media file that was deleted from the zip. mammoth (and python-docx,
    and markitdown which wraps mammoth) all raise ``KeyError`` doing
    the zip lookup. Word/WPS silently skip the missing image.

  * **bad_crc_image** — rel points at a media file that IS in the zip
    but whose CRC-32 in the local file header doesn't match the actual
    bytes. Word/WPS skip the CRC check; Python's stdlib ``zipfile``
    validates on read so mammoth crashes mid-image-extraction with
    ``BadZipFile: Bad CRC-32 for file 'word/media/imageN.jpeg'``.

Both produce the same downstream symptom: every engine in the docx
fallback chain returns empty content and the doc gets misclassified
as ``empty_content`` — even though all the text is right there.

The fix is the same shape for both patterns and mirrors what Word
does when re-saving the file:

  1. Scan every ``*.rels`` file in the package
  2. Drop image-type ``Relationship`` entries that are dangling or
     point to bad-CRC members
  3. Strip the now-orphan ``<a:blip>`` / ``<v:imagedata>`` references
     from the part XMLs that pointed at them — paragraphs survive,
     only the broken image node disappears
  4. When re-packing the zip, skip the bad-CRC media members entirely
     (they're now orphan binary data, and reading them would re-raise)

Returns the original bytes when no repairs are needed (the cheap
common path) and fails open — any internal exception logs and
returns the input unchanged so the parser still tries its normal
path on the original bytes.

Image rels are the only category we touch today. Other rel types
(hyperlinks, footnotes, headers) degrade silently in mammoth when
broken — no exception, no need to pre-repair. Extend the classifier
if a new crash pattern shows up.
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
    kind: str        # "dangling_image_rel" | "bad_crc_image"
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

        # Pass 1: classify each image rel — is it dangling (target not
        # in zip) or bad-CRC (target in zip but unreadable)? Also collect
        # the bad-CRC zip member paths so Pass 3 can skip them when
        # re-packing (reading them would re-raise the same BadZipFile).
        issues_by_rels: dict[str, dict[str, tuple[str, str]]] = {}
        bad_crc_members: set[str] = set()
        for name in names:
            if not name.endswith(".rels"):
                continue
            issues, bad_members = _classify_image_rels(zin, name, names)
            if issues:
                issues_by_rels[name] = issues
            bad_crc_members.update(bad_members)

        if not issues_by_rels:
            return raw, []

        actions = [
            RepairAction(kind, rels_path, rid, target)
            for rels_path, issues in issues_by_rels.items()
            for rid, (kind, target) in issues.items()
        ]

        # Pass 2: figure out which part XMLs need <a:blip>/<v:imagedata>
        # references stripped. The mapping is structural — a rels file
        # at ``word/_rels/document.xml.rels`` describes the part at
        # ``word/document.xml``; same shape for header/footer/comments.
        dropped_rids_by_part: dict[str, set[str]] = {}
        for rels_path, issues in issues_by_rels.items():
            part_path = _rels_path_to_part_path(rels_path)
            if part_path is None or part_path not in names:
                continue
            dropped_rids_by_part.setdefault(part_path, set()).update(issues.keys())

        # Pass 3: write the patched zip out. Bad-CRC media members are
        # dropped entirely (the rel that pointed at them is gone, so
        # they're orphan bytes, and trying to read them would re-raise).
        bio_out = io.BytesIO()
        with zipfile.ZipFile(bio_out, "w", zipfile.ZIP_DEFLATED) as zout:
            for item in zin.infolist():
                if item.filename in bad_crc_members:
                    continue
                data = zin.read(item.filename)
                if item.filename in issues_by_rels:
                    data = _strip_rels(
                        data, set(issues_by_rels[item.filename].keys()),
                    )
                if item.filename in dropped_rids_by_part:
                    data = _strip_blip_refs(
                        data, dropped_rids_by_part[item.filename],
                    )
                zout.writestr(item, data)

        return bio_out.getvalue(), actions


def _classify_image_rels(
    zin: zipfile.ZipFile, rels_path: str, names: set[str],
) -> tuple[dict[str, tuple[str, str]], set[str]]:
    """Inspect every image relationship in ``rels_path`` and return:

      * ``issues``: ``{rel_id: (kind, target)}`` for image rels that
        need repair. ``kind`` is one of:

          - ``"dangling_image_rel"`` — Target doesn't resolve to any
            archive member (WPS ``"../NULL"`` pattern).
          - ``"bad_crc_image"`` — Target IS in the zip but raises
            ``BadZipFile`` when read (WPS writes media with bad CRC).

      * ``bad_crc_members``: zip member paths for the ``bad_crc_image``
        entries, so the caller can skip them when re-packing.

    Healthy image rels and external-target rels return nothing — they
    pass through untouched. Read-errors that aren't ``BadZipFile`` are
    deliberately not caught here: surfacing them at parse time keeps
    the sanitizer narrow ("only repair what we know how to repair").
    """
    issues: dict[str, tuple[str, str]] = {}
    bad_crc_members: set[str] = set()

    try:
        rels_xml = zin.read(rels_path)
    except KeyError:
        return issues, bad_crc_members
    try:
        root = etree.fromstring(rels_xml)
    except etree.XMLSyntaxError:
        return issues, bad_crc_members

    # rels Targets are relative to the directory holding the resource
    # the rels file describes: ``word/_rels/document.xml.rels`` resolves
    # Targets against ``word/`` (the directory of document.xml).
    resource_dir = ""
    if "/" in rels_path:
        resource_dir = posixpath.dirname(posixpath.dirname(rels_path))

    for rel in root.findall(_REL_TAG):
        if rel.get("Type") != _IMAGE_REL_TYPE:
            continue
        if rel.get("TargetMode") == "External":
            continue
        target = rel.get("Target", "")
        if not target:
            continue
        rid = rel.get("Id")
        if not rid:
            continue

        # Resolve relative target → absolute path in the zip.
        if resource_dir:
            resolved = posixpath.normpath(posixpath.join(resource_dir, target))
        else:
            resolved = posixpath.normpath(target)
        # Accept either form — different OOXML producers normalize
        # differently and we want to match the same member mammoth would.
        member = (
            resolved if resolved in names
            else (target if target in names else None)
        )
        if member is None:
            issues[rid] = ("dangling_image_rel", target)
            continue

        try:
            zin.read(member)
        except zipfile.BadZipFile:
            issues[rid] = ("bad_crc_image", target)
            bad_crc_members.add(member)

    return issues, bad_crc_members


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
