"""Parsers package.

W6-6 removed the ``BaseImageExtractor`` override seam (W4-b) along
with its single implementation ``PptxImageExtractor``. That seam
existed because docling — the former .pptx parser — dropped
speaker-notes images; a post-parse plugin filled the gap. With
``PptxParser`` producing text + body images + notes images in a
single slide walk, the indirection had zero consumers and was pure
overhead.
"""
