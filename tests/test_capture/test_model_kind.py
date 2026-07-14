def test_document_kind_column_config():
    from db.models import Document
    col = Document.__table__.c.kind
    assert col.default.arg == "document"
    assert col.server_default.arg == "document"
    assert Document.__table__.c.profile.nullable is True
