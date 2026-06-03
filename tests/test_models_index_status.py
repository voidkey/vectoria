from db.models import Document


def test_document_has_index_status_default():
    col = Document.__table__.c.index_status
    assert col.nullable is False
    assert col.default.arg == "pending"
    assert col.server_default.arg == "pending"
