"""Knowledge-base ownership.

A KB created by an end user (JWT) records that user as ``owner_uid``. Every
``/knowledgebases/{kb_id}/...`` route then requires the caller to own the KB:

* JWT caller — must be the owner.
* Service key acting on behalf of a user (``X-Vectoria-On-Behalf-Of``) —
  same rule for that user. Services use this to validate a KB id a user
  hands them; after that they read it as themselves.
* Service key without a user — trusted with every KB (service-created KBs,
  operators).

A KB owned by someone else answers exactly like a missing one (404), so ids
can't be probed. KBs created before ownership existed have no owner; while
``kb_legacy_unowned_access`` is on, scoped callers can still reach them and
each access is logged as backfill evidence.
"""
import logging

from fastapi import Depends, Request
from sqlalchemy import select

from api.auth import Principal, get_principal, verify_auth
from api.errors import AppError, ErrorCode
from config import get_settings
from db.base import get_session
from db.models import KnowledgeBase

logger = logging.getLogger(__name__)


def kb_not_found() -> AppError:
    return AppError(404, ErrorCode.NOT_FOUND, "KnowledgeBase not found")


def owner_filter(principal: Principal):
    """WHERE clause limiting a KB listing to what ``principal`` may see, or
    None for an unscoped (trusted service / dev) caller."""
    if not principal.is_scoped:
        return None
    return KnowledgeBase.owner_uid == principal.uid


async def authorize_kb(request: Request, _claims=Depends(verify_auth)) -> None:
    """Router dependency: enforce ownership of the ``{kb_id}`` in the path.

    A no-op on routes without one (create / list), and for unscoped callers.
    A KB that doesn't exist is left to the route's own 404."""
    kb_id = request.path_params.get("kb_id")
    principal = get_principal(request)
    if kb_id is None or not principal.is_scoped:
        return
    async with get_session() as session:
        row = (await session.execute(
            select(KnowledgeBase.id, KnowledgeBase.owner_uid)
            .where(KnowledgeBase.id == kb_id)
        )).first()
    if row is None:
        return
    owner = row.owner_uid
    if owner == principal.uid:
        return
    if owner is None:
        if get_settings().kb_legacy_unowned_access:
            logger.info("kb_legacy_access kb=%s caller=%s", kb_id, principal.label())
            return
        logger.warning("kb_access_denied kb=%s caller=%s owner=<none>", kb_id, principal.label())
        raise kb_not_found()
    logger.warning("kb_access_denied kb=%s caller=%s", kb_id, principal.label())
    raise kb_not_found()
