"""Async I/O layer — the hand-written source that ``_sync`` is generated from.

Keep everything here as thin as possible: validation, planning and result
assembly belong in ``firedantic_extras.common`` so that only the Firestore
calls themselves differ between the two flavours.  Run ``unasync.py`` (or the
pre-commit hook) after editing anything in this package.
"""
