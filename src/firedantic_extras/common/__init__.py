"""Pure, I/O-free logic shared by the async and sync flavours of the library.

Nothing in this package touches Firestore.  The I/O layers live in
``firedantic_extras._async`` (hand-written) and ``firedantic_extras._sync``
(generated from ``_async`` by ``unasync.py``); both import from here.

Import the public API from the top-level modules (``firedantic_extras.query``,
``firedantic_extras.cursor_pagination``, ...) rather than from this package.
"""
