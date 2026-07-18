# Nexus libsql patch

This is libsql 0.6.0 with one local teardown correction. `LibsqlConnection::drop` calls
`Connection::disconnect`, after which Rust drops the wrapped `Connection` and calls `disconnect`
again. The unmodified method leaves the closed pointer intact, so the second call passes freed
memory to `sqlite3_close_v2`.

Nexus makes `disconnect` idempotent by ignoring an already-null handle and nulling the handle after
the owning clone closes it. This preserves libsql's clone ownership behavior and prevents the
Windows access violation reproduced by the materializer lifecycle stress gate.
