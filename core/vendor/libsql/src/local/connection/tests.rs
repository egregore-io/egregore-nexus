use super::*;

unsafe extern "C" fn deny_rollback(
    _: *mut std::ffi::c_void,
    action: c_int,
    arg: *const std::ffi::c_char,
    _: *const std::ffi::c_char,
    _: *const std::ffi::c_char,
    _: *const std::ffi::c_char,
) -> c_int {
    if action == ffi::SQLITE_TRANSACTION
        && !arg.is_null()
        && unsafe { std::ffi::CStr::from_ptr(arg) }.to_bytes() == b"ROLLBACK"
    {
        ffi::SQLITE_DENY
    } else {
        ffi::SQLITE_OK
    }
}

#[test]
fn failed_owned_rollback_quarantines_clones_prepared_statements_and_rows() {
    let db = Database::open(":memory:", crate::OpenFlags::default()).unwrap();
    let conn = db.connect().unwrap();
    conn.execute_batch("CREATE TABLE probe (id INTEGER PRIMARY KEY); INSERT INTO probe VALUES (1)")
        .unwrap();
    let clone = conn.clone();
    let prepared = clone.prepare("INSERT INTO probe VALUES (2)").unwrap();
    let query = clone
        .prepare("SELECT 'held' UNION ALL SELECT 'next'")
        .unwrap();
    let rows = query.query(&Params::None).unwrap();
    let held_row = rows.next().unwrap().unwrap();
    assert_eq!(held_row.get::<String>(0).unwrap(), "held");
    assert!(matches!(
        held_row.get_ref(0).unwrap(),
        crate::ValueRef::Text(b"held")
    ));
    let cached_query = clone.prepare("SELECT 'cached'").unwrap();
    let cached_rows = cached_query.query(&Params::None).unwrap();
    // Test-only native fault injection: deny precisely ROLLBACK preparation,
    // leaving the failed owned batch genuinely open rather than faking a flag.
    assert_eq!(
        unsafe { ffi::sqlite3_set_authorizer(conn.raw, Some(deny_rollback), std::ptr::null_mut()) },
        ffi::SQLITE_OK
    );
    let error = conn
        .execute_transactional_batch("INSERT INTO probe VALUES (1)")
        .unwrap_err();
    let diagnostic = error.to_string();
    assert!(
        diagnostic.contains("UNIQUE constraint failed"),
        "{diagnostic}"
    );
    assert!(diagnostic.contains("rollback failed"), "{diagnostic}");
    assert!(!conn.is_autocommit());
    let cached_access = [
        ("unconsumed initial Rows", cached_rows.next().err()),
        ("held Row.get", held_row.get::<String>(0).err()),
        ("held Row.get_value", held_row.get_value(0).err()),
        ("held Row.get_ref", held_row.get_ref(0).err()),
        ("held Row.column_type", held_row.column_type(0).err()),
        ("cached Rows.column_type", cached_rows.column_type(0).err()),
    ]
    .map(|(label, error)| (label, error.map(|error| error.to_string())));
    assert!(
        cached_access.iter().all(|(_, error)| error
            .as_ref()
            .is_some_and(|error| error.contains("quarantined"))),
        "every native row access must reject after quarantine: {cached_access:?}"
    );
    for error in [
        clone
            .execute("INSERT INTO probe VALUES (3)", Params::None)
            .unwrap_err(),
        prepared.run(&Params::None).unwrap_err(),
        rows.next()
            .err()
            .expect("existing cursor must be quarantined"),
        clone
            .prepare("SELECT 42")
            .err()
            .expect("prepare must be quarantined"),
        clone
            .execute_transactional_batch("INSERT INTO probe VALUES (4)")
            .unwrap_err(),
    ] {
        assert!(error.to_string().contains("quarantined"), "{error}");
    }
    // Removing the injection cannot unpoison old clones. Dropping statements
    // and the final connection remains legal and lets SQLite clean up safely.
    assert_eq!(
        unsafe { ffi::sqlite3_set_authorizer(conn.raw, None, std::ptr::null_mut()) },
        ffi::SQLITE_OK
    );
    assert!(clone
        .execute("ROLLBACK", Params::None)
        .unwrap_err()
        .to_string()
        .contains("quarantined"));
}
