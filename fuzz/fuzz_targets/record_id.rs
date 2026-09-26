//! A rendered record id must name exactly the record it was built from.
//!
//! The engine's parser reads the rendered text back: the table and key must
//! match, the key must stay a string key, and splicing the id into a
//! statement must still be one statement. `RecordID::parse` must also invert
//! `Display`.

#![no_main]

use libfuzzer_sys::fuzz_target;
use surql::types::RecordID;
use surrealdb_core::syn;
use surrealdb_types::RecordIdKey;

fuzz_target!(|input: (String, String)| {
    let (table, key) = input;
    let Ok(rid) = RecordID::<()>::new(table.as_str(), key.as_str()) else {
        return;
    };
    let text = rid.to_string();

    let parsed = syn::record_id(&text).unwrap_or_else(|e| panic!("engine rejected {text:?}: {e}"));
    assert_eq!(parsed.table.as_str(), table, "table of {text:?}");
    match &parsed.key {
        RecordIdKey::String(k) => assert_eq!(k, &key, "key of {text:?}"),
        other => panic!("{text:?} names a non-string key {other:?}"),
    }

    let statement = format!("SELECT * FROM {text};");
    let ast =
        syn::parse(&statement).unwrap_or_else(|e| panic!("engine rejected {statement:?}: {e}"));
    assert_eq!(ast.num_statements(), 1, "{statement:?}");

    let back = RecordID::<()>::parse(&text)
        .unwrap_or_else(|e| panic!("RecordID::parse rejected {text:?}: {e}"));
    assert_eq!(back, rid, "{text:?}");
});
