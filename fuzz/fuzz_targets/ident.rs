//! Quoted identifiers and string literals must read back as themselves.
//!
//! `quote_ident` output must parse as a table name equal to the input and
//! keep a statement a single statement; `quote_str` output must parse as the
//! same string; the crate's own unquoting must invert both.

#![no_main]

use libfuzzer_sys::fuzz_target;
use surql::types::escape::{quote_ident, quote_str, unquote_str};
use surrealdb_core::syn;
use surrealdb_types::Value;

fuzz_target!(|s: String| {
    let ident = quote_ident(&s);
    let table = syn::table(&ident).unwrap_or_else(|e| panic!("engine rejected {ident:?}: {e}"));
    assert_eq!(table.as_str(), s, "{ident:?}");
    let statement = format!("SELECT * FROM {ident};");
    let ast = syn::parse(&statement)
        .unwrap_or_else(|e| panic!("engine rejected {statement:?}: {e}"));
    assert_eq!(ast.num_statements(), 1, "{statement:?}");

    let literal = quote_str(&s);
    match syn::value(&literal) {
        Ok(Value::String(back)) => assert_eq!(back, s, "{literal:?}"),
        other => panic!("{literal:?} parsed as {other:?}"),
    }
    let statement = format!("RETURN {literal};");
    let ast = syn::parse(&statement)
        .unwrap_or_else(|e| panic!("engine rejected {statement:?}: {e}"));
    assert_eq!(ast.num_statements(), 1, "{statement:?}");
    assert_eq!(unquote_str(&literal).as_deref(), Some(s.as_str()));
});
