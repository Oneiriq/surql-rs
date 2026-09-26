//! Unit tests for the field definitions and builders.

use super::*;

#[test]
fn nullable_wraps_type_in_option() {
    let (f, _) = datetime_field("deleted_at").nullable(true).build().unwrap();
    assert_eq!(
        f.to_surql("file"),
        "DEFINE FIELD deleted_at ON TABLE file TYPE option<datetime>;",
    );
}

#[test]
fn nullable_wraps_record_target() {
    let (f, _) = record_field("prior", Some("file_version"))
        .nullable(true)
        .build()
        .unwrap();
    assert_eq!(
        f.to_surql("file_version"),
        "DEFINE FIELD prior ON TABLE file_version TYPE option<record<file_version>>;",
    );
}

#[test]
fn nullable_composes_with_other_clauses() {
    let (f, _) = string_field("lock_mode")
        .nullable(true)
        .assertion("$value INSIDE ['governance', 'compliance']")
        .build()
        .unwrap();
    assert_eq!(
        f.to_surql("blob"),
        "DEFINE FIELD lock_mode ON TABLE blob TYPE option<string> \
             ASSERT $value INSIDE ['governance', 'compliance'];",
    );
}

#[test]
fn non_nullable_rendering_is_unchanged() {
    // Guards the default path: byte-identical to pre-nullable output.
    let f = FieldDefinition::new("email", FieldType::String);
    assert_eq!(
        f.to_surql("user"),
        "DEFINE FIELD email ON TABLE user TYPE string;",
    );
    assert!(!f.nullable);
}

#[test]
fn builder_file_field_emits_type_file() {
    let (f, _) = file_field("avatar").build().unwrap();
    assert_eq!(f.field_type, FieldType::File);
    assert_eq!(
        f.to_surql("user"),
        "DEFINE FIELD avatar ON TABLE user TYPE file;"
    );
}

#[test]
fn builder_bytes_field_emits_type_bytes() {
    let (f, _) = bytes_field("blob").build().unwrap();
    assert_eq!(f.field_type, FieldType::Bytes);
    assert_eq!(
        f.to_surql("doc"),
        "DEFINE FIELD blob ON TABLE doc TYPE bytes;"
    );
}

#[test]
fn new_sets_defaults() {
    let f = FieldDefinition::new("email", FieldType::String);
    assert_eq!(f.name, "email");
    assert_eq!(f.field_type, FieldType::String);
    assert!(f.assertion.is_none());
    assert!(!f.readonly);
    assert!(!f.flexible);
}

#[test]
fn to_surql_minimal() {
    let f = FieldDefinition::new("email", FieldType::String);
    assert_eq!(
        f.to_surql("user"),
        "DEFINE FIELD email ON TABLE user TYPE string;"
    );
}

#[test]
fn to_surql_with_assertion() {
    let f = FieldDefinition::new("email", FieldType::String)
        .with_assertion("string::is::email($value)");
    assert_eq!(
        f.to_surql("user"),
        "DEFINE FIELD email ON TABLE user TYPE string ASSERT string::is::email($value);"
    );
}

#[test]
fn to_surql_with_default() {
    let f = FieldDefinition::new("created_at", FieldType::Datetime).with_default("time::now()");
    assert_eq!(
        f.to_surql("audit"),
        "DEFINE FIELD created_at ON TABLE audit TYPE datetime DEFAULT time::now();"
    );
}

#[test]
fn to_surql_readonly_flexible() {
    // FLEXIBLE immediately after TYPE is the only ordering the v3
    // parser accepts alongside READONLY; the previous trailing
    // position was a parse error on a live server.
    let f = FieldDefinition::new("meta", FieldType::Object)
        .readonly(true)
        .flexible(true);
    assert_eq!(
        f.to_surql("user"),
        "DEFINE FIELD meta ON TABLE user TYPE object FLEXIBLE READONLY;"
    );
}

#[test]
fn to_surql_flexible_composes_with_option_and_default() {
    let f = FieldDefinition::new("metadata", FieldType::Object)
        .flexible(true)
        .with_nullable(true)
        .with_default("{}");
    assert_eq!(
        f.to_surql("file"),
        "DEFINE FIELD metadata ON TABLE file TYPE option<object> FLEXIBLE DEFAULT {};"
    );
}

#[test]
fn to_surql_with_value_expression() {
    let f = FieldDefinition::new("full", FieldType::String).with_value("string::concat(a,b)");
    assert!(f.to_surql("t").contains("VALUE string::concat(a,b)"));
}

#[test]
fn to_surql_if_not_exists() {
    let f = FieldDefinition::new("name", FieldType::String);
    assert_eq!(
        f.to_surql_with_options("user", true),
        "DEFINE FIELD IF NOT EXISTS name ON TABLE user TYPE string;"
    );
}

#[test]
fn validate_rejects_empty_name() {
    let f = FieldDefinition::new("", FieldType::String);
    assert!(f.validate().is_err());
}

#[test]
fn validate_rejects_bad_leading_digit() {
    let f = FieldDefinition::new("1bad", FieldType::String);
    assert!(f.validate().is_err());
}

#[test]
fn validate_allows_dot_nested() {
    let f = FieldDefinition::new("address.city", FieldType::String);
    assert!(f.validate().is_ok());
}

#[test]
fn validate_rejects_empty_segment() {
    let f = FieldDefinition::new("address..city", FieldType::String);
    assert!(f.validate().is_err());
}

#[test]
fn builder_string_field() {
    let (f, _) = string_field("email").build().unwrap();
    assert_eq!(f.field_type, FieldType::String);
}

#[test]
fn builder_int_field_with_assertion() {
    let (f, _) = int_field("age").assertion("$value >= 0").build().unwrap();
    assert_eq!(f.field_type, FieldType::Int);
    assert_eq!(f.assertion.as_deref(), Some("$value >= 0"));
}

#[test]
fn builder_float_field() {
    let (f, _) = float_field("price").build().unwrap();
    assert_eq!(f.field_type, FieldType::Float);
}

#[test]
fn builder_bool_field_with_default() {
    let (f, _) = bool_field("active").default("true").build().unwrap();
    assert_eq!(f.field_type, FieldType::Bool);
    assert_eq!(f.default.as_deref(), Some("true"));
}

#[test]
fn builder_datetime_field_readonly() {
    let (f, _) = datetime_field("created_at")
        .default("time::now()")
        .readonly(true)
        .build()
        .unwrap();
    assert!(f.readonly);
    assert_eq!(f.default.as_deref(), Some("time::now()"));
}

#[test]
fn builder_array_field() {
    let (f, _) = array_field("tags").default("[]").build().unwrap();
    assert_eq!(f.field_type, FieldType::Array);
}

#[test]
fn builder_object_field_defaults_flexible() {
    let (f, _) = object_field("metadata").build().unwrap();
    assert_eq!(f.field_type, FieldType::Object);
    assert!(f.flexible);
}

#[test]
fn builder_record_field_with_table() {
    let (f, _) = record_field("author", Some("user")).build().unwrap();
    assert_eq!(f.field_type, FieldType::Record);
    assert_eq!(f.target_table.as_deref(), Some("user"));
    assert_eq!(
        f.to_surql("post"),
        "DEFINE FIELD author ON TABLE post TYPE record<user>;"
    );
}

#[test]
fn builder_record_field_no_table() {
    let (f, _) = record_field("link", None).build().unwrap();
    assert!(f.assertion.is_none());
    assert!(f.target_table.is_none());
}

#[test]
fn detect_target_table_matches_canonical_coercion() {
    assert_eq!(
        detect_target_table_from_value("type::record(\"plan\", $value)").as_deref(),
        Some("plan")
    );
    assert_eq!(
        detect_target_table_from_value("type::record('user', $value)").as_deref(),
        Some("user")
    );
    assert!(detect_target_table_from_value("$value.id").is_none());
    assert!(detect_target_table_from_value("type::record(\"a\", $other)").is_none());
}

#[test]
fn explicit_target_table_renders_typed_record() {
    let (f, _) = field("author", FieldType::Record)
        .target_table("user")
        .build()
        .unwrap();
    assert_eq!(f.target_table.as_deref(), Some("user"));
    assert_eq!(
        f.to_surql("post"),
        "DEFINE FIELD author ON TABLE post TYPE record<user>;"
    );
}

#[test]
fn value_coercion_lifts_target_table_and_drops_value() {
    let (f, _) = field("workspace_id", FieldType::Record)
        .value("type::record(\"workspace\", $value)")
        .build()
        .unwrap();
    assert_eq!(f.target_table.as_deref(), Some("workspace"));
    assert!(f.value.is_none());
    assert_eq!(
        f.to_surql("task"),
        "DEFINE FIELD workspace_id ON TABLE task TYPE record<workspace>;"
    );
}

#[test]
fn builder_computed_field_is_readonly() {
    let (f, _) = computed_field("full", "a + b", FieldType::String)
        .build()
        .unwrap();
    assert!(f.readonly);
    assert_eq!(f.value.as_deref(), Some("a + b"));
}

#[test]
fn builder_rejects_invalid_name() {
    let err = string_field("1bad").build().unwrap_err();
    assert!(matches!(err, SurqlError::Validation { .. }));
}

#[test]
fn builder_flags_reserved_word() {
    let (_f, warning) = string_field("select").build().unwrap();
    assert!(warning.is_some());
}

#[test]
fn builder_permissions_are_stored() {
    let (f, _) = string_field("name")
        .permissions([("select", "true")])
        .build()
        .unwrap();
    assert_eq!(
        f.permissions
            .as_ref()
            .unwrap()
            .get("select")
            .map(String::as_str),
        Some("true")
    );
}

#[test]
fn field_permissions_render_in_the_statement() {
    // Regression: the permissions used to be dropped, leaving the field
    // at the engine default, FULL.
    let (f, _) = string_field("ssn")
        .permissions([("select", "$auth.admin"), ("update", "NONE")])
        .build()
        .unwrap();
    assert_eq!(
        f.to_surql("user"),
        "DEFINE FIELD ssn ON TABLE user TYPE string \
             PERMISSIONS FOR select WHERE $auth.admin FOR update NONE;"
    );
    assert!(f
        .to_surql_overwrite("user")
        .ends_with("PERMISSIONS FOR select WHERE $auth.admin FOR update NONE;"));
}

#[test]
fn field_permissions_refuse_delete_and_unknown_actions() {
    assert!(string_field("x")
        .permissions([("delete", "true")])
        .build()
        .is_err());
    assert!(string_field("x")
        .permissions([("select WHERE true FOR create", "true")])
        .build()
        .is_err());
}

#[test]
fn reserved_and_odd_names_are_quoted() {
    let f = FieldDefinition::new("value", FieldType::Int);
    assert_eq!(
        f.to_surql("select"),
        "DEFINE FIELD `value` ON TABLE `select` TYPE int;"
    );
    let f = FieldDefinition::new("meta.type", FieldType::String);
    assert_eq!(
        f.to_surql("my-table"),
        "DEFINE FIELD meta.`type` ON TABLE `my-table` TYPE string;"
    );
    let f = FieldDefinition::new("tags[*]", FieldType::String);
    assert_eq!(
        f.to_surql("t"),
        "DEFINE FIELD tags[*] ON TABLE t TYPE string;"
    );
    let f = FieldDefinition::new("tags.*", FieldType::String);
    assert_eq!(
        f.to_surql("t"),
        "DEFINE FIELD tags.* ON TABLE t TYPE string;"
    );
    let f = FieldDefinition::new("link", FieldType::Record).with_target_table("user | order");
    assert_eq!(
        f.to_surql("t"),
        "DEFINE FIELD link ON TABLE t TYPE record<user | `order`>;"
    );
}

#[test]
fn validate_field_name_helper() {
    assert!(validate_field_name("ok").is_ok());
    assert!(validate_field_name("ok.nested").is_ok());
    assert!(validate_field_name("").is_err());
    assert!(validate_field_name("bad seg").is_err());
}
