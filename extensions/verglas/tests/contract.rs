//! Package-level guards for the environment-only, database-scoped transport.

#[test]
fn credentials_are_environment_only() {
    let source = include_str!("../src/lib.rs");
    let parameters = source.split("fn parameters()").skip(1).collect::<Vec<_>>();
    assert!(source.contains("required(\"VERGLAS_TOKEN\")"));
    assert!(
        !parameters
            .iter()
            .any(|block| block.contains("VERGLAS_TOKEN"))
    );
}

#[test]
fn template_symbols_are_not_shipped() {
    let source = include_str!("../src/lib.rs");
    let sql = include_str!("../test/sql/verglas.test");
    assert!(!source.contains("multiply_numbers"));
    assert!(!sql.contains("capi_quack"));
}
