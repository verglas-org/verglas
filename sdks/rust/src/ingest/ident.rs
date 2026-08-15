//! Parsing of `namespace.name` table identifiers.
//!
//! Callers name tables as a dotted string. The last component is the table
//! name; everything before it is the (possibly multi-level) namespace. Iceberg
//! namespaces are lists of strings, so `a.b.c` is namespace `[a, b]`, table `c`.

use iceberg::{NamespaceIdent, TableIdent};

use super::error::{IngestError, Result};

/// Parses `namespace.name` into an Iceberg [`TableIdent`]. Requires at least
/// one namespace level and a table name; an empty component (`a..b`) is
/// rejected.
pub fn parse_table_ident(dotted: &str) -> Result<TableIdent> {
    let parts: Vec<String> = dotted
        .split('.')
        .filter(|s| !s.is_empty())
        .map(str::to_owned)
        .collect();
    if parts.len() < 2 || parts.len() != dotted.split('.').count() {
        return Err(IngestError::BadIdent(dotted.to_owned()));
    }
    let (name, namespace) = parts
        .split_last()
        .ok_or_else(|| IngestError::BadIdent(dotted.to_owned()))?;
    let namespace = NamespaceIdent::from_vec(namespace.to_vec())
        .map_err(|_| IngestError::BadIdent(dotted.to_owned()))?;
    Ok(TableIdent::new(namespace, name.clone()))
}

/// Renders a [`TableIdent`] back to its dotted `namespace.name` string.
pub fn ident_to_dotted(ident: &TableIdent) -> String {
    let mut parts = ident.namespace().as_ref().clone();
    parts.push(ident.name().to_owned());
    parts.join(".")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_two_and_three_level_idents() {
        let two = parse_table_ident("sales.orders").expect("valid");
        assert_eq!(two.namespace().as_ref(), &vec!["sales".to_owned()]);
        assert_eq!(two.name(), "orders");
        assert_eq!(ident_to_dotted(&two), "sales.orders");

        let three = parse_table_ident("a.b.c").expect("valid");
        assert_eq!(
            three.namespace().as_ref(),
            &vec!["a".to_owned(), "b".to_owned()]
        );
        assert_eq!(three.name(), "c");
    }

    #[test]
    fn rejects_bare_and_empty_component_idents() {
        assert!(parse_table_ident("orders").is_err());
        assert!(parse_table_ident("sales..orders").is_err());
        assert!(parse_table_ident("").is_err());
    }
}
