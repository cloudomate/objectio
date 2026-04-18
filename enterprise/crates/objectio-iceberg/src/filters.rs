//! Column-level and row-level security filters for Iceberg tables.
//!
//! When data filters are configured for a (table, principal) pair, the
//! `load_table` response is modified:
//! - **Column security**: Schema fields are filtered to only include allowed columns.
//! - **Row security**: A `obio.row-filter` property is injected into table properties
//!   so query engines can apply the WHERE clause.

use serde_json::Value;

/// Apply column-level security by filtering schema fields.
///
/// Modifies the metadata in-place:
/// 1. Filters `schemas[].fields` to only include allowed columns
///    (or exclude specific columns).
/// 2. Updates `last-column-id` to reflect the remaining fields.
pub fn apply_column_filter(
    metadata: &mut Value,
    allowed_columns: &[String],
    excluded_columns: &[String],
) {
    let Some(schemas) = metadata.get_mut("schemas").and_then(Value::as_array_mut) else {
        return;
    };

    for schema in &mut *schemas {
        let Some(fields) = schema.get_mut("fields").and_then(Value::as_array_mut) else {
            continue;
        };

        fields.retain(|field| {
            let name = field
                .get("name")
                .and_then(Value::as_str)
                .unwrap_or_default();

            // If excluded_columns is non-empty, remove those columns
            if !excluded_columns.is_empty() && excluded_columns.iter().any(|e| e == name) {
                return false;
            }
            // If allowed_columns is non-empty, only keep those columns
            if !allowed_columns.is_empty() && !allowed_columns.iter().any(|a| a == name) {
                return false;
            }
            true
        });

        // Update last-column-id to max remaining field id
        let max_id = fields
            .iter()
            .filter_map(|f| f.get("id").and_then(Value::as_i64))
            .max()
            .unwrap_or(0);
        schema["last-column-id"] = Value::from(max_id);
    }

    // Update top-level last-column-id
    if let Some(max_id) = schemas
        .iter()
        .filter_map(|s| s.get("last-column-id").and_then(Value::as_i64))
        .max()
    {
        metadata["last-column-id"] = Value::from(max_id);
    }
}

/// Inject a row filter expression into table properties.
///
/// Query engines that support `ObjectIO` read the `obio.row-filter` property
/// and apply the WHERE clause when scanning the table.
pub fn apply_row_filter(metadata: &mut Value, row_filter_expression: &str) {
    if row_filter_expression.is_empty() {
        return;
    }

    // Ensure properties object exists
    if metadata.get("properties").is_none() {
        metadata["properties"] = Value::Object(serde_json::Map::new());
    }

    if let Some(props) = metadata
        .get_mut("properties")
        .and_then(Value::as_object_mut)
    {
        props.insert(
            "obio.row-filter".to_string(),
            Value::String(row_filter_expression.to_string()),
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn sample_metadata() -> Value {
        json!({
            "format-version": 2,
            "table-uuid": "test-uuid",
            "last-column-id": 4,
            "current-schema-id": 0,
            "schemas": [{
                "type": "struct",
                "schema-id": 0,
                "fields": [
                    {"id": 1, "name": "id", "type": "long", "required": true},
                    {"id": 2, "name": "name", "type": "string", "required": false},
                    {"id": 3, "name": "email", "type": "string", "required": false},
                    {"id": 4, "name": "ssn", "type": "string", "required": false}
                ],
                "last-column-id": 4
            }],
            "properties": {}
        })
    }

    #[test]
    fn test_column_filter_allowed_only() {
        let mut md = sample_metadata();
        apply_column_filter(&mut md, &["id".into(), "name".into()], &[]);

        let fields = md["schemas"][0]["fields"].as_array().unwrap();
        assert_eq!(fields.len(), 2);
        assert_eq!(fields[0]["name"], "id");
        assert_eq!(fields[1]["name"], "name");
        assert_eq!(md["last-column-id"], 2);
    }

    #[test]
    fn test_column_filter_excluded() {
        let mut md = sample_metadata();
        apply_column_filter(&mut md, &[], &["ssn".into(), "email".into()]);

        let fields = md["schemas"][0]["fields"].as_array().unwrap();
        assert_eq!(fields.len(), 2);
        assert_eq!(fields[0]["name"], "id");
        assert_eq!(fields[1]["name"], "name");
    }

    #[test]
    fn test_column_filter_empty_keeps_all() {
        let mut md = sample_metadata();
        apply_column_filter(&mut md, &[], &[]);

        let fields = md["schemas"][0]["fields"].as_array().unwrap();
        assert_eq!(fields.len(), 4);
    }

    #[test]
    fn test_row_filter_injection() {
        let mut md = sample_metadata();
        apply_row_filter(&mut md, "department = 'engineering'");

        let filter = md["properties"]["obio.row-filter"].as_str().unwrap();
        assert_eq!(filter, "department = 'engineering'");
    }

    #[test]
    fn test_row_filter_empty_noop() {
        let mut md = sample_metadata();
        apply_row_filter(&mut md, "");

        assert!(md["properties"].get("obio.row-filter").is_none());
    }

    #[test]
    fn test_excluded_takes_precedence_over_allowed() {
        let mut md = sample_metadata();
        // Allow all but exclude ssn
        apply_column_filter(
            &mut md,
            &["id".into(), "name".into(), "ssn".into()],
            &["ssn".into()],
        );

        let fields = md["schemas"][0]["fields"].as_array().unwrap();
        assert_eq!(fields.len(), 2);
        assert!(
            !fields
                .iter()
                .map(|f| f["name"].as_str().unwrap())
                .any(|n| n == "ssn")
        );
    }
}
