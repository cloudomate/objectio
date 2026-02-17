//! Iceberg table metadata helpers.
//!
//! Implements requirement validation and update application for the
//! commit-table flow per the Iceberg REST Catalog spec.

use serde_json::Value;

use crate::error::IcebergError;

type Result<T> = std::result::Result<T, IcebergError>;

/// Validate all table requirements against the current metadata.
///
/// Returns an error on the first requirement that is not satisfied, following
/// the Iceberg REST spec semantics for `CommitTableRequest.requirements`.
///
/// # Errors
///
/// Returns `IcebergError::commit_failed` if any requirement does not match
/// the current metadata state.
pub fn validate_requirements(
    metadata: &Value,
    requirements: &[crate::types::TableRequirement],
) -> Result<()> {
    use crate::types::TableRequirement;

    for req in requirements {
        match req {
            TableRequirement::AssertCreate => {
                if metadata.get("table-uuid").is_some() {
                    return Err(IcebergError::commit_failed(
                        "requirement failed: table already exists",
                    ));
                }
            }
            TableRequirement::AssertTableUuid { uuid } => {
                let current = metadata
                    .get("table-uuid")
                    .and_then(Value::as_str)
                    .unwrap_or("");
                if current != uuid {
                    return Err(IcebergError::commit_failed(format!(
                        "requirement failed: table UUID mismatch (expected {uuid}, got {current})"
                    )));
                }
            }
            TableRequirement::AssertRefSnapshotId { r#ref, snapshot_id } => {
                let current = metadata
                    .get("refs")
                    .and_then(|r| r.get(r#ref.as_str()))
                    .and_then(|r| r.get("snapshot-id"))
                    .and_then(Value::as_i64);
                if current != *snapshot_id {
                    return Err(IcebergError::commit_failed(format!(
                        "requirement failed: ref '{ref}' snapshot-id mismatch \
                         (expected {snapshot_id:?}, got {current:?})"
                    )));
                }
            }
            TableRequirement::AssertLastAssignedFieldId {
                last_assigned_field_id,
            } => {
                assert_i64_field(metadata, "last-column-id", *last_assigned_field_id)?;
            }
            TableRequirement::AssertCurrentSchemaId { current_schema_id } => {
                assert_i64_field(metadata, "current-schema-id", *current_schema_id)?;
            }
            TableRequirement::AssertLastAssignedPartitionId {
                last_assigned_partition_id,
            } => {
                assert_i64_field(metadata, "last-partition-id", *last_assigned_partition_id)?;
            }
            TableRequirement::AssertDefaultSpecId { default_spec_id } => {
                assert_i64_field(metadata, "default-spec-id", *default_spec_id)?;
            }
            TableRequirement::AssertDefaultSortOrderId {
                default_sort_order_id,
            } => {
                assert_i64_field(metadata, "default-sort-order-id", *default_sort_order_id)?;
            }
        }
    }
    Ok(())
}

/// Assert an integer field matches the expected value.
fn assert_i64_field(metadata: &Value, field: &str, expected: i64) -> Result<()> {
    let current = metadata.get(field).and_then(Value::as_i64).unwrap_or(0);
    if current != expected {
        return Err(IcebergError::commit_failed(format!(
            "requirement failed: {field} mismatch (expected {expected}, got {current})"
        )));
    }
    Ok(())
}

/// Apply a sequence of table updates to mutable metadata JSON.
///
/// Each update is applied in order per the Iceberg REST spec semantics.
///
/// # Errors
///
/// Returns `IcebergError::internal` if a required metadata array (e.g.
/// `schemas`, `partition-specs`, `sort-orders`, `snapshots`) is missing.
pub fn apply_updates(metadata: &mut Value, updates: &[crate::types::TableUpdate]) -> Result<()> {
    for update in updates {
        apply_single_update(metadata, update)?;
    }
    Ok(())
}

fn apply_single_update(metadata: &mut Value, update: &crate::types::TableUpdate) -> Result<()> {
    use crate::types::TableUpdate;

    match update {
        TableUpdate::AssignUuid { uuid } => {
            metadata["table-uuid"] = Value::String(uuid.clone());
        }
        TableUpdate::UpgradeFormatVersion { format_version } => {
            metadata["format-version"] = Value::from(*format_version);
        }
        TableUpdate::AddSchema {
            schema,
            last_column_id,
        } => {
            push_to_array(metadata, "schemas", schema.clone())?;
            if let Some(id) = last_column_id {
                metadata["last-column-id"] = Value::from(*id);
            }
        }
        TableUpdate::SetCurrentSchema { schema_id } => {
            metadata["current-schema-id"] = Value::from(*schema_id);
        }
        TableUpdate::AddSpec { spec } => {
            push_to_array(metadata, "partition-specs", spec.clone())?;
        }
        TableUpdate::SetDefaultSpec { spec_id } => {
            metadata["default-spec-id"] = Value::from(*spec_id);
        }
        TableUpdate::AddSortOrder { sort_order } => {
            push_to_array(metadata, "sort-orders", sort_order.clone())?;
        }
        TableUpdate::SetDefaultSortOrder { sort_order_id } => {
            metadata["default-sort-order-id"] = Value::from(*sort_order_id);
        }
        TableUpdate::AddSnapshot { snapshot } => {
            push_to_array(metadata, "snapshots", snapshot.clone())?;
        }
        TableUpdate::SetSnapshotRef { ref_name, rest } => {
            if !metadata.get("refs").is_some_and(Value::is_object) {
                metadata["refs"] = Value::Object(serde_json::Map::new());
            }
            metadata["refs"][ref_name] = rest.clone();
        }
        TableUpdate::RemoveSnapshots { snapshot_ids } => {
            if let Some(snaps) = metadata.get_mut("snapshots").and_then(Value::as_array_mut) {
                snaps.retain(|s| {
                    s.get("snapshot-id")
                        .and_then(Value::as_i64)
                        .is_none_or(|id| !snapshot_ids.contains(&id))
                });
            }
        }
        TableUpdate::RemoveSnapshotRef { ref_name } => {
            if let Some(refs) = metadata.get_mut("refs").and_then(Value::as_object_mut) {
                refs.remove(ref_name);
            }
        }
        TableUpdate::SetLocation { location } => {
            metadata["location"] = Value::String(location.clone());
        }
        TableUpdate::SetProperties { updates } => {
            if !metadata.get("properties").is_some_and(Value::is_object) {
                metadata["properties"] = Value::Object(serde_json::Map::new());
            }
            if let Some(obj) = metadata["properties"].as_object_mut() {
                for (k, v) in updates {
                    obj.insert(k.clone(), Value::String(v.clone()));
                }
            }
        }
        TableUpdate::RemoveProperties { removals } => {
            if let Some(obj) = metadata
                .get_mut("properties")
                .and_then(Value::as_object_mut)
            {
                for key in removals {
                    obj.remove(key);
                }
            }
        }
    }
    Ok(())
}

/// Push a value to a named array field in the metadata, returning an error if
/// the field is missing or not an array.
fn push_to_array(metadata: &mut Value, field: &str, value: Value) -> Result<()> {
    let arr = metadata
        .get_mut(field)
        .and_then(Value::as_array_mut)
        .ok_or_else(|| IcebergError::internal(format!("metadata missing '{field}' array")))?;
    arr.push(value);
    Ok(())
}
