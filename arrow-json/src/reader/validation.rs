// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Validation error collection for JSON deserialization

use arrow_schema::DataType;
use std::sync::Arc;

use super::tape::{Tape, TapeElement};

/// Default maximum number of errors to collect
pub const DEFAULT_MAX_ERRORS: usize = 1000;

const MAX_VALUE_LENGTH: usize = 256;

/// Type of validation failure
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum FailureKind {
    /// Required field is not present in JSON object
    MissingField,
    /// Field has null value but is non-nullable
    NullValue,
    /// JSON type doesn't match expected schema type
    TypeMismatch,
    /// Value failed to parse into target type
    ParseFailure,
}

/// Error marker stored during validation
#[derive(Debug, Clone)]
pub struct ErrorMarker<'tape> {
    /// Row index in the batch
    pub row_index: usize,
    /// Position in tape where error occurred
    pub tape_pos: Option<u32>,
    /// Field name extracted from tape
    pub field_name: Option<&'tape str>,
    /// Array indices from parent arrays (innermost to outermost)
    pub array_indices: Vec<usize>,
    /// Type of validation failure
    pub error_kind: FailureKind,
    /// Expected schema type
    pub expected_type: Arc<DataType>,
}

impl ErrorMarker<'_> {
    /// Create a validation error result with a single error marker
    ///
    /// This is a convenience method for the common case of returning a single
    /// validation error with no field name or array indices.
    pub fn err(
        row_idx: usize,
        pos: u32,
        kind: FailureKind,
        expected_type: Arc<DataType>,
    ) -> Result<(), Vec<Self>> {
        Err(vec![Self {
            row_index: row_idx,
            tape_pos: Some(pos),
            field_name: None,
            array_indices: Vec::new(),
            error_kind: kind,
            expected_type,
        }])
    }
}

/// Detailed validation error with extracted context
#[derive(Debug, Clone)]
pub struct ValidationError {
    /// Row index in the batch
    pub row_index: usize,
    /// Field path where error occurred (e.g., "items[2]", "user.age")
    pub field_path: String,
    /// Type of validation failure
    pub failure_kind: FailureKind,
    /// Expected schema type
    pub expected_type: Arc<DataType>,
    /// Actual JSON type encountered
    pub actual_type: Option<JsonType>,
    /// Actual JSON value encountered
    pub actual_value: Option<String>,
}

impl std::fmt::Display for ValidationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "row {}, field '{}': ", self.row_index, self.field_path)?;
        match self.failure_kind {
            FailureKind::MissingField => {
                write!(f, "required field is missing")?;
            }
            FailureKind::NullValue => {
                write!(f, "null value for non-nullable field")?;
            }
            FailureKind::TypeMismatch | FailureKind::ParseFailure => {
                if let Some(actual_type) = self.actual_type {
                    write!(
                        f,
                        "cannot deserialize {} value as {}",
                        actual_type, self.expected_type
                    )?;
                } else {
                    write!(f, "cannot deserialize value as {}", self.expected_type)?;
                }
                if matches!(self.failure_kind, FailureKind::ParseFailure) {
                    write!(f, " (parse failure)")?;
                }
            }
        }
        if let Some(val) = &self.actual_value {
            write!(f, ", got: {}", val)?;
        }
        Ok(())
    }
}

/// JSON value type
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum JsonType {
    /// JSON null
    Null,
    /// JSON boolean
    Boolean,
    /// JSON number
    Number,
    /// JSON string
    String,
    /// JSON array
    Array,
    /// JSON object
    Object,
}

impl std::fmt::Display for JsonType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            JsonType::Null => write!(f, "null"),
            JsonType::Boolean => write!(f, "boolean"),
            JsonType::Number => write!(f, "number"),
            JsonType::String => write!(f, "string"),
            JsonType::Array => write!(f, "array"),
            JsonType::Object => write!(f, "object"),
        }
    }
}

fn truncate_value(v: String) -> String {
    if v.len() > MAX_VALUE_LENGTH {
        format!("{}...", &v[..MAX_VALUE_LENGTH])
    } else {
        v
    }
}

/// Extract string representation of tape element for error reporting
pub fn extract_value(tape: &Tape<'_>, pos: u32) -> Option<String> {
    Some(match tape.get(pos) {
        TapeElement::Null => "null".to_string(),
        TapeElement::True => "true".to_string(),
        TapeElement::False => "false".to_string(),
        TapeElement::String(idx) => format!("\"{}\"", tape.get_string(idx)),
        TapeElement::Number(idx) => tape.get_string(idx).to_string(),
        TapeElement::I32(v) => v.to_string(),
        TapeElement::F32(v) => f32::from_bits(v).to_string(),
        TapeElement::I64(high) => match tape.get(pos + 1) {
            TapeElement::I32(low) => {
                let v = ((high as i64) << 32) | (low as u32) as i64;
                v.to_string()
            }
            _ => return None,
        },
        TapeElement::F64(high) => match tape.get(pos + 1) {
            TapeElement::F32(low) => {
                let v = f64::from_bits(((high as u64) << 32) | low as u64);
                v.to_string()
            }
            _ => return None,
        },
        TapeElement::StartObject(_) => "{...}".to_string(),
        TapeElement::StartList(_) => "[...]".to_string(),
        _ => return None,
    })
}

/// Convert TapeElement to JsonType
pub fn tape_element_type(tape: &Tape<'_>, pos: u32) -> JsonType {
    match tape.get(pos) {
        TapeElement::Null => JsonType::Null,
        TapeElement::True | TapeElement::False => JsonType::Boolean,
        TapeElement::Number(_)
        | TapeElement::I32(_)
        | TapeElement::F32(_)
        | TapeElement::I64(_)
        | TapeElement::F64(_) => JsonType::Number,
        TapeElement::String(_) => JsonType::String,
        TapeElement::StartList(_) => JsonType::Array,
        TapeElement::StartObject(_) => JsonType::Object,
        _ => JsonType::Null,
    }
}

/// Extract field name from tape at error position
///
/// In JSON objects, the tape structure is: [StartObject, String(field_name), value, ...]
/// So the field name precedes its value by one position.
/// Returns None for array elements, invalid positions, or if field name cannot be extracted.
fn extract_field_name_from_tape(tape: &Tape<'_>, error_pos: Option<u32>) -> Option<String> {
    if let Some(pos) = error_pos {
        if pos > 0 {
            if let TapeElement::String(idx) = tape.get(pos - 1) {
                return Some(tape.get_string(idx).to_string());
            }
        }
    }
    None
}

/// Build detailed errors from markers collected during validation
pub fn build_detailed_errors(
    tape: &Tape<'_>,
    markers: Vec<ErrorMarker<'_>>,
) -> Vec<ValidationError> {
    markers
        .into_iter()
        .map(|marker| {
            let field_path = build_field_path(&marker, tape);

            let actual_type = marker.tape_pos.map(|pos| tape_element_type(tape, pos));

            let actual_value = marker
                .tape_pos
                .and_then(|pos| extract_value(tape, pos))
                .map(truncate_value);

            ValidationError {
                row_index: marker.row_index,
                field_path,
                failure_kind: marker.error_kind,
                expected_type: marker.expected_type,
                actual_type,
                actual_value,
            }
        })
        .collect()
}

/// Build field path from marker
fn build_field_path(marker: &ErrorMarker<'_>, tape: &Tape<'_>) -> String {
    let base_name = marker
        .field_name
        .map(|s| s.to_string())
        .or_else(|| extract_field_name_from_tape(tape, marker.tape_pos));

    if marker.array_indices.is_empty() {
        base_name.unwrap_or_else(|| "<unknown>".to_string())
    } else {
        let indices = marker
            .array_indices
            .iter()
            .rev()
            .map(|i| format!("[{}]", i))
            .collect::<String>();

        match base_name {
            Some(name) => format!("{}{}", name, indices),
            None => indices,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_build_field_path_simple() {
        let marker = ErrorMarker {
            row_index: 0,
            tape_pos: Some(0),
            field_name: Some("age"),
            array_indices: Vec::new(),
            error_kind: FailureKind::MissingField,
            expected_type: Arc::new(DataType::Int32),
        };

        let path = build_field_path(&marker, &crate::reader::tape::Tape::new(&[], "", &[]));
        assert_eq!(path, "age");
    }

    #[test]
    fn test_build_field_path_with_array_indices() {
        let marker = ErrorMarker {
            row_index: 0,
            tape_pos: Some(0),
            field_name: Some("items"),
            array_indices: vec![2, 5], // innermost first, reversed in output
            error_kind: FailureKind::ParseFailure,
            expected_type: Arc::new(DataType::Int32),
        };

        let path = build_field_path(&marker, &crate::reader::tape::Tape::new(&[], "", &[]));
        assert_eq!(path, "items[5][2]");
    }

    #[test]
    fn test_build_field_path_array_only() {
        let marker = ErrorMarker {
            row_index: 0,
            tape_pos: Some(0),
            field_name: None,
            array_indices: vec![1, 2],
            error_kind: FailureKind::ParseFailure,
            expected_type: Arc::new(DataType::Int32),
        };

        let path = build_field_path(&marker, &crate::reader::tape::Tape::new(&[], "", &[]));
        assert_eq!(path, "[2][1]");
    }

    #[test]
    fn test_build_field_path_unknown() {
        let marker = ErrorMarker {
            row_index: 0,
            tape_pos: Some(0),
            field_name: None,
            array_indices: Vec::new(),
            error_kind: FailureKind::TypeMismatch,
            expected_type: Arc::new(DataType::Int32),
        };

        let path = build_field_path(&marker, &crate::reader::tape::Tape::new(&[], "", &[]));
        assert_eq!(path, "<unknown>");
    }

    #[test]
    fn test_json_type_display() {
        assert_eq!(JsonType::Null.to_string(), "null");
        assert_eq!(JsonType::Boolean.to_string(), "boolean");
        assert_eq!(JsonType::Number.to_string(), "number");
        assert_eq!(JsonType::String.to_string(), "string");
        assert_eq!(JsonType::Array.to_string(), "array");
        assert_eq!(JsonType::Object.to_string(), "object");
    }
}
