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

use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::{Array, ArrayRef, StructArray};
use arrow_buffer::NullBufferBuilder;
use arrow_schema::{ArrowError, DataType, Fields};

use crate::reader::tape::{Tape, TapeElement};
use crate::reader::validation::{ErrorMarker, FailureKind};
use crate::reader::{ArrayDecoder, DecoderContext, StructMode};

/// Reusable buffer for tape positions, indexed by (field_idx, row_idx).
/// A value of 0 indicates the field is absent for that row.
struct FieldTapePositions {
    data: Vec<u32>,
    row_count: usize,
}

impl FieldTapePositions {
    fn new() -> Self {
        Self {
            data: Vec::new(),
            row_count: 0,
        }
    }

    fn resize(&mut self, field_count: usize, row_count: usize) -> Result<(), ArrowError> {
        let total_len = field_count.checked_mul(row_count).ok_or_else(|| {
            ArrowError::JsonError(format!(
                "FieldTapePositions buffer size overflow for rows={row_count} fields={field_count}"
            ))
        })?;
        self.data.clear();
        self.data.resize(total_len, 0);
        self.row_count = row_count;
        Ok(())
    }

    fn try_set(&mut self, field_idx: usize, row_idx: usize, pos: u32) -> Option<()> {
        let idx = field_idx
            .checked_mul(self.row_count)?
            .checked_add(row_idx)?;
        *self.data.get_mut(idx)? = pos;
        Some(())
    }

    fn set(&mut self, field_idx: usize, row_idx: usize, pos: u32) {
        self.data[field_idx * self.row_count + row_idx] = pos;
    }

    fn field_positions(&self, field_idx: usize) -> &[u32] {
        let start = field_idx * self.row_count;
        &self.data[start..start + self.row_count]
    }
}

pub struct StructArrayDecoder {
    data_type: Arc<DataType>,
    decoders: Vec<Box<dyn ArrayDecoder>>,
    strict_mode: bool,
    ignore_type_conflicts: bool,
    is_nullable: bool,
    struct_mode: StructMode,
    field_name_to_index: Option<HashMap<String, usize>>,
    field_tape_positions: FieldTapePositions,
}

impl StructArrayDecoder {
    pub fn new(
        ctx: &DecoderContext,
        data_type: &DataType,
        is_nullable: bool,
    ) -> Result<Self, ArrowError> {
        let fields = struct_fields(data_type);
        let decoders = fields
            .iter()
            .map(|f| {
                // If this struct nullable, need to permit nullability in child array
                // StructArrayDecoder::decode verifies that if the child is not nullable
                // it doesn't contain any nulls not masked by its parent
                let nullable = f.is_nullable() || is_nullable;
                ctx.make_field_decoder(f, nullable)
            })
            .collect::<Result<Vec<_>, ArrowError>>()?;

        let struct_mode = ctx.struct_mode();
        let field_name_to_index = if struct_mode == StructMode::ObjectOnly {
            build_field_index(fields)
        } else {
            None
        };

        Ok(Self {
            data_type: Arc::new(data_type.clone()),
            decoders,
            strict_mode: ctx.strict_mode(),
            ignore_type_conflicts: ctx.ignore_type_conflicts(),
            is_nullable,
            struct_mode,
            field_name_to_index,
            field_tape_positions: FieldTapePositions::new(),
        })
    }
}

impl ArrayDecoder for StructArrayDecoder {
    fn decode(&mut self, tape: &Tape<'_>, pos: &[u32]) -> Result<ArrayRef, ArrowError> {
        let fields = struct_fields(&self.data_type);
        let row_count = pos.len();
        let field_count = fields.len();
        self.field_tape_positions.resize(field_count, row_count)?;
        let mut nulls = self.is_nullable.then(|| NullBufferBuilder::new(pos.len()));

        {
            // We avoid having the match on self.struct_mode inside the hot loop for performance
            // TODO: Investigate how to extract duplicated logic.
            match self.struct_mode {
                StructMode::ObjectOnly => {
                    for (row, p) in pos.iter().enumerate() {
                        let end_idx = match (tape.get(*p), nulls.as_mut()) {
                            (TapeElement::StartObject(end_idx), None) => end_idx,
                            (TapeElement::StartObject(end_idx), Some(nulls)) => {
                                nulls.append_non_null();
                                end_idx
                            }
                            (TapeElement::Null, Some(nulls)) => {
                                nulls.append_null();
                                continue;
                            }
                            (_, Some(nulls)) if self.ignore_type_conflicts => {
                                nulls.append_null();
                                continue;
                            }
                            (_, _) => return Err(tape.error(*p, "{")),
                        };

                        let mut cur_idx = *p + 1;
                        while cur_idx < end_idx {
                            // Read field name
                            let field_name = match tape.get(cur_idx) {
                                TapeElement::String(s) => tape.get_string(s),
                                _ => return Err(tape.error(cur_idx, "field name")),
                            };

                            // Update child pos if match found
                            let field_idx = match &self.field_name_to_index {
                                Some(map) => map.get(field_name).copied(),
                                None => fields.iter().position(|x| x.name() == field_name),
                            };
                            match field_idx {
                                Some(field_idx) => {
                                    self.field_tape_positions.set(field_idx, row, cur_idx + 1);
                                }
                                None => {
                                    if self.strict_mode {
                                        return Err(ArrowError::JsonError(format!(
                                            "column '{field_name}' missing from schema",
                                        )));
                                    }
                                }
                            }
                            // Advance to next field
                            cur_idx = tape.next(cur_idx + 1, "field value")?;
                        }
                    }
                }
                StructMode::ListOnly => {
                    for (row, p) in pos.iter().enumerate() {
                        let end_idx = match (tape.get(*p), nulls.as_mut()) {
                            (TapeElement::StartList(end_idx), None) => end_idx,
                            (TapeElement::StartList(end_idx), Some(nulls)) => {
                                nulls.append_non_null();
                                end_idx
                            }
                            (TapeElement::Null, Some(nulls)) => {
                                nulls.append_null();
                                continue;
                            }
                            (_, Some(nulls)) if self.ignore_type_conflicts => {
                                nulls.append_null();
                                continue;
                            }
                            (_, _) => return Err(tape.error(*p, "[")),
                        };

                        let mut cur_idx = *p + 1;
                        let mut entry_idx = 0;
                        while cur_idx < end_idx {
                            self.field_tape_positions
                                .try_set(entry_idx, row, cur_idx)
                                .ok_or_else(|| {
                                    ArrowError::JsonError(format!(
                                        "found extra columns for {} fields",
                                        fields.len()
                                    ))
                                })?;
                            entry_idx += 1;
                            // Advance to next field
                            cur_idx = tape.next(cur_idx, "field value")?;
                        }
                        if entry_idx != fields.len() {
                            return Err(ArrowError::JsonError(format!(
                                "found {} columns for {} fields",
                                entry_idx,
                                fields.len()
                            )));
                        }
                    }
                }
            }
        }

        let child_arrays = self
            .decoders
            .iter_mut()
            .enumerate()
            .zip(fields)
            .map(|((field_idx, d), f)| {
                let pos = self.field_tape_positions.field_positions(field_idx);
                d.decode(tape, pos).map_err(|e| match e {
                    ArrowError::JsonError(s) => {
                        ArrowError::JsonError(format!("whilst decoding field '{}': {s}", f.name()))
                    }
                    e => e,
                })
            })
            .collect::<Result<Vec<_>, ArrowError>>()?;

        let nulls = nulls.as_mut().and_then(|x| x.finish());

        for (c, f) in child_arrays.iter().zip(fields) {
            // Sanity check
            assert_eq!(c.len(), pos.len());
            if let Some(a) = c.nulls() {
                let nulls_valid =
                    f.is_nullable() || nulls.as_ref().map(|n| n.contains(a)).unwrap_or_default();

                if !nulls_valid {
                    return Err(ArrowError::JsonError(format!(
                        "Encountered unmasked nulls in non-nullable StructArray child: {f}"
                    )));
                }
            }
        }

        // SAFETY: fields, child array lengths, and nullability are validated above
        let array = unsafe {
            StructArray::new_unchecked_with_length(fields.clone(), child_arrays, nulls, row_count)
        };
        Ok(Arc::new(array))
    }

    fn validate_row<'tape>(
        &'tape self,
        tape: &'tape Tape<'_>,
        pos: u32,
        row_idx: usize,
    ) -> Result<(), Vec<ErrorMarker<'tape>>> {
        if self.struct_mode == StructMode::ListOnly {
            let end = match tape.get(pos) {
                TapeElement::Null if self.is_nullable => return Ok(()),
                TapeElement::Null => {
                    return ErrorMarker::err(
                        row_idx,
                        pos,
                        FailureKind::NullValue,
                        self.data_type.clone(),
                    );
                }
                TapeElement::StartList(end) => end,
                _ => {
                    return ErrorMarker::err(
                        row_idx,
                        pos,
                        FailureKind::TypeMismatch,
                        self.data_type.clone(),
                    );
                }
            };
            let mut child = pos + 1;
            for field_idx in 0..self.decoders.len() {
                if child >= end {
                    return ErrorMarker::err(
                        row_idx,
                        pos,
                        FailureKind::TypeMismatch,
                        self.data_type.clone(),
                    );
                }
                self.validate_child(tape, child, row_idx, field_idx)?;
                let Ok(next) = tape.next(child, "struct value") else {
                    return ErrorMarker::err(
                        row_idx,
                        child,
                        FailureKind::TypeMismatch,
                        self.data_type.clone(),
                    );
                };
                child = next;
            }
            return if child == end {
                Ok(())
            } else {
                ErrorMarker::err(
                    row_idx,
                    pos,
                    FailureKind::TypeMismatch,
                    self.data_type.clone(),
                )
            };
        }
        let end_idx = match tape.get(pos) {
            TapeElement::StartObject(end_idx) => end_idx,
            TapeElement::Null => {
                if self.is_nullable {
                    return Ok(());
                } else {
                    return ErrorMarker::err(
                        row_idx,
                        pos,
                        FailureKind::NullValue,
                        Arc::clone(&self.data_type),
                    );
                }
            }
            _ => {
                return ErrorMarker::err(
                    row_idx,
                    pos,
                    FailureKind::TypeMismatch,
                    Arc::clone(&self.data_type),
                );
            }
        };

        let fields = struct_fields(&self.data_type);
        let mut validated_fields = vec![false; fields.len()];

        let mut cur_idx = pos + 1;
        while cur_idx < end_idx {
            let field_name = match tape.get(cur_idx) {
                TapeElement::String(s) => tape.get_string(s),
                _ => {
                    return ErrorMarker::err(
                        row_idx,
                        cur_idx,
                        FailureKind::TypeMismatch,
                        Arc::clone(&self.data_type),
                    );
                }
            };

            match fields.iter().position(|x| x.name() == field_name) {
                Some(field_idx) => {
                    let child_pos = cur_idx + 1;
                    self.validate_child(tape, child_pos, row_idx, field_idx)?;

                    validated_fields[field_idx] = true;
                }
                None => {
                    if self.strict_mode {
                        // Custom field_name - can't use helper
                        return Err(vec![ErrorMarker {
                            row_index: row_idx,
                            tape_pos: Some(cur_idx),
                            field_name: Some(field_name),
                            array_indices: Vec::new(),
                            error_kind: FailureKind::TypeMismatch,
                            expected_type: Arc::clone(&self.data_type),
                        }]);
                    }
                }
            }

            cur_idx = match tape.next(cur_idx + 1, "field value") {
                Ok(i) => i,
                Err(_) => {
                    return ErrorMarker::err(
                        row_idx,
                        cur_idx,
                        FailureKind::TypeMismatch,
                        Arc::clone(&self.data_type),
                    );
                }
            };
        }

        // Early exit on happy path - no missing field errors
        let all_valid = validated_fields
            .iter()
            .zip(fields)
            .all(|(validated, field)| *validated || field.is_nullable());

        if all_valid {
            return Ok(());
        }

        // Error path: collect missing field errors
        let mut missing_errors = Vec::new();
        for (validated, field) in validated_fields.iter().zip(fields) {
            if !validated && !field.is_nullable() {
                missing_errors.push(ErrorMarker {
                    row_index: row_idx,
                    tape_pos: None,
                    field_name: Some(field.name()),
                    array_indices: Vec::new(),
                    error_kind: FailureKind::MissingField,
                    expected_type: Arc::new(field.data_type().clone()),
                });
            }
        }

        Err(missing_errors)
    }
}

impl StructArrayDecoder {
    fn validate_child<'tape>(
        &'tape self,
        tape: &'tape Tape<'_>,
        pos: u32,
        row_idx: usize,
        field_idx: usize,
    ) -> Result<(), Vec<ErrorMarker<'tape>>> {
        let field = &struct_fields(&self.data_type)[field_idx];
        let raw_json = field.data_type() == &DataType::Utf8
            && field
                .metadata()
                .get("ARROW:extension:name")
                .is_some_and(|v| v == "arroyo.json");
        let result =
            if !field.is_nullable() && !raw_json && matches!(tape.get(pos), TapeElement::Null) {
                ErrorMarker::err(
                    row_idx,
                    pos,
                    FailureKind::NullValue,
                    Arc::new(field.data_type().clone()),
                )
            } else {
                self.decoders[field_idx].validate_row(tape, pos, row_idx)
            };
        result.map_err(|mut errors| {
            // Add field name for leaf validator errors that lack field context.
            for error in &mut errors {
                // Preserve field names already set by nested validators.
                if error.field_name.is_none() {
                    error.field_name = Some(field.name());
                }
            }
            errors
        })
    }
}

fn struct_fields(data_type: &DataType) -> &Fields {
    match &data_type {
        DataType::Struct(f) => f,
        _ => unreachable!(),
    }
}

fn build_field_index(fields: &Fields) -> Option<HashMap<String, usize>> {
    // Heuristic threshold: for small field counts, linear scan avoids HashMap overhead.
    const FIELD_INDEX_LINEAR_THRESHOLD: usize = 16;
    if fields.len() < FIELD_INDEX_LINEAR_THRESHOLD {
        return None;
    }

    let mut map = HashMap::with_capacity(fields.len());
    for (idx, field) in fields.iter().enumerate() {
        let name = field.name();
        if !map.contains_key(name) {
            map.insert(name.to_string(), idx);
        }
    }
    Some(map)
}
