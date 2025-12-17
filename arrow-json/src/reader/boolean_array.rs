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

use std::sync::Arc;

use arrow_array::builder::BooleanBuilder;
use arrow_array::Array;
use arrow_data::ArrayData;
use arrow_schema::{ArrowError, DataType};

use crate::reader::tape::{Tape, TapeElement};
use crate::reader::validation::{ErrorMarker, FailureKind};
use crate::reader::ArrayDecoder;

pub struct BooleanArrayDecoder {
    data_type: Arc<DataType>,
    is_nullable: bool,
}

impl BooleanArrayDecoder {
    pub fn new(is_nullable: bool) -> Self {
        Self {
            data_type: Arc::new(DataType::Boolean),
            is_nullable,
        }
    }
}

impl ArrayDecoder for BooleanArrayDecoder {
    fn decode(&mut self, tape: &Tape<'_>, pos: &[u32]) -> Result<ArrayData, ArrowError> {
        let mut builder = BooleanBuilder::with_capacity(pos.len());
        for p in pos {
            match tape.get(*p) {
                TapeElement::Null => builder.append_null(),
                TapeElement::True => builder.append_value(true),
                TapeElement::False => builder.append_value(false),
                _ => return Err(tape.error(*p, "boolean")),
            }
        }

        Ok(builder.finish().into_data())
    }

    fn validate_row<'tape>(
        &'tape self,
        tape: &'tape Tape<'_>,
        pos: u32,
        row_idx: usize,
    ) -> Result<(), Vec<ErrorMarker<'tape>>> {
        let failure = match tape.get(pos) {
            TapeElement::True | TapeElement::False => return Ok(()),
            TapeElement::Null => {
                if self.is_nullable {
                    return Ok(());
                }
                FailureKind::NullValue
            }
            _ => FailureKind::TypeMismatch,
        };

        ErrorMarker::err(row_idx, pos, failure, Arc::clone(&self.data_type))
    }
}
