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

use crate::reader::tape::{Tape, TapeElement};
use crate::reader::validation::{ErrorMarker, FailureKind};
use crate::reader::ArrayDecoder;
use arrow_data::{ArrayData, ArrayDataBuilder};
use arrow_schema::{ArrowError, DataType};

pub struct NullArrayDecoder {
    data_type: Arc<DataType>,
}

impl Default for NullArrayDecoder {
    fn default() -> Self {
        Self::new()
    }
}

impl NullArrayDecoder {
    pub fn new() -> Self {
        Self {
            data_type: Arc::new(DataType::Null),
        }
    }
}

impl ArrayDecoder for NullArrayDecoder {
    fn decode(&mut self, tape: &Tape<'_>, pos: &[u32]) -> Result<ArrayData, ArrowError> {
        for p in pos {
            if !matches!(tape.get(*p), TapeElement::Null) {
                return Err(tape.error(*p, "null"));
            }
        }
        ArrayDataBuilder::new(DataType::Null).len(pos.len()).build()
    }

    fn validate_row<'tape>(
        &'tape self,
        tape: &'tape Tape<'_>,
        pos: u32,
        row_idx: usize,
    ) -> Result<(), Vec<ErrorMarker<'tape>>> {
        let failure = match tape.get(pos) {
            TapeElement::Null => return Ok(()),
            _ => FailureKind::TypeMismatch,
        };

        ErrorMarker::err(row_idx, pos, failure, Arc::clone(&self.data_type))
    }
}
