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

use crate::reader::tape::{Tape, TapeElement};
use crate::reader::ArrayDecoder;
use arrow_array::builder::GenericBinaryBuilder;
use arrow_array::{Array, GenericStringArray, OffsetSizeTrait};
use arrow_data::ArrayData;
use arrow_schema::ArrowError;
use base64::prelude::BASE64_STANDARD;
use base64::Engine;
use std::marker::PhantomData;

pub struct BinaryArrayDecoder<O: OffsetSizeTrait> {
    is_nullable: bool,
    phantom: PhantomData<O>,
}

impl<O: OffsetSizeTrait> BinaryArrayDecoder<O> {
    pub fn new(is_nullable: bool) -> Self {
        Self {
            is_nullable,
            phantom: Default::default(),
        }
    }
}

impl<O: OffsetSizeTrait> ArrayDecoder for BinaryArrayDecoder<O> {
    fn decode(&mut self, tape: &Tape<'_>, pos: &[u32]) -> Result<ArrayData, ArrowError> {
        let mut data_capacity = 0;
        for p in pos {
            match tape.get(*p) {
                TapeElement::String(idx) => {
                    data_capacity += base64::decoded_len_estimate(tape.get_string(idx).len());
                }
                TapeElement::Null => {}
                _ => {
                    return Err(tape.error(*p, "string"));
                }
            }
        }

        if O::from_usize(data_capacity).is_none() {
            return Err(ArrowError::JsonError(format!(
                "offset overflow decoding {}",
                GenericStringArray::<O>::DATA_TYPE
            )));
        }

        let mut builder = GenericBinaryBuilder::<O>::with_capacity(pos.len(), data_capacity);

        for p in pos {
            match tape.get(*p) {
                TapeElement::String(idx) => {
                    builder.append_value(
                        BASE64_STANDARD
                            .decode(tape.get_string(idx))
                            .map_err(|_| tape.error(*p, "base64-encoded binary"))?,
                    );
                }
                TapeElement::Null => builder.append_null(),
                _ => unreachable!(),
            }
        }

        Ok(builder.finish().into_data())
    }

    fn validate_row(&self, tape: &Tape<'_>, pos: u32) -> bool {
        match tape.get(pos) {
            TapeElement::String(p) => BASE64_STANDARD.decode(&tape.get_string(p)).is_ok(),
            TapeElement::Null => self.is_nullable,
            _ => false,
        }
    }
}
