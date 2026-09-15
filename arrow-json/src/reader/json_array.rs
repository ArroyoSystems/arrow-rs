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

use crate::reader::ArrayDecoder;
use crate::reader::tape::{Tape, TapeElement};
use arrow_array::ArrayRef;
use arrow_array::builder::GenericStringBuilder;
use arrow_schema::ArrowError;
use std::sync::Arc;

pub struct JsonArrayDecoder;

fn push_json_escaped(dst: &mut String, s: &str) {
    for ch in s.chars() {
        match ch {
            '"' => dst.push_str("\\\""),
            '\\' => dst.push_str("\\\\"),
            '\u{08}' => dst.push_str("\\b"),
            '\u{0C}' => dst.push_str("\\f"),
            '\n' => dst.push_str("\\n"),
            '\r' => dst.push_str("\\r"),
            '\t' => dst.push_str("\\t"),
            c if (c as u32) < 0x20 => {
                use std::fmt::Write as _;
                let _ = write!(dst, "\\u{:04X}", c as u32);
            }
            c => dst.push(c),
        }
    }
}

impl JsonArrayDecoder {
    pub fn new() -> Self {
        Self
    }

    fn decode_int(s: &mut String, tape: &Tape<'_>, pos: u32) -> Result<(), ArrowError> {
        match tape.get(pos) {
            TapeElement::StartObject(end) => {
                s.push('{');
                let mut cur_idx = pos + 1;
                let mut key = true;
                while cur_idx < end {
                    Self::decode_int(s, tape, cur_idx)?;
                    cur_idx = tape.next(cur_idx, "json")?;
                    if cur_idx < end {
                        if key {
                            s.push(':');
                        } else {
                            s.push(',');
                        }
                        key = !key;
                    }
                }

                s.push('}');
            }
            TapeElement::StartList(end) => {
                s.push('[');

                let mut cur_idx = pos + 1;
                while cur_idx < end {
                    Self::decode_int(s, tape, cur_idx)?;
                    cur_idx = tape.next(cur_idx, "json")?;
                    if cur_idx < end {
                        s.push(',');
                    }
                }

                s.push(']');
            }
            TapeElement::String(idx) => {
                s.push('"');
                push_json_escaped(s, tape.get_string(idx));
                s.push('"');
            }
            TapeElement::Number(idx) => s.push_str(tape.get_string(idx)),
            TapeElement::True => {
                s.push_str("true");
            }
            TapeElement::False => s.push_str("false"),
            TapeElement::Null => {
                s.push_str("null");
            }
            el => {
                unreachable!("unexpected {:?}", el);
            }
        }

        Ok(())
    }
}

impl ArrayDecoder for JsonArrayDecoder {
    fn decode(&mut self, tape: &Tape<'_>, pos: &[u32]) -> Result<ArrayRef, ArrowError> {
        let mut builder = GenericStringBuilder::<i32>::new();

        for p in pos {
            // the struct decoder uses 0 as a sentinel value to mark missing fields, we use this
            // to distinguish between explicit JSON `null` values (which are carried forward as
            // a non-null arrow field) and null arrow values
            if *p == 0 {
                builder.append_null();
                continue;
            }

            let mut s = String::with_capacity(32);
            Self::decode_int(&mut s, tape, *p)?;
            builder.append_value(s);
        }

        Ok(Arc::new(builder.finish()))
    }

    fn validate_row<'tape>(
        &'tape self,
        _tape: &'tape Tape<'_>,
        _pos: u32,
        _row_idx: usize,
    ) -> Result<(), Vec<super::ErrorMarker<'tape>>> {
        Ok(())
    }
}
