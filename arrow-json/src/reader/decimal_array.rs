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

use std::marker::PhantomData;
use std::sync::Arc;

use arrow_array::ArrayRef;
use arrow_array::builder::PrimitiveBuilder;
use arrow_array::types::DecimalType;
use arrow_cast::parse::parse_decimal;
use arrow_schema::ArrowError;

use crate::reader::tape::{Tape, TapeElement};
use crate::reader::{ArrayDecoder, DecoderContext};

pub struct DecimalArrayDecoder<D: DecimalType> {
    precision: u8,
    scale: i8,
    ignore_type_conflicts: bool,
    is_nullable: bool,
    // Invariant and Send
    phantom: PhantomData<fn(D) -> D>,
}

impl<D: DecimalType> DecimalArrayDecoder<D> {
    pub fn new(ctx: &DecoderContext, precision: u8, scale: i8, is_nullable: bool) -> Self {
        Self {
            precision,
            scale,
            ignore_type_conflicts: ctx.ignore_type_conflicts(),
            is_nullable,
            phantom: PhantomData,
        }
    }
}

impl<D> ArrayDecoder for DecimalArrayDecoder<D>
where
    D: DecimalType,
{
    fn decode(&mut self, tape: &Tape<'_>, pos: &[u32]) -> Result<ArrayRef, ArrowError> {
        let mut builder = PrimitiveBuilder::<D>::with_capacity(pos.len());

        #[allow(unused)] // initial value overwritten without ever being read
        let mut anchor = String::default();
        for p in pos {
            let value = match tape.get(*p) {
                TapeElement::Null => {
                    builder.append_null();
                    continue;
                }
                TapeElement::String(idx) | TapeElement::Number(idx) => tape.get_string(idx),
                TapeElement::I64(high) => match tape.get(*p + 1) {
                    TapeElement::I32(low) => {
                        anchor = (((high as i64) << 32) | (low as u32) as i64).to_string();
                        anchor.as_str()
                    }
                    _ => unreachable!(),
                },
                TapeElement::I32(val) => {
                    anchor = val.to_string();
                    anchor.as_str()
                }
                TapeElement::F64(high) => match tape.get(*p + 1) {
                    TapeElement::F32(low) => {
                        anchor = f64::from_bits(((high as u64) << 32) | low as u64).to_string();
                        anchor.as_str()
                    }
                    _ => unreachable!(),
                },
                TapeElement::F32(val) => {
                    anchor = f32::from_bits(val).to_string();
                    anchor.as_str()
                }
                _ if self.ignore_type_conflicts => {
                    builder.append_null();
                    continue;
                }
                _ => return Err(tape.error(*p, "decimal")),
            };

            match parse_decimal::<D>(value, self.precision, self.scale) {
                Ok(value) => builder.append_value(value),
                Err(_) if self.ignore_type_conflicts => builder.append_null(),
                Err(e) => return Err(e),
            }
        }

        Ok(Arc::new(
            builder
                .finish()
                .with_precision_and_scale(self.precision, self.scale)?,
        ))
    }

    fn validate_row(&self, tape: &Tape<'_>, pos: u32) -> bool {
        match tape.get(pos) {
            TapeElement::Null => self.is_nullable,
            TapeElement::String(idx) => {
                let s = tape.get_string(idx);
                parse_decimal::<D>(s, self.precision, self.scale).is_ok()
            }
            TapeElement::Number(idx) => {
                let s = tape.get_string(idx);
                parse_decimal::<D>(s, self.precision, self.scale).is_ok()
            }
            TapeElement::I32(v) => {
                parse_decimal::<D>(&v.to_string(), self.precision, self.scale).is_ok()
            }
            TapeElement::F32(v) => {
                parse_decimal::<D>(&f32::from_bits(v).to_string(), self.precision, self.scale)
                    .is_ok()
            }
            TapeElement::I64(high) => match tape.get(pos + 1) {
                TapeElement::I32(low) => {
                    let v = ((high as i64) << 32) | (low as u32) as i64;
                    parse_decimal::<D>(&v.to_string(), self.precision, self.scale).is_ok()
                }
                _ => unreachable!(),
            },
            TapeElement::F64(high) => match tape.get(pos + 1) {
                TapeElement::F32(low) => {
                    let v = f64::from_bits(((high as u64) << 32) | low as u64);
                    parse_decimal::<D>(&v.to_string(), self.precision, self.scale).is_ok()
                }
                _ => unreachable!(),
            },
            _ => false,
        }
    }
}
