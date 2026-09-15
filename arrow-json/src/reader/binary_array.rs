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

use std::io::Write;
use std::marker::PhantomData;
use std::sync::Arc;

use arrow_array::builder::{BinaryViewBuilder, FixedSizeBinaryBuilder, GenericBinaryBuilder};
use arrow_array::{ArrayRef, GenericStringArray, OffsetSizeTrait};
use arrow_schema::ArrowError;

use crate::reader::tape::{Tape, TapeElement};
use crate::reader::validation::{ErrorMarker, FailureKind};
use crate::reader::{ArrayDecoder, BinaryEncoding};
use base64::{Engine, prelude::BASE64_STANDARD};

#[inline]
fn decode_hex_digit(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        _ => None,
    }
}

fn invalid_hex_error_at(index: usize, byte: u8) -> ArrowError {
    ArrowError::JsonError(format!(
        "invalid hex encoding in binary data: invalid digit 0x{byte:02x} at position {index}"
    ))
}

fn decode_hex_to_writer<W: Write>(hex_string: &str, writer: &mut W) -> Result<(), ArrowError> {
    let bytes = hex_string.as_bytes();
    let mut iter = bytes.chunks_exact(2);
    let mut buffer = [0u8; 64];
    let mut buffered = 0;

    for (pair_index, pair) in (&mut iter).enumerate() {
        let base = pair_index * 2;
        let high = decode_hex_digit(pair[0]).ok_or_else(|| invalid_hex_error_at(base, pair[0]))?;
        let low =
            decode_hex_digit(pair[1]).ok_or_else(|| invalid_hex_error_at(base + 1, pair[1]))?;
        buffer[buffered] = (high << 4) | low;
        buffered += 1;

        if buffered == buffer.len() {
            writer
                .write_all(&buffer)
                .map_err(|e| ArrowError::JsonError(format!("failed to write binary data: {e}")))?;
            buffered = 0;
        }
    }

    let remainder = iter.remainder();
    if !remainder.is_empty() {
        let index = (bytes.len() / 2) * 2;
        let low = decode_hex_digit(remainder[0])
            .ok_or_else(|| invalid_hex_error_at(index, remainder[0]))?;
        buffer[buffered] = low;
        buffered += 1;
    }

    if buffered > 0 {
        writer
            .write_all(&buffer[..buffered])
            .map_err(|e| ArrowError::JsonError(format!("failed to write binary data: {e}")))?;
    }

    Ok(())
}

#[derive(Default)]
pub struct BinaryArrayDecoder<O: OffsetSizeTrait> {
    phantom: PhantomData<O>,
    is_nullable: bool,
    encoding: BinaryEncoding,
}

impl<O: OffsetSizeTrait> BinaryArrayDecoder<O> {
    pub fn new(is_nullable: bool, encoding: BinaryEncoding) -> Self {
        Self {
            encoding,
            phantom: PhantomData,
            is_nullable,
        }
    }
}

impl<O: OffsetSizeTrait> ArrayDecoder for BinaryArrayDecoder<O> {
    fn validate_row<'tape>(
        &'tape self,
        tape: &'tape Tape<'_>,
        pos: u32,
        row_idx: usize,
    ) -> Result<(), Vec<ErrorMarker<'tape>>> {
        validate_binary(
            tape,
            pos,
            self.is_nullable,
            None,
            self.encoding,
            row_idx,
            arrow_array::GenericBinaryArray::<O>::DATA_TYPE,
        )
    }

    fn decode(&mut self, tape: &Tape<'_>, pos: &[u32]) -> Result<ArrayRef, ArrowError> {
        let data_capacity = estimate_data_capacity(tape, pos)?;

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
                    let string = tape.get_string(idx);
                    // Decode directly into the builder for performance. If decoding fails,
                    // the error is terminal and the builder is discarded by the caller.
                    decode_binary_to_writer(string, &mut builder, self.encoding)?;
                    builder.append_value(b"");
                }
                TapeElement::Null => builder.append_null(),
                _ => unreachable!(),
            }
        }

        Ok(Arc::new(builder.finish()))
    }
}

#[derive(Default)]
pub struct FixedSizeBinaryArrayDecoder {
    len: i32,
    is_nullable: bool,
    encoding: BinaryEncoding,
}

impl FixedSizeBinaryArrayDecoder {
    pub fn new(len: i32, is_nullable: bool, encoding: BinaryEncoding) -> Self {
        Self {
            len,
            is_nullable,
            encoding,
        }
    }
}

impl ArrayDecoder for FixedSizeBinaryArrayDecoder {
    fn validate_row<'tape>(
        &'tape self,
        tape: &'tape Tape<'_>,
        pos: u32,
        row_idx: usize,
    ) -> Result<(), Vec<ErrorMarker<'tape>>> {
        validate_binary(
            tape,
            pos,
            self.is_nullable,
            Some(self.len),
            self.encoding,
            row_idx,
            arrow_schema::DataType::FixedSizeBinary(self.len),
        )
    }

    fn decode(&mut self, tape: &Tape<'_>, pos: &[u32]) -> Result<ArrayRef, ArrowError> {
        let mut builder = FixedSizeBinaryBuilder::with_capacity(pos.len(), self.len);
        // Preallocate for the decoded byte width (FixedSizeBinary len), not the hex string length.
        let mut scratch = Vec::with_capacity(self.len as usize);

        for p in pos {
            match tape.get(*p) {
                TapeElement::String(idx) => {
                    let string = tape.get_string(idx);
                    scratch.clear();
                    scratch.reserve(string.len().div_ceil(2));
                    decode_binary_to_writer(string, &mut scratch, self.encoding)?;
                    builder.append_value(&scratch)?;
                }
                TapeElement::Null => builder.append_null(),
                _ => unreachable!(),
            }
        }

        Ok(Arc::new(builder.finish()))
    }
}

#[derive(Default)]
pub struct BinaryViewDecoder {
    is_nullable: bool,
    encoding: BinaryEncoding,
}

impl BinaryViewDecoder {
    pub fn new(is_nullable: bool, encoding: BinaryEncoding) -> Self {
        Self {
            is_nullable,
            encoding,
        }
    }
}

impl ArrayDecoder for BinaryViewDecoder {
    fn validate_row<'tape>(
        &'tape self,
        tape: &'tape Tape<'_>,
        pos: u32,
        row_idx: usize,
    ) -> Result<(), Vec<ErrorMarker<'tape>>> {
        validate_binary(
            tape,
            pos,
            self.is_nullable,
            None,
            self.encoding,
            row_idx,
            arrow_schema::DataType::BinaryView,
        )
    }

    fn decode(&mut self, tape: &Tape<'_>, pos: &[u32]) -> Result<ArrayRef, ArrowError> {
        let data_capacity = estimate_data_capacity(tape, pos)?;
        let mut builder = BinaryViewBuilder::with_capacity(data_capacity);
        let mut scratch = Vec::new();

        for p in pos {
            match tape.get(*p) {
                TapeElement::String(idx) => {
                    let string = tape.get_string(idx);
                    scratch.clear();
                    scratch.reserve(string.len().div_ceil(2));
                    decode_binary_to_writer(string, &mut scratch, self.encoding)?;
                    builder.append_value(&scratch);
                }
                TapeElement::Null => builder.append_null(),
                _ => unreachable!(),
            }
        }

        Ok(Arc::new(builder.finish()))
    }
}

fn decode_binary_to_writer<W: Write>(
    value: &str,
    writer: &mut W,
    encoding: BinaryEncoding,
) -> Result<(), ArrowError> {
    match encoding {
        BinaryEncoding::Hex => decode_hex_to_writer(value, writer),
        BinaryEncoding::Base64 => {
            let bytes = BASE64_STANDARD.decode(value).map_err(|e| {
                ArrowError::JsonError(format!("invalid base64-encoded binary: {e}"))
            })?;
            writer
                .write_all(&bytes)
                .map_err(|e| ArrowError::JsonError(format!("failed to write binary data: {e}")))
        }
    }
}

fn validate_binary<'tape>(
    tape: &'tape Tape<'_>,
    pos: u32,
    is_nullable: bool,
    len: Option<i32>,
    encoding: BinaryEncoding,
    row_idx: usize,
    data_type: arrow_schema::DataType,
) -> Result<(), Vec<ErrorMarker<'tape>>> {
    let failure = match tape.get(pos) {
        TapeElement::Null if is_nullable => return Ok(()),
        TapeElement::Null => FailureKind::NullValue,
        TapeElement::String(idx) => {
            let value = tape.get_string(idx);
            let valid = match encoding {
                BinaryEncoding::Hex => {
                    len.is_none_or(|len| usize::try_from(len).ok() == Some(value.len().div_ceil(2)))
                        && decode_hex_to_writer(value, &mut std::io::sink()).is_ok()
                }
                BinaryEncoding::Base64 => BASE64_STANDARD.decode(value).is_ok_and(|v| {
                    len.is_none_or(|len| usize::try_from(len).ok() == Some(v.len()))
                }),
            };
            if valid {
                return Ok(());
            }
            FailureKind::ParseFailure
        }
        _ => FailureKind::TypeMismatch,
    };
    ErrorMarker::err(row_idx, pos, failure, Arc::new(data_type))
}

fn estimate_data_capacity(tape: &Tape<'_>, pos: &[u32]) -> Result<usize, ArrowError> {
    let mut data_capacity = 0;
    for p in pos {
        match tape.get(*p) {
            TapeElement::String(idx) => {
                let string_len = tape.get_string(idx).len();
                // Upper bound for either hex or base64, avoiding reallocation for base64.
                data_capacity += string_len;
            }
            TapeElement::Null => {}
            _ => {
                return Err(tape.error(*p, "binary data encoded as string"));
            }
        }
    }
    Ok(data_capacity)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ReaderBuilder;
    use arrow_schema::{DataType, Field};
    use std::io::Cursor;

    #[test]
    fn test_decode_hex_to_writer_empty() {
        let mut out = Vec::new();
        decode_hex_to_writer("", &mut out).unwrap();
        assert!(out.is_empty());
    }

    #[test]
    fn test_decode_hex_to_writer_odd_length() {
        let mut out = Vec::new();
        decode_hex_to_writer("0f0", &mut out).unwrap();
        assert_eq!(out, vec![0x0f, 0x00]);

        out.clear();
        decode_hex_to_writer("a", &mut out).unwrap();
        assert_eq!(out, vec![0x0a]);
    }

    #[test]
    fn test_decode_hex_to_writer_invalid() {
        let mut out = Vec::new();
        let err = decode_hex_to_writer("0f0g", &mut out).unwrap_err();
        match err {
            ArrowError::JsonError(msg) => {
                assert!(msg.contains("invalid hex encoding in binary data"));
                assert!(msg.contains("position 3"));
            }
            _ => panic!("expected JsonError"),
        }
    }

    #[test]
    fn test_binary_reader_invalid_hex_is_terminal() {
        let field = Field::new("item", DataType::Binary, false);
        let data = b"\"0f0g\"\n\"0f00\"\n";
        let mut reader = ReaderBuilder::new_with_field(field)
            .with_binary_encoding(BinaryEncoding::Hex)
            .build(Cursor::new(data))
            .unwrap();

        let err = reader.next().unwrap().unwrap_err().to_string();
        assert!(err.contains("invalid hex encoding in binary data"));

        match reader.next() {
            None => {}
            Some(Err(_)) => {}
            Some(Ok(_)) => panic!("expected terminal error after invalid hex"),
        }
    }
}
