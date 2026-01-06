use crate::reader::tape::{Tape, TapeElement};
use crate::reader::ArrayDecoder;
use arrow_array::builder::GenericStringBuilder;
use arrow_array::Array;
use arrow_data::ArrayData;
use arrow_schema::ArrowError;

pub struct JsonArrayDecoder {
    // TODO: in the future, we may want a way to distinguish between a literal null value and absent
    //  fields, however this likely requires changing the tape representation to record that
    #[allow(unused)]
    is_nullable: bool,
}

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
    pub fn new(is_nullable: bool) -> Self {
        Self { is_nullable }
    }

    fn decode_int(&self, s: &mut String, tape: &Tape<'_>, pos: u32) -> Result<(), ArrowError> {
        match tape.get(pos) {
            TapeElement::StartObject(end) => {
                s.push('{');
                let mut cur_idx = pos + 1;
                let mut key = true;
                while cur_idx < end {
                    self.decode_int(s, tape, cur_idx)?;
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
                    self.decode_int(s, tape, cur_idx)?;
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
    fn decode(&mut self, tape: &Tape<'_>, pos: &[u32]) -> Result<ArrayData, ArrowError> {
        let mut builder = GenericStringBuilder::<i32>::new();

        for p in pos {
            let mut s = String::with_capacity(32);
            self.decode_int(&mut s, tape, *p)?;
            builder.append_value(s);
        }

        Ok(builder.finish().into_data())
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
