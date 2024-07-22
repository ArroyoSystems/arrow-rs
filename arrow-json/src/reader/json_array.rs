use crate::reader::tape::{Tape, TapeElement};
use crate::reader::ArrayDecoder;
use arrow_array::builder::GenericStringBuilder;
use arrow_array::Array;
use arrow_data::ArrayData;
use arrow_schema::ArrowError;

pub struct JsonArrayDecoder {
    is_nullable: bool,
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
                s.push_str(tape.get_string(idx));
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
            if self.is_nullable {
                if matches!(tape.get(*p), TapeElement::Null) {
                    builder.append_null();
                    continue;
                }
            }
            self.decode_int(&mut s, tape, *p)?;
            builder.append_value(s);
        }

        Ok(builder.finish().into_data())
    }

    fn validate_row(&self, tape: &Tape<'_>, pos: u32) -> bool {
        match tape.get(pos) {
            TapeElement::Null => self.is_nullable,
            _ => true,
        }
    }
}
