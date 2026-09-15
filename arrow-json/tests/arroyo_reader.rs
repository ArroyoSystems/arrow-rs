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

use arrow_json::reader::BinaryEncoding;
use arrow_json::{ReaderBuilder, StructMode};
use arrow_schema::{DataType, Field};

#[test]
fn bad_row_filtering_supports_new_arrow_types() {
    let child = Arc::new(Field::new("item", DataType::Int32, false));
    for (data_type, input) in [
        (DataType::Utf8View, "\"ok\"\n{}\nnull"),
        (DataType::ListView(child.clone()), "[1]\n[null]\nnull"),
        (DataType::LargeListView(child.clone()), "[1]\n[null]\nnull"),
        (DataType::FixedSizeList(child, 2), "[1,2]\n[1]\n[1,null]"),
        (DataType::Decimal32(8, 2), "1.25\n\"bad\"\nnull"),
        (DataType::Decimal64(12, 2), "1.25\n\"bad\"\nnull"),
        (DataType::BinaryView, "\"6162\"\n\"zz\"\nnull"),
        (DataType::FixedSizeBinary(2), "\"6162\"\n\"61\"\nnull"),
        (
            DataType::RunEndEncoded(
                Arc::new(Field::new("run_ends", DataType::Int32, false)),
                Arc::new(Field::new("values", DataType::Int32, false)),
            ),
            "1\n\"bad\"\nnull",
        ),
    ] {
        let mut decoder =
            ReaderBuilder::new_with_field(Field::new("value", data_type.clone(), false))
                .with_binary_encoding(BinaryEncoding::Hex)
                .with_allow_bad_data(true)
                .build_decoder()
                .unwrap();
        decoder.decode(input.as_bytes()).unwrap();
        let batch = decoder.flush().unwrap();
        assert_eq!(batch.unwrap().num_rows(), 1, "{data_type}");
    }
}

#[test]
fn bad_row_filtering_respects_struct_modes_and_child_nullability() {
    let data_type = DataType::Struct(vec![Field::new("n", DataType::Int32, false)].into());
    for (mode, input) in [
        (StructMode::ObjectOnly, "{\"n\":1}\n{\"n\":null}\n{}"),
        (StructMode::ListOnly, "[1]\n[null]\n[]\n[1,2]"),
    ] {
        let mut decoder =
            ReaderBuilder::new_with_field(Field::new("value", data_type.clone(), true))
                .with_struct_mode(mode)
                .with_allow_bad_data(true)
                .build_decoder()
                .unwrap();
        decoder.decode(input.as_bytes()).unwrap();
        assert_eq!(decoder.flush().unwrap().unwrap().num_rows(), 1);
    }
}

#[test]
fn unlimited_batch_input_and_serialized_decimals() {
    let mut decoder =
        ReaderBuilder::new_with_field(Field::new("value", DataType::Decimal64(12, 2), false))
            .with_batch_size(1)
            .with_limit_to_batch_size(false)
            .with_allow_bad_data(true)
            .build_decoder()
            .unwrap();
    let input = b"1.25\n2.5\n3.75\n";
    assert_eq!(decoder.decode(input).unwrap(), input.len());
    assert_eq!(decoder.flush().unwrap().unwrap().num_rows(), 3);
    decoder.serialize(&[1.25_f64, 2.5]).unwrap();
    assert_eq!(decoder.flush().unwrap().unwrap().num_rows(), 2);
}

#[test]
fn binary_base64_and_hex_are_explicit_and_filter_invalid_rows() {
    use arrow_array::Array;
    use arrow_array::cast::AsArray;
    for data_type in [
        DataType::Binary,
        DataType::LargeBinary,
        DataType::BinaryView,
        DataType::FixedSizeBinary(2),
    ] {
        for (encoding, value) in [
            (BinaryEncoding::Base64, "YWI="),
            (BinaryEncoding::Hex, "6162"),
        ] {
            let mut decoder =
                ReaderBuilder::new_with_field(Field::new("value", data_type.clone(), true))
                    .with_binary_encoding(encoding)
                    .with_allow_bad_data(true)
                    .build_decoder()
                    .unwrap();
            decoder
                .decode(format!("\"{value}\"\n\"!\"\nnull\n").as_bytes())
                .unwrap();
            let batch = decoder.flush().unwrap().unwrap();
            let array = batch.column(0);
            assert_eq!(array.len(), 2);
            assert!(array.is_null(1));
            let bytes = match data_type {
                DataType::Binary => array.as_binary::<i32>().value(0),
                DataType::LargeBinary => array.as_binary::<i64>().value(0),
                DataType::BinaryView => array.as_binary_view().value(0),
                DataType::FixedSizeBinary(_) => array.as_fixed_size_binary().value(0),
                _ => unreachable!(),
            };
            assert_eq!(bytes, b"ab");
        }
    }
    let mut decoder = ReaderBuilder::new_with_field(Field::new("value", DataType::Binary, false))
        .build_decoder()
        .unwrap();
    decoder.decode(b"\"aGVsbG8=\"\n").unwrap();
    assert_eq!(
        decoder
            .flush()
            .unwrap()
            .unwrap()
            .column(0)
            .as_binary::<i32>()
            .value(0),
        b"hello"
    );
}
