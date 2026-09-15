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

#![cfg(feature = "arrow")]

use arrow_array::cast::AsArray;
use arrow_array::{Int32Array, RecordBatch};
use bytes::Bytes;
use parquet::arrow::ArrowWriter;
use parquet::arrow::arrow_reader::{ArrowReaderOptions, ParquetRecordBatchReaderBuilder};
use parquet::file::metadata::PageIndexPolicy;
use parquet::file::metadata::{KeyValue, ParquetMetaData};
use parquet::file::properties::{
    BloomFilterPosition, EnabledStatistics, ReaderProperties, WriterProperties,
};
use parquet::file::reader::FileReader;
use parquet::file::serialized_reader::{ReadOptionsBuilder, SerializedFileReader};
use std::io::{self, Write};
use std::sync::{Arc, Mutex};

#[derive(Clone, Default)]
struct Output {
    bytes: Arc<Mutex<Vec<u8>>>,
    remaining: Option<usize>,
    fail_flush: bool,
}

impl Output {
    fn bytes(&self) -> Vec<u8> {
        self.bytes.lock().unwrap().clone()
    }
}

impl Write for Output {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        let len = match &mut self.remaining {
            Some(0) => return Err(io::Error::other("injected target write failure")),
            Some(remaining) => {
                let len = bytes.len().min(*remaining);
                *remaining -= len;
                len
            }
            None => bytes.len(),
        };
        self.bytes.lock().unwrap().extend_from_slice(&bytes[..len]);
        Ok(len)
    }
    fn flush(&mut self) -> io::Result<()> {
        if self.fail_flush {
            Err(io::Error::other("injected target flush failure"))
        } else {
            Ok(())
        }
    }
}

fn batch(values: Vec<i32>) -> RecordBatch {
    RecordBatch::try_from_iter([("value", Arc::new(Int32Array::from(values)) as _)]).unwrap()
}

fn snapshot(writer: &mut ArrowWriter<Output>) -> (Vec<u8>, ParquetMetaData) {
    let (suffix, metadata) = writer.get_trailing_bytes(Output::default()).unwrap();
    let mut bytes = writer.inner().bytes();
    assert_eq!(bytes.len(), writer.bytes_written());
    bytes.extend_from_slice(&suffix.bytes());
    (bytes, metadata)
}

fn read_values(bytes: Vec<u8>, options: ArrowReaderOptions) -> Vec<i32> {
    ParquetRecordBatchReaderBuilder::try_new_with_options(Bytes::from(bytes), options)
        .unwrap()
        .build()
        .unwrap()
        .flat_map(|b| {
            b.unwrap()
                .column(0)
                .as_primitive::<arrow_array::types::Int32Type>()
                .values()
                .to_vec()
        })
        .collect()
}

fn check_snapshot(bytes: Vec<u8>, expected: &[i32], metadata: &ParquetMetaData, indexes: bool) {
    assert_eq!(metadata.file_metadata().num_rows(), expected.len() as i64);
    let options = ArrowReaderOptions::new().with_page_index_policy(if indexes {
        PageIndexPolicy::Required
    } else {
        PageIndexPolicy::Skip
    });
    let reader = ParquetRecordBatchReaderBuilder::try_new_with_options(
        Bytes::from(bytes.clone()),
        options.clone(),
    )
    .unwrap();
    // The reader normalizes encoding-stat caches and legacy statistics flags.
    // Compare the persisted row counts, offsets and statistics, not those caches.
    assert_eq!(
        reader.metadata().num_row_groups(),
        metadata.num_row_groups()
    );
    for (actual, expected) in reader
        .metadata()
        .row_groups()
        .iter()
        .zip(metadata.row_groups())
    {
        assert_eq!(actual.num_rows(), expected.num_rows());
        assert_eq!(actual.file_offset(), expected.file_offset());
        let (actual, expected) = (actual.column(0), expected.column(0));
        assert_eq!(actual.data_page_offset(), expected.data_page_offset());
        assert_eq!(
            actual.dictionary_page_offset(),
            expected.dictionary_page_offset()
        );
        assert_eq!(actual.compressed_size(), expected.compressed_size());
        assert_eq!(actual.bloom_filter_offset(), expected.bloom_filter_offset());
        assert_eq!(actual.column_index_offset(), expected.column_index_offset());
        assert_eq!(actual.offset_index_offset(), expected.offset_index_offset());
        let (actual, expected) = (actual.statistics().unwrap(), expected.statistics().unwrap());
        assert_eq!(actual.min_bytes_opt(), expected.min_bytes_opt());
        assert_eq!(actual.max_bytes_opt(), expected.max_bytes_opt());
        assert_eq!(actual.null_count_opt(), expected.null_count_opt());
    }
    if indexes && !expected.is_empty() {
        assert_eq!(reader.metadata().column_index(), metadata.column_index());
        assert_eq!(reader.metadata().offset_index(), metadata.offset_index());
        for rg in reader.metadata().offset_index().unwrap() {
            assert_eq!(rg[0].page_locations().len(), 2);
        }
    }
    let reader = SerializedFileReader::new_with_options(
        Bytes::from(bytes.clone()),
        ReadOptionsBuilder::new()
            .with_reader_properties(
                ReaderProperties::builder()
                    .set_read_bloom_filter(true)
                    .build(),
            )
            .build(),
    )
    .unwrap();
    for rg in 0..reader.num_row_groups() {
        let row_group = reader.get_row_group(rg).unwrap();
        let bloom = row_group.get_column_bloom_filter(0).unwrap();
        for value in &expected[rg * 2..rg * 2 + 2] {
            assert!(bloom.check(value));
        }
    }
    assert_eq!(read_values(bytes, options), expected);
}

#[test]
fn repeated_checkpoints_keep_rows_indexes_blooms_and_live_output() {
    for position in [BloomFilterPosition::AfterRowGroup, BloomFilterPosition::End] {
        for indexes in [false, true] {
            let props = WriterProperties::builder()
                .set_bloom_filter_enabled(true)
                .set_bloom_filter_position(position)
                .set_bloom_filter_ndv(16)
                .set_statistics_enabled(if indexes {
                    EnabledStatistics::Page
                } else {
                    EnabledStatistics::Chunk
                })
                .set_offset_index_disabled(!indexes)
                .set_data_page_row_count_limit(1)
                .set_write_batch_size(1)
                .set_key_value_metadata(Some(vec![KeyValue::new(
                    "source".into(),
                    "arroyo".to_string(),
                )]))
                .build();
            let mut writer =
                ArrowWriter::try_new(Output::default(), batch(vec![]).schema(), Some(props))
                    .unwrap();
            let mut expected = vec![];
            for step in 0..3 {
                writer.write(&batch(vec![step * 2, step * 2 + 1])).unwrap();
                expected.extend([step * 2, step * 2 + 1]);
                let (bytes, metadata) = snapshot(&mut writer);
                let live_bytes = writer.inner().bytes();
                let live_metadata = writer.flushed_row_groups().to_vec();
                let (again, again_metadata) = snapshot(&mut writer);
                assert_eq!(bytes, again);
                assert_eq!(metadata, again_metadata);
                assert_eq!(writer.inner().bytes(), live_bytes);
                assert_eq!(writer.flushed_row_groups(), live_metadata);
                assert!(
                    metadata
                        .file_metadata()
                        .key_value_metadata()
                        .unwrap()
                        .iter()
                        .any(|kv| kv.key == "source")
                );
                check_snapshot(bytes, &expected, &metadata, indexes);
            }
            writer.write(&batch(vec![6, 7])).unwrap();
            expected.extend([6, 7]);
            let metadata = writer.finish().unwrap();
            check_snapshot(writer.inner().bytes(), &expected, &metadata, indexes);
            assert!(writer.get_trailing_bytes(Output::default()).is_err());
        }
    }
}

#[test]
fn checkpoint_output_errors_leave_live_writer_usable() {
    for target in [
        Output {
            remaining: Some(0),
            ..Default::default()
        },
        Output {
            remaining: Some(7),
            ..Default::default()
        },
        Output {
            fail_flush: true,
            ..Default::default()
        },
    ] {
        let props = WriterProperties::builder()
            .set_bloom_filter_enabled(true)
            .set_bloom_filter_position(BloomFilterPosition::End)
            .set_statistics_enabled(EnabledStatistics::Page)
            .set_data_page_row_count_limit(1)
            .set_write_batch_size(1)
            .build();
        let mut writer =
            ArrowWriter::try_new(Output::default(), batch(vec![]).schema(), Some(props)).unwrap();
        writer.write(&batch(vec![1, 2])).unwrap();
        let (before, metadata) = snapshot(&mut writer);
        let live = writer.inner().bytes();
        assert!(writer.get_trailing_bytes(target).is_err());
        assert_eq!(writer.inner().bytes(), live);
        let (after, after_metadata) = snapshot(&mut writer);
        assert_eq!(before, after);
        assert_eq!(metadata, after_metadata);
        writer.write(&batch(vec![3, 4])).unwrap();
        let metadata = writer.finish().unwrap();
        check_snapshot(writer.inner().bytes(), &[1, 2, 3, 4], &metadata, true);
    }
}

#[test]
fn empty_checkpoint_can_be_read_then_writing_continues() {
    let mut writer = ArrowWriter::try_new(Output::default(), batch(vec![]).schema(), None).unwrap();
    let (bytes, metadata) = snapshot(&mut writer);
    assert_eq!(metadata.file_metadata().num_rows(), 0);
    assert!(read_values(bytes, ArrowReaderOptions::new()).is_empty());
    writer.write(&batch(vec![42])).unwrap();
    writer.finish().unwrap();
    assert_eq!(
        read_values(writer.inner().bytes(), ArrowReaderOptions::new()),
        vec![42]
    );
}

#[cfg(feature = "encryption")]
#[test]
fn encrypted_checkpoints_and_final_output_remain_readable() {
    use parquet::encryption::{
        decrypt::FileDecryptionProperties, encrypt::FileEncryptionProperties,
    };
    let key = vec![7; 16];
    for plaintext in [false, true] {
        let encryption = FileEncryptionProperties::builder(key.clone())
            .with_plaintext_footer(plaintext)
            .build()
            .unwrap();
        let props = WriterProperties::builder()
            .with_file_encryption_properties(encryption)
            .build();
        let options = ArrowReaderOptions::new().with_file_decryption_properties(
            FileDecryptionProperties::builder(key.clone())
                .build()
                .unwrap(),
        );
        let mut writer =
            ArrowWriter::try_new(Output::default(), batch(vec![]).schema(), Some(props)).unwrap();
        for value in 0..3 {
            writer.write(&batch(vec![value])).unwrap();
            for _ in 0..2 {
                let (bytes, _) = snapshot(&mut writer);
                assert_eq!(
                    read_values(bytes, options.clone()),
                    (0..=value).collect::<Vec<_>>()
                );
            }
        }
        writer.finish().unwrap();
        assert_eq!(read_values(writer.inner().bytes(), options), vec![0, 1, 2]);
    }
}
