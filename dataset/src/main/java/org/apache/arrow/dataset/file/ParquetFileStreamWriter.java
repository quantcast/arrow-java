/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.arrow.dataset.file;

import java.util.Map;
import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.dictionary.DictionaryProvider;

/**
 * Streaming Parquet writer that keeps all written batches in a single buffered row group.
 *
 * <p>Unlike {@link DatasetFileWriter}, which uses the Arrow Dataset API and calls {@code
 * WriteTable} once per {@link org.apache.arrow.vector.ipc.ArrowReader} batch (producing one
 * row group per batch), this writer calls {@code
 * parquet::arrow::FileWriter::NewBufferedRowGroup()} once at open time and then routes each
 * batch through {@code WriteRecordBatch} so all batches land in the same row group until
 * {@code max_row_group_length} is exceeded.
 *
 * <p>Intended for workloads that produce many small batches and expect a single row group per
 * file (i.e. log-style append patterns): call {@link #writeBatch} once per completed {@link
 * VectorSchemaRoot} batch on a producer/consumer thread, then {@link #close} to finalize
 * the file.
 *
 * <p>This class is not thread safe; all {@link #writeBatch} and {@link #close} calls must
 * happen on a single thread.
 */
public class ParquetFileStreamWriter implements AutoCloseable {

  private final BufferAllocator allocator;
  private final DictionaryProvider dictionaryProvider;
  private long nativeHandle;

  /**
   * Opens a Parquet file at {@code uri} for streaming writes.
   *
   * @param allocator buffer allocator used for C Data Interface exports
   * @param root a {@link VectorSchemaRoot} with the target schema (only its schema is read at
   *     open; the root's vector contents are not consumed). The same schema must be used for
   *     every subsequent {@link #writeBatch} call.
   * @param dictionaryProvider dictionary provider for the schema, or null if no dictionaries
   *     are used.
   * @param uri target file URI (e.g. {@code file:///var/log/bid-event/foo.parquet})
   * @param writerOptions Parquet writer options, same keys accepted as {@link
   *     DatasetFileWriter}: {@code compression}, {@code data_page_size}, {@code
   *     max_row_group_length}, {@code write_batch_size}, {@code use_dictionary}. May be null
   *     or empty to use defaults.
   */
  public ParquetFileStreamWriter(
      BufferAllocator allocator,
      VectorSchemaRoot root,
      DictionaryProvider dictionaryProvider,
      String uri,
      Map<String, String> writerOptions) {
    this.allocator = allocator;
    this.dictionaryProvider = dictionaryProvider;
    try (final ArrowSchema schemaStruct = ArrowSchema.allocateNew(allocator)) {
      Data.exportSchema(allocator, root.getSchema(), dictionaryProvider, schemaStruct);
      this.nativeHandle =
          JniWrapper.get()
              .openParquetStreamWriter(schemaStruct.memoryAddress(), uri, toKeyValueArray(writerOptions));
    }
  }

  /**
   * Writes a {@link VectorSchemaRoot} as a single {@code RecordBatch} into the currently open
   * buffered row group. The native writer may start a new row group if {@code
   * max_row_group_length} would be exceeded, but otherwise all batches append to the same
   * row group.
   *
   * <p>The caller retains ownership of {@code root}; the native side copies any data it
   * needs before returning, so the caller may clear and reuse the vectors immediately after
   * this call returns.
   */
  public void writeBatch(VectorSchemaRoot root) {
    if (nativeHandle == 0L) {
      throw new IllegalStateException("writer has been closed");
    }
    try (final ArrowArray arrayStruct = ArrowArray.allocateNew(allocator);
        final ArrowSchema schemaStruct = ArrowSchema.allocateNew(allocator)) {
      Data.exportVectorSchemaRoot(allocator, root, dictionaryProvider, arrayStruct, schemaStruct);
      JniWrapper.get()
          .writeBatchToParquetStream(
              nativeHandle, arrayStruct.memoryAddress(), schemaStruct.memoryAddress());
    }
  }

  /**
   * Finalizes the Parquet file (writes the footer and closes the output stream). Idempotent.
   */
  @Override
  public void close() {
    if (nativeHandle == 0L) {
      return;
    }
    try {
      JniWrapper.get().closeParquetStreamWriter(nativeHandle);
    } finally {
      nativeHandle = 0L;
    }
  }

  private static String[] toKeyValueArray(Map<String, String> map) {
    if (map == null || map.isEmpty()) {
      return new String[0];
    }
    String[] arr = new String[map.size() * 2];
    int i = 0;
    for (Map.Entry<String, String> e : map.entrySet()) {
      if (e.getKey() == null || e.getValue() == null) {
        throw new IllegalArgumentException("Writer options must not contain null keys or values");
      }
      arr[i++] = e.getKey();
      arr[i++] = e.getValue();
    }
    return arr;
  }
}
