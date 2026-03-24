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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.File;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.arrow.dataset.ParquetWriteSupport;
import org.apache.arrow.dataset.TestDataset;
import org.apache.arrow.dataset.jni.NativeMemoryPool;
import org.apache.arrow.dataset.scanner.ArrowScannerReader;
import org.apache.arrow.dataset.scanner.ScanOptions;
import org.apache.arrow.dataset.scanner.Scanner;
import org.apache.arrow.dataset.source.Dataset;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class TestDatasetFileWriter extends TestDataset {

  @TempDir public File TMP;

  public static final String AVRO_SCHEMA_USER = "user.avsc";

  @Test
  public void testParquetWriteSimple() throws Exception {
    ParquetWriteSupport writeSupport =
        ParquetWriteSupport.writeTempFile(AVRO_SCHEMA_USER, TMP, 1, "a", 2, "b", 3, "c", 2, "d");
    String sampleParquet = writeSupport.getOutputURI();
    ScanOptions options = new ScanOptions(new String[0], 100);
    final File writtenFolder = new File(TMP, "writtenFolder");
    writtenFolder.mkdirs();
    final String writtenParquet = writtenFolder.toURI().toString();
    try (FileSystemDatasetFactory factory =
            new FileSystemDatasetFactory(
                rootAllocator(), NativeMemoryPool.getDefault(), FileFormat.PARQUET, sampleParquet);
        final Dataset dataset = factory.finish();
        final Scanner scanner = dataset.newScan(options);
        final ArrowScannerReader reader = new ArrowScannerReader(scanner, rootAllocator()); ) {
      DatasetFileWriter.write(rootAllocator(), reader, FileFormat.PARQUET, writtenParquet);
      assertParquetFileEquals(
          sampleParquet, Objects.requireNonNull(writtenFolder.listFiles())[0].toURI().toString());
    }
  }

  @Test
  public void testParquetWriteWithPartitions() throws Exception {
    ParquetWriteSupport writeSupport =
        ParquetWriteSupport.writeTempFile(AVRO_SCHEMA_USER, TMP, 1, "a", 2, "b", 3, "c", 2, "d");
    String sampleParquet = writeSupport.getOutputURI();
    ScanOptions options = new ScanOptions(new String[0], 100);
    final File writtenFolder = new File(TMP, "writtenFolder");
    writtenFolder.mkdirs();
    final String writtenParquet = writtenFolder.toURI().toString();

    try (FileSystemDatasetFactory factory =
            new FileSystemDatasetFactory(
                rootAllocator(), NativeMemoryPool.getDefault(), FileFormat.PARQUET, sampleParquet);
        final Dataset dataset = factory.finish();
        final Scanner scanner = dataset.newScan(options);
        final ArrowScannerReader reader = new ArrowScannerReader(scanner, rootAllocator()); ) {
      DatasetFileWriter.write(
          rootAllocator(),
          reader,
          FileFormat.PARQUET,
          writtenParquet,
          new String[] {"id", "name"},
          100,
          "data_{i}");
      final Set<String> expectedOutputFiles =
          new HashSet<>(
              Arrays.asList(
                  "id=1/name=a/data_0",
                  "id=2/name=b/data_0",
                  "id=3/name=c/data_0",
                  "id=2/name=d/data_0"));
      final Set<String> outputFiles =
          FileUtils.listFiles(writtenFolder, null, true).stream()
              .map(
                  file -> {
                    return writtenFolder.toURI().relativize(file.toURI()).toString();
                  })
              .collect(Collectors.toSet());
      assertEquals(expectedOutputFiles, outputFiles);
    }
  }

  @Test
  public void testParquetWriteWithCompressionOption() throws Exception {
    ParquetWriteSupport writeSupport =
        ParquetWriteSupport.writeTempFile(AVRO_SCHEMA_USER, TMP, 1, "a", 2, "b", 3, "c", 2, "d");
    String sampleParquet = writeSupport.getOutputURI();
    ScanOptions options = new ScanOptions(100);
    final File writtenFolder = new File(TMP, "writtenGzip");
    writtenFolder.mkdirs();
    final String writtenParquet = writtenFolder.toURI().toString();

    try (FileSystemDatasetFactory factory =
            new FileSystemDatasetFactory(
                rootAllocator(), NativeMemoryPool.getDefault(), FileFormat.PARQUET, sampleParquet);
        final Dataset dataset = factory.finish();
        final Scanner scanner = dataset.newScan(options);
        final ArrowScannerReader reader = new ArrowScannerReader(scanner, rootAllocator())) {
      Map<String, String> writerOptions = new HashMap<>();
      writerOptions.put("compression", "GZIP");
      DatasetFileWriter.write(
          rootAllocator(),
          reader,
          FileFormat.PARQUET,
          writtenParquet,
          new String[0],
          1024,
          "data_{i}",
          writerOptions);
      File outputFile = Objects.requireNonNull(writtenFolder.listFiles())[0];
      assertParquetFileEquals(sampleParquet, outputFile.toURI().toString());
      // Verify the written file actually uses GZIP compression
      try (ParquetFileReader parquetReader =
          ParquetFileReader.open(
              HadoopInputFile.fromPath(
                  new org.apache.hadoop.fs.Path(outputFile.toURI()), new Configuration()))) {
        parquetReader
            .getFooter()
            .getBlocks()
            .forEach(
                block ->
                    block
                        .getColumns()
                        .forEach(
                            col ->
                                assertEquals(
                                    org.apache.parquet.hadoop.metadata.CompressionCodecName.GZIP,
                                    col.getCodec())));
      }
    }
  }

  @Test
  public void testParquetWriteWithNullOptions() throws Exception {
    ParquetWriteSupport writeSupport =
        ParquetWriteSupport.writeTempFile(AVRO_SCHEMA_USER, TMP, 1, "a", 2, "b", 3, "c", 2, "d");
    String sampleParquet = writeSupport.getOutputURI();
    ScanOptions options = new ScanOptions(100);
    final File writtenFolder = new File(TMP, "writtenNullOptions");
    writtenFolder.mkdirs();
    final String writtenParquet = writtenFolder.toURI().toString();

    try (FileSystemDatasetFactory factory =
            new FileSystemDatasetFactory(
                rootAllocator(), NativeMemoryPool.getDefault(), FileFormat.PARQUET, sampleParquet);
        final Dataset dataset = factory.finish();
        final Scanner scanner = dataset.newScan(options);
        final ArrowScannerReader reader = new ArrowScannerReader(scanner, rootAllocator())) {
      DatasetFileWriter.write(
          rootAllocator(),
          reader,
          FileFormat.PARQUET,
          writtenParquet,
          new String[0],
          1024,
          "data_{i}",
          null);
      assertParquetFileEquals(
          sampleParquet, Objects.requireNonNull(writtenFolder.listFiles())[0].toURI().toString());
    }
  }

  @Test
  public void testParquetWriteWithEmptyOptions() throws Exception {
    ParquetWriteSupport writeSupport =
        ParquetWriteSupport.writeTempFile(AVRO_SCHEMA_USER, TMP, 1, "a", 2, "b", 3, "c", 2, "d");
    String sampleParquet = writeSupport.getOutputURI();
    ScanOptions options = new ScanOptions(100);
    final File writtenFolder = new File(TMP, "writtenEmptyOptions");
    writtenFolder.mkdirs();
    final String writtenParquet = writtenFolder.toURI().toString();

    try (FileSystemDatasetFactory factory =
            new FileSystemDatasetFactory(
                rootAllocator(), NativeMemoryPool.getDefault(), FileFormat.PARQUET, sampleParquet);
        final Dataset dataset = factory.finish();
        final Scanner scanner = dataset.newScan(options);
        final ArrowScannerReader reader = new ArrowScannerReader(scanner, rootAllocator())) {
      DatasetFileWriter.write(
          rootAllocator(), reader, FileFormat.PARQUET, writtenParquet, Collections.emptyMap());
      assertParquetFileEquals(
          sampleParquet, Objects.requireNonNull(writtenFolder.listFiles())[0].toURI().toString());
    }
  }

  @Test
  public void testParquetWriteWithInvalidCompression() throws Exception {
    ParquetWriteSupport writeSupport =
        ParquetWriteSupport.writeTempFile(AVRO_SCHEMA_USER, TMP, 1, "a", 2, "b", 3, "c", 2, "d");
    String sampleParquet = writeSupport.getOutputURI();
    ScanOptions options = new ScanOptions(100);
    final File writtenFolder = new File(TMP, "writtenInvalidCompression");
    writtenFolder.mkdirs();
    final String writtenParquet = writtenFolder.toURI().toString();

    try (FileSystemDatasetFactory factory =
            new FileSystemDatasetFactory(
                rootAllocator(), NativeMemoryPool.getDefault(), FileFormat.PARQUET, sampleParquet);
        final Dataset dataset = factory.finish();
        final Scanner scanner = dataset.newScan(options);
        final ArrowScannerReader reader = new ArrowScannerReader(scanner, rootAllocator())) {
      Map<String, String> writerOptions = new HashMap<>();
      writerOptions.put("compression", "INVALID_CODEC");
      assertThrows(
          RuntimeException.class,
          () ->
              DatasetFileWriter.write(
                  rootAllocator(),
                  reader,
                  FileFormat.PARQUET,
                  writtenParquet,
                  new String[0],
                  1024,
                  "data_{i}",
                  writerOptions));
    }
  }

  @Test
  public void testIpcWriteWithOptionsThrows() throws Exception {
    ParquetWriteSupport writeSupport =
        ParquetWriteSupport.writeTempFile(AVRO_SCHEMA_USER, TMP, 1, "a", 2, "b", 3, "c", 2, "d");
    String sampleParquet = writeSupport.getOutputURI();
    ScanOptions options = new ScanOptions(100);
    final File writtenFolder = new File(TMP, "writtenIpcWithOptions");
    writtenFolder.mkdirs();
    final String writtenUri = writtenFolder.toURI().toString();

    try (FileSystemDatasetFactory factory =
            new FileSystemDatasetFactory(
                rootAllocator(), NativeMemoryPool.getDefault(), FileFormat.PARQUET, sampleParquet);
        final Dataset dataset = factory.finish();
        final Scanner scanner = dataset.newScan(options);
        final ArrowScannerReader reader = new ArrowScannerReader(scanner, rootAllocator())) {
      Map<String, String> writerOptions = new HashMap<>();
      writerOptions.put("compression", "GZIP");
      assertThrows(
          RuntimeException.class,
          () ->
              DatasetFileWriter.write(
                  rootAllocator(),
                  reader,
                  FileFormat.ARROW_IPC,
                  writtenUri,
                  new String[0],
                  1024,
                  "data_{i}",
                  writerOptions));
    }
  }
}
