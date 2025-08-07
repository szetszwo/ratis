/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.ratis.benchmark;

import org.apache.ratis.util.FileUtils;
import org.apache.ratis.util.Preconditions;
import org.apache.ratis.util.SizeInBytes;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.util.Random;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.ThreadLocalRandom;

import static org.apache.ratis.benchmark.SingleFileWriter.*;
import static org.apache.ratis.benchmark.MultiFileWriter.*;

/**
 * Benchmark for writing output to disk using a single file verse multiple files.
 */
public class ReadWriteBenchmark {
  public static void main(String[] args) throws Exception {
//    Slf4jUtils.setLogLevel(FileUtils.LOG, Level.TRACE);

    final String totalSizeString = args.length > 0 ? args[0] : "2GB";
    final String chunkSizeString = args.length > 1 ? args[1] : "128kB";
    System.out.println("totalSize: " + totalSizeString);
    System.out.println("chunkSize: " + chunkSizeString);

    final long totalSize = SizeInBytes.valueOf(totalSizeString).getSize();
    final int chunkSize = SizeInBytes.valueOf(chunkSizeString).getSizeInt();
    final File tmpDir = new File("tmp_" + ReadWriteBenchmark.class.getSimpleName());
    System.out.println("tmpDir   : " + tmpDir);

    runBenchmark(tmpDir, totalSize, chunkSize, Type.RANDOM);
    runBenchmark(tmpDir, totalSize, chunkSize, Type.FILE);
  }

  enum Type {FILE, RANDOM}

  static void generateFile(File outFile, long fileSize, int chunkSize) throws IOException {
    final byte[] chunk = new byte[chunkSize];
    try (OutputStream out = Files.newOutputStream(outFile.toPath(), StandardOpenOption.CREATE)) {
      for (long size = 0; size < fileSize; size += chunkSize) {
        ThreadLocalRandom.current().nextBytes(chunk);
        out.write(chunk);
      }
    }
  }

  static void runBenchmark(File tmpDir, long totalSize, int chunkSize, Type type) throws Exception {
    FileUtils.deleteFully(tmpDir);
    FileUtils.createDirectories(tmpDir);

    final File inFile;
    if (type == Type.FILE) {
      inFile = new File(tmpDir, "inputFile");
      generateFile(inFile, totalSize, chunkSize);
    } else {
      inFile = null;
    }

    for (int numParts = 2; numParts <= 16; numParts <<= 1) {
      final WriteBenchmark[] benchmarks = {
          new SingleFileDoubleByteBufferThread(),
          new SingleFileDoubleByteBufferExecutor(),
          new SingleFileDoubleByteArrayThread(),
          new SingleFileDoubleByteArrayExecutor(),
          new MultiFileByteArray(numParts),
          new MultiFileByteBuffer(numParts),
          new SingleFileByteArray(),
          new SingleFileByteBuffer()
      };

      final SortedMap<Long, WriteBenchmark> sorted = new TreeMap<>();

      for(WriteBenchmark benchmark : benchmarks) {
        final Long elapsed = benchmark.run(inFile, totalSize, tmpDir, chunkSize);
        if (elapsed != null) {
          sorted.put(elapsed, benchmark);
        }
      }

      System.out.println("Sorted: " + sorted.values());
      System.out.println();
    }
  }

  static void read(FileChannel in, byte[] array) throws IOException {
    final ByteBuffer chunk = ByteBuffer.wrap(array);
    Preconditions.assertSame(array.length, chunk.remaining(), "remaining");
    int size = 0;
    for (; size < chunk.capacity(); ) {
      final int readLength = in.read(chunk);
      if (readLength == -1) {
        break;
      }
      size += readLength;
    }
    Preconditions.assertSame(chunk.capacity(), size, "readLength");
  }

  static long transferTo(FileChannel in, long offset, long size, FileChannel out) throws IOException {
    long transferred = 0;
    for (; transferred < size; ) {
      transferred += in.transferTo(offset + transferred, size - transferred, out);
    }
    return transferred;
  }

  static int fillRandom(Random random, ByteBuffer buffer) {
    for (; buffer.remaining() > 0; ) {
      buffer.putInt(random.nextInt());
    }
    buffer.flip();
    return buffer.remaining();
  }

  static int writeRandom(Random random, ByteBuffer buffer, int chunkSize, FileChannel out) throws IOException {
    Preconditions.assertSame(chunkSize, buffer.remaining(), "remaining");
    final int filled = fillRandom(random, buffer);
    Preconditions.assertSame(chunkSize, filled, "filled");
    final int written = writeTo(buffer, out);
    Preconditions.assertSame(chunkSize, written, "written");
    return filled;
  }

  static int writeTo(ByteBuffer buffer, FileChannel out) throws IOException {
    int written = 0;
    while (buffer.hasRemaining()) {
      written += out.write(buffer);
    }
    Preconditions.assertSame(0, buffer.remaining(), "remaining");
    Preconditions.assertSame(buffer.capacity(), written, "written");

    buffer.clear();
    Preconditions.assertSame(buffer.capacity(), buffer.remaining(), "remaining");
    return written;
  }
}
