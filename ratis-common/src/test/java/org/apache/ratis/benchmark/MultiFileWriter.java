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

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

import static org.apache.ratis.benchmark.ReadWriteBenchmark.*;

/**
 * Benchmark for writing output to disk using a single file verse multiple files.
 */
public class MultiFileWriter {
  static abstract class MultiFileBenchmark<WORKER extends Worker> extends WriteBenchmark {
    private final int numParts;

    MultiFileBenchmark(int numParts) {
      this.numParts = numParts;
    }

    @Override
    public String toString() {
      return super.toString() + "_" + numParts;
    }

    List<WORKER> newWorkers(File outDir, Function<Path, WORKER> constructor) {
      final List<WORKER> workers = new ArrayList<>(numParts);
      for (int i = 0; i < numParts; i++) {
        final Path outFile = outDir.toPath().resolve(String.format("part%02d.out", i));
        workers.add(constructor.apply(outFile));
      }
      return workers;
    }

    long submitAndJoin(List<WORKER> workers, long totalSize, int chunkSize) throws Exception {
      int i = 0;
      for (long offset = 0; offset < totalSize; offset += chunkSize) {
        workers.get(i % numParts).submit(offset);
        i++;
      }

      for (WORKER worker : workers) {
        worker.getThread().start();
      }
      long size = 0;
      for (WORKER worker : workers) {
        worker.getThread().join();
        size += worker.getSize();
      }
      return size;
    }
  }

  static class MultiFileByteArray extends MultiFileBenchmark<ByteArrayWorker> {
    MultiFileByteArray(int numParts) {
      super(numParts);
    }

    @Override
    public long write(long totalSize, File outDir, int chunkSize, File inFile) throws Exception {
      FileUtils.createDirectories(outDir);

      if (inFile != null) {
        try (RandomAccessFile raf = new RandomAccessFile(inFile, "r")) {
          final List<ByteArrayWorker> workers = newWorkers(outDir, out -> new ByteArrayWorker(raf, out, chunkSize));
          return submitAndJoin(workers, totalSize, chunkSize);
        }
      } else {
        final List<ByteArrayWorker> workers = newWorkers(outDir, out -> new ByteArrayWorker(null, out, chunkSize));
        return submitAndJoin(workers, totalSize, chunkSize);
      }
    }
  }

  static abstract class Worker {
    private final Queue<Long> queue = new LinkedList<>();
    private final Thread thread = new Thread(this::run);
    private final ThreadLocalRandom random = ThreadLocalRandom.current();
    private final AtomicLong written = new AtomicLong();

    abstract long runImpl() throws IOException;

    long getSize() {
      return written.get();
    }

    ThreadLocalRandom getRandom() {
      return random;
    }

    void run() {
      try {
        written.set(runImpl());
      } catch (IOException e) {
        throw new IllegalStateException(e);
      }
    }

    void submit(long offset) {
      queue.offer(offset);
    }

    Queue<Long> getQueue() {
      return queue;
    }

    Thread getThread() {
      return thread;
    }

  }

  static class ByteArrayWorker extends Worker {
    private final FileChannel in;
    private final Path outFile;
    private final byte[] chunk;

    ByteArrayWorker(RandomAccessFile in, Path outFile, int chunkSize) {
      this.in = in == null ? null : in.getChannel();
      this.outFile = outFile;
      this.chunk = new byte[chunkSize];
    }

    @Override
    long runImpl() throws IOException {
      long size = 0;
      try (OutputStream out = Files.newOutputStream(outFile)) {
        while (!getQueue().isEmpty()) {
          final long offset = Objects.requireNonNull(getQueue().poll());

          if (in != null) {
            in.position(offset);
            read(in, chunk);
          } else {
            getRandom().nextBytes(chunk);
          }

          out.write(chunk);
          size += chunk.length;
        }
      }
      return size;
    }
  }

  static class MultiFileByteBuffer extends MultiFileBenchmark<ByteBufferWorker> {
    MultiFileByteBuffer(int numParts) {
      super(numParts);
    }

    @Override
    public long write(long totalSize, File outDir, int chunkSize, File inFile) throws Exception {
      FileUtils.createDirectories(outDir);

      if (inFile != null) {
        try (RandomAccessFile raf = new RandomAccessFile(inFile, "r")) {
          final List<ByteBufferWorker> workers = newWorkers(outDir, out -> new ByteBufferWorker(raf, out, chunkSize));
          return submitAndJoin(workers, totalSize, chunkSize);
        }
      } else {
        final List<ByteBufferWorker> workers = newWorkers(outDir, out -> new ByteBufferWorker(null, out, chunkSize));
        return submitAndJoin(workers, totalSize, chunkSize);
      }
    }
  }

  static class ByteBufferWorker extends Worker {
    private final FileChannel in;
    private final Path outFile;
    private final ByteBuffer buffer;
    private final int chunkSize;

    ByteBufferWorker(RandomAccessFile inFile, Path outFile, int chunkSize) {
      if (inFile == null) {
        this.in = null;
        this.buffer = ByteBuffer.allocateDirect(chunkSize);
      } else {
        this.in = inFile.getChannel();
        this.buffer = null;
      }

      this.outFile = outFile;
      this.chunkSize = chunkSize;
    }

    @Override
    long runImpl() throws IOException {
      long size = 0;
      try (FileChannel out = FileChannel.open(outFile, StandardOpenOption.WRITE, StandardOpenOption.CREATE)) {
        while (!getQueue().isEmpty()) {
          final long offset = Objects.requireNonNull(getQueue().poll());
          size += in != null
              ? transferTo(in, offset, chunkSize, out)
              : writeRandom(getRandom(), buffer, chunkSize, out);
        }
      }
      return size;
    }
  }
}
