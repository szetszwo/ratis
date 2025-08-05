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
package org.apache.ratis;

import org.apache.ratis.util.FileUtils;
import org.apache.ratis.util.Preconditions;
import org.apache.ratis.util.SizeInBytes;

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
import java.util.Random;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

/**
 * Benchmark for writing output to disk using a single file verse multiple files.
 */
public class MultiFileWriterBenchmark {
  public static void main(String[] args) throws Exception {
//    Slf4jUtils.setLogLevel(FileUtils.LOG, Level.TRACE);

    final String totalSizeString = args.length > 0 ? args[0] : "2GB";
    final String chunkSizeString = args.length > 1 ? args[1] : "128kB";
    System.out.println("totalSize: " + totalSizeString);
    System.out.println("chunkSize: " + chunkSizeString);

    final long totalSize = SizeInBytes.valueOf(totalSizeString).getSize();
    final int chunkSize = SizeInBytes.valueOf(chunkSizeString).getSizeInt();
    final File tmpDir = new File("tmp_" + MultiFileWriterBenchmark.class.getSimpleName());
    System.out.println("tmpDir   : " + tmpDir);

    FileUtils.deleteFully(tmpDir);
    FileUtils.createDirectories(tmpDir);

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
    final File inFile;
    if (type == Type.FILE) {
      inFile = new File(tmpDir, "inputFile");
      generateFile(inFile, totalSize, chunkSize);
    } else {
      inFile = null;
    }

    for (int numParts = 2; numParts <= 16; numParts <<= 1) {
      System.out.println();

      runBenchmark(inFile, totalSize, tmpDir, chunkSize, new SingleFileDoubleByteBuffer());
      runBenchmark(inFile, totalSize, tmpDir, chunkSize, new MultiFileByteArray(numParts));
      runBenchmark(inFile, totalSize, tmpDir, chunkSize, new MultiFileByteBuffer(numParts));
      runBenchmark(inFile, totalSize, tmpDir, chunkSize, new SingleFileByteArray());
      runBenchmark(inFile, totalSize, tmpDir, chunkSize, new SingleFileByteBuffer());
    }
  }

  static void runBenchmark(File inFile, long totalSize, File tmpDir, int chunkSize, Benchmark benchmark) throws Exception {
    if (chunkSize % 4 != 0) {
      throw new IllegalArgumentException("chunkSize (=" + totalSize + ") must be a multiple of 4");
    }
    if (totalSize % chunkSize != 0) {
      throw new IllegalArgumentException("totalSize (=" + totalSize + ") must be a multiple of chunkSize (=" + chunkSize + ")");
    }
    if (inFile != null) {
      final long inFileSize = inFile.length();
      if (inFileSize < totalSize) {
        throw new IllegalArgumentException("File size (=" + inFileSize + ") < totalSize (=" + totalSize + "): " + inFile);
      }
    }

    final File outPath = benchmark.getOutputPath(tmpDir);
    FileUtils.deleteFully(outPath);

    final long start = System.nanoTime();
    final long writeSize = benchmark.write(totalSize, outPath, chunkSize, inFile);
    final long elapsed = System.nanoTime() - start;
    Preconditions.assertSame(totalSize, writeSize, "writeSize");
    System.out.printf("%-40s [%6s]: %9.3f ms%n",
        benchmark, inFile == null ? "random" : "file", elapsed / 1_000_000.0);
  }

  interface Benchmark {
    default File getOutputPath(File tmpDir) {
      return new File(tmpDir, getClass().getSimpleName());
    }

    long write(long totalSize, File outFile, int chunkSize, File inFile) throws Exception;
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

  static class SingleFileByteArray implements Benchmark {
    @Override
    public String toString() {
      return getClass().getSimpleName();
    }

    @Override
    public long write(long totalSize, File outFile, int chunkSize, File inFile) throws Exception {
      final byte[] chunk = new byte[chunkSize];
      long size = 0;
      try (OutputStream out = Files.newOutputStream(outFile.toPath())) {
        if (inFile != null) {
          try (RandomAccessFile inRAF = new RandomAccessFile(inFile, "r");
               FileChannel in = inRAF.getChannel()) {
            for (; size < totalSize; size += chunk.length) {
              read(in, chunk);
              out.write(chunk);
            }
          }
        } else {
          final ThreadLocalRandom random = ThreadLocalRandom.current();
          for (; size < totalSize; size += chunk.length) {
            random.nextBytes(chunk);
            out.write(chunk);
          }
          return totalSize;
        }
      }
      return size;
    }
  }

  static long transferTo(FileChannel in, long offset, long size, FileChannel out) throws IOException {
    long transferred = 0;
    for (; transferred < size; ) {
      transferred += in.transferTo(offset + transferred, size - transferred, out);
    }
    return transferred;
  }

  static class SingleFileByteBuffer implements Benchmark {
    @Override
    public String toString() {
      return getClass().getSimpleName();
    }

    @Override
    public long write(long totalSize, File outFile, int chunkSize, File inFile) throws Exception {
      if (inFile != null) {
        try (FileChannel in = FileChannel.open(inFile.toPath(), StandardOpenOption.READ);
             FileChannel out = FileChannel.open(outFile.toPath(), StandardOpenOption.CREATE, StandardOpenOption.WRITE)) {
          return transferTo(in, 0, totalSize, out);
        }
      } else {
        final ByteBuffer buffer = ByteBuffer.allocateDirect(chunkSize);
        final ThreadLocalRandom random = ThreadLocalRandom.current();
        try (FileChannel out = FileChannel.open(outFile.toPath(), StandardOpenOption.WRITE, StandardOpenOption.CREATE)) {
          for (long size = 0; size < totalSize; ) {
            size += writeRandom(random, buffer, chunkSize, out);
          }
        }
        return totalSize;
      }
    }
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

  static class MultiFileByteArray implements Benchmark {
    private final int numParts;

    MultiFileByteArray(int numParts) {
      this.numParts = numParts;
    }

    @Override
    public String toString() {
      return getClass().getSimpleName() + "(" + numParts + ")";
    }

    @Override
    public long write(long totalSize, File outDir, int chunkSize, File inFile) throws Exception {
      FileUtils.createDirectories(outDir);

      if (inFile != null) {
        try (RandomAccessFile raf = new RandomAccessFile(inFile, "r")) {
          final List<Worker> workers = newWorkers(outDir, numParts, out -> new ByteArrayWorker(raf, out, chunkSize));
          return submitAndJoin(workers, totalSize, chunkSize, numParts);
        }
      } else {
        final List<Worker> workers = newWorkers(outDir, numParts, out -> new ByteArrayWorker(null, out, chunkSize));
        return submitAndJoin(workers, totalSize, chunkSize, numParts);
      }
    }
  }

  static long submitAndJoin(List<Worker> workers, long totalSize, int chunkSize, int numParts) throws Exception {
    int i = 0;
    for (long offset = 0; offset < totalSize; offset += chunkSize) {
      workers.get(i % numParts).submit(offset);
      i++;
    }

    for (Worker worker : workers) {
      worker.getThread().start();
    }
    long size = 0;
    for (Worker worker : workers) {
      worker.getThread().join();
      size += worker.getSize();
    }
    return size;
  }

  static <W extends Worker> List<W> newWorkers(File outDir, int numParts, Function<Path, W> constructor) {
    final List<W> workers = new ArrayList<>(numParts);
    for (int i = 0; i < numParts; i++) {
      final Path outFile = outDir.toPath().resolve(String.format("part%02d.out", i));
      workers.add(constructor.apply(outFile));
    }
    return workers;
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

  static class MultiFileByteBuffer implements Benchmark {
    private final int numParts;

    MultiFileByteBuffer(int numParts) {
      this.numParts = numParts;
    }

    @Override
    public String toString() {
      return getClass().getSimpleName() + "(" + numParts + ")";
    }

    @Override
    public long write(long totalSize, File outDir, int chunkSize, File inFile) throws Exception {
      FileUtils.createDirectories(outDir);

      if (inFile != null) {
        try (RandomAccessFile raf = new RandomAccessFile(inFile, "r")) {
          final List<Worker> workers = newWorkers(outDir, numParts, out -> new ByteBufferWorker(raf, out, chunkSize));
          return submitAndJoin(workers, totalSize, chunkSize, numParts);
        }
      } else {
        final List<Worker> workers = newWorkers(outDir, numParts, out -> new ByteBufferWorker(null, out, chunkSize));
        return submitAndJoin(workers, totalSize, chunkSize, numParts);
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

  static class SingleFileDoubleByteBuffer implements Benchmark {

    @Override
    public String toString() {
      return getClass().getSimpleName();
    }

    @Override
    public long write(long totalSize, File outFile, int chunkSize, File inFile) throws Exception {
      if (inFile != null) {
        try (FileChannel in = FileChannel.open(inFile.toPath(), StandardOpenOption.READ);
             FileChannel out = FileChannel.open(outFile.toPath(), StandardOpenOption.CREATE, StandardOpenOption.WRITE)) {
          return transferTo(in, 0, totalSize, out);
        }
      } else {
        final ThreadLocalRandom random = ThreadLocalRandom.current();
        final DoubleBuffer<ByteBuffer> buffers = new DoubleBuffer<>(
            ByteBuffer.allocateDirect(chunkSize),
            ByteBuffer.allocateDirect(chunkSize));

        try (FileChannel out = FileChannel.open(outFile.toPath(), StandardOpenOption.WRITE, StandardOpenOption.CREATE)) {
          final Writer writer = new Writer(buffers, out);
          writer.thread.start();
          CompletableFuture<Void> writeFuture = CompletableFuture.completedFuture(null);

          for (long size = 0; size < totalSize; ) {
            // fill
            final SingleBuffer<ByteBuffer> fillBuffer = buffers.getFillBuffer();
            final ByteBuffer buffer = fillBuffer.acquire();
            size += fillRandom(random, buffer);
            fillBuffer.release(buffer);

            // swap
            writeFuture.get();
            buffers.swapBuffers();

            // write
            writeFuture = writer.submit();
          }

          writeFuture.get();
          writer.stop.set(true);
          return totalSize;
        }
      }
    }

    static class Writer {
      private final Thread thread = new Thread(this::run);
      private final BlockingQueue<CompletableFuture<Void>> queue = new LinkedBlockingQueue<>();
      private final AtomicBoolean stop = new AtomicBoolean(false);
      private final DoubleBuffer<ByteBuffer> buffers;
      private final FileChannel out;

      Writer(DoubleBuffer<ByteBuffer> buffers, FileChannel out) {
        this.buffers = buffers;
        this.out = out;
      }

      CompletableFuture<Void> submit() {
        final CompletableFuture<Void> future = new CompletableFuture<>();
        queue.add(future);
        return future;
      }

      void run() {
        try {
          for (; !stop.get(); ) {
            final CompletableFuture<Void> future = queue.take();
            write(out, buffers.getWriteBuffer());
            future.complete(null);
          }
        } catch (Exception e) {
          throw new IllegalStateException(e);
        }
      }
    }

    static Void write(FileChannel out, SingleBuffer<ByteBuffer> single) {
      final ByteBuffer buffer = single.acquire();
      try {
        writeTo(buffer, out);
      } catch (IOException e) {
        throw new CompletionException("Failed to write buffer " + buffer, e);
      } finally {
        single.release(buffer);
      }
//      System.out.println("write");
      return null;
    }

    static class SingleBuffer<BUFFER> {
      private BUFFER bufferAcquired;
      private BUFFER bufferReleased;

      SingleBuffer(BUFFER bufferReleased) {
        this.bufferReleased = bufferReleased;
      }

      synchronized BUFFER acquire() {
        Preconditions.assertNull(bufferAcquired, "bufferAcquired");
        Objects.requireNonNull(bufferReleased, "bufferReleased == null");
        bufferAcquired = bufferReleased;
        bufferReleased = null;
        return bufferAcquired;
      }

      synchronized Void release(BUFFER buffer) {
        Preconditions.assertSame(bufferAcquired, buffer, "buffer");
        Preconditions.assertNull(bufferReleased, "bufferReleased");
        bufferReleased = bufferAcquired;
        bufferAcquired = null;
        return null;
      }

      synchronized boolean isReleased() {
        Preconditions.assertTrue(bufferAcquired == null ^ bufferReleased == null);
        return bufferReleased != null;
      }

      @Override
      public synchronized String toString() {
        return "bufferAcquired=" + bufferAcquired + ", bufferReleased=" + bufferReleased;
      }
    }

    static class DoubleBuffer<BUFFER> {
      // for synchronized
      private final SingleBuffer<BUFFER> first;
      private final SingleBuffer<BUFFER> second;

      // for swapping
      private SingleBuffer<BUFFER> fillBuffer;
      private SingleBuffer<BUFFER> writeBuffer;

      DoubleBuffer(BUFFER fillBuffer, BUFFER writeBuffer) {
        this.first = this.fillBuffer = new SingleBuffer<>(fillBuffer);
        this.second = this.writeBuffer = new SingleBuffer<>(writeBuffer);
      }

      SingleBuffer<BUFFER> getFillBuffer() {
        return fillBuffer;
      }

      SingleBuffer<BUFFER> getWriteBuffer() {
        return writeBuffer;
      }

      Void swapBuffers() {
        synchronized (first) {
          synchronized (second) {
            Preconditions.assertTrue(fillBuffer.isReleased());
            Preconditions.assertTrue(writeBuffer.isReleased());
            final SingleBuffer<BUFFER> tmp = fillBuffer;
            fillBuffer = writeBuffer;
            writeBuffer = tmp;
          }
        }
//        System.out.println("swap");
        return null;
      }
    }
  }


}
