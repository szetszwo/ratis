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

import org.apache.ratis.util.Preconditions;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.util.Objects;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.ratis.benchmark.MultiFileWriterBenchmark.Benchmark;
import static org.apache.ratis.benchmark.MultiFileWriterBenchmark.fillRandom;
import static org.apache.ratis.benchmark.MultiFileWriterBenchmark.writeTo;

abstract class SingleFileDoubleBuffer<BUFFER> extends Benchmark {
  static abstract class Writer<BUFFER> {
    private final DoubleBuffer<BUFFER> buffers;
    private final FileChannel out;

    Writer(DoubleBuffer<BUFFER> buffers, FileChannel out) {
      this.buffers = buffers;
      this.out = out;
    }

    abstract Future<?> submit();

    abstract void stop() throws InterruptedException;

    void write() {
      final SingleBuffer<BUFFER> buffer = buffers.getWriteBuffer();
      try {
        writeBuffer(out, buffer);
      } catch (Exception e) {
        throw new IllegalStateException("Failed to write " + buffer, e);
      }
    }
  }

  abstract DoubleBuffer<BUFFER> newBuffers(int chunkSize);

  abstract Writer<BUFFER> getWriter(DoubleBuffer<BUFFER> buffers, FileChannel out);

  @Override
  public long write(long totalSize, File outFile, int chunkSize, File inFile) throws Exception {
    if (inFile != null) {
      return 0;
    }

    final ThreadLocalRandom random = ThreadLocalRandom.current();
    final DoubleBuffer<BUFFER> buffers = newBuffers(chunkSize);

    try (FileChannel out = FileChannel.open(outFile.toPath(), StandardOpenOption.WRITE, StandardOpenOption.CREATE)) {
      final Writer<BUFFER> writer = getWriter(buffers, out);
      Future<?> writeFuture = CompletableFuture.completedFuture(null);

      for (long size = 0; size < totalSize; ) {
        // fill
        final SingleBuffer<BUFFER> fillBuffer = buffers.getFillBuffer();
        final BUFFER buffer = fillBuffer.acquire();
        size += fillRandom(random, getByteBuffer(buffer));
        fillBuffer.release(buffer);

        // swap
        writeFuture.get();
        buffers.swapBuffers();

        // write
        writeFuture = writer.submit();
      }

      writeFuture.get();
      writer.stop();
      return totalSize;
    }
  }

  static <BUFFER> ByteBuffer getByteBuffer(BUFFER buffer) {
    if (buffer instanceof ByteBuffer) {
      return (ByteBuffer) buffer;
    } else if (buffer instanceof byte[]) {
      return ByteBuffer.wrap((byte[]) buffer);
    } else {
      throw new IllegalArgumentException("Unsupported buffer " + buffer.getClass());
    }
  }
  static <BUFFER> void writeBuffer(FileChannel out, SingleBuffer<BUFFER> single) {
    final BUFFER buffer = single.acquire();
    final ByteBuffer byteBuffer = getByteBuffer(buffer);

    try {
      writeTo(byteBuffer, out);
    } catch (IOException e) {
      throw new CompletionException("Failed to write buffer " + buffer, e);
    } finally {
      single.release(buffer);
    }
//      System.out.println("write");
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

    synchronized void release(BUFFER buffer) {
      Preconditions.assertSame(bufferAcquired, buffer, "buffer");
      Preconditions.assertNull(bufferReleased, "bufferReleased");
      bufferReleased = bufferAcquired;
      bufferAcquired = null;
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

    void swapBuffers() {
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
    }
  }

  static abstract class SingleFileDoubleByteBuffer extends SingleFileDoubleBuffer<ByteBuffer> {
    @Override
    DoubleBuffer<ByteBuffer> newBuffers(int chunkSize) {
      return new DoubleBuffer<>(
          ByteBuffer.allocateDirect(chunkSize),
          ByteBuffer.allocateDirect(chunkSize));
    }
  }

  static class SingleFileDoubleByteBufferThread extends SingleFileDoubleByteBuffer {
    @Override
    WriterThread<ByteBuffer> getWriter(DoubleBuffer<ByteBuffer> buffers, FileChannel out) {
      return WriterThread.newWriter(buffers, out);
    }
  }

  static class WriterThread<BUFFER> extends Writer<BUFFER> {
    private final Thread thread = new Thread(this::run);
    private final BlockingQueue<CompletableFuture<Void>> queue = new LinkedBlockingQueue<>();
    private final AtomicBoolean stop = new AtomicBoolean(false);

    static <BUFFER> WriterThread<BUFFER> newWriter(DoubleBuffer<BUFFER> buffers, FileChannel out) {
      final WriterThread<BUFFER> writer = new WriterThread<>(buffers, out);
      writer.thread.start();
      return writer;
    }

    WriterThread(DoubleBuffer<BUFFER> buffers, FileChannel out) {
      super(buffers, out);
    }

    @Override
    public CompletableFuture<?> submit() {
      final CompletableFuture<Void> future = new CompletableFuture<>();
      queue.add(future);
      return future;
    }

    @Override
    public void stop() throws InterruptedException {
      stop.set(true);
      thread.interrupt();
      thread.join();
    }

    void run() {
      try {
        for (; !stop.get(); ) {
          final CompletableFuture<Void> future = queue.take();
          write();
          future.complete(null);
        }
      } catch (InterruptedException ignored) {
        Preconditions.assertTrue(stop.get());
      }
    }
  }

  static class SingleFileDoubleByteBufferExecutor extends SingleFileDoubleByteBuffer {
    @Override
    WriterExecutor<ByteBuffer> getWriter(DoubleBuffer<ByteBuffer> buffers, FileChannel out) {
      return new WriterExecutor<>(buffers, out);
    }
  }

  static class WriterExecutor<BUFFER> extends Writer<BUFFER> {
    private final ExecutorService executor = Executors.newSingleThreadExecutor();

    WriterExecutor(DoubleBuffer<BUFFER> buffers, FileChannel out) {
      super(buffers, out);
    }

    @Override
    public Future<?> submit() {
      return executor.submit(this::write);
    }

    @Override
    public void stop() throws InterruptedException {
      executor.shutdown();
      final boolean terminated = executor.awaitTermination(10, TimeUnit.SECONDS);
      Preconditions.assertTrue(terminated);
    }
  }

  static abstract class SingleFileDoubleByteArray extends SingleFileDoubleBuffer<byte[]> {
    @Override
    DoubleBuffer<byte[]> newBuffers(int chunkSize) {
      return new DoubleBuffer<>(
          new byte[chunkSize],
          new byte[chunkSize]);
    }
  }

  static class SingleFileDoubleByteArrayThread extends SingleFileDoubleByteArray {
    @Override
    WriterThread<byte[]> getWriter(DoubleBuffer<byte[]> buffers, FileChannel out) {
      return WriterThread.newWriter(buffers, out);
    }
  }

  static class SingleFileDoubleByteArrayExecutor extends SingleFileDoubleByteArray {
    @Override
    WriterExecutor<byte[]> getWriter(DoubleBuffer<byte[]> buffers, FileChannel out) {
      return new WriterExecutor<>(buffers, out);
    }
  }

}
