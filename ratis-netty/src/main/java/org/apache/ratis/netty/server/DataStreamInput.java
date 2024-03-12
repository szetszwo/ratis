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
package org.apache.ratis.netty.server;

import org.apache.ratis.io.CloseAsync;
import org.apache.ratis.thirdparty.io.netty.buffer.ByteBuf;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

/** An asynchronous input stream supporting zero buffer copying. */
public interface DataStreamInput extends CloseAsync<Long> {
  /** A factory to create {@link DataStreamInput}s. */
  interface Factory {
    default CompletableFuture<DataStreamInput> createAsync(ByteBuf buf, Executor executor) {
      final Descriptor descriptor = decode(buf);
      return createAsync(descriptor, executor);
    }

    /** Create asynchronously an {@link DataStreamInput} specified by the given descriptor. */
    CompletableFuture<DataStreamInput> createAsync(Descriptor descriptor, Executor executor);

    /** Decode the give buffer to a {@link Descriptor}. */
    Descriptor decode(ByteBuf buf);
  }

  /** Containing information describing the stream. */
  interface Descriptor {
  }

  /**
   * Read data from this input asynchronously.
   *
   * @return a future of the buffer containing the data.
   */
  CompletableFuture<ByteBuf> readAsync();

  /**
   * Close this input asynchronously.
   *
   * @return a future of the number of bytes read.
   */
  @Override
  CompletableFuture<Long> closeAsync();
}