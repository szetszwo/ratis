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
package org.apache.ratis.client.impl;

import org.apache.ratis.client.DataStreamClientRpc;
import org.apache.ratis.client.api.DataStreamInput;
import org.apache.ratis.datastream.DataStreamObserver;
import org.apache.ratis.datastream.impl.DataStreamRequestByteBuffer;
import org.apache.ratis.io.StandardWriteOption;
import org.apache.ratis.proto.RaftProtos.DataStreamPacketHeaderProto.Type;
import org.apache.ratis.protocol.ClientId;
import org.apache.ratis.protocol.DataStreamReply;
import org.apache.ratis.protocol.DataStreamRequestHeader;
import org.apache.ratis.protocol.RaftClientRequest;
import org.apache.ratis.protocol.exceptions.AlreadyClosedException;
import org.apache.ratis.util.AsyncQueue;
import org.apache.ratis.util.ReferenceCountedObject;

import java.io.EOFException;
import java.nio.ByteBuffer;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

final class DataStreamInputImpl implements DataStreamInput,
    DataStreamObserver<ReferenceCountedObject<DataStreamReply>> {
  private final RaftClientRequest header;
  private final ClientId clientId;
  private final AsyncQueue<ReferenceCountedObject<DataStreamReply>> queue;

  /*
   * null                  : the stream is open.
   * EOFException          : the stream is ended, i.e. no more data.
   * AlreadyClosedException: the stream is closed by the caller.
   * Other exception       : the stream is failed.
   */

  DataStreamInputImpl(DataStreamClientRpc dataStreamClientRpc, RaftClientRequest request) {
    this.header = request;
    this.clientId = request.getClientId();
    this.queue = new AsyncQueue<>(clientId);
    final ByteBuffer buffer = ClientProtoUtils.toRaftClientRequestProtoByteBuffer(header);
    final DataStreamRequestHeader h = new DataStreamRequestHeader(clientId, Type.STREAM_HEADER,
        header.getCallId(), 0, buffer.remaining(), StandardWriteOption.FLUSH, StandardWriteOption.CLOSE);
    dataStreamClientRpc.streamAsync(new DataStreamRequestByteBuffer(h, buffer), this)
        .whenComplete(asWhenCompleteBiConsumer());
  }

  @Override
  public synchronized void onNext(ReferenceCountedObject<DataStreamReply> reply) {
    if (queue.getFailure() != null) {
      return;
    }

    // receive new reply, offer it to the queue.
    reply.retain();
    queue.offer(reply);
  }

  @Override
  public synchronized void onError(Throwable throwable) {
    Objects.requireNonNull(throwable, "throwable == null");
    // An error case, release the replies
    fail(true, () -> throwable);
  }

  @Override
  public synchronized void onCompleted() {
    // Not an error case, do not release the replies

    // End-of-stream, no more onNext(), the pending reads cannot be completed
    fail(false, () -> new EOFException(clientId + ": end of stream, request=" + header));
  }

  private void fail(boolean release, Supplier<Throwable> throwable) {
    queue.fail(throwable, release ? ReferenceCountedObject::release : null);
  }

  @Override
  public synchronized CompletableFuture<ReferenceCountedObject<DataStreamReply>> readAsync() {
    return queue.poll();
  }

  @Override
  public synchronized void close() {
    // When close() is called the first time, we set
    // (1) release all replies
    // (2) set readException to AlreadyClosedException
    // (3) complete all pendingReads, and
    fail(true, () -> new AlreadyClosedException(clientId + ": stream already closed, request=" + header));
  }
}
