/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.ratis.netty.server;

import org.apache.ratis.client.impl.ClientProtoUtils;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.datastream.impl.DataStreamReplyByteBuffer;
import org.apache.ratis.datastream.impl.DataStreamRequestByteBuf;
import org.apache.ratis.netty.metrics.NettyServerStreamRpcMetrics;
import org.apache.ratis.netty.metrics.NettyServerStreamRpcMetrics.RequestType;
import org.apache.ratis.proto.RaftProtos.DataStreamPacketHeaderProto.Type;
import org.apache.ratis.protocol.ClientId;
import org.apache.ratis.protocol.ClientInvocationId;
import org.apache.ratis.protocol.RaftClientReply;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.protocol.exceptions.DataStreamException;
import org.apache.ratis.server.RaftServerConfigKeys;
import org.apache.ratis.thirdparty.io.netty.channel.ChannelHandlerContext;
import org.apache.ratis.thirdparty.io.netty.channel.ChannelId;
import org.apache.ratis.util.ConcurrentUtils;
import org.apache.ratis.util.JavaUtils;
import org.apache.ratis.util.MemoizedSupplier;
import org.apache.ratis.util.StringUtils;
import org.apache.ratis.util.TimeDuration;
import org.apache.ratis.util.TimeoutExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;

public class ReadStreamManagement {
  public static final Logger LOG = LoggerFactory.getLogger(ReadStreamManagement.class);

  static class ReadInfo {
    private final CompletableFuture<DataStreamInput> streamFuture;
    private final CompletableFuture<Long> closeFuture;

    ReadInfo(CompletableFuture<DataStreamInput> streamFuture) {
      this.streamFuture = streamFuture;
      this.closeFuture = streamFuture.isDone()? streamFuture.thenApply(stream -> 0L)
          : new CompletableFuture<>();
    }

    CompletableFuture<Long> getCloseFuture() {
      return closeFuture;
    }

    void cleanUp() {
      streamFuture.thenCompose(DataStreamInput::closeAsync)
          .thenAccept(closeFuture::complete);
    }

    @Override
    public String toString() {
      return JavaUtils.getClassSimpleName(getClass())
          + "{stream:" + StringUtils.completableFuture2String(streamFuture, false)
          + ", close:" + StringUtils.completableFuture2String(closeFuture, false)
          + "}";
    }
  }

  private final RaftPeerId id;
  private final String name;

  private final StreamMap<ReadInfo> streams = new StreamMap<>();
  private final ChannelMap channels;
  private final ExecutorService requestExecutor;
  private final ExecutorService readExecutor;

  private final DataStreamInput.Factory factory;
  private final NettyServerStreamRpcMetrics nettyServerStreamRpcMetrics;

  ReadStreamManagement(RaftPeerId id, DataStreamInput.Factory factory, RaftProperties properties,
      NettyServerStreamRpcMetrics metrics) {
    this.id = id;
    this.name = id + "-" + JavaUtils.getClassSimpleName(getClass());

    this.channels = new ChannelMap();
    final boolean useCachedThreadPool = RaftServerConfigKeys.DataStream.asyncRequestThreadPoolCached(properties);
    this.requestExecutor = ConcurrentUtils.newThreadPoolWithMax(useCachedThreadPool,
          RaftServerConfigKeys.DataStream.asyncRequestThreadPoolSize(properties),
          name + "-request-");
    this.readExecutor = ConcurrentUtils.newThreadPoolWithMax(useCachedThreadPool,
          RaftServerConfigKeys.DataStream.asyncWriteThreadPoolSize(properties),
          name + "-read-");

    this.factory = factory;
    this.nettyServerStreamRpcMetrics = metrics;
  }

  void shutdown() {
    ConcurrentUtils.shutdownAndWait(TimeDuration.ONE_SECOND, requestExecutor,
        timeout -> LOG.warn("{}: requestExecutor shutdown timeout in {}", this, timeout));
    ConcurrentUtils.shutdownAndWait(TimeDuration.ONE_SECOND, readExecutor,
        timeout -> LOG.warn("{}: writeExecutor shutdown timeout in {}", this, timeout));
  }

  void replyException(Throwable cause, DataStreamRequestByteBuf request, ChannelHandlerContext ctx) {
    final RaftClientReply reply = RaftClientReply.newBuilder()
        .setClientId(ClientId.emptyClientId())
        .setServerId(id)
        .setGroupId(RaftGroupId.emptyGroupId())
        .setException(new DataStreamException(id, cause))
        .build();
    sendException(cause, request, reply, ctx);
  }

  static void sendException(Throwable throwable, DataStreamRequestByteBuf request, RaftClientReply reply,
      ChannelHandlerContext ctx) {
    LOG.warn("Failed to process {}",  request, throwable);
    try {
      ctx.writeAndFlush(newDataStreamReplyByteBuffer(request, reply));
    } catch (Throwable t) {
      LOG.warn("Failed to sendDataStreamException {} for {}", throwable, request, t);
    } finally {
      request.release();
    }
  }

  static DataStreamReplyByteBuffer newDataStreamReplyByteBuffer(DataStreamRequestByteBuf request,
      RaftClientReply reply) {
    final ByteBuffer buffer = ClientProtoUtils.toRaftClientReplyProto(reply).toByteString().asReadOnlyByteBuffer();
    return DataStreamReplyByteBuffer.newBuilder()
        .setDataStreamPacket(request)
        .setBuffer(buffer)
        .setSuccess(reply.isSuccess())
        .build();
  }

  static void sendReply(DataStreamRequestByteBuf request, ChannelHandlerContext ctx) {
    final DataStreamReplyByteBuffer.Builder builder = DataStreamReplyByteBuffer.newBuilder()
        .setDataStreamPacket(request);
    ctx.writeAndFlush(builder.build());
  }

  void cleanUp(Set<ClientInvocationId> ids) {
    for (ClientInvocationId clientInvocationId : ids) {
      Optional.ofNullable(streams.remove(clientInvocationId))
          .ifPresent(ReadInfo::cleanUp);
    }
  }

  void cleanUpOnChannelInactive(ChannelId channelId, TimeDuration channelInactiveGracePeriod) {
    // Delayed memory garbage cleanup
    Optional.ofNullable(channels.remove(channelId)).ifPresent(ids -> {
      LOG.info("Channel {} is inactive, cleanup clientInvocationIds={}", channelId, ids);
      TimeoutExecutor.getInstance().onTimeout(channelInactiveGracePeriod, () -> cleanUp(ids),
          LOG, () -> "Timeout check failed, clientInvocationIds=" + ids);
    });
  }

  private ReadInfo newReadInfo(DataStreamRequestByteBuf request) {
    final CompletableFuture<DataStreamInput> in = factory.createAsync(request.slice(), requestExecutor);
    return new ReadInfo(in);
  }

  void read(DataStreamRequestByteBuf request, ChannelHandlerContext ctx) {
    LOG.debug("{}: read {}", this, request);
    try {
      readImpl(request, ctx);
    } catch (Throwable t) {
//      replyDataStreamException(t, request, ctx);
    }
  }

  private void readImpl(DataStreamRequestByteBuf request, ChannelHandlerContext ctx) {
    final ClientInvocationId key =  ClientInvocationId.valueOf(request.getClientId(), request.getStreamId());

    // add to ChannelMap
    final ChannelId channelId = ctx.channel().id();
    channels.add(channelId, key);

    final ReadInfo info;
    if (request.getType() == Type.STREAM_HEADER) {
      final MemoizedSupplier<ReadInfo> supplier = JavaUtils.memoize(() -> newReadInfo(request));
      info = streams.computeIfAbsent(key, id -> supplier.get());
      if (!supplier.isInitialized()) {
        final IllegalStateException e = new IllegalStateException("Failed to create a new stream for " + request
            + " since a stream already exists Key: " + key + " StreamInfo:" + info);
        replyException(e, request, ctx);
        return;
      }
      getMetrics().onRequestCreate(RequestType.HEADER);
    } else {
      throw new IllegalStateException(this + ": Unexpected type " + request.getType() + ", request=" + request);
    }

    info.getCloseFuture().whenComplete((v, exception) -> {
      try {
        if (exception != null) {
          final ReadInfo removed = streams.remove(key);
          if (removed != null) {
            removed.cleanUp();
          }
//          replyDataStreamException(server, exception, info.getRequest(), request, ctx);
        }
      } finally {
        request.release();
        channels.remove(channelId, key);
      }
    });
  }

  NettyServerStreamRpcMetrics getMetrics() {
    return nettyServerStreamRpcMetrics;
  }

  @Override
  public String toString() {
    return name;
  }
}
