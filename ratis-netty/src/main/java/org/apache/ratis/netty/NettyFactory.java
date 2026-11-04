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
package org.apache.ratis.netty;

import org.apache.ratis.client.ClientFactory;
import org.apache.ratis.conf.Parameters;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.netty.client.NettyClientRpc;
import org.apache.ratis.netty.server.NettyRpcService;
import org.apache.ratis.protocol.ClientId;
import org.apache.ratis.rpc.SupportedRpcType;
import org.apache.ratis.security.TlsConf;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.ServerFactory;
import org.apache.ratis.thirdparty.io.netty.handler.ssl.SslContext;
import org.apache.ratis.util.JavaUtils;
import org.apache.ratis.util.MemoizedSupplier;
import org.apache.ratis.util.NettyUtils;

public class NettyFactory implements ServerFactory, ClientFactory {
  /** Build lazily since a client does not need the server context. When TLS is disabled, the supplier is null. */
  private final MemoizedSupplier<SslContext> serverSslContext;
  private final MemoizedSupplier<SslContext> clientSslContext;

  public NettyFactory(Parameters parameters) {
    final TlsConf serverTlsConf = NettyConfigKeys.Server.tlsConf(parameters);
    this.serverSslContext = serverTlsConf == null ? null
        : JavaUtils.memoize(() -> NettyUtils.buildSslContextForServer(serverTlsConf));
    final TlsConf clientTlsConf = NettyConfigKeys.Client.tlsConf(parameters);
    this.clientSslContext = clientTlsConf == null ? null
        : JavaUtils.memoize(() -> NettyUtils.buildSslContextForClient(clientTlsConf));
  }

  private SslContext getServerSslContext() {
    return serverSslContext == null ? null : serverSslContext.get();
  }

  private SslContext getClientSslContext() {
    return clientSslContext == null ? null : clientSslContext.get();
  }

  @Override
  public SupportedRpcType getRpcType() {
    return SupportedRpcType.NETTY;
  }

  @Override
  public NettyRpcService newRaftServerRpc(RaftServer server) {
    return NettyRpcService.newBuilder()
        .setServer(server)
        .setServerSslContext(getServerSslContext())
        .setClientSslContext(getClientSslContext())
        .build();
  }

  @Override
  public NettyClientRpc newRaftClientRpc(ClientId clientId, RaftProperties properties) {
    return new NettyClientRpc(clientId, getClientSslContext(), properties);
  }
}
