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
package org.apache.ratis.grpc;

import org.apache.ratis.RaftConfigKeys;
import org.apache.ratis.RaftTestUtil;
import org.apache.ratis.conf.Parameters;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.grpc.metrics.ZeroCopyMetrics;
import org.apache.ratis.grpc.server.GrpcServicesImpl;
import org.apache.ratis.protocol.RaftGroup;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.rpc.SupportedRpcType;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.impl.DelayLocalExecutionInjection;
import org.apache.ratis.server.impl.MiniRaftCluster;
import org.apache.ratis.server.impl.RaftServerTestUtil;
import org.apache.ratis.util.NetUtils;
import org.apache.ratis.util.ReferenceCountedLeakDetector;
import org.junit.Assert;

import java.util.Optional;

/**
 * A {@link MiniRaftCluster} with {{@link SupportedRpcType#GRPC}} and data stream disabled.
 */
public class MiniRaftClusterWithGrpc extends MiniRaftCluster.RpcBase {
  public static final Factory<MiniRaftClusterWithGrpc> FACTORY
      = new Factory<MiniRaftClusterWithGrpc>() {
    @Override
    public MiniRaftClusterWithGrpc newCluster(String[] ids, String[] listenerIds, RaftProperties prop) {
      RaftConfigKeys.Rpc.setType(prop, SupportedRpcType.GRPC);
      return new MiniRaftClusterWithGrpc(ids, listenerIds, prop, null);
    }
  };

  static {
    // TODO move it to MiniRaftCluster for detecting non-gRPC cases
    ReferenceCountedLeakDetector.enable(true);
  }

  public interface FactoryGet extends Factory.Get<MiniRaftClusterWithGrpc> {
    @Override
    default Factory<MiniRaftClusterWithGrpc> getFactory() {
      return FACTORY;
    }
  }

  public static final DelayLocalExecutionInjection SEND_SERVER_REQUEST_INJECTION =
      new DelayLocalExecutionInjection(GrpcServicesImpl.GRPC_SEND_SERVER_REQUEST);

  public MiniRaftClusterWithGrpc(String[] ids, RaftProperties properties, Parameters parameters) {
    this(ids, new String[0], properties, parameters);
  }

  public MiniRaftClusterWithGrpc(String[] ids, String[] listenerIds, RaftProperties properties, Parameters parameters) {
    super(ids, listenerIds, properties, parameters);
  }

  @Override
  protected Parameters setPropertiesAndInitParameters(RaftPeerId id, RaftGroup group, RaftProperties properties) {
    GrpcConfigKeys.Server.setPort(properties, getPort(id, group));
    Optional.ofNullable(getAddress(id, group, RaftPeer::getClientAddress)).ifPresent(address ->
        GrpcConfigKeys.Client.setPort(properties, NetUtils.createSocketAddr(address).getPort()));
    Optional.ofNullable(getAddress(id, group, RaftPeer::getAdminAddress)).ifPresent(address ->
        GrpcConfigKeys.Admin.setPort(properties, NetUtils.createSocketAddr(address).getPort()));
    // Always run grpc integration tests with zero-copy enabled because the path of nonzero-copy is not risky.
    GrpcConfigKeys.Server.setZeroCopyEnabled(properties, true);
    return parameters;
  }

  @Override
  protected void blockQueueAndSetDelay(String leaderId, int delayMs)
      throws InterruptedException {
    RaftTestUtil.blockQueueAndSetDelay(getServers(), SEND_SERVER_REQUEST_INJECTION,
        leaderId, delayMs, getTimeoutMax());
  }

  @Override
  public void shutdown() {
    super.shutdown();
    assertZeroCopyMetrics();
  }

  public void assertZeroCopyMetrics() {
    getServers().forEach(server -> server.getGroupIds().forEach(id -> {
      LOG.info("Checking {}-{}", server.getId(), id);
      RaftServer.Division division = RaftServerTestUtil.getDivision(server, id);
      final GrpcServicesImpl service = (GrpcServicesImpl) RaftServerTestUtil.getServerRpc(division);
      ZeroCopyMetrics zeroCopyMetrics = service.getZeroCopyMetrics();
      Assert.assertEquals(0, zeroCopyMetrics.nonZeroCopyMessages());
      Assert.assertEquals("Zero copy messages are not released, please check logs to find leaks. ",
          zeroCopyMetrics.zeroCopyMessages(), zeroCopyMetrics.releasedMessages());
    }));
  }
}
