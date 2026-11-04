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

import org.apache.ratis.BaseTest;
import org.apache.ratis.RaftTestUtil;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.conf.Parameters;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.protocol.RaftClientReply;
import org.apache.ratis.retry.RetryPolicies;
import org.apache.ratis.rpc.SupportedRpcType;
import org.apache.ratis.RaftConfigKeys;
import org.apache.ratis.security.SecurityTestUtils;
import org.apache.ratis.security.TlsConf;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.impl.MiniRaftCluster;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;

/** Test TLS for {@link org.apache.ratis.netty.server.NettyRpcService} and the Netty client/server proxies. */
public class TestRaftWithNettyTls extends BaseTest {
  static Parameters newParameters(boolean mutualTls) {
    final Parameters parameters = new Parameters();
    NettyConfigKeys.Server.setTlsConf(parameters, SecurityTestUtils.newServerTlsConfig(mutualTls));
    NettyConfigKeys.Client.setTlsConf(parameters, SecurityTestUtils.newClientTlsConfig(mutualTls));
    return parameters;
  }

  static MiniRaftClusterWithNetty newCluster(int numServers, Parameters parameters) {
    final RaftProperties properties = new RaftProperties();
    RaftConfigKeys.Rpc.setType(properties, SupportedRpcType.NETTY);
    final String[] ids = MiniRaftCluster.generateIds(numServers, 0);
    return new MiniRaftClusterWithNetty(ids, new String[0], properties, parameters);
  }

  @Test
  public void testDataStreamTlsConfFallback() {
    final TlsConf server = SecurityTestUtils.newServerTlsConfig(true);
    final TlsConf client = SecurityTestUtils.newClientTlsConfig(true);

    // nothing set
    final Parameters parameters = new Parameters();
    Assertions.assertNull(NettyConfigKeys.DataStream.Server.tlsConf(parameters));
    Assertions.assertNull(NettyConfigKeys.DataStream.Client.tlsConf(parameters));

    // only the Netty RPC confs are set: fall back
    NettyConfigKeys.Server.setTlsConf(parameters, server);
    NettyConfigKeys.Client.setTlsConf(parameters, client);
    Assertions.assertSame(server, NettyConfigKeys.DataStream.Server.tlsConf(parameters));
    Assertions.assertSame(client, NettyConfigKeys.DataStream.Client.tlsConf(parameters));

    // the data stream confs are set: no fall back
    final TlsConf dataStreamServer = SecurityTestUtils.newServerTlsConfig(false);
    final TlsConf dataStreamClient = SecurityTestUtils.newClientTlsConfig(false);
    NettyConfigKeys.DataStream.Server.setTlsConf(parameters, dataStreamServer);
    NettyConfigKeys.DataStream.Client.setTlsConf(parameters, dataStreamClient);
    Assertions.assertSame(dataStreamServer, NettyConfigKeys.DataStream.Server.tlsConf(parameters));
    Assertions.assertSame(dataStreamClient, NettyConfigKeys.DataStream.Client.tlsConf(parameters));
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testTls(boolean mutualTls) throws Exception {
    try (MiniRaftClusterWithNetty cluster = newCluster(3, newParameters(mutualTls))) {
      cluster.start();
      // leader election and log replication use the server-to-server TLS connections
      final RaftServer.Division leader = RaftTestUtil.waitForLeader(cluster);
      try (RaftClient client = cluster.createClient(leader.getId())) {
        for (int i = 0; i < 10; i++) {
          final RaftClientReply reply = client.io().send(new RaftTestUtil.SimpleMessage("m" + i));
          Assertions.assertTrue(reply.isSuccess());
        }
      }
      // all followers should catch up through the TLS connections
      final long leaderCommit = leader.getRaftLog().getLastCommittedIndex();
      for (RaftServer.Division d : cluster.iterateDivisions()) {
        RaftTestUtil.waitFor(() -> d.getRaftLog().getLastCommittedIndex() >= leaderCommit, 100, 10_000);
      }
    }
  }

  @Test
  public void testPlaintextClientRejected() throws Exception {
    try (MiniRaftClusterWithNetty cluster = newCluster(1, newParameters(true))) {
      cluster.start();
      final RaftServer.Division leader = RaftTestUtil.waitForLeader(cluster);
      try (RaftClient client = RaftClient.newBuilder()
          .setRaftGroup(cluster.getGroup())
          .setLeaderId(leader.getId())
          .setProperties(cluster.getProperties())
          .setParameters(new Parameters()) // no TLS
          .setRetryPolicy(RetryPolicies.noRetry())
          .build()) {
        Assertions.assertThrows(IOException.class,
            () -> client.io().send(new RaftTestUtil.SimpleMessage("plaintext")));
      }
    }
  }
}
