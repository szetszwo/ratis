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
package org.apache.ratis.security.ssl;

import javax.net.ssl.KeyManager;
import javax.net.ssl.TrustManager;
import java.io.IOException;
import java.security.GeneralSecurityException;

/**
 * Interface to a {@link KeyManager} and a {@link TrustManager} implementations.
 */
public interface KeyStoreFactory {

  /**
   * Initializes the key stores of the factory.
   *
   * @param mode if the key stores are to be used in client or server mode.
   * @throws IOException thrown if there is an IO error.
   * @throws GeneralSecurityException thrown if there is a security error.
   */
  void init(SslConfigKeys.Mode mode) throws IOException, GeneralSecurityException;

  /**
   * Releases any resources being used.
   */
  void destroy();

  /**
   * @return the key managers for owned certificates.
   */
  KeyManager[] getKeyManagers();

  /**
   * @return the trust managers for trusted certificates.
   */
  TrustManager[] getTrustManagers();
}
