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

import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.security.Password;
import org.apache.ratis.security.ssl.SslConfigKeys.Mode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.TrustManager;
import java.io.File;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.util.function.Supplier;

/**
 * {@link KeyStoreFactory} implementation that reads the certificates from
 * keystore files.
 * <p>
 * If either the truststore or the keystore certificates file changes, it
 * would be refreshed under the corresponding wrapper implementation -
 * {@link FileBasedX509KeyManager} or {@link FileBasedX509TrustManager}.
 * </p>
 */
public class FileBasedKeyStoreFactory implements KeyStoreFactory {
  private static final Logger LOG = LoggerFactory.getLogger(FileBasedKeyStoreFactory.class);

  private RaftProperties properties;
  private KeyManager[] keyManagers;
  private TrustManager[] trustManagers;

  static FileBasedX509TrustManager newTrustManager(Mode mode,
      String truststoreType, Supplier<Password> truststorePassword, File truststoreLocation)
      throws IOException, GeneralSecurityException {
    if (LOG.isDebugEnabled()) {
      LOG.debug(mode + " TrustStore: " + truststoreLocation);
    }
    final KeyStoreLoader keyStoreLoader = new KeyStoreLoader(truststoreType, truststorePassword);
    return new FileBasedX509TrustManager(keyStoreLoader, truststoreLocation);
  }

  /**
   * Implements logic of initializing the KeyManagers with the options to reload keystores.
   * @param mode client or server
   * @param keystoreType The keystore type.
   */
  static FileBasedX509KeyManager newKeyManager(Mode mode, String keystoreType,
      Supplier<Password> keystorePassword, Supplier<Password> keyPassword, File keystoreLocation)
      throws GeneralSecurityException, IOException {
    // Key password defaults to the same value as store password for
    // compatibility with legacy configurations that did not use a separate
    // configuration property for key password.
    if (LOG.isDebugEnabled()) {
      LOG.debug(mode + " KeyStore: " + keystoreLocation);
    }

    final KeyStoreLoader keyStoreLoader = new KeyStoreLoader(keystoreType, keystorePassword);
    return new FileBasedX509KeyManager(keyStoreLoader, keyPassword, keystoreLocation);
  }

  /**
   * Initializes the keystores of the factory.
   *
   * @param mode if the keystores are to be used in client or server mode.
   * @throws IOException thrown if the keystores could not be initialized due
   * to an IO error.
   * @throws GeneralSecurityException thrown if the keystores could not be
   * initialized due to a security error.
   */
  @Override
  public void init(Mode mode) throws IOException, GeneralSecurityException {
    boolean requireClientCert = properties.getBoolean(SslFactory.SSL_REQUIRE_CLIENT_CERT_KEY, SslFactory.SSL_REQUIRE_CLIENT_CERT_DEFAULT);

    // certificate store
    final String keystoreType = SslConfigKeys.KeyStore.type(mode, properties);
    final File keystoreLocation = SslConfigKeys.KeyStore.location(mode, properties);
    if (requireClientCert || mode == Mode.SERVER) {
      keyManagers = new KeyManager[]{newKeyManager(mode, keystoreType,
          () -> new Password("keystore", SslConfigKeys.KeyStore.password(mode, properties), LOG::info),
          () -> new Password("keystore-key", SslConfigKeys.KeyStore.keyPassword(mode, properties), LOG::info),
          keystoreLocation)};
    } else {
      KeyStore keystore = KeyStore.getInstance(keystoreType);
      keystore.load(null, null);
      final KeyManagerFactory keyMgrFactory = KeyManagerFactory.getInstance(SslFactory.KEY_MANAGER_SSL_CERTIFICATE_ALGORITHM);

      keyMgrFactory.init(keystore, null);
      keyManagers = keyMgrFactory.getKeyManagers();
    }

    //trust store
    final String truststoreType = SslConfigKeys.TrustStore.type(mode, properties);
    final File truststoreLocation = SslConfigKeys.TrustStore.location(mode, properties);
    if (truststoreLocation != null) {
      trustManagers = new TrustManager[]{newTrustManager(mode, truststoreType,
          () -> new Password("keystore", SslConfigKeys.KeyStore.password(mode, properties), LOG::info),
          truststoreLocation)};
    } else {
      trustManagers = null;
    }
  }

  /**
   * Releases any resources being used.
   */
  @Override
  public synchronized void destroy() {
    trustManagers = null;
    keyManagers = null;
  }

  /**
   * Returns the keymanagers for owned certificates.
   *
   * @return the keymanagers for owned certificates.
   */
  @Override
  public KeyManager[] getKeyManagers() {
    return keyManagers;
  }

  /**
   * @return the trustmanagers for trusted certificates.
   */
  @Override
  public TrustManager[] getTrustManagers() {
    return trustManagers;
  }

}
