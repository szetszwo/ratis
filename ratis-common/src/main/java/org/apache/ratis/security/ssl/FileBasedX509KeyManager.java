/**
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

import org.apache.ratis.security.Password;

import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509ExtendedKeyManager;
import java.io.File;
import java.io.IOException;
import java.net.Socket;
import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.security.Principal;
import java.security.PrivateKey;
import java.security.cert.X509Certificate;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

/**
 * An implementation of {@link X509ExtendedKeyManager}
 * that exposes a {@link #load(Path)} method to reload its configuration.
 *
 * Note that it is necessary to implement the {@link X509ExtendedKeyManager}
 * in order to properly delegate the additional methods.
 * Otherwise, the SSL handshake will fail.
 */
public class FileBasedX509KeyManager extends X509ExtendedKeyManager {
  private final KeyStoreLoader keyStoreLoader;
  private final Supplier<Password> keyPassword;
  private final AtomicReference<X509ExtendedKeyManager> ref = new AtomicReference<>();

  /**
   * Construct a <code>Reloading509KeystoreManager</code>
   *
   * @param keyStoreLoader for initializing a {@link TrustManagerFactory}.
   * @param location path to the keystore file.
   * @param keyPassword The password of the key.
   */
  public FileBasedX509KeyManager(KeyStoreLoader keyStoreLoader, Supplier<Password> keyPassword, File location)
      throws GeneralSecurityException, IOException {
    this.keyStoreLoader = keyStoreLoader;
    this.keyPassword = keyPassword;
    this.ref.set(loadKeyManager(location));
  }

  public X509ExtendedKeyManager getKeyManager() {
    return Objects.requireNonNull(ref.get(), "KeyManager is null");
  }

  @Override
  public String chooseEngineClientAlias(String[] keyType, Principal[] principals, SSLEngine sslEngine) {
    return getKeyManager().chooseEngineClientAlias(keyType, principals, sslEngine);
  }

  @Override
  public String chooseEngineServerAlias(String keyType, Principal[] principals, SSLEngine sslEngine) {
    return getKeyManager().chooseEngineServerAlias(keyType, principals, sslEngine);
  }

  @Override
  public String[] getClientAliases(String keyType, Principal[] principals) {
    return getKeyManager().getClientAliases(keyType, principals);
  }

  @Override
  public String chooseClientAlias(String[] keyType, Principal[] principals, Socket socket) {
    return getKeyManager().chooseClientAlias(keyType, principals, socket);
  }

  @Override
  public String[] getServerAliases(String keyType, Principal[] principals) {
    return getKeyManager().getServerAliases(keyType, principals);
  }

  @Override
  public String chooseServerAlias(String keyType, Principal[] principals, Socket socket) {
    return getKeyManager().chooseServerAlias(keyType, principals, socket);
  }

  @Override
  public X509Certificate[] getCertificateChain(String alias) {
    return getKeyManager().getCertificateChain(alias);
  }

  @Override
  public PrivateKey getPrivateKey(String alias) {
    return getKeyManager().getPrivateKey(alias);
  }

  public FileBasedX509KeyManager load(File file) throws GeneralSecurityException, IOException {
    this.ref.set(loadKeyManager(file));
    return this;
  }

  private X509ExtendedKeyManager loadKeyManager(File file) throws GeneralSecurityException, IOException {
    final KeyStore keyStore = keyStoreLoader.load(file);
    final KeyManagerFactory factory = KeyManagerFactory.getInstance(SslFactory.KEY_MANAGER_SSL_CERTIFICATE_ALGORITHM);
    try(Password password = keyPassword.get()) {
      factory.init(keyStore, password.toCharArray());
    }
    for (KeyManager keyManager : factory.getKeyManagers()) {
      if (keyManager instanceof X509ExtendedKeyManager) {
        return (X509ExtendedKeyManager)keyManager;
      }
    }
    return null;
  }
}
