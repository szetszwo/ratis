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

import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509TrustManager;
import java.io.File;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

/**
 * An implementation of {@link X509TrustManager}
 * that exposes a {@link #load(File)} method to reload its configuration.
 */
public final class FileBasedX509TrustManager implements X509TrustManager {
  private final KeyStoreLoader keyStoreLoader;
  private final AtomicReference<X509TrustManager> ref = new AtomicReference<>();

  /**
   * Creates a reloadable trust manager.
   * The trust manager reloads itself if the underlying truststore file has changed.
   *
   * @param keyStoreLoader for initializing a {@link TrustManagerFactory}.
   * @param location path to the truststore file.
   * @throws IOException thrown if the truststore could not be loaded due to an IO error.
   * @throws GeneralSecurityException thrown if the truststore could not be loaded due to a security error.
   */
  public FileBasedX509TrustManager(KeyStoreLoader keyStoreLoader, File location)
    throws IOException, GeneralSecurityException {
    this.keyStoreLoader = keyStoreLoader;
    this.ref.set(loadTrustManager(location));
  }

  private X509TrustManager getTrustManager() {
    return Objects.requireNonNull(ref.get(), "X509TrustManager is null");
  }

  private Optional<X509TrustManager> getOptionalTrustManager() {
    return Optional.ofNullable(ref.get());
  }

  @Override
  public void checkClientTrusted(X509Certificate[] chain, String authType) throws CertificateException {
    getTrustManager().checkClientTrusted(chain, authType);
  }

  @Override
  public void checkServerTrusted(X509Certificate[] chain, String authType) throws CertificateException {
    getTrustManager().checkServerTrusted(chain, authType);
  }

  @Override
  public X509Certificate[] getAcceptedIssuers() {
    return getOptionalTrustManager().map(X509TrustManager::getAcceptedIssuers).orElse(null);
  }

  public FileBasedX509TrustManager load(File file) throws GeneralSecurityException, IOException {
    ref.set(loadTrustManager(file));
    return this;
  }

  private X509TrustManager loadTrustManager(File file) throws GeneralSecurityException, IOException {
    final TrustManagerFactory factory = TrustManagerFactory.getInstance(SslFactory.TRUST_MANAGER_SSL_CERTIFICATE_ALGORITHM);
    factory.init(keyStoreLoader.load(file));
    for (TrustManager trustManager : factory.getTrustManagers()) {
      if (trustManager instanceof X509TrustManager) {
        return (X509TrustManager) trustManager;
      }
    }
    return null;
  }
}
