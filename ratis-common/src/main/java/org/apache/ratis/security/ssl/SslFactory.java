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

import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.security.ssl.SslConfigKeys.Mode;
import org.apache.ratis.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLServerSocketFactory;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.TrustManagerFactory;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;

/**
 * Factory that creates {@link SSLEngine} and {@link SSLSocketFactory} instances.
 *
 * This {@link SslFactory} uses a {@link FileBasedX509TrustManager} instance,
 * which may reloads the underlying keystore file changes.
 */
public class SslFactory {
  static final Logger LOG = LoggerFactory.getLogger(SslFactory.class);

  public static final String SSL_CLIENT_CONF_KEY = "ssl.client.conf";
  public static final String SSL_CLIENT_CONF_DEFAULT = "ssl-client.xml";

  public static final String SSL_SERVER_CONF_KEY = "ssl.server.conf";
  public static final String SSL_SERVER_CONF_DEFAULT = "ssl-server.xml";

  public static final String SSL_REQUIRE_CLIENT_CERT_KEY = "ssl.require.client.cert";
  public static final boolean SSL_REQUIRE_CLIENT_CERT_DEFAULT = false;

  public static final String SSL_HOSTNAME_VERIFIER_KEY = "ssl.hostname.verifier";
  public static final String SSL_ENABLED_PROTOCOLS_KEY = "ssl.enabled.protocols";
  public static final String SSL_ENABLED_PROTOCOLS_DEFAULT = "TLSv1.2";

  public static final String SSL_SERVER_NEED_CLIENT_AUTH = "ssl.server.need.client.auth";
  public static final boolean SSL_SERVER_NEED_CLIENT_AUTH_DEFAULT = false;

  public static final String SSL_SERVER_KEYSTORE_LOCATION = "ssl.server.keystore.location";
  public static final String SSL_SERVER_KEYSTORE_PASSWORD = "ssl.server.keystore.password";
  public static final String SSL_SERVER_KEYSTORE_TYPE = "ssl.server.keystore.type";
  public static final String SSL_SERVER_KEYSTORE_TYPE_DEFAULT = "jks";
  public static final String SSL_SERVER_KEYSTORE_KEYPASSWORD = "ssl.server.keystore.keypassword";

  public static final String SSL_SERVER_TRUSTSTORE_LOCATION = "ssl.server.truststore.location";
  public static final String SSL_SERVER_TRUSTSTORE_PASSWORD = "ssl.server.truststore.password";
  public static final String SSL_SERVER_TRUSTSTORE_TYPE = "ssl.server.truststore.type";
  public static final String SSL_SERVER_TRUSTSTORE_TYPE_DEFAULT = "jks";

  public static final String SSL_SERVER_EXCLUDE_CIPHER_LIST = "ssl.server.exclude.cipher.list";

  public static final String JAVA_VENDOR_NAME = System.getProperty("java.vendor");
  public static final String KEY_MANAGER_SSL_CERTIFICATE_ALGORITHM;
  public static final String TRUST_MANAGER_SSL_CERTIFICATE_ALGORITHM;
  static {
    final boolean isIBM = JAVA_VENDOR_NAME.contains("IBM");
    KEY_MANAGER_SSL_CERTIFICATE_ALGORITHM = isIBM? "ibmX509" : KeyManagerFactory.getDefaultAlgorithm();
    TRUST_MANAGER_SSL_CERTIFICATE_ALGORITHM = isIBM? "ibmX509" : TrustManagerFactory.getDefaultAlgorithm();
  }

  private final RaftProperties conf;
  private final Mode mode;
  private final boolean requireClientCert;
  private SSLContext context;
  // the java keep-alive cache relies on instance equivalence of the SSL socket
  // factory.  in many java versions, SSLContext#getSocketFactory always
  // returns a new instance which completely breaks the cache...
  private SSLSocketFactory socketFactory;
  private HostnameVerifier hostnameVerifier;
  private final KeyStoreFactory keystoresFactory;

  private final String[] enabledProtocols;
  private final List<String> excludeCiphers;

  /**
   * Creates an SSLFactory.
   *
   * @param mode SSLFactory mode, client or server.
   * @param conf Hadoop configuration from where the SSLFactory configuration
   * will be read.
   */
  public SslFactory(Mode mode, RaftProperties conf, RaftProperties sslConf) {
    this.conf = conf;
    this.mode = Objects.requireNonNull(mode, "mode == null");

    this.requireClientCert = sslConf.getBoolean(SSL_REQUIRE_CLIENT_CERT_KEY, SSL_REQUIRE_CLIENT_CERT_DEFAULT);
    this.keystoresFactory = new FileBasedKeyStoreFactory();

    final String enabledProtocolsString = conf.get(SSL_ENABLED_PROTOCOLS_KEY, SSL_ENABLED_PROTOCOLS_DEFAULT);
    enabledProtocols = StringUtils.getTrimmedStrings(enabledProtocolsString);
    excludeCiphers = Arrays.asList( sslConf.getTrimmedStrings(SSL_SERVER_EXCLUDE_CIPHER_LIST));
    if (LOG.isDebugEnabled()) {
      LOG.debug("will exclude cipher suites: {}", excludeCiphers);
    }
  }

  /**
   * Initializes the factory.
   *
   * @throws GeneralSecurityException thrown if an SSL initialization error
   * happened.
   * @throws IOException thrown if an IO error happened while reading the SSL
   * configuration.
   */
  public void init() throws GeneralSecurityException, IOException {
    keystoresFactory.init(mode);
    context = SSLContext.getInstance("TLS");
    context.init(keystoresFactory.getKeyManagers(),
        keystoresFactory.getTrustManagers(), null);
    context.getDefaultSSLParameters().setProtocols(enabledProtocols);
    if (mode == Mode.CLIENT) {
      socketFactory = context.getSocketFactory();
    }
    hostnameVerifier = getHostnameVerifier(conf);
  }

  /**
   * Releases any resources being used.
   */
  public void destroy() {
    keystoresFactory.destroy();
  }
  /**
   * Returns the SSLFactory KeyStoresFactory instance.
   *
   * @return the SSLFactory KeyStoresFactory instance.
   */
  public KeyStoreFactory getKeystoresFactory() {
    return keystoresFactory;
  }

  /**
   * Returns a configured SSLEngine.
   *
   * @return the configured SSLEngine.
   * @throws GeneralSecurityException thrown if the SSL engine could not
   * be initialized.
   * @throws IOException thrown if and IO error occurred while loading
   * the server keystore.
   */
  public SSLEngine createSSLEngine()
      throws GeneralSecurityException, IOException {
    SSLEngine sslEngine = context.createSSLEngine();
    if (mode == Mode.CLIENT) {
      sslEngine.setUseClientMode(true);
    } else {
      sslEngine.setUseClientMode(false);
      sslEngine.setNeedClientAuth(requireClientCert);
      disableExcludedCiphers(sslEngine);
    }
    sslEngine.setEnabledProtocols(enabledProtocols);
    return sslEngine;
  }

  private void disableExcludedCiphers(SSLEngine sslEngine) {
    String[] cipherSuites = sslEngine.getEnabledCipherSuites();

    ArrayList<String> defaultEnabledCipherSuites =
        new ArrayList<String>(Arrays.asList(cipherSuites));
    Iterator iterator = excludeCiphers.iterator();

    while(iterator.hasNext()) {
      String cipherName = (String)iterator.next();
      if(defaultEnabledCipherSuites.contains(cipherName)) {
        defaultEnabledCipherSuites.remove(cipherName);
        LOG.debug("Disabling cipher suite {}.", cipherName);
      }
    }

    cipherSuites = defaultEnabledCipherSuites.toArray(
        new String[defaultEnabledCipherSuites.size()]);
    sslEngine.setEnabledCipherSuites(cipherSuites);
  }

  /**
   * Returns a configured SSLServerSocketFactory.
   *
   * @return the configured SSLSocketFactory.
   * @throws GeneralSecurityException thrown if the SSLSocketFactory could not
   * be initialized.
   * @throws IOException thrown if and IO error occurred while loading
   * the server keystore.
   */
  public SSLServerSocketFactory createSSLServerSocketFactory()
      throws GeneralSecurityException, IOException {
    if (mode != Mode.SERVER) {
      throw new IllegalStateException(
          "Factory is not in SERVER mode. Actual mode is " + mode.toString());
    }
    return context.getServerSocketFactory();
  }

  /**
   * Returns a configured SSLSocketFactory.
   *
   * @return the configured SSLSocketFactory.
   * @throws GeneralSecurityException thrown if the SSLSocketFactory could not
   * be initialized.
   * @throws IOException thrown if and IO error occurred while loading
   * the server keystore.
   */
  public SSLSocketFactory createSSLSocketFactory()
      throws GeneralSecurityException, IOException {
    if (mode != Mode.CLIENT) {
      throw new IllegalStateException(
          "Factory is not in CLIENT mode. Actual mode is " + mode.toString());
    }
    return socketFactory;
  }

  /**
   * Returns the hostname verifier it should be used in HttpsURLConnections.
   *
   * @return the hostname verifier.
   */
  public HostnameVerifier getHostnameVerifier(RaftProperties conf) {
    if (mode != Mode.CLIENT) {
      throw new IllegalStateException("Factory is not in CLIENT mode. Actual mode is " + mode);
    }
    return hostnameVerifier;
  }

  /**
   * Returns if client certificates are required or not.
   *
   * @return if client certificates are required or not.
   */
  public boolean isClientCertRequired() {
    return requireClientCert;
  }
}

