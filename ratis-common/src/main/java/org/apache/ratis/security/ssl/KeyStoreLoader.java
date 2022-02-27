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

import org.apache.ratis.security.Password;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.util.function.Supplier;

/**
 * To load a {@link KeyStore} from a file.
 */
final class KeyStoreLoader {
  static final Logger LOG = LoggerFactory.getLogger(KeyStoreLoader.class);

  private final String keyStoreType;
  private final Supplier<Password> keyStorePassword;

  /**
   * Creates a reloadable trust manager.
   * The trust manager reloads itself if the underlying truststore file has changed.
   *
   * @param keyStoreType type of the {@link KeyStore}, typically 'jks'.
   * @param keyStorePassword the password of the {@link KeyStore}.
   */
  KeyStoreLoader(String keyStoreType, Supplier<Password> keyStorePassword) {
    this.keyStoreType = keyStoreType;
    this.keyStorePassword = keyStorePassword;
  }

  KeyStore load(File file) throws GeneralSecurityException, IOException {
    final KeyStore keyStore = KeyStore.getInstance(keyStoreType);
    try (InputStream in = new FileInputStream(file);
         Password password = keyStorePassword.get()) {
      keyStore.load(in, password.toCharArray());
      LOG.debug("Loaded keystore from {}",  file);
    }
    return keyStore;
  }
}
