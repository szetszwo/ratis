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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.text.MessageFormat;
import java.util.Locale;
import java.util.function.Consumer;

import static org.apache.ratis.conf.ConfUtils.get;
import static org.apache.ratis.conf.ConfUtils.getChars;
import static org.apache.ratis.conf.ConfUtils.getFile;
import static org.apache.ratis.conf.ConfUtils.printAll;
import static org.apache.ratis.conf.ConfUtils.set;
import static org.apache.ratis.conf.ConfUtils.setChars;
import static org.apache.ratis.conf.ConfUtils.setFile;

public interface SslConfigKeys {
  Logger LOG = LoggerFactory.getLogger(SslConfigKeys.class);
  static Consumer<String> getDefaultLog() {
    return LOG::info;
  }

  enum Mode {
    CLIENT, SERVER;

    private final String lowerCaseName = name().toLowerCase(Locale.ENGLISH);

    public String getLowerCaseName() {
      return lowerCaseName;
    }

    public String getKey(String template) {
      return MessageFormat.format(template, getLowerCaseName());
    }
  }

  String PREFIX = "ssl.{0}";

  interface KeyStore {
    String PREFIX = SslConfigKeys.PREFIX + ".keystore";

    String LOCATION_TEMPLATE = PREFIX + ".location";
    File LOCATION_DEFAULT = new File("./keystore");
    static File location(Mode mode, RaftProperties properties) {
      final String key = mode.getKey(LOCATION_TEMPLATE);
      return getFile(properties::getFile, key, LOCATION_DEFAULT, getDefaultLog());
    }
    static void setLocation(Mode mode, RaftProperties properties, File location) {
      final String key = mode.getKey(LOCATION_TEMPLATE);
      setFile(properties::setFile, key, location);
    }

    String TYPE_TEMPLATE = PREFIX + ".type";
    String TYPE_DEFAULT = "jks";
    static String type(Mode mode, RaftProperties properties) {
      final String key = mode.getKey(TYPE_TEMPLATE);
      return get(properties::get, key, TYPE_DEFAULT, getDefaultLog());
    }
    static void setType(Mode mode, RaftProperties properties, String type) {
      final String key = mode.getKey(LOCATION_TEMPLATE);
      set(properties::set, key, type);
    }

    String PASSWORD_TEMPLATE = PREFIX + ".password";
    char[] PASSWORD_DEFAULT = null;
    static char[] password(Mode mode, RaftProperties properties) {
      final String key = mode.getKey(PASSWORD_TEMPLATE);
      return getChars(properties::get, key, PASSWORD_DEFAULT, getDefaultLog());
    }
    static void setPassword(Mode mode, RaftProperties properties, char[] password) {
      final String key = mode.getKey(PASSWORD_TEMPLATE);
      setChars(properties::set, key, password);
    }

    String KEY_PASSWORD_TEMPLATE = PREFIX + ".key-password";
    char[] KEY_PASSWORD_DEFAULT = null;
    static char[] keyPassword(Mode mode, RaftProperties properties) {
      final String key = mode.getKey(KEY_PASSWORD_TEMPLATE);
      return getChars(properties::get, key, KEY_PASSWORD_DEFAULT, getDefaultLog());
    }
    static void setKeyPassword(Mode mode, RaftProperties properties, char[] password) {
      final String key = mode.getKey(KEY_PASSWORD_TEMPLATE);
      setChars(properties::set, key, password);
    }

  }

  interface TrustStore {
    String PREFIX = SslConfigKeys.PREFIX + ".truststore";

    String LOCATION_TEMPLATE = PREFIX + ".location";
    File LOCATION_DEFAULT = new File("./keystore");
    static String locationKey(Mode mode) {
      return mode.getKey(LOCATION_TEMPLATE);
    }
    static File location(Mode mode, RaftProperties properties) {
      final String key = mode.getKey(LOCATION_TEMPLATE);
      return getFile(properties::getFile, key, LOCATION_DEFAULT, getDefaultLog());
    }
    static void setLocation(Mode mode, RaftProperties properties, File location) {
      final String key = mode.getKey(LOCATION_TEMPLATE);
      setFile(properties::setFile, key, location);
    }

    String TYPE_TEMPLATE = PREFIX + ".type";
    String TYPE_DEFAULT = "jks";
    static String type(Mode mode, RaftProperties properties) {
      final String key = mode.getKey(TYPE_TEMPLATE);
      return get(properties::get, key, TYPE_DEFAULT, getDefaultLog());
    }
    static void setType(Mode mode, RaftProperties properties, String type) {
      final String key = mode.getKey(LOCATION_TEMPLATE);
      set(properties::set, key, type);
    }
  }

  static void main(String[] args) {
    printAll(SslConfigKeys.class);
  }
}
