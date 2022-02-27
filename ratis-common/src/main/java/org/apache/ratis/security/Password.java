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
package org.apache.ratis.security;

import org.apache.ratis.util.TimeDuration;
import org.apache.ratis.util.TimeoutScheduler;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

/**
 * Temporarily storing a password in memory.
 * In order to minimize the window of opportunity for an attacker,
 * the password is expected to be cleared in a short period of time.
 * If it is not cleared until it times out, it will be auto-cleared.
 */
public class Password implements AutoCloseable {
  private static final TimeDuration DEFAULT_TIMEOUT = TimeDuration.ONE_SECOND;

  private final String name;
  private final char[] password;
  private final AtomicBoolean isCleared = new AtomicBoolean();

  /**
   * Construct a {@link Password}.
   *
   * @param name The name such as username
   * @param password the actual password.
   * @param log for printing log messages
   * @param timeout for auto-clearing the password.
   */
  public Password(String name, char[] password, Consumer<String> log, TimeDuration timeout) {
    this.name = name;
    this.password = password;
    if (password != null) {
      TimeoutScheduler.getInstance().onTimeout(timeout, () -> handleTimeout(timeout, log));
    }
  }

  public Password(String name, char[] password, Consumer<String> log) {
    this(name, password, log, DEFAULT_TIMEOUT);
  }


  private void handleTimeout(TimeDuration timeout, Consumer<String> log) {
    if (clear()) {
      log.accept("Auto-clear the password of " + name + " (timed out " + timeout + ")");
    }
  }

  public char[] toCharArray() {
    return password;
  }

  /**
   * Clear the password stored in this object.
   *
   * @return true if the password is cleared by this call;
   *         otherwise, this is a no-op since the password is already cleared.
   */
  public boolean clear() {
    if (isCleared.compareAndSet(false, true)) {
      if (password != null) {
        Arrays.fill(password, ' ');
      }
      return true;
    }
    return false;
  }

  /** The same as {@link #clear()}. */
  @Override
  public void close() {
    clear();
  }

  @Override
  public String toString() {
    return "password of " + name;
  }
}