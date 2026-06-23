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
package org.apache.ratis.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;
import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;

/**
 * An {@link AsyncQueue} for an unbounded, asynchronous queue.
 * <p>
 * - Unbounded: {@link #offer(Object)} always add the given element to the queue.
 * <p>
 * - Asynchronous: {@link #poll()} returns immediately a {@link CompletableFuture} even if the queue is empty
 * <p>
 * Null element is NOT supported.
 * <p>
 * This class is threadsafe.
 */
public class AsyncQueue<E> {
  public static final Logger LOG = LoggerFactory.getLogger(AsyncQueue.class);

  private final String name;
  /** The queue of offered elements but not yet polled. */
  private final Queue<E> outstandingOffers = new LinkedList<>();
  /** The queue of polled futures but not yet offered. */
  private final Queue<CompletableFuture<E>> outstandingPolls = new LinkedList<>();

  public AsyncQueue(Object name) {
    this.name = name + "-" + JavaUtils.getClassSimpleName(getClass());
  }

  /**
   * Invariants:
   * (1) at least one of {@link #outstandingOffers} and {@link #outstandingPolls} is empty.
   * (2) all the futures in {@link #outstandingPolls} have {@link CompletableFuture#isDone()} == false.
   */
  private void assertInvariant() {
    Preconditions.assertTrue(outstandingOffers.isEmpty() || outstandingPolls.isEmpty(),
        () -> "Both queue are non-empty: outstandingOffers.size()=" + outstandingOffers.size()
            + ", outstandingPolls.size()=" + outstandingPolls.size());
  }

  /**
   * Similar to {@link Queue#offer(Object)} except that
   * this method always adds the given element to the queue and returns void.
   */
  public synchronized void offer(E element) {
    Objects.requireNonNull(element, "element == null");
    assertInvariant();
    final CompletableFuture<E> f = outstandingPolls.poll();
    if (f != null) {
      f.complete(element);
    }
    final boolean offered = outstandingOffers.offer(element);
    Preconditions.assertTrue(offered, "Failed to offer an element.");
  }

  /**
   * Similar to {@link Queue#poll()} except that
   * this method returns immediately a {@link CompletableFuture}.
   * If the queue is non-empty, poll of the head element and the return a completed future with it.
   * If the queue is empty, return an uncompleted future,
   * which will be completed when the corresponding element is offered.
   * <p>
   * Example:
   * <pre>
   * Step | Call      | Action              | Queue
   * -----+-----------+---------------------+---------------------------
   * 0.   |           |                     | [empty]
   * 1.   | offer(e1) | add e1              | tail -> e1 <- head
   * 1.   | offer(e2) | add e2              | tail -> e2 <- e1 <- head
   * 2.   | pull()    | return future(e1)   | tail -> e2 <- head
   * 2.   | pull()    | return future(e2)   | [empty]
   * 3.   | pull()    | return f1           | tail -> f1 <- head
   * 4.   | pull()    | return f2           | tail -> f2 <- f1 <- head
   * 5.   | offer(e3) | complete f1 with e3 | tail -> f2 <- head
   * 5.   | offer(e4) | complete f2 with e4 | [empty]
   * </pre>
   */
  public synchronized CompletableFuture<E> poll() {
    assertInvariant();
    final E e = outstandingOffers.poll();
    if (e != null) {
      return CompletableFuture.completedFuture(e);
    }
    //
    final CompletableFuture<E> f = new CompletableFuture<>();
    final boolean offered = outstandingPolls.offer(f);
    Preconditions.assertTrue(offered, "Failed to offer a future.");
    return f;
  }

  /**
   * @return the number of outstanding offers minus the number outstanding polls.
   *         When the returned value non-negative, it is the same as the size of the queue.
   */
  public synchronized int outstandingCount() {
    assertInvariant();
    return outstandingOffers.size() - outstandingPolls.size();
  }

  @Override
  public String toString() {
    return name + ":" + outstandingCount();
  }
}
