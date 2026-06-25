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
import java.util.function.Consumer;
import java.util.function.Supplier;

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
  /** The queue of existing elements (offered but not yet polled) */
  private final Queue<E> existingElements = new LinkedList<>();
  /** The queue of future elements (polled but not yet offered) */
  private final Queue<CompletableFuture<E>> futureElements = new LinkedList<>();

  private Throwable failure = null;

  public AsyncQueue(Object name) {
    this.name = name + "-" + JavaUtils.getClassSimpleName(getClass());
  }

  /**
   * Invariants:
   * (1) at least one of {@link #existingElements} and {@link #futureElements} is empty;
   * (2) if the queue is failed, {@link #futureElements} must be empty.
   */
  private void assertInvariant() {
    Preconditions.assertTrue(existingElements.isEmpty() || futureElements.isEmpty(),
        () -> "Both queue are non-empty: existingElements.size()=" + existingElements.size()
            + ", futureElements.size()=" + futureElements.size());
    if (failure != null) {
      Preconditions.assertTrue(futureElements.isEmpty(),
          () -> "Failed but still have " + futureElements.size() + " future elements, failure=" + failure);
    }
  }

  /**
   * Similar to {@link Queue#offer(Object)} except that
   * this method always adds the given element to the queue and returns void.
   */
  public synchronized void offer(E element) {
    Objects.requireNonNull(element, "element == null");
    assertInvariant();
    final CompletableFuture<E> f = futureElements.poll();
    if (f != null) {
      final boolean completed = f.complete(element);
      Preconditions.assertTrue(completed);
    }
    existingElements.add(element);
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
   *
   * </empty>
   */
  public synchronized CompletableFuture<E> poll() {
    assertInvariant();
    final E e = existingElements.poll();
    if (e != null) {
      return CompletableFuture.completedFuture(e);
    }
    if (failure != null) {
      return JavaUtils.completeExceptionally(failure);
    }
    final CompletableFuture<E> f = new CompletableFuture<>();
    futureElements.add(f);
    return f;
  }

  public Throwable getFailure() {
    return failure;
  }

  /**
   * Fail all future elements and consume all existing elements.
   *
   * @param throwable       to fail all future elements
   * @param elementConsumer to process existing elements
   */
  public synchronized void fail(Supplier<Throwable> throwable, Consumer<E> elementConsumer) {
    Objects.requireNonNull(throwable, "throwable == null");
    if (elementConsumer != null) {
      for (E element; (element = existingElements.poll()) != null; ) {
        elementConsumer.accept(element);
      }
    }
    if (failure != null && !futureElements.isEmpty()) {
      failure = throwable.get();
      Objects.requireNonNull(failure, "throwable.get() == null");
      for (CompletableFuture<?> p; (p = futureElements.poll()) != null; ) {
        p.completeExceptionally(failure);
      }
    }
  }

  /**
   * @return the number of elements N.
   *         When N is non-negative, N is the number of existing elements.
   *         Otherwise, N is negative, -N is the number of future elements.
   */
  public synchronized int elementCount() {
    assertInvariant();
    return existingElements.size() - futureElements.size();
  }

  @Override
  public String toString() {
    return name + ":" + elementCount();
  }
}
