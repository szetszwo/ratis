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

import java.lang.ref.ReferenceQueue;
import java.lang.ref.WeakReference;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Predicate;

/**
 * Simple general resource leak detector using {@link ReferenceQueue} and {@link java.lang.ref.WeakReference} to
 * observe resource object life-cycle and assert proper resource closure before they are GCed.
 *
 * <p>
 * Example usage:
 *
 * <pre> {@code
 * class MyResource implements AutoClosable {
 *   static final LeakDetector LEAK_DETECTOR = new LeakDetector("MyResource");
 *
 *   private final UncheckedAutoCloseable leakTracker = LEAK_DETECTOR.track(this, () -> {
 *      // report leaks, don't refer to the original object (MyResource) here.
 *      System.out.println("MyResource is not closed before being discarded.");
 *   });
 *
 *   @Override
 *   public void close() {
 *     // proper resources cleanup...
 *     // inform tracker that this object is closed properly.
 *     leakTracker.close();
 *   }
 * }
 *
 * }</pre>
 */
public class LeakDetector {
  private static final Logger LOG = LoggerFactory.getLogger(LeakDetector.class);

  private static class LeakTrackerSet {
    private final Set<LeakTracker> set = Collections.newSetFromMap(new ConcurrentHashMap<>());

    synchronized boolean remove(LeakTracker tracker) {
      return set.remove(tracker);
    }

    synchronized void removeExisting(LeakTracker tracker) {
      final boolean removed = set.remove(tracker);
      Preconditions.assertTrue(removed, () -> "Failed to remove existing " + tracker);
    }

    synchronized LeakTracker add(Object referent, ReferenceQueue<Object> queue, Runnable leakReporter) {
      final LeakTracker tracker = new LeakTracker(referent, queue, this::removeExisting, leakReporter);
      final boolean added = set.add(tracker);
      Preconditions.assertTrue(added, () -> "Failed to add " + tracker + " for " + referent);
      return tracker;
    }

    synchronized void assertNoLeaks() {
      Preconditions.assertTrue(set.isEmpty(), this::allLeaksString);
    }

    private String allLeaksString() {
      if (set.isEmpty()) {
        return "allLeaks = <empty>";
      }
      set.forEach(LeakTracker::reportLeak);
      return "allLeaks.size = " + set.size();
    }
  }

  private static final AtomicLong COUNTER = new AtomicLong();

  private final ReferenceQueue<Object> queue = new ReferenceQueue<>();
  private final LeakTrackerSet allLeaks = new LeakTrackerSet();
  private final String name;

  public LeakDetector(String name) {
    this.name = name + COUNTER.getAndIncrement();
  }

  LeakDetector start() {
    Thread t = new Thread(this::run);
    t.setName(LeakDetector.class.getSimpleName() + "-" + name);
    t.setDaemon(true);
    LOG.info("Starting leak detector thread {}.", name);
    t.start();
    return this;
  }

  private void run() {
    while (true) {
      try {
        LeakTracker tracker = (LeakTracker) queue.remove();
        // Original resource already been GCed, if tracker is not closed yet,
        // report a leak.
        if (allLeaks.remove(tracker)) {
          tracker.reportLeak();
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        LOG.warn("Thread interrupted, exiting.", e);
        break;
      }
    }

    LOG.warn("Exiting leak detector {}.", name);
  }

  public Predicate<ReferenceCountedObject<?>> track(Object leakable, Runnable reportLeak) {
    // A rate filter can be put here to only track a subset of all objects, e.g. 5%, 10%,
    // if we have proofs that leak tracking impacts performance, or a single LeakDetector
    // thread can't keep up with the pace of object allocation.
    // For now, it looks effective enough and let keep it simple.
    return allLeaks.add(leakable, queue, reportLeak)::releaseAndCheckRemove;
  }

  public void assertNoLeaks() {
    allLeaks.assertNoLeaks();
  }

  private static final class LeakTracker extends WeakReference<Object> {
    private final Consumer<LeakTracker> removeMethod;
    private final Runnable leakReporter;

    LeakTracker(Object referent, ReferenceQueue<Object> referenceQueue,
        Consumer<LeakTracker> removeMethod, Runnable leakReporter) {
      super(referent, referenceQueue);
      this.removeMethod = removeMethod;
      this.leakReporter = leakReporter;
    }

    /**
     * Called by the tracked resource when releasing the object.
     */
    boolean releaseAndCheckRemove(ReferenceCountedObject<?> referenceCountedObject) {
      if (referenceCountedObject.release()) {
        removeMethod.accept(this);
        return true;
      } else {
        return false;
      }
    }

    void reportLeak() {
      leakReporter.run();
    }
  }
}
