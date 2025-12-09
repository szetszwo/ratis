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

package org.apache.ratis.server.raftlog.segmented;

import org.apache.ratis.proto.RaftProtos.LogEntryProto;
import org.apache.ratis.server.protocol.TermIndex;
import org.apache.ratis.server.raftlog.LogEntryHeader;
import org.apache.ratis.util.Preconditions;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

/**
 * Log record of a RaftLog segment.
 */
public final class LogRecord {
  public static LogRecord valueOf(long offset, LogEntryProto entry) {
    return new LogRecord(offset, LogEntryHeader.valueOf(entry));
  }

  /** starting offset in the file */
  private final long offset;
  private final LogEntryHeader header;

  private LogRecord(long offset, LogEntryHeader header) {
    this.offset = offset;
    this.header = header;
  }

  public LogEntryHeader getHeader() {
    return header;
  }

  public TermIndex getTermIndex() {
    return getHeader().getTermIndex();
  }

  public long getOffset() {
    return offset;
  }

  /**
   * A thread-safed mop supporting random read but NOT random write.
   * For write operations, it supports append, removeLast and clear.
   */
  public static class AppendOnlyMap {
    private final ConcurrentNavigableMap<Long, LogRecord> map = new ConcurrentSkipListMap<>();

    public int size() {
      return map.size();
    }

    public LogRecord getFirst() {
      final Map.Entry<Long, LogRecord> first = map.firstEntry();
      return first != null? first.getValue() : null;
    }

    public LogRecord getLast() {
      final Map.Entry<Long, LogRecord> last = map.lastEntry();
      return last != null? last.getValue() : null;
    }

    public LogRecord get(long i) {
      return map.get(i);
    }

    public long append(LogRecord record) {
      final long index = record.getTermIndex().getIndex();
      final LogRecord previous = map.put(index, record);
      Preconditions.assertNull(previous, "previous");
      return index;
    }

    public LogRecord removeLast() {
      final Map.Entry<Long, LogRecord> last = map.pollLastEntry();
      return Objects.requireNonNull(last, "last == null").getValue();
    }

    public void clear() {
      map.clear();
    }
  }
}
