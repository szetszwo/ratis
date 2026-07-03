/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ratis.testing;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.file.FileStore;
import java.nio.file.FileSystem;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.PathMatcher;
import java.nio.file.WatchEvent;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.nio.file.attribute.UserPrincipalLookupService;
import java.util.Iterator;
import java.util.Set;

class LoggingFileSystem extends FileSystem {
  private final LoggingFileSystemProvider provider;
  private final FileSystem delegate;
  private final boolean useMyPath;

  LoggingFileSystem(LoggingFileSystemProvider provider, FileSystem delegate, boolean useMyPath) {
    this.provider = provider;
    this.delegate = delegate;
    this.useMyPath = useMyPath;
  }

  private Path newPath(Path path) {
    return useMyPath ? new MyPath(path) : path;
  }

  private Iterator<Path> newIterator(Iterator<Path> i) {
    return new Iterator<Path>() {
      @Override
      public boolean hasNext() {
        return i.hasNext();
      }
      @Override
      public Path next() {
        return newPath(i.next());
      }
    };
  }

  @Override
  public LoggingFileSystemProvider provider() {
    return provider;
  }

  @Override
  public void close() throws IOException {
    delegate.close();
  }

  @Override
  public boolean isOpen() {
    return delegate.isOpen();
  }

  @Override
  public boolean isReadOnly() {
    return delegate.isReadOnly();
  }

  @Override
  public String getSeparator() {
    return delegate.getSeparator();
  }

  @Override
  public Iterable<Path> getRootDirectories() {
    return () -> newIterator(delegate.getRootDirectories().iterator());
  }

  @Override
  public Iterable<FileStore> getFileStores() {
    return delegate.getFileStores();
  }

  @Override
  public Set<String> supportedFileAttributeViews() {
    return delegate.supportedFileAttributeViews();
  }

  @Override
  public Path getPath(String first, String... more) {
    return newPath(delegate.getPath(first, more));
  }

  @Override
  public PathMatcher getPathMatcher(String syntaxAndPattern) {
    return delegate.getPathMatcher(syntaxAndPattern);
  }

  @Override
  public UserPrincipalLookupService getUserPrincipalLookupService() {
    return delegate.getUserPrincipalLookupService();
  }

  @Override
  public WatchService newWatchService() throws IOException {
    return delegate.newWatchService();
  }

  class MyPath implements Path {
    private final Path delegate;

    MyPath(Path delegate) {
      this.delegate = delegate;
    }

    @Override
    public FileSystem getFileSystem() {
      return LoggingFileSystem.this;
    }

    @Override
    public boolean isAbsolute() {
      return delegate.isAbsolute();
    }

    @Override
    public Path getRoot() {
      return newPath(delegate.getRoot());
    }

    @Override
    public Path getFileName() {
      return newPath(delegate.getFileName());
    }

    @Override
    public Path getParent() {
      return newPath(delegate.getParent());
    }

    @Override
    public int getNameCount() {
      return delegate.getNameCount();
    }

    @Override
    public Path getName(int index) {
      return newPath(delegate.getName(index));
    }

    @Override
    public Path subpath(int beginIndex, int endIndex) {
      return newPath(delegate.subpath(beginIndex, endIndex));
    }

    @Override
    public boolean startsWith(Path other) {
      return delegate.startsWith(other);
    }

    @Override
    public boolean endsWith(Path other) {
      return delegate.endsWith(other);
    }

    @Override
    public Path normalize() {
      return newPath(delegate.normalize());
    }

    @Override
    public Path resolve(Path other) {
      return newPath(delegate.resolve(other));
    }

    @Override
    public Path relativize(Path other) {
      return newPath(delegate.relativize(other));
    }

    @Override
    public URI toUri() {
      return delegate.toUri();
    }

    @Override
    public Path toAbsolutePath() {
      return newPath(delegate.toAbsolutePath());
    }

    @Override
    public Path toRealPath(LinkOption... options) throws IOException {
      return newPath(delegate.toRealPath(options));
    }

    @Override
    public WatchKey register(WatchService watcher, WatchEvent.Kind<?>[] events, WatchEvent.Modifier... modifiers)
        throws IOException {
      return delegate.register(watcher, events, modifiers);
    }

    @Override
    public int compareTo(Path other) {
      return delegate.compareTo(other);
    }

    @Override
    public Iterator<Path> iterator() {
      return newIterator(delegate.iterator());
    }

    @Override
    public WatchKey register(WatchService watcher, WatchEvent.Kind<?>... events) throws IOException {
      return delegate.register(watcher, events);
    }

    @Override
    public File toFile() {
      return delegate.toFile();
    }

    @Override
    public Path resolveSibling(Path other) {
      return newPath(delegate.resolveSibling(other));
    }

    @Override
    public Path resolveSibling(String other) {
      return newPath(delegate.resolveSibling(other));
    }

    @Override
    public Path resolve(String other) {
      return newPath(delegate.resolve(other));
    }

    @Override
    public boolean endsWith(String other) {
      return delegate.endsWith(other);
    }

    @Override
    public boolean startsWith(String other) {
      return delegate.startsWith(other);
    }

    @Override
    public String toString() {
      return delegate.toString();
    }
  }
}