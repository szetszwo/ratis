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
import java.nio.channels.SeekableByteChannel;
import java.nio.file.AccessMode;
import java.nio.file.CopyOption;
import java.nio.file.DirectoryStream;
import java.nio.file.FileStore;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileAttribute;
import java.nio.file.attribute.FileAttributeView;
import java.nio.file.spi.FileSystemProvider;
import java.util.Map;
import java.util.Set;

public class LoggingFileSystemProvider extends FileSystemProvider {
  private static boolean useMyPath = true;

  private final FileSystemProvider delegate;
  private final StringBuilder log = new StringBuilder();

  public LoggingFileSystemProvider(FileSystemProvider delegate) {
    this.delegate = delegate;
    printf("create %s", this);
    new Throwable().printStackTrace(System.out);
  }

  private FileSystemProvider getDelegate() {
    return delegate;
  }

  @Override
  public String getScheme() {
    return delegate.getScheme();
  }

  @Override
  public FileSystem newFileSystem(URI uri, Map<String, ?> env) throws IOException {
    printf("newFileSystem: uri=%s, env=%s%n", uri, env);
    final FileSystem fs = getDelegate().newFileSystem(uri, env);
    return new LoggingFileSystem(this, fs, useMyPath);
  }

  @Override
  public FileSystem getFileSystem(URI uri) {
    printf("newFileSystem: uri=%s%n", uri);
    final FileSystem fs = getDelegate().getFileSystem(uri);
    return new LoggingFileSystem(this, fs, useMyPath);
  }

  @Override
  public Path getPath(URI uri) {
    return getDelegate().getPath(uri);
  }

  @Override
  public SeekableByteChannel newByteChannel(Path path, Set<? extends OpenOption> options, FileAttribute<?>... attrs) throws IOException {
    log("newByteChannel(%s, %s, %s)", path, options, attrs);
    return getDelegate().newByteChannel(path, options, attrs);
  }

  @Override
  public DirectoryStream<Path> newDirectoryStream(Path dir, DirectoryStream.Filter<? super Path> filter) throws IOException {
    log("newDirectoryStream(%s, %s)", dir, filter);
    return getDelegate().newDirectoryStream(dir, filter);
  }

  @Override
  public void createDirectory(Path dir, FileAttribute<?>... attrs) throws IOException {
    log("createDirectory(%s, %s)", dir, attrs);
    getDelegate().createDirectory(dir, attrs);
  }

  @Override
  public void delete(Path path) throws IOException {
    log("delete(%s)", path);
    getDelegate().delete(path);
  }

  @Override
  public void copy(Path source, Path target, CopyOption... options) throws IOException {
    log("copy(%s, %s, %s)", source, target, options);
    getDelegate().copy(source, target, options);
  }

  @Override
  public void move(Path source, Path target, CopyOption... options) throws IOException {
    log("move(%s, %s, %s)", source, target, options);
    getDelegate().move(source, target, options);
  }

  @Override
  public boolean isSameFile(Path path, Path path2) throws IOException {
    log("isSameFile(%s, %s)", path, path2);
    return getDelegate().isSameFile(path, path2);
  }

  @Override
  public boolean isHidden(Path path) throws IOException {
    log("isHidden(%s)", path);
    return getDelegate().isHidden(path);
  }

  @Override
  public FileStore getFileStore(Path path) throws IOException {
    log("getFileStore(%s)", path);
    return getDelegate().getFileStore(path);
  }

  @Override
  public void checkAccess(Path path, AccessMode... modes) throws IOException {
    log("checkAccess(%s, %s)", path, modes);
    getDelegate().checkAccess(path, modes);
  }

  @Override
  public <V extends FileAttributeView> V getFileAttributeView(Path path, Class<V> type, LinkOption... options) {
    log("getFileAttributeView(%s, %s, %s)", path, type, options);
    return getDelegate().getFileAttributeView(path, type, options);
  }

  @Override
  public <A extends BasicFileAttributes> A readAttributes(Path path, Class<A> type, LinkOption... options) throws IOException {
    log("readAttributes(%s, %s, %s)", path, type, options);
    return getDelegate().readAttributes(path, type, options);
  }

  @Override
  public Map<String, Object> readAttributes(Path path, String attributes, LinkOption... options) throws IOException {
    log("readAttributes(%s, %s, %s)", path, attributes, options);
    return getDelegate().readAttributes(path, attributes, options);
  }

  @Override
  public void setAttribute(Path path, String attribute, Object value, LinkOption... options) throws IOException {
    log("setAttribute(%s, %s, %s, %s)", path, attribute, value, options);
    getDelegate().setAttribute(path, attribute, value, options);
  }

  @Override
  public String toString() {
    return getClass().getSimpleName()
        + "(useMyPath? " + useMyPath + ")-"
        + delegate.getClass().getSimpleName();
  }

  private void log(String format, Object... args) {
    final String s = String.format("LOG: " + format, args);
    log.append(s);
    printf(s);
  }

  static void printf(String format, Object... args) {
    System.out.printf(format + "%n", args);
  }

  static LoggingFileSystem getDefaultFileSystem() {
    final FileSystem defaultFileSystem = FileSystems.getDefault();
    printf("FileSystems.getDefault(): %s", defaultFileSystem.getClass().getSimpleName());
    return (LoggingFileSystem) defaultFileSystem;
  }

  public static void main(String[] args) {
    useMyPath = true; // it won't work when useMyPath = false.
    final LoggingFileSystem fs = getDefaultFileSystem();

    testPath(Paths.get("Paths.get"));

    final File f = new File("bar");
    printTestCase("File.delete()", f.toPath());
    f.delete(); // log is missing for File.delete()

    testPath(new File("File.toPath").toPath());

    if (fs.provider().log.length() == 0) {
      throw new AssertionError("log is empty");
    }
  }

  static void testPath(Path p) {
    printTestCase("Files.delete(Path)", p);
    try {
      Files.delete(p);
    } catch (Exception e) {
    }
  }

  static void printTestCase(String method, Path p) {
    printf("Test %s for %s: %s", method, p, p.getFileSystem().provider());
  }
}
