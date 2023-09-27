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
package org.apache.ratis.examples.filestore.cli;

import com.beust.jcommander.Parameters;
import org.apache.ratis.examples.filestore.FileStoreClient;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Subcommand to generate metadata load in filestore state machine.
 */
@Parameters(commandDescription = "Metadata Load Generator for FileStore")
public class MetadataGen extends Client {

  @Override
  protected void operation(List<FileStoreClient> clients) throws IOException, ExecutionException, InterruptedException {
    final ExecutorService executor = Executors.newFixedThreadPool(getNumThread());
    dropCache();
    System.out.println("Starting Async write now ");

    final List<String> paths = generatePaths();

    long startTime = System.currentTimeMillis();

    waitWriteFinish(write(paths, clients));

    long endTime = System.currentTimeMillis();

    System.out.println("Total files written: " + getNumFiles());
    System.out.println("Each files size: " + getFileSizeInBytes());
    System.out.println("Total time taken: " + (endTime - startTime) + " millis");

    stop(clients);
  }

  private Map<String, CompletableFuture<Long>> write(
      List<String> paths, List<FileStoreClient> clients) {
    final Map<String, CompletableFuture<Long>> fileMap = new HashMap<>();
    for(int i = 0; i < paths.size(); i++) {
      final String path = paths.get(i);
      final FileStoreClient client = clients.get(i % clients.size());
      final CompletableFuture<Long> future = client.writeAsync(path);
      fileMap.put(path, future);
    }

    return fileMap;
  }

  private void waitWriteFinish(Map<String, CompletableFuture<Long>> fileMap)
      throws ExecutionException, InterruptedException {
    for (CompletableFuture<Long> future : fileMap.values()) {
      final long writtenLen = future.join();
      if (writtenLen != 0) {
        System.out.println("File written:" + writtenLen + " does not match expected:" + getFileSizeInBytes());
      }
    }
  }
}
