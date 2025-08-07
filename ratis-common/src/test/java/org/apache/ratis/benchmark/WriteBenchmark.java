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
package org.apache.ratis.benchmark;

import org.apache.ratis.util.FileUtils;
import org.apache.ratis.util.Preconditions;

import java.io.File;

abstract class WriteBenchmark {
  File getOutputPath(File tmpDir) {
    return new File(tmpDir, toString());
  }

  abstract long write(long totalSize, File outFile, int chunkSize, File inFile) throws Exception;

  @Override
  public String toString() {
    return getClass().getSimpleName();
  }

  Long run(File inFile, long totalSize, File tmpDir, int chunkSize) throws Exception {
    if (chunkSize % 4 != 0) {
      throw new IllegalArgumentException("chunkSize (=" + totalSize + ") must be a multiple of 4");
    }
    if (totalSize % chunkSize != 0) {
      throw new IllegalArgumentException("totalSize (=" + totalSize + ") must be a multiple of chunkSize (=" + chunkSize + ")");
    }
    if (inFile != null) {
      final long inFileSize = inFile.length();
      if (inFileSize < totalSize) {
        throw new IllegalArgumentException("File size (=" + inFileSize + ") < totalSize (=" + totalSize + "): " + inFile);
      }
    }

    final File outPath = getOutputPath(tmpDir);
    FileUtils.deleteFully(outPath);

    final long start = System.nanoTime();
    final long writeSize = write(totalSize, outPath, chunkSize, inFile);
    final long elapsed = System.nanoTime() - start;

    if (writeSize <= 0) {
      return null;
    }
    Preconditions.assertSame(totalSize, writeSize, "writeSize");
    System.out.printf("  %-40s [%6s]: %9.3f ms%n",
        this, inFile == null ? "random" : "file", elapsed / 1_000_000.0);
    return elapsed;
  }
}
