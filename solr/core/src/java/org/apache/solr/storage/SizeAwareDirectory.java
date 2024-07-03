/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.solr.storage;

import com.carrotsearch.hppc.procedures.LongObjectProcedure;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.LongAdder;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FilterDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.RamUsageEstimator;
import org.apache.solr.core.DirectoryFactory;
import org.apache.solr.handler.admin.CoreAdminHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SizeAwareDirectory extends FilterDirectory
    implements DirectoryFactory.SizeAware, Accountable {
  @SuppressWarnings("rawtypes")
  private static final long BASE_RAM_BYTES_USED =
      RamUsageEstimator.shallowSizeOfInstance(SizeAwareDirectory.class)
          + RamUsageEstimator.shallowSizeOfInstance(LongAdder.class)
          + RamUsageEstimator.shallowSizeOf(new Future[1]);

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private final long reconcileTTLNanos;
  private boolean initialized = false;
  private volatile long reconciledTimeNanos;
  private volatile LongAdder size = new LongAdder();
  private volatile LongObjectProcedure<String> sizeWriter = (size, name) -> this.size.add(size);

  private final ConcurrentHashMap<String, Long> fileSizeMap = new ConcurrentHashMap<>();

  private final ConcurrentHashMap<String, SizeAccountingIndexOutput> liveOutputs =
      new ConcurrentHashMap<>();

  @SuppressWarnings({"unchecked", "rawtypes"})
  private final Future<Long>[] computingSize = new Future[1];

  public SizeAwareDirectory(Directory in, long reconcileTTLNanos) {
    super(in);
    this.reconcileTTLNanos = reconcileTTLNanos;
    if (reconcileTTLNanos == Long.MAX_VALUE) {
      this.reconciledTimeNanos = 0;
    } else {
      this.reconciledTimeNanos = System.nanoTime() - reconcileTTLNanos; // ensure initialization
    }
  }

  @Override
  public long ramBytesUsed() {
    return BASE_RAM_BYTES_USED
        + RamUsageEstimator.sizeOfMap(fileSizeMap)
        + RamUsageEstimator.sizeOfMap(liveOutputs);
  }

  @Override
  public long fileLength(String name) throws IOException {
    Long ret = fileSizeMap.get(name);
    SizeAccountingIndexOutput live;
    if (ret != null) {
      return ret;
    } else if ((live = liveOutputs.get(name)) != null) {
      return live.backing.getFilePointer();
    } else {
      // fallback delegate to wrapped Directory
      return in.fileLength(name);
    }
  }

  @Override
  public long size() throws IOException {
    Integer reconcileThreshold = CoreAdminHandler.getReconcileThreshold();
    if (initialized
        && (reconcileThreshold == null
            || System.nanoTime() - reconciledTimeNanos < reconcileTTLNanos)) {
      return size.sum();
    }
    CompletableFuture<Long> weCompute;
    Future<Long> theyCompute;
    synchronized (computingSize) {
      theyCompute = computingSize[0];
      if (theyCompute == null) {
        weCompute = new CompletableFuture<>();
      } else {
        weCompute = null;
      }
    }
    if (weCompute == null) {
      try {
        return theyCompute.get();
      } catch (InterruptedException | ExecutionException e) {
        throw new RuntimeException(e);
      }
    }

    try {
      final String[] files = in.listAll();

      LongAdder recomputeSize = new LongAdder();
      Set<String> recomputed = ConcurrentHashMap.newKeySet();
      LongObjectProcedure<String> dualSizeWriter =
          (fileSize, name) -> {
            size.add(fileSize);
            if (fileSize >= 0 || recomputed.remove(name)) {
              // if it's a removal, we only want to adjust if we've already
              // incorporated this file in our count!
              recomputeSize.add(fileSize);
            }
          };
      sizeWriter = dualSizeWriter;
      for (final String file : files) {
        long fileSize;
        recomputed.add(file);
        SizeAccountingIndexOutput liveOutput = liveOutputs.get(file);
        if (liveOutput != null) {
          // get fileSize already written at this moment
          fileSize = liveOutput.setSizeWriter(dualSizeWriter);
        } else {
          fileSize = DirectoryFactory.sizeOf(in, file);
          if (fileSize > 0) {
            // whether the file exists or not, we don't care about it if it has zero size.
            // more often though, 0 size means the file isn't there.
            fileSizeMap.put(file, fileSize);
            if (DirectoryFactory.sizeOf(in, file) == 0) {
              // during reconciliation, we have to check for file presence _after_ adding
              // to the map, to prevent a race condition that could leak entries into `fileSizeMap`
              fileSizeMap.remove(file);
            }
          }
        }
        recomputeSize.add(fileSize);
        // TODO: do we really need to check for overflow here?
        //        if (recomputeSize < 0) {
        //          break;
        //        }
      }

      long ret = recomputeSize.sum();
      long extant = size.sum();
      long diff = extant - ret;
      boolean initializing = !initialized;
      if (!initializing && Math.abs(diff) < reconcileThreshold) {
        double ratio = (double) extant / ret;
        if (log.isInfoEnabled()) {
          log.info(
              "no need to reconcile (diff {}; ratio {}; overhead {}; sizes {}/{}/{})",
              humanReadableByteDiff(diff),
              ratio,
              RamUsageEstimator.humanReadableUnits(ramBytesUsed()),
              liveOutputs.size(),
              fileSizeMap.size(),
              files.length);
        }
        ret = extant;
      } else {
        // swap the new objects into place
        LongObjectProcedure<String> replaceSizeWriter = (size, name) -> recomputeSize.add(size);
        sizeWriter = replaceSizeWriter;
        size = recomputeSize;
        for (SizeAccountingIndexOutput liveOutput : liveOutputs.values()) {
          liveOutput.setSizeWriter(replaceSizeWriter);
        }
        reconciledTimeNanos = System.nanoTime();
        if (initializing) {
          initialized = true;
          if (log.isInfoEnabled()) {
            log.info(
                "initialized heap-tracked size {} (overhead: {})",
                RamUsageEstimator.humanReadableUnits(ret),
                RamUsageEstimator.humanReadableUnits(ramBytesUsed()));
          }
        } else {
          double ratio = (double) extant / ret;
          log.warn(
              "reconcile size {} => {}  (diff {}; ratio {}; overhead {})",
              extant,
              ret,
              humanReadableByteDiff(diff),
              ratio,
              RamUsageEstimator.humanReadableUnits(ramBytesUsed()));
        }
      }

      weCompute.complete(ret);

      return ret;
    } finally {
      synchronized (computingSize) {
        computingSize[0] = null;
      }
    }
  }

  private static String humanReadableByteDiff(long diff) {
    if (diff >= 0) {
      return RamUsageEstimator.humanReadableUnits(diff);
    } else {
      return "-".concat(RamUsageEstimator.humanReadableUnits(-diff));
    }
  }

  @Override
  public void deleteFile(String name) throws IOException {
    try {
      in.deleteFile(name);
    } finally {
      Long fileSize = fileSizeMap.remove(name);
      if (fileSize != null) {
        sizeWriter.apply(-fileSize, name);
      }
    }
  }

  @Override
  public IndexOutput createOutput(String name, IOContext context) throws IOException {
    SizeAccountingIndexOutput ret =
        new SizeAccountingIndexOutput(
            name, in.createOutput(name, context), fileSizeMap, liveOutputs, sizeWriter);
    liveOutputs.put(name, ret);
    return ret;
  }

  @Override
  public IndexOutput createTempOutput(String prefix, String suffix, IOContext context)
      throws IOException {
    IndexOutput backing = in.createTempOutput(prefix, suffix, context);
    String name = backing.getName();
    SizeAccountingIndexOutput ret =
        new SizeAccountingIndexOutput(name, backing, fileSizeMap, liveOutputs, sizeWriter);
    liveOutputs.put(name, ret);
    return ret;
  }

  private static final class SizeAccountingIndexOutput extends IndexOutput implements Accountable {

    private static final long BASE_RAM_BYTES_USED =
        RamUsageEstimator.shallowSizeOfInstance(SizeAccountingIndexOutput.class);

    private final String name;

    private final IndexOutput backing;

    private final ConcurrentHashMap<String, Long> fileSizeMap;

    private final ConcurrentHashMap<String, SizeAccountingIndexOutput> liveOutputs;

    private volatile LongObjectProcedure<String> sizeWriter;

    private SizeAccountingIndexOutput(
        String name,
        IndexOutput backing,
        ConcurrentHashMap<String, Long> fileSizeMap,
        ConcurrentHashMap<String, SizeAccountingIndexOutput> liveOutputs,
        LongObjectProcedure<String> sizeWriter) {
      super("byteSize(" + name + ")", name);
      this.name = name;
      this.backing = backing;
      this.liveOutputs = liveOutputs;
      this.sizeWriter = sizeWriter;
      this.fileSizeMap = fileSizeMap;
    }

    public long setSizeWriter(LongObjectProcedure<String> sizeWriter) {
      if (this.sizeWriter == sizeWriter) {
        return 0;
      } else {
        // NOTE: there's an unavoidable race condition between these two
        // lines, and this ordering may occasionally overestimate directory size.
        this.sizeWriter = sizeWriter;
        return backing.getFilePointer();
      }
    }

    @Override
    @SuppressWarnings("try")
    public void close() throws IOException {
      try (backing) {
        fileSizeMap.put(name, backing.getFilePointer());
      } finally {
        liveOutputs.remove(name);
      }
    }

    @Override
    public long getFilePointer() {
      return backing.getFilePointer();
    }

    @Override
    public long getChecksum() throws IOException {
      return backing.getChecksum();
    }

    @Override
    public void writeByte(byte b) throws IOException {
      backing.writeByte(b);
      sizeWriter.apply(1, name);
    }

    @Override
    public void writeBytes(byte[] b, int offset, int length) throws IOException {
      backing.writeBytes(b, offset, length);
      sizeWriter.apply(length, name);
    }

    @Override
    public long ramBytesUsed() {
      // all fields have to exist regardless; we're only interested in the overhead we add
      return BASE_RAM_BYTES_USED;
    }
  }

  @Override
  public void rename(String source, String dest) throws IOException {
    in.rename(source, dest);
    Long extant = fileSizeMap.put(dest, fileSizeMap.remove(source));
    assert extant == null; // it's illegal for dest to already exist
  }
}
