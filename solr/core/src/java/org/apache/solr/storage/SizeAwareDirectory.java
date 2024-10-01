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
    implements DirectoryFactory.SizeAware, Accountable, DirectoryFactory.OnDiskSizeDirectory {
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
  private volatile LongAdder onDiskSize = new LongAdder();
  private volatile SizeWriter sizeWriter =
      (size, onDiskSize, name) -> {
        this.size.add(size);
        this.onDiskSize.add(onDiskSize);
      };

  private final ConcurrentHashMap<String, Sizes> fileSizeMap = new ConcurrentHashMap<>();

  private final ConcurrentHashMap<String, SizeAccountingIndexOutput> liveOutputs =
      new ConcurrentHashMap<>();

  @SuppressWarnings({"unchecked", "rawtypes"})
  private final Future<Sizes>[] computingSize = new Future[1];

  private interface SizeWriter {
    void apply(long size, long onDiskSize, String name);
  }

  private static class Sizes implements Accountable {
    private static final long RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(Sizes.class);

    long size;
    long onDiskSize;

    Sizes(long size, long onDiskSize) {
      this.size = size;
      this.onDiskSize = onDiskSize;
    }

    @Override
    public long ramBytesUsed() {
      return RAM_BYTES_USED;
    }
  }

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
    Sizes ret = fileSizeMap.get(name);
    SizeAccountingIndexOutput live;
    if (ret != null) {
      return ret.size;
    } else if ((live = liveOutputs.get(name)) != null) {
      return live.backing.getFilePointer();
    } else {
      // fallback delegate to wrapped Directory
      return in.fileLength(name);
    }
  }

  @Override
  public long onDiskFileLength(String name) throws IOException {
    Sizes ret = fileSizeMap.get(name);
    SizeAccountingIndexOutput live;
    if (ret != null) {
      return ret.onDiskSize;
    } else if ((live = liveOutputs.get(name)) != null) {
      if (live.backing instanceof CompressingDirectory.SizeReportingIndexOutput) {
        return ((CompressingDirectory.SizeReportingIndexOutput) live.backing).getBytesWritten();
      } else if (in instanceof DirectoryFactory.OnDiskSizeDirectory) {
        // backing IndexOutput does not allow us to get onDiskSize
        return 0;
      } else {
        return live.backing.getFilePointer();
      }
    } else if (in instanceof DirectoryFactory.OnDiskSizeDirectory) {
      // fallback delegate to wrapped Directory
      return ((DirectoryFactory.OnDiskSizeDirectory) in).onDiskFileLength(name);
    } else {
      // directory does not implement onDiskSize
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
    return initSize().size;
  }

  @Override
  public long onDiskSize() throws IOException {
    Integer reconcileThreshold = CoreAdminHandler.getReconcileThreshold();
    if (initialized
        && (reconcileThreshold == null
            || System.nanoTime() - reconciledTimeNanos < reconcileTTLNanos)) {
      return onDiskSize.sum();
    }
    return initSize().onDiskSize;
  }

  private Sizes initSize() throws IOException {
    Integer reconcileThreshold = CoreAdminHandler.getReconcileThreshold();
    CompletableFuture<Sizes> weCompute;
    Future<Sizes> theyCompute;
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
      LongAdder recomputeOnDiskSize = new LongAdder();
      Set<String> recomputed = ConcurrentHashMap.newKeySet();
      SizeWriter dualSizeWriter =
          (fileSize, onDiskFileSize, name) -> {
            size.add(fileSize);
            onDiskSize.add(onDiskFileSize);
            if (fileSize >= 0 || onDiskFileSize >= 0 || recomputed.remove(name)) {
              // if it's a removal, we only want to adjust if we've already
              // incorporated this file in our count!
              recomputeSize.add(fileSize);
              recomputeOnDiskSize.add(onDiskFileSize);
            }
          };
      sizeWriter = dualSizeWriter;
      for (final String file : files) {
        Sizes sizes;
        recomputed.add(file);
        SizeAccountingIndexOutput liveOutput = liveOutputs.get(file);
        if (liveOutput != null) {
          // get fileSize already written at this moment
          sizes = liveOutput.setSizeWriter(dualSizeWriter);
        } else {
          long fileSize = DirectoryFactory.sizeOf(in, file);
          long onDiskFileSize = DirectoryFactory.onDiskSizeOf(in, file);
          sizes = new Sizes(fileSize, onDiskFileSize);
          if (fileSize > 0) {
            // whether the file exists or not, we don't care about it if it has zero size.
            // more often though, 0 size means the file isn't there.
            fileSizeMap.put(file, sizes);
            if (DirectoryFactory.sizeOf(in, file) == 0) {
              // during reconciliation, we have to check for file presence _after_ adding
              // to the map, to prevent a race condition that could leak entries into `fileSizeMap`
              fileSizeMap.remove(file);
            }
          }
        }
        recomputeSize.add(sizes.size);
        recomputeOnDiskSize.add(sizes.onDiskSize);
        // TODO: do we really need to check for overflow here?
        //        if (recomputeSize < 0) {
        //          break;
        //        }
      }

      Sizes ret = new Sizes(recomputeSize.sum(), recomputeOnDiskSize.sum());
      Sizes extant = new Sizes(size.sum(), onDiskSize.sum());
      long diff = extant.size - ret.size;
      long onDiskDiff = extant.onDiskSize - ret.onDiskSize;
      boolean initializing = !initialized;
      if (!initializing
          && Math.abs(diff) < reconcileThreshold
          && Math.abs(onDiskDiff) < reconcileThreshold) {
        double ratio = (double) extant.size / ret.size;
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
        SizeWriter replaceSizeWriter =
            (size, onDiskSize, name) -> {
              recomputeSize.add(size);
              recomputeOnDiskSize.add(onDiskSize);
            };
        sizeWriter = replaceSizeWriter;
        size = recomputeSize;
        onDiskSize = recomputeOnDiskSize;
        for (SizeAccountingIndexOutput liveOutput : liveOutputs.values()) {
          liveOutput.setSizeWriter(replaceSizeWriter);
        }
        reconciledTimeNanos = System.nanoTime();
        if (initializing) {
          initialized = true;
          if (log.isInfoEnabled()) {
            log.info(
                "initialized heap-tracked size {} (overhead: {})",
                RamUsageEstimator.humanReadableUnits(ret.size),
                RamUsageEstimator.humanReadableUnits(ramBytesUsed()));
          }
        } else {
          double ratio = (double) extant.size / ret.size;
          double onDiskRatio = (double) extant.onDiskSize / ret.onDiskSize;
          log.warn(
              "reconcile size {} => {}  (diff {}; ratio {}; onDiskRatio {}; overhead {})",
              extant,
              ret,
              humanReadableByteDiff(diff),
              ratio,
              onDiskRatio,
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
      Sizes fileSize = fileSizeMap.remove(name);
      if (fileSize != null) {
        sizeWriter.apply(-fileSize.size, -fileSize.onDiskSize, name);
      }
    }
  }

  @Override
  public IndexOutput createOutput(String name, IOContext context) throws IOException {
    SizeAccountingIndexOutput ret =
        new SizeAccountingIndexOutput(
            name, in.createOutput(name, context), fileSizeMap, liveOutputs, sizeWriter, in);
    liveOutputs.put(name, ret);
    return ret;
  }

  @Override
  public IndexOutput createTempOutput(String prefix, String suffix, IOContext context)
      throws IOException {
    IndexOutput backing = in.createTempOutput(prefix, suffix, context);
    String name = backing.getName();
    SizeAccountingIndexOutput ret =
        new SizeAccountingIndexOutput(name, backing, fileSizeMap, liveOutputs, sizeWriter, in);
    liveOutputs.put(name, ret);
    return ret;
  }

  private static final class SizeAccountingIndexOutput extends IndexOutput implements Accountable {

    private static final long BASE_RAM_BYTES_USED =
        RamUsageEstimator.shallowSizeOfInstance(SizeAccountingIndexOutput.class);

    private final String name;

    private final IndexOutput backing;

    private final Directory backingDirectory;

    private final ConcurrentHashMap<String, Sizes> fileSizeMap;

    private final ConcurrentHashMap<String, SizeAccountingIndexOutput> liveOutputs;

    private volatile SizeWriter sizeWriter;

    private long lastBytesWritten = 0;

    private SizeAccountingIndexOutput(
        String name,
        IndexOutput backing,
        ConcurrentHashMap<String, Sizes> fileSizeMap,
        ConcurrentHashMap<String, SizeAccountingIndexOutput> liveOutputs,
        SizeWriter sizeWriter,
        Directory backingDirectory) {
      super("byteSize(" + name + ")", name);
      this.name = name;
      this.backing = backing;
      this.liveOutputs = liveOutputs;
      this.sizeWriter = sizeWriter;
      this.fileSizeMap = fileSizeMap;
      this.backingDirectory = backingDirectory;
    }

    public Sizes setSizeWriter(SizeWriter sizeWriter) {
      if (this.sizeWriter == sizeWriter) {
        return new Sizes(0, 0);
      } else {
        // NOTE: there's an unavoidable race condition between these two
        // lines, and this ordering may occasionally overestimate directory size.
        this.sizeWriter = sizeWriter;
        long onDiskSize;
        if (backing instanceof CompressingDirectory.SizeReportingIndexOutput) {
          onDiskSize = ((CompressingDirectory.SizeReportingIndexOutput) backing).getBytesWritten();
        } else if (backingDirectory instanceof DirectoryFactory.OnDiskSizeDirectory) {
          onDiskSize = 0;
        } else {
          onDiskSize = getFilePointer();
        }
        return new Sizes(this.getFilePointer(), onDiskSize);
      }
    }

    @Override
    @SuppressWarnings("try")
    public void close() throws IOException {
      backing.close();

      long onDiskSize;
      if (backing instanceof CompressingDirectory.SizeReportingIndexOutput) {
        long finalBytesWritten = getBytesWritten(backing);
        onDiskSize = finalBytesWritten;
        // logical size should already be set through writeByte(s), but we need to finalize the
        // on-disk size here
        sizeWriter.apply(0, finalBytesWritten - lastBytesWritten, name);
      } else if (backingDirectory instanceof DirectoryFactory.OnDiskSizeDirectory) {
        onDiskSize = 0;
      } else {
        onDiskSize = getFilePointer();
      }
      fileSizeMap.put(name, new Sizes(backing.getFilePointer(), onDiskSize));
      liveOutputs.remove(name);
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
      long postBytesWritten = getBytesWritten(backing);
      sizeWriter.apply(1, postBytesWritten - lastBytesWritten, name);
      lastBytesWritten = postBytesWritten;
    }

    private long getBytesWritten(IndexOutput out) {
      if (backing instanceof CompressingDirectory.SizeReportingIndexOutput) {
        return ((CompressingDirectory.SizeReportingIndexOutput) backing).getBytesWritten();
      }
      return 0;
    }

    @Override
    public void writeBytes(byte[] b, int offset, int length) throws IOException {
      backing.writeBytes(b, offset, length);
      long postBytesWritten = getBytesWritten(backing);
      sizeWriter.apply(length, postBytesWritten - lastBytesWritten, name);
      lastBytesWritten = postBytesWritten;
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
    Sizes extant = fileSizeMap.put(dest, fileSizeMap.remove(source));
    assert extant == null; // it's illegal for dest to already exist
  }
}
