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

import static org.apache.solr.storage.CompressingDirectory.BLOCK_SIZE_ESTIMATE;
import static org.apache.solr.storage.CompressingDirectory.COMPRESSION_BLOCK_MASK_LOW;
import static org.apache.solr.storage.CompressingDirectory.COMPRESSION_BLOCK_SHIFT;
import static org.apache.solr.storage.CompressingDirectory.COMPRESSION_BLOCK_SIZE;
import static org.apache.solr.storage.CompressingDirectory.DirectIOIndexOutput.HEADER_SIZE;
import static org.apache.solr.storage.CompressingDirectory.readLengthFromHeader;

import java.io.Closeable;
import java.io.EOFException;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.io.UncheckedIOException;
import java.lang.invoke.MethodHandles;
import java.lang.ref.WeakReference;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.FloatBuffer;
import java.nio.IntBuffer;
import java.nio.LongBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerArray;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.IntUnaryOperator;
import java.util.stream.Collectors;
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.store.ByteArrayDataInput;
import org.apache.lucene.store.ByteBufferGuard;
import org.apache.lucene.store.ByteBufferIndexInput;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.LockFactory;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.store.MappedByteBufferIndexInputProvider;
import org.apache.lucene.store.RandomAccessInput;
import org.apache.lucene.util.BitUtil;
import org.apache.lucene.util.CollectionUtil;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.automaton.Automaton;
import org.apache.lucene.util.automaton.ByteRunAutomaton;
import org.apache.lucene.util.automaton.CompiledAutomaton;
import org.apache.lucene.util.automaton.Operations;
import org.apache.lucene.util.automaton.RegExp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AccessDirectory extends MMapDirectory {

  /**
   * Determines chunk size for mmapping files. `1` yields 1g chunks, but this may be set higher to
   * stress test buffer boundaries (as low as 15, which yields the min chunk size of 64k, equal to
   * {@link CompressingDirectory#COMPRESSION_BLOCK_SIZE}).
   */
  private static final int MAP_BUF_DIVIDE_SHIFT = 1;

  public static final int MAX_MAP_SIZE = Integer.MIN_VALUE >>> MAP_BUF_DIVIDE_SHIFT;
  public static final int MAX_MAP_MASK = MAX_MAP_SIZE - 1;
  public static final int MAX_MAP_SHIFT = Integer.numberOfTrailingZeros(MAX_MAP_SIZE);

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private static final AtomicInteger RESERVATION = new AtomicInteger();

  private final Path path;
  private final Path compressedPath;
  private final LinkedBlockingQueue<LazyEntry> activationQueue;
  private final ConcurrentHashMap<ConcurrentIntSet, Boolean> priorityActivate;
  private final LongAdder rawCt;
  private final LongAdder loadedCt;
  private final LongAdder populatedCt;
  private final LongAdder lazyCt;
  private final LongAdder lazyMapSize;
  private final LongAdder lazyMapDiskUsage;
  private final LongAdder lazyLoadedBlockBytes;

  public AccessDirectory(
      Path path,
      LockFactory lockFactory,
      Path compressedPath,
      TeeDirectoryFactory.NodeLevelTeeDirectoryState s)
      throws IOException {
    super(path, lockFactory);
    this.path = path;
    this.compressedPath = compressedPath;
    this.activationQueue = s.activationQueue;
    this.priorityActivate = s.priorityActivate;
    this.rawCt = s.rawCt;
    this.loadedCt = s.loadedCt;
    this.populatedCt = s.populatedCt;
    this.lazyCt = s.lazyCt;
    this.lazyMapSize = s.lazyMapSize;
    this.lazyMapDiskUsage = s.lazyMapDiskUsage;
    this.lazyLoadedBlockBytes = s.lazyLoadedBlockBytes;
  }

  @Override
  public long fileLength(String name) throws IOException {
    try {
      return super.fileLength(name);
    } catch (NoSuchFileException | FileNotFoundException ex) {
      // we don't want to lazy `open()` here, because if all we want is the fileLength (which
      // is sometimes done from a context where the file doesn't exist, and shouldn't be
      // restored), then we don't want to incur the hit of restoring and mapping the lazy file.
      LazyEntry lazy = activeLazy.get(name);
      if (lazy != null) {
        return lazy.input.length();
      } else {
        try {
          return readLengthFromHeader(compressedPath.resolve(name));
        } catch (Throwable t) {
          t.addSuppressed(ex);
          throw t;
        }
      }
    }
  }

  @Override
  public void rename(String source, String dest) throws IOException {
    try {
      super.rename(source, dest);
    } catch (IOException ex) {
      LazyEntry lazy = activeLazy.get(source);
      if (lazy == null) {
        super.rename(source, dest); // will usually throw another copy of same exception
        return;
      }
      if (isClosed) {
        throw new AlreadyClosedException("already closed");
      }
      LazyEntry extant = activeLazy.putIfAbsent(dest, lazy);
      if (extant != null) {
        throw new IllegalStateException(
            "already have a mapping for " + dest + " => " + extant.lazyName);
      }
      if (!activeLazy.remove(source, lazy)) {
        activeLazy.remove(dest);
        throw new IllegalStateException("source is mapped to a new value");
      }
      lazy.canonicalName = dest;
    }
  }

  @Override
  public void sync(Collection<String> names) throws IOException {
    ensureOpen();

    for (String name : names) {
      try {
        fsync(name); // this covers the normal, actively-writing case.
      } catch (NoSuchFileException | FileNotFoundException ex) {
        try {
          if (activeLazy.get(name) != null
              || CompressingDirectory.readLengthFromHeader(compressedPath.resolve(name)) > 0) {
            // if we have a lazy entry, or if the length header is written (indicating that the
            // compressed copy is completely written) then trust that the compressed/canonical
            // copy is complete, and return, ignoring the sync request (since sync'ing a partially
            // populated file would be pointless).
            continue;
          }
        } catch (Throwable t) {
          ex.addSuppressed(t);
        }
        throw ex;
      }
    }

    // to trigger `maybeDeletePendingFiles()`
    super.sync(Collections.emptySet());
  }

  private volatile boolean isClosed = false;
  private final ConcurrentHashMap<String, LazyEntry> activeLazy = new ConcurrentHashMap<>();

  private void activate(LazyEntry lazyEntry) throws IOException {
    LazyLoadInput canonical = lazyEntry.input;
    String lazyName = lazyEntry.lazyName;
    String name = lazyEntry.canonicalName;
    long fileSize = canonical.length();
    boolean success = false;
    try {
      canonical.force();
      super.rename(lazyName, name);
      success = true;
      syncMetaData();
      if (!canonical.loaded.compareAndSet(null, super.openInput(name, IOContext.READ))) {
        log.warn("out-of-band loading detected for file {}", name);
      }
      canonical.loadedLatch.countDown();
    } finally {
      if (!success) {
        IOUtils.deleteFilesIgnoringExceptions(this, lazyName);
      }
      if (!activeLazy.remove(name, lazyEntry)) {
        log.warn("out-of-band lazy removal detected for file {}", name);
      } else {
        lazyMapSize.decrement();
        lazyMapDiskUsage.add(-fileSize);
      }
    }
  }

  static final class LazyEntry {
    private String canonicalName;
    private final String lazyName;
    private final LazyLoadInput input;
    private final WeakReference<AccessDirectory> dir;
    private final int[] from = new int[1];

    private LazyEntry(
        String canonicalName, String lazyName, LazyLoadInput input, AccessDirectory dir) {
      this.canonicalName = canonicalName;
      this.lazyName = lazyName;
      this.input = input;
      this.dir = new WeakReference<>(dir);
    }

    public int load() throws IOException {
      AccessDirectory dir = this.dir.get();
      if (dir == null) {
        return -1;
      }
      long offset = ((long) from[0]) << COMPRESSION_BLOCK_SHIFT;

      LazyLoadInput in;
      synchronized (input.closed) {
        if (input.closed[0]) {
          return -1;
        }
        // NOTE: regular activate (what we're doing here) should bypass `slice`, since
        // we don't want crosstalk with `priorityActivate`, `populated()`, etc.
        in = new LazyLoadInput("activation", input, offset, input.length - offset);
      }

      final int ret;
      try {
        ret = in.load(from);
      } catch (AlreadyClosedException ex) {
        return -1;
      }
      if (ret >= 0) {
        return ret;
      }
      // we are finished, so finally activate the file
      dir.activate(this);
      return ret;
    }
  }

  private static final String LAZY_TMP_FILE_SUFFIX = "lazy";
  private static final String LAZY_TMP_FILE_REGEX =
      ".*_" + LAZY_TMP_FILE_SUFFIX + "_[0-9a-z]+\\.tmp";
  private static final int PATTERN_LOWEST = '.';
  private static final int PATTERN_HIGHEST = 'z';
  private static final ByteRunAutomaton LAZY_TMP_FILE_AUTOMATON;
  private static final int LAZY_TMP_FILE_AUTOMATON_ACCEPT_STATE;

  static {
    // reverse the pattern automaton and step through in reverse
    Automaton a =
        Operations.reverse(
            new RegExp(LAZY_TMP_FILE_REGEX).toAutomaton(Operations.DEFAULT_DETERMINIZE_WORK_LIMIT));
    LAZY_TMP_FILE_AUTOMATON =
        new CompiledAutomaton(a, null, true, Operations.DEFAULT_DETERMINIZE_WORK_LIMIT, false)
            .runAutomaton;
    int s = 0;
    String matchingFilename = "_lazy_1.tmp";
    for (int i = matchingFilename.length() - 1; i >= 0; i--) {
      char c = matchingFilename.charAt(i);
      s = LAZY_TMP_FILE_AUTOMATON.step(s, c & 0x7f);
      if (LAZY_TMP_FILE_AUTOMATON.isAccept(s)) {
        break;
      }
    }
    if (!LAZY_TMP_FILE_AUTOMATON.isAccept(s)) {
      throw new AssertionError();
    }
    // because the pattern is non-branching, we know there is exactly one accept state
    LAZY_TMP_FILE_AUTOMATON_ACCEPT_STATE = s;
  }

  static int lazyTmpFileSuffixStartIdx(String filename) {
    int s = 0;
    for (int i = filename.length() - 1; i >= 0; i--) {
      char c = filename.charAt(i);
      if (c < PATTERN_LOWEST || c > PATTERN_HIGHEST) {
        // this allows us to use char input with byte-based ByteRunAutomaton
        return -1;
      }
      if ((s = LAZY_TMP_FILE_AUTOMATON.step(s, c & 0x7f)) == -1) {
        return -1;
      }
      if (s == LAZY_TMP_FILE_AUTOMATON_ACCEPT_STATE) {
        return i;
      }
    }
    return -1;
  }

  @SuppressWarnings("try")
  private LazyEntry open(String name) throws IOException {
    LazyEntry lazyEntry;
    try {
      boolean[] weComputed = new boolean[1];
      lazyEntry =
          activeLazy.computeIfAbsent(
              name,
              (k) -> {
                String lazyName = null;
                LazyEntry ret = null;
                try (IndexOutput out =
                    super.createTempOutput(name, LAZY_TMP_FILE_SUFFIX, IOContext.DEFAULT)) {
                  // create a stub
                  lazyName = out.getName();
                  ret =
                      new LazyEntry(
                          name,
                          lazyName,
                          new LazyLoadInput(
                              compressedPath.resolve(name),
                              path.resolve(lazyName),
                              rawCt,
                              loadedCt,
                              populatedCt,
                              lazyCt,
                              priorityActivate,
                              lazyLoadedBlockBytes),
                          this);
                  weComputed[0] = true;
                  lazyMapDiskUsage.add(ret.input.length());
                  lazyMapSize.increment();
                  if (isClosed) {
                    try (Closeable c = ret.input) {
                      lazyMapDiskUsage.add(-ret.input.length());
                      lazyMapSize.decrement();
                    }
                    ret = null;
                    throw new AlreadyClosedException("Directory already closed");
                  }
                  return ret;
                } catch (IOException e) {
                  throw new UncheckedIOException(e);
                } finally {
                  if (ret == null && lazyName != null) {
                    try {
                      super.deleteFile(lazyName);
                    } catch (IOException e) {
                      // another exception has been thrown; we can ignore this one
                    }
                  }
                }
              });
      if (weComputed[0]) {
        activationQueue.add(lazyEntry);
      }
      return lazyEntry;
    } catch (UncheckedIOException e) {
      throw e.getCause();
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  @Override
  public IndexInput openInput(String name, IOContext context) throws IOException {
    try {
      IndexInput ret = super.openInput(name, context);
      rawCt.increment();
      return ret;
    } catch (NoSuchFileException | FileNotFoundException ex) {
      try {
        LazyEntry lazy = open(name);
        if (lazy == null) {
          throw ex;
        } else {
          return lazy.input.clone();
        }
      } catch (IOException ex1) {
        ex1.addSuppressed(ex);
        throw ex1;
      }
    }
  }

  @Override
  @SuppressWarnings("try")
  public void deleteFile(String name) throws IOException {
    try (Closeable c = () -> super.deleteFile(name)) {
      LazyEntry lazyRemoved = activeLazy.remove(name);
      if (lazyRemoved != null) {
        long fileSize = lazyRemoved.input.length();
        try (lazyRemoved.input) {
          super.deleteFile(lazyRemoved.lazyName);
        } finally {
          lazyMapDiskUsage.add(-fileSize);
          lazyMapSize.decrement();
        }
      }
    }
  }

  @Override
  public synchronized void close() throws IOException {
    isClosed = true;
    try {
      // try to remove and delete all the active lazy files
      Iterator<LazyEntry> iter = activeLazy.values().iterator();
      while (iter.hasNext()) {
        LazyEntry e = iter.next();
        try (e.input) {
          IOUtils.deleteFilesIgnoringExceptions(this, e.lazyName);
          iter.remove();
          lazyMapSize.decrement();
          lazyMapDiskUsage.add(-e.input.length());
        } catch (Exception ex) {
          log.warn("exception closing lazy input for {}", e.canonicalName, ex);
        }
      }
    } finally {
      if (!activeLazy.isEmpty()) {
        log.error(
            "found residual lazy input after close: {}",
            activeLazy.values().stream().map((e) -> e.canonicalName).collect(Collectors.toList()));
        activeLazy.clear();
      }
      super.close();
    }
  }

  private static final ByteBuffer EMPTY = ByteBuffer.allocate(0);

  private static final int COMPRESSION_TO_MAP_TRANSLATE_SHIFT =
      MAX_MAP_SHIFT - COMPRESSION_BLOCK_SHIFT;

  static class ConcurrentIntSet
      implements Comparable<ConcurrentIntSet>, Callable<Integer>, AutoCloseable {
    private volatile boolean closed = false;
    private final String tmpPath;
    private final int lastIdx;
    private final int blocksSize;
    private final AtomicIntegerArray bits;
    private final ConcurrentHashMap<ConcurrentIntSet, Boolean> priorityActivate;
    private LazyLoadInput in;

    ConcurrentIntSet(
        String tmpPath,
        int size,
        int blocksSize,
        LazyLoadInput in,
        ConcurrentHashMap<ConcurrentIntSet, Boolean> priorityActivate) {
      this.tmpPath = tmpPath;
      this.lastIdx = size - 1;
      this.blocksSize = ((blocksSize - 1) / Integer.SIZE) + 1;
      bits = new AtomicIntegerArray(this.blocksSize);
      this.priorityActivate = priorityActivate;
      this.in = in;
    }

    boolean add(int val) {
      int alignment = val & RESERVE_MASK;
      int extant = bits.getAndUpdate(val >> RESERVE_SHIFT, COMPLETE_SET[alignment]);
      try {
        return (extant & RESERVE_MASKS[alignment]) == 0;
      } finally {
        priorityActivate.putIfAbsent(this, Boolean.TRUE);
      }
    }

    @Override
    public Integer call() throws IOException {
      LazyLoadInput in = this.in;
      if (in == null) {
        return 0;
      }
      int loadedCt = 0;
      int blockIdx = 0;
      assert lastIdx == in.lastBlockIdx;
      do {
        int blockVal;
        while ((blockVal = bits.getAndSet(blockIdx, 0)) == 0) {
          if (++blockIdx >= blocksSize) {
            return loadedCt;
          }
          // keep advancing
        }
        int mask = Integer.MIN_VALUE;
        int blockIdxBaseline =
            blockIdx << RESERVE_SHIFT; // the first of group of indexes into `complete` array
        int i = 0;
        do {
          if ((blockVal & mask) != 0) {
            // get the actual base compressed block idx
            int compressedBlockBaseline = (blockIdxBaseline + i) << RESERVE_SHIFT;
            for (int idx = compressedBlockBaseline, lim = compressedBlockBaseline + Integer.SIZE;
                idx < lim;
                idx++) {
              int decompressedLen;
              if (idx < lastIdx) {
                decompressedLen = COMPRESSION_BLOCK_SIZE;
              } else if (idx == lastIdx) {
                decompressedLen = in.lastBlockDecompressedLen;
              } else {
                return loadedCt;
              }
              if (closed) {
                return loadedCt;
              } else if (in.loadBlock(idx, decompressedLen)) {
                loadedCt++;
              }
            }
          }
          i++;
        } while ((mask >>>= 1) != 0);
      } while (++blockIdx < blocksSize);
      return loadedCt;
    }

    @Override
    public int hashCode() {
      return tmpPath.hashCode() ^ ConcurrentIntSet.class.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
      return tmpPath.equals(((ConcurrentIntSet) obj).tmpPath);
    }

    @Override
    public int compareTo(ConcurrentIntSet o) {
      return tmpPath.compareTo(o.tmpPath);
    }

    @Override
    public void close() {
      closed = true;
      in = null;
    }
  }

  private static final int RESERVE_SHIFT = 5;
  private static final int RESERVE_MASK = (1 << RESERVE_SHIFT) - 1;
  private static final int[] RESERVE_MASKS = new int[Integer.SIZE];
  private static final IntUnaryOperator[] RESERVE_RELEASE = new IntUnaryOperator[Integer.SIZE];
  private static final IntUnaryOperator[] COMPLETE_SET = new IntUnaryOperator[Integer.SIZE];

  static {
    int mask = 1;
    for (int i = Integer.SIZE - 1; i >= 0; i--) {
      RESERVE_MASKS[i] = mask;
      int invert = ~mask;
      int maskF = mask;
      RESERVE_RELEASE[i] = (j) -> j & invert;
      COMPLETE_SET[i] = (j) -> j | maskF;
      mask <<= 1;
    }
  }

  private static ByteBufferGuard.BufferCleaner unmapHack() {
    Object hack = MappedByteBufferIndexInputProvider.unmapHackImpl();
    if (hack instanceof ByteBufferGuard.BufferCleaner) {
      return (ByteBufferGuard.BufferCleaner) hack;
    } else {
      throw new UnsupportedOperationException("unmap not available");
    }
  }

  /**
   * This interface is mainly useful for testing. We don't want to directly expose {@link
   * LazyLoadInput} (the only implementing class, at time of writing); but we expose this interface
   * to allow tests to efficiently block before checking that the filesystem has been updated (i.e.,
   * that the access copy of a given file is present and fully populated).
   */
  public interface LazyLoad {
    void blockUntilLoaded() throws InterruptedException;
  }

  static final class LazyLoadInput extends IndexInput implements RandomAccessInput, LazyLoad {
    private final AtomicIntegerArray populatedUpTo;
    private final ConcurrentIntSet priorityLoad;
    private final AtomicReference<IndexInput> loaded;
    private final CountDownLatch loadedLatch;
    private final AtomicIntegerArray reserve;
    private final AtomicIntegerArray complete;
    private final ByteBufferGuard compressedGuard;
    private final ByteBufferGuard guard;
    private final boolean[] closed;
    private final long length;
    private final boolean isClone;
    private final long[] blockOffsets;
    private final int blockCount;
    private final int lastBlockIdx;
    private final int lastBlockDecompressedLen;
    private ByteBuffer[] mapped;
    private ByteBuffer[] accessMapped;

    private final IndexInput accessPopulated;

    private final long offset;
    private final long sliceLength;

    private long seekPos = -1;
    private long filePointer = 0;
    private ByteBuffer postBuffer = EMPTY;
    private int postBufferBaseline;
    private int currentBlockIdx = -1;

    private final LongBuffer[][] multiLongViews;
    private final IntBuffer[][] multiIntViews;
    private final FloatBuffer[][] multiFloatViews;
    private static final LongBuffer EMPTY_LONGBUFFER = LongBuffer.allocate(0);
    private static final IntBuffer EMPTY_INTBUFFER = IntBuffer.allocate(0);
    private static final FloatBuffer EMPTY_FLOATBUFFER = FloatBuffer.allocate(0);
    private LongBuffer[] longViews;
    private IntBuffer[] intViews;
    private FloatBuffer[] floatViews;

    private final LongAdder rawCt;
    private final LongAdder loadedCt;
    private final LongAdder populatedCt;
    private final LongAdder lazyCt;
    private final LongAdder lazyLoadedBlockBytes;

    private LazyLoadInput(
        String resourceDescription, LazyLoadInput parent, long offset, long length) {
      super(resourceDescription);
      this.lazyLoadedBlockBytes = parent.lazyLoadedBlockBytes;
      this.populatedUpTo = parent.populatedUpTo;
      this.priorityLoad = parent.priorityLoad;
      this.loaded = parent.loaded;
      this.loadedLatch = parent.loadedLatch;
      this.guard = parent.guard;
      this.closed = parent.closed;
      this.compressedGuard = parent.compressedGuard;
      this.accessPopulated = parent.accessPopulated;
      this.isClone = true;
      this.length = parent.length;
      this.blockOffsets = parent.blockOffsets;
      this.blockCount = parent.blockCount;
      this.lastBlockIdx = parent.lastBlockIdx;
      this.lastBlockDecompressedLen = parent.lastBlockDecompressedLen;
      this.reserve = parent.reserve;
      this.complete = parent.complete;
      this.offset = parent.offset + offset;
      this.seekPos = this.offset;
      this.sliceLength = length;
      this.multiFloatViews = parent.multiFloatViews;
      this.multiLongViews = parent.multiLongViews;
      this.multiIntViews = parent.multiIntViews;
      ByteBuffer[] parentMapped = parent.mapped;
      this.mapped = new ByteBuffer[parentMapped.length];
      for (int i = parentMapped.length - 1; i >= 0; i--) {
        mapped[i] = parentMapped[i].duplicate();
      }
      ByteBuffer[] parentAccessMapped = parent.accessMapped;
      this.accessMapped = new ByteBuffer[parentAccessMapped.length];
      for (int i = parentAccessMapped.length - 1; i >= 0; i--) {
        accessMapped[i] = parentAccessMapped[i].duplicate().order(ByteOrder.LITTLE_ENDIAN);
      }
      this.rawCt = parent.rawCt;
      this.loadedCt = parent.loadedCt;
      this.populatedCt = parent.populatedCt;
      this.lazyCt = parent.lazyCt;
    }

    private LazyLoadInput(
        Path source,
        Path lazy,
        LongAdder rawCt,
        LongAdder loadedCt,
        LongAdder populatedCt,
        LongAdder lazyCt,
        ConcurrentHashMap<ConcurrentIntSet, Boolean> priorityActivate,
        LongAdder lazyLoadedBlockBytes)
        throws IOException {
      super("lazy");
      this.lazyLoadedBlockBytes = lazyLoadedBlockBytes;
      this.rawCt = rawCt;
      this.loadedCt = loadedCt;
      this.populatedCt = populatedCt;
      this.lazyCt = lazyCt;
      loaded = new AtomicReference<>();
      loadedLatch = new CountDownLatch(1);
      guard = new ByteBufferGuard("accessGuard", unmapHack());
      this.closed = new boolean[1];
      compressedGuard = new ByteBufferGuard("compressedGuard", unmapHack());
      try (FileChannel channel = FileChannel.open(source, StandardOpenOption.READ)) {
        long compressedFileSize = channel.size();
        mapped =
            new MappedByteBuffer[Math.toIntExact(((compressedFileSize - 1) >> MAX_MAP_SHIFT) + 1)];
        long pos = 0;
        long limit = MAX_MAP_SIZE;
        for (int i = 0, lim = mapped.length; i < lim; i++) {
          int size = (int) (Math.min(limit, compressedFileSize) - pos);
          mapped[i] = channel.map(FileChannel.MapMode.READ_ONLY, pos, size);
          pos = limit;
          limit += MAX_MAP_SIZE;
        }

        isClone = false;
        long size = channel.size();
        if (size == 0) {
          length = 0;
          blockOffsets = null;
          blockCount = 0;
          lastBlockIdx = -1;
          lastBlockDecompressedLen = 0;
          reserve = null;
          complete = null;
          priorityLoad = null;
          populatedUpTo = null;
        } else {
          ByteBuffer initial = mapped[0];
          length = guard.getLong(initial, 0);
          if (length >> COMPRESSION_BLOCK_SHIFT > Integer.MAX_VALUE) {
            throw new IllegalArgumentException(
                "file too long " + Long.toHexString(length) + ", " + source);
          }
          int blockDeltaFooterSize = guard.getInt(initial, Long.BYTES);
          int cTypeId = guard.getByte(initial, HEADER_SIZE - Integer.BYTES) & 0xff;
          if (cTypeId != CompressingDirectory.COMPRESSION_TYPE.id) {
            throw new IllegalArgumentException("unrecognized compression type id: " + cTypeId);
          }
          int cBlockTypeId = guard.getByte(initial, HEADER_SIZE - Integer.BYTES + 1) & 0xff;
          if (cBlockTypeId != CompressingDirectory.COMPRESSION_TYPE.id) {
            throw new IllegalArgumentException(
                "unrecognized compression block type id: " + cBlockTypeId);
          }
          byte[] footer = new byte[blockDeltaFooterSize];
          long blockDeltaFooterOffset = size - blockDeltaFooterSize;

          // TODO: read this from mapped instead?
          channel.read(ByteBuffer.wrap(footer), blockDeltaFooterOffset);
          ByteArrayDataInput in = new ByteArrayDataInput(footer);

          long blockOffset = HEADER_SIZE;
          int lastBlockSize = BLOCK_SIZE_ESTIMATE;
          blockCount = (int) (((length - 1) >> COMPRESSION_BLOCK_SHIFT) + 1);
          blockOffsets = new long[blockCount + 1];
          lastBlockIdx = blockCount - 1;
          lastBlockDecompressedLen = (int) (((length - 1) & COMPRESSION_BLOCK_MASK_LOW) + 1);
          blockOffsets[0] = blockOffset;
          for (int i = 1; i < blockCount; i++) {
            int delta = in.readZInt();
            int nextBlockSize = lastBlockSize + delta;
            blockOffset += nextBlockSize;
            blockOffsets[i] = blockOffset;
            lastBlockSize = nextBlockSize;
          }
          blockOffsets[blockCount] = blockDeltaFooterOffset;
          int reserveSize = ((blockCount - 1) / Integer.SIZE) + 1;
          reserve = new AtomicIntegerArray(reserveSize);
          complete = new AtomicIntegerArray(reserveSize);
          priorityLoad =
              new ConcurrentIntSet(
                  lazy.toString(), blockCount, reserveSize, this, priorityActivate);
          populatedUpTo = new AtomicIntegerArray(reserveSize);
          populatedUpTo.set(0, -1); // every index must initially have a value less than itself.
        }
      }

      this.offset = 0;
      this.sliceLength = length;

      try (RandomAccessFile raf = new RandomAccessFile(lazy.toFile(), "rw")) {
        // allocate the proper amount of space
        raf.setLength(length);
        FileChannel channel = raf.getChannel();
        int mapChunkCount = Math.toIntExact(((length - 1) >> MAX_MAP_SHIFT) + 1);
        accessMapped = new MappedByteBuffer[mapChunkCount];
        multiLongViews = new LongBuffer[mapChunkCount][];
        multiIntViews = new IntBuffer[mapChunkCount][];
        multiFloatViews = new FloatBuffer[mapChunkCount][];
        long pos = 0;
        long limit = MAX_MAP_SIZE;
        for (int i = 0, lim = accessMapped.length; i < lim; i++) {
          int size = (int) (Math.min(limit, length) - pos);
          MappedByteBuffer mbb = channel.map(FileChannel.MapMode.READ_WRITE, pos, size);
          accessMapped[i] = mbb;
          LongBuffer[] lbb = new LongBuffer[Long.BYTES];
          IntBuffer[] ibb = new IntBuffer[Integer.BYTES];
          FloatBuffer[] fbb = new FloatBuffer[Float.BYTES];
          multiLongViews[i] = lbb;
          multiIntViews[i] = ibb;
          multiFloatViews[i] = fbb;
          mbb.order(ByteOrder.LITTLE_ENDIAN);
          for (int j = Long.BYTES - 1; j >= 0; j--) {
            mbb.position(j);
            lbb[j] = mbb.asLongBuffer();
            if (j < Integer.BYTES) {
              ibb[j] = mbb.asIntBuffer();
              fbb[j] = mbb.asFloatBuffer();
            }
          }
          pos = limit;
          limit += MAX_MAP_SIZE;
        }
      }
      boolean padCopy = (length & MAX_MAP_SHIFT) == 0;
      ByteBuffer[] copy = new ByteBuffer[padCopy ? accessMapped.length + 1 : accessMapped.length];
      for (int i = accessMapped.length - 1; i >= 0; i--) {
        copy[i] = accessMapped[i].duplicate().order(ByteOrder.LITTLE_ENDIAN);
      }
      if (padCopy) {
        // ByteBufferIndexInput seems to overallocate number of buffers on chunk size boundaries,
        // so add padding empty buffer to avoid creating problems.
        copy[accessMapped.length] = EMPTY_PAD_BUFFER;
      }
      this.accessPopulated =
          ByteBufferIndexInput.newInstance("accessPopulated", copy, length, MAX_MAP_SHIFT, guard);
    }

    private static final ByteBuffer EMPTY_PAD_BUFFER =
        ByteBuffer.allocateDirect(0).order(ByteOrder.LITTLE_ENDIAN);

    private boolean acquireWrite(int blockIdx) {
      final int reserveBlockIdx = blockIdx >>> RESERVE_SHIFT;
      final int reserveMask = RESERVE_MASKS[blockIdx & RESERVE_MASK];
      int witness = reserve.get(reserveBlockIdx);
      int expected;
      while (((expected = witness) & reserveMask) == 0) {
        witness = reserve.compareAndExchange(reserveBlockIdx, expected, expected | reserveMask);
        if (witness == expected) {
          return true;
        }
      }
      return false;
    }

    private void releaseWrite(int blockIdx) {
      reserve.updateAndGet(blockIdx >>> RESERVE_SHIFT, RESERVE_RELEASE[blockIdx & RESERVE_MASK]);
    }

    private void setReadable(int blockIdx) {
      complete.updateAndGet(blockIdx >>> RESERVE_SHIFT, COMPLETE_SET[blockIdx & RESERVE_MASK]);
    }

    private boolean acquireRead(int blockIdx) {
      final int reserveBlockIdx = blockIdx >>> RESERVE_SHIFT;
      final int reserveMask = RESERVE_MASKS[blockIdx & RESERVE_MASK];
      while ((complete.get(reserveBlockIdx) & reserveMask) == 0) {
        // keep checking
        Thread.yield();
        if ((reserve.get(reserveBlockIdx) & reserveMask) != reserveMask) {
          // the computation we were waiting for must have failed
          return false;
        }
        // TODO: this should be ok to busy-wait, but maybe re-evaluate based on stats?
      }
      return true;
    }

    private boolean populated(long offset, long length) {
      int startBlockIdx = (int) (offset >> COMPRESSION_BLOCK_SHIFT);
      int lastBlockIdx = (int) ((offset + length - 1) >> COMPRESSION_BLOCK_SHIFT);
      final int startCompleteBlockIdx = startBlockIdx >>> RESERVE_SHIFT;
      final int lastCompleteBlockIdx = lastBlockIdx >>> RESERVE_SHIFT;
      final int firstSubsequentPartialBlock = populatedUpTo.get(startCompleteBlockIdx);
      if (firstSubsequentPartialBlock > lastCompleteBlockIdx) {
        // we've already checked and recorded that this is a populated range
        return true;
      } else if (firstSubsequentPartialBlock < lastCompleteBlockIdx) {
        // we haven't checked the full range
        final boolean registerRun;
        if (firstSubsequentPartialBlock >= startCompleteBlockIdx) {
          // the first block is populated
          registerRun = true;
        } else {
          // the first block is not populated
          final int startCompleteBlockVal = complete.get(startCompleteBlockIdx);
          registerRun =
              startCompleteBlockVal == -1
                  || (blockCount <= Integer.SIZE
                      && Integer.bitCount(startCompleteBlockVal) == blockCount);
          if (!registerRun) {
            priorityLoad.add(startCompleteBlockIdx);
            final int startMask = -1 >>> (startBlockIdx & RESERVE_MASK); // not sign-extended
            if ((startCompleteBlockVal & startMask) != startMask) {
              return false;
            }
          }
        }
        for (int i = Math.max(startCompleteBlockIdx + 1, firstSubsequentPartialBlock);
            i < lastCompleteBlockIdx;
            i++) {
          if (complete.get(i) != -1) {
            priorityLoad.add(i);
            if (registerRun) {
              registerRun(startCompleteBlockIdx, i);
            }
            return false;
          }
        }
        if (registerRun) {
          registerRun(startCompleteBlockIdx, lastCompleteBlockIdx);
        }
      }
      final int lastMask = Integer.MIN_VALUE >> (lastBlockIdx & RESERVE_MASK); // sign-extended
      if ((complete.get(lastCompleteBlockIdx) & lastMask) == lastMask) {
        return true;
      } else {
        priorityLoad.add(lastCompleteBlockIdx);
        return false;
      }
    }

    private void registerRun(int startCompleteBlockIdx, int upTo) {
      int expected = populatedUpTo.get(startCompleteBlockIdx);
      int witnessUpTo;
      while (upTo > expected
          && (witnessUpTo = populatedUpTo.compareAndExchange(startCompleteBlockIdx, expected, upTo))
              != expected) {
        expected = witnessUpTo;
      }
    }

    private void actualSeek(final long pos) throws IOException {
      filePointer = pos;
      final int blockIdx = (int) (pos >> COMPRESSION_BLOCK_SHIFT);
      if (blockIdx != currentBlockIdx) {
        initBlock(blockIdx);
      }
      postBuffer.position(postBufferBaseline + (int) (pos & COMPRESSION_BLOCK_MASK_LOW));
    }

    private void initBlock(int blockIdx) throws IOException {
      // NOTE: it is very important to keep this method small so that it can be inlined.
      if (blockIdx > lastBlockIdx) {
        throw new EOFException();
      }
      final long blockOffset = blockOffsets[blockIdx];
      final int compressedLen = (int) (blockOffsets[blockIdx + 1] - blockOffset);
      refill(blockOffset, compressedLen, blockIdx);
    }

    private void refill() throws IOException {
      final int blockIdx = currentBlockIdx + 1;
      if (blockIdx > lastBlockIdx) {
        throw new EOFException();
      }
      final long blockOffset = blockOffsets[blockIdx];
      final int compressedLen = (int) (blockOffsets[blockIdx + 1] - blockOffset);
      refill(blockOffset, compressedLen, blockIdx);
    }

    private ByteBuffer supply(long pos, int compressedLen, int decompressedLen) throws IOException {
      final byte[] preBuffer = new byte[compressedLen];
      final byte[] decompressBuffer =
          new byte[decompressedLen + 7]; // +7 overhead for optimal decompression
      ByteBuffer bb = mapped[(int) (pos >> MAX_MAP_SHIFT)].position((int) (pos & MAX_MAP_MASK));
      int offset = 0;
      int left = bb.remaining();
      int toRead = compressedLen;
      RESERVATION.incrementAndGet();
      try {
        while (left < toRead) {
          compressedGuard.getBytes(bb, preBuffer, offset, left);
          toRead -= left;
          offset += left;
          pos += left;
          bb = mapped[(int) (pos >> MAX_MAP_SHIFT)].position((int) (pos & MAX_MAP_MASK));
          left = bb.remaining();
        }
        compressedGuard.getBytes(bb, preBuffer, offset, toRead);
      } finally {
        RESERVATION.decrementAndGet();
      }
      CompressingDirectory.decompress(preBuffer, 0, decompressedLen, decompressBuffer, 0);
      lazyLoadedBlockBytes.add(decompressedLen);
      return ByteBuffer.wrap(
          decompressBuffer,
          0,
          decompressedLen); // usually decompressedLen == COMPRESSION_BLOCK_SIZE
    }

    private boolean loadBlock(int blockIdx, int decompressedLen) throws IOException {
      if (!acquireWrite(blockIdx)) {
        return false;
      } else {
        try {
          final long blockOffset = blockOffsets[blockIdx];
          final int compressedLen = (int) (blockOffsets[blockIdx + 1] - blockOffset);
          int reservationCt;
          while ((reservationCt = RESERVATION.get()) > 0) {
            // Yes, we are busy-waiting here. But we can't afford synchronization in the
            // block-cache supply method, which is the code that modifies `reservation`
            // ... whereas we really don't care about the performance of this method, it's
            // async, and best-effort only.
            try {
              Thread.sleep((long) reservationCt << 3);
            } catch (InterruptedException ex) {
              throw new RuntimeException(ex);
            }
          }
          long accessBlockStart = ((long) blockIdx) << COMPRESSION_BLOCK_SHIFT;
          ByteBuffer dest = accessMapped[(int) (accessBlockStart >> MAX_MAP_SHIFT)];
          int destPos = (int) (accessBlockStart & MAX_MAP_MASK);
          synchronized (closed) {
            // all read/write access to mapped buffers is protected by `guard`, but
            // it costs us essentially nothing to add another layer of protection here.
            // The extra protection is particularly relevant for the case of background
            // loading, since it by definition takes place asynchronously, and may be
            // reasonably expected to long outlive the mmap'd buffers used here (as
            // opposed to normal synchronous use, which would only relatively rarely
            // persist beyond the closing of the associated input.
            // NOTE: "costs us nothing" because `loadBlock()` is only ever called from
            // a single thread, so the only lock contention we have is the one-time
            // call to `close()` on the root input. We also don't really care about
            // any locking overhead here because async loading runs in the background.
            if (closed[0]) {
              throw new AlreadyClosedException("already closed");
            }
            ByteBuffer supply = supply(blockOffset, compressedLen, decompressedLen);
            guard.putBytes(dest.clear().position(destPos), supply);
          }
          setReadable(blockIdx);
        } catch (Throwable t) {
          releaseWrite(blockIdx);
          throw t; // this should really never happen; if it does, just bail
        }
        return true;
      }
    }

    private int load(int[] from) throws IOException {
      int ret = 0;
      for (int blockIdx = from[0]; blockIdx <= lastBlockIdx; blockIdx++) {
        do {
          boolean isLastBlock = blockIdx == lastBlockIdx;
          if (loadBlock(
              blockIdx, isLastBlock ? lastBlockDecompressedLen : COMPRESSION_BLOCK_SIZE)) {
            if (++ret >= Integer.SIZE) {
              from[0] = blockIdx + 1;
              return isLastBlock ? ~ret : ret;
            }
          }
        } while (!acquireRead(blockIdx));
      }
      return ~ret;
    }

    private void refill(final long pos, final int compressedLen, final int blockIdx)
        throws IOException {
      final int decompressedLen =
          blockIdx == lastBlockIdx ? lastBlockDecompressedLen : COMPRESSION_BLOCK_SIZE;
      if (acquireWrite(blockIdx)) {
        try {
          ByteBuffer supply = supply(pos, compressedLen, decompressedLen);
          long accessBlockStart = ((long) blockIdx) << COMPRESSION_BLOCK_SHIFT;
          ByteBuffer dest = accessMapped[(int) (accessBlockStart >> MAX_MAP_SHIFT)];
          int destPos = (int) (accessBlockStart & MAX_MAP_MASK);
          dest.clear().position(destPos).limit(destPos + decompressedLen).mark();
          guard.putBytes(dest, supply).reset();
          postBuffer = dest;
          postBufferBaseline = destPos;
          setReadable(blockIdx);
        } catch (Throwable t) {
          releaseWrite(blockIdx);
          throw t;
        }
      } else if (acquireRead(blockIdx)) {
        long accessBlockStart = ((long) blockIdx) << COMPRESSION_BLOCK_SHIFT;
        ByteBuffer dest = accessMapped[(int) (accessBlockStart >> MAX_MAP_SHIFT)];
        int destPos = (int) (accessBlockStart & MAX_MAP_MASK);
        dest.clear().position(destPos).limit(destPos + decompressedLen);
        postBuffer = dest;
        postBufferBaseline = destPos;
      } else {
        postBuffer = supply(pos, compressedLen, decompressedLen);
        postBufferBaseline = postBuffer.position();
        postBuffer.order(ByteOrder.LITTLE_ENDIAN);
      }
      if (currentBlockIdx != -1
          && currentBlockIdx >> COMPRESSION_TO_MAP_TRANSLATE_SHIFT
              != blockIdx >> COMPRESSION_TO_MAP_TRANSLATE_SHIFT) {
        // views have gone out of scope
        floatViews = null;
        intViews = null;
        longViews = null;
      }
      currentBlockIdx = blockIdx;
    }

    @Override
    public byte readByte(final long pos) throws IOException {
      final long absolutePos = pos + offset;
      final int blockIdx = (int) (absolutePos >> COMPRESSION_BLOCK_SHIFT);
      if (blockIdx != currentBlockIdx) {
        initBlock(blockIdx);
      }
      return guard.getByte(
          postBuffer, postBufferBaseline + (int) (absolutePos & COMPRESSION_BLOCK_MASK_LOW));
    }

    @Override
    public short readShort(final long pos) throws IOException {
      final long absolutePos = pos + offset;
      final int blockIdx = (int) (absolutePos >> COMPRESSION_BLOCK_SHIFT);
      if (blockIdx != currentBlockIdx) {
        initBlock(blockIdx);
      }
      final int limit = postBuffer.limit();
      final int localPos = postBufferBaseline + (int) (absolutePos & COMPRESSION_BLOCK_MASK_LOW);
      final int remaining = limit - localPos;
      if (remaining < Short.BYTES) {
        return slowReadShort(remaining, localPos);
      } else {
        return guard.getShort(postBuffer, localPos);
      }
    }

    @Override
    public int readInt(final long pos) throws IOException {
      final long absolutePos = pos + offset;
      final int blockIdx = (int) (absolutePos >> COMPRESSION_BLOCK_SHIFT);
      if (blockIdx != currentBlockIdx) {
        initBlock(blockIdx);
      }
      final int limit = postBuffer.limit();
      final int localPos = postBufferBaseline + (int) (absolutePos & COMPRESSION_BLOCK_MASK_LOW);
      final int remaining = limit - localPos;
      if (remaining < Integer.BYTES) {
        return slowReadInt(remaining, localPos);
      } else {
        return guard.getInt(postBuffer, localPos);
      }
    }

    @Override
    public long readLong(final long pos) throws IOException {
      final long absolutePos = pos + offset;
      final int blockIdx = (int) (absolutePos >> COMPRESSION_BLOCK_SHIFT);
      if (blockIdx != currentBlockIdx) {
        initBlock(blockIdx);
      }
      final int limit = postBuffer.limit();
      final int localPos = postBufferBaseline + (int) (absolutePos & COMPRESSION_BLOCK_MASK_LOW);
      final int remaining = limit - localPos;
      if (remaining < Long.BYTES) {
        return slowReadLong(remaining, localPos);
      } else {
        return guard.getLong(postBuffer, localPos);
      }
    }

    @Override
    public byte readByte() throws IOException {
      final long pos = seekPos;
      if (pos != -1) {
        seekPos = -1;
        actualSeek(pos);
      }
      return _readByte(postBuffer.hasRemaining() ? 1 : 0);
    }

    private byte _readByte(final int remaining) throws IOException {
      if (remaining == 0) {
        refill();
      }
      filePointer++;

      return guard.getByte(postBuffer);
    }

    @Override
    public void readBytes(byte[] dst, int offset, int len) throws IOException {
      final long pos = seekPos;
      if (pos != -1) {
        seekPos = -1;
        actualSeek(pos);
      }
      final int left = postBuffer.remaining();
      filePointer += len;
      if (left < len) {
        slowReadBytes(dst, offset, len, left);
      } else {
        guard.getBytes(postBuffer, dst, offset, len);
      }
    }

    private void slowReadBytes(final byte[] dst, int offset, int toRead, int left)
        throws IOException {
      do {
        guard.getBytes(postBuffer, dst, offset, left);
        toRead -= left;
        offset += left;
        refill();
        left = postBuffer.remaining();
      } while (left < toRead);
      guard.getBytes(postBuffer, dst, offset, toRead);
    }

    @Override
    public short readShort() throws IOException {
      final long pos = seekPos;
      if (pos != -1) {
        seekPos = -1;
        actualSeek(pos);
      }
      final int remaining = postBuffer.remaining();
      if (remaining < Short.BYTES) {
        return slowReadShort(remaining);
      } else {
        filePointer += Short.BYTES;
        return guard.getShort(postBuffer);
      }
    }

    private short slowReadShort(final int remaining) throws IOException {
      final byte b1 = _readByte(remaining);
      final byte b2 = _readByte(remaining - 1);
      return (short) (((b2 & 0xFF) << 8) | (b1 & 0xFF));
    }

    private short slowReadShort(final int remaining, final int pos) throws IOException {
      assert remaining == 1;
      final byte b1 = guard.getByte(postBuffer, pos);
      refill();
      final byte b2 = guard.getByte(postBuffer, postBufferBaseline);
      return (short) (((b2 & 0xFF) << 8) | (b1 & 0xFF));
    }

    @Override
    public int readInt() throws IOException {
      final long pos = seekPos;
      if (pos != -1) {
        seekPos = -1;
        actualSeek(pos);
      }
      final int remaining = postBuffer.remaining();
      if (remaining < Integer.BYTES) {
        return slowReadInt(remaining);
      } else {
        filePointer += Integer.BYTES;
        return guard.getInt(postBuffer);
      }
    }

    private int _readInt(final int remaining) throws IOException {
      if (remaining < Integer.BYTES) {
        return slowReadInt(remaining);
      } else {
        filePointer += Integer.BYTES;
        return guard.getInt(postBuffer);
      }
    }

    private int slowReadInt(int remaining) throws IOException {
      final byte b1 = _readByte(remaining);
      final byte b2 = _readByte(--remaining);
      final byte b3 = _readByte(--remaining);
      final byte b4 = _readByte(--remaining);
      return ((b4 & 0xFF) << 24) | ((b3 & 0xFF) << 16) | ((b2 & 0xFF) << 8) | (b1 & 0xFF);
    }

    private int _readInt(final int remaining, final int pos) throws IOException {
      if (remaining < Integer.BYTES) {
        return slowReadInt(remaining, pos);
      } else {
        return guard.getInt(postBuffer, pos);
      }
    }

    private int slowReadInt(int remaining, int pos) throws IOException {
      assert remaining > 0;
      final byte b1 = guard.getByte(postBuffer, pos++);
      if (--remaining == 0) {
        refill();
        pos = postBufferBaseline;
      }
      final byte b2 = guard.getByte(postBuffer, pos++);
      if (--remaining == 0) {
        refill();
        pos = postBufferBaseline;
      }
      final byte b3 = guard.getByte(postBuffer, pos++);
      if (--remaining == 0) {
        refill();
        pos = postBufferBaseline;
      }
      final byte b4 = guard.getByte(postBuffer, pos);
      return ((b4 & 0xFF) << 24) | ((b3 & 0xFF) << 16) | ((b2 & 0xFF) << 8) | (b1 & 0xFF);
    }

    @Override
    public long readLong() throws IOException {
      final long pos = seekPos;
      if (pos != -1) {
        seekPos = -1;
        actualSeek(pos);
      }
      final int remaining = postBuffer.remaining();
      if (remaining < Long.BYTES) {
        return (_readInt(remaining) & 0xFFFFFFFFL)
            | (((long) _readInt(postBuffer.remaining())) << 32);
      } else {
        filePointer += Long.BYTES;
        return guard.getLong(postBuffer);
      }
    }

    public long _readLong(final int remaining) throws IOException {
      if (remaining < Long.BYTES) {
        return (_readInt(remaining) & 0xFFFFFFFFL)
            | (((long) _readInt(postBuffer.remaining())) << 32);
      } else {
        filePointer += Long.BYTES;
        return guard.getLong(postBuffer);
      }
    }

    private long slowReadLong(final int remaining, final int pos) throws IOException {
      final long l1 = _readInt(remaining, pos);
      final long l2;
      if (remaining < Integer.BYTES) {
        // the first _readInt will have refilled the buffer, so we adjust here
        l2 = _readInt(Integer.BYTES, postBufferBaseline + (Integer.BYTES - remaining));
      } else if (remaining == Integer.BYTES) {
        // aligned, so we can refill directly
        refill();
        l2 = guard.getInt(postBuffer, postBufferBaseline);
      } else {
        // the first _readInt will _not_ have refilled the buffer, so proceed normally
        l2 = _readInt(remaining - Integer.BYTES, pos + Integer.BYTES);
      }
      return (l1 & 0xFFFFFFFFL) | (l2 << 32);
    }

    @Override
    public int readVInt() throws IOException {
      final long pos = seekPos;
      if (pos != -1) {
        seekPos = -1;
        actualSeek(pos);
      }
      return _readVInt(postBuffer.remaining());
    }

    public int _readVInt(int remaining) throws IOException {
      byte b;
      if (remaining <= Integer.BYTES) {
        b = _readByte(remaining);
        if (b >= 0) return b;
        int i = b & 0x7F;
        b = _readByte(--remaining);
        i |= (b & 0x7F) << 7;
        if (b >= 0) return i;
        b = _readByte(--remaining);
        i |= (b & 0x7F) << 14;
        if (b >= 0) return i;
        b = _readByte(--remaining);
        i |= (b & 0x7F) << 21;
        if (b >= 0) return i;
        b = _readByte(--remaining);
        // Warning: the next ands use 0x0F / 0xF0 - beware copy/paste errors:
        i |= (b & 0x0F) << 28;
        if ((b & 0xF0) == 0) return i;
      } else {
        b = guard.getByte(postBuffer);
        filePointer++;
        if (b >= 0) return b;
        int i = b & 0x7F;
        b = guard.getByte(postBuffer);
        filePointer++;
        i |= (b & 0x7F) << 7;
        if (b >= 0) return i;
        b = guard.getByte(postBuffer);
        filePointer++;
        i |= (b & 0x7F) << 14;
        if (b >= 0) return i;
        b = guard.getByte(postBuffer);
        filePointer++;
        i |= (b & 0x7F) << 21;
        if (b >= 0) return i;
        b = guard.getByte(postBuffer);
        filePointer++;
        // Warning: the next ands use 0x0F / 0xF0 - beware copy/paste errors:
        i |= (b & 0x0F) << 28;
        if ((b & 0xF0) == 0) return i;
      }
      throw new IOException("Invalid vInt detected (too many bits)");
    }

    @Override
    public long readVLong() throws IOException {
      return readVLong(false);
    }

    @Override
    public long readZLong() throws IOException {
      return BitUtil.zigZagDecode(readVLong(true));
    }

    private long readVLong(final boolean allowNegative) throws IOException {
      final long pos = seekPos;
      if (pos != -1) {
        seekPos = -1;
        actualSeek(pos);
      }
      int remaining = postBuffer.remaining();
      byte b;
      long i;
      if (remaining <= (allowNegative ? Long.BYTES + 1 : Long.BYTES)) {
        b = _readByte(remaining);
        if (b >= 0) return b;
        i = b & 0x7FL;
        b = _readByte(--remaining);
        i |= (b & 0x7FL) << 7;
        if (b >= 0) return i;
        b = _readByte(--remaining);
        i |= (b & 0x7FL) << 14;
        if (b >= 0) return i;
        b = _readByte(--remaining);
        i |= (b & 0x7FL) << 21;
        if (b >= 0) return i;
        b = _readByte(--remaining);
        i |= (b & 0x7FL) << 28;
        if (b >= 0) return i;
        b = _readByte(--remaining);
        i |= (b & 0x7FL) << 35;
        if (b >= 0) return i;
        b = _readByte(--remaining);
        i |= (b & 0x7FL) << 42;
        if (b >= 0) return i;
        b = _readByte(--remaining);
        i |= (b & 0x7FL) << 49;
        if (b >= 0) return i;
        b = _readByte(--remaining);
        i |= (b & 0x7FL) << 56;
        if (b >= 0) return i;
        if (!allowNegative) {
          throw new IOException("Invalid vLong detected (negative values disallowed)");
        }
        b = _readByte(--remaining);
      } else {
        b = guard.getByte(postBuffer);
        filePointer++;
        if (b >= 0) return b;
        i = b & 0x7FL;
        b = guard.getByte(postBuffer);
        filePointer++;
        i |= (b & 0x7FL) << 7;
        if (b >= 0) return i;
        b = guard.getByte(postBuffer);
        filePointer++;
        i |= (b & 0x7FL) << 14;
        if (b >= 0) return i;
        b = guard.getByte(postBuffer);
        filePointer++;
        i |= (b & 0x7FL) << 21;
        if (b >= 0) return i;
        b = guard.getByte(postBuffer);
        filePointer++;
        i |= (b & 0x7FL) << 28;
        if (b >= 0) return i;
        b = guard.getByte(postBuffer);
        filePointer++;
        i |= (b & 0x7FL) << 35;
        if (b >= 0) return i;
        b = guard.getByte(postBuffer);
        filePointer++;
        i |= (b & 0x7FL) << 42;
        if (b >= 0) return i;
        b = guard.getByte(postBuffer);
        filePointer++;
        i |= (b & 0x7FL) << 49;
        if (b >= 0) return i;
        b = guard.getByte(postBuffer);
        filePointer++;
        i |= (b & 0x7FL) << 56;
        if (b >= 0) return i;
        if (!allowNegative) {
          throw new IOException("Invalid vLong detected (negative values disallowed)");
        }
        b = guard.getByte(postBuffer);
        filePointer++;
      }
      i |= (b & 0x7FL) << 63;
      if (b == 0 || b == 1) return i;
      throw new IOException("Invalid vLong detected (more than 64 bits)");
    }

    @Override
    public void readLongs(final long[] dst, final int offset, final int length) throws IOException {
      final long pos = seekPos;
      if (pos != -1) {
        seekPos = -1;
        actualSeek(pos);
      }
      if (longViews == null) {
        longViews = initLongViews();
      }
      final int remaining = postBuffer.remaining();
      final long bytesRequested = (long) length << 3;
      if (remaining < bytesRequested) {
        dst[offset] = _readLong(remaining);
        for (int i = 1; i < length; ++i) {
          dst[offset + i] = _readLong(postBuffer.remaining());
        }
      } else {
        final int position = postBuffer.position();
        guard.getLongs(longViews[position & 0x07].position(position >>> 3), dst, offset, length);
        filePointer += bytesRequested;
        postBuffer.position(position + (int) bytesRequested);
      }
    }

    @Override
    public void readInts(final int[] dst, final int offset, final int length) throws IOException {
      final long pos = seekPos;
      if (pos != -1) {
        seekPos = -1;
        actualSeek(pos);
      }
      if (intViews == null) {
        intViews = initIntViews();
      }
      final int remaining = postBuffer.remaining();
      final long bytesRequested = (long) length << 2;
      if (remaining < bytesRequested) {
        dst[offset] = _readInt(remaining);
        for (int i = 1; i < length; ++i) {
          dst[offset + i] = _readInt(postBuffer.remaining());
        }
      } else {
        final int position = postBuffer.position();
        guard.getInts(intViews[position & 0x03].position(position >>> 2), dst, offset, length);
        filePointer += bytesRequested;
        postBuffer.position(position + (int) bytesRequested);
      }
    }

    @Override
    public void readFloats(final float[] dst, final int offset, final int length)
        throws IOException {
      final long pos = seekPos;
      if (pos != -1) {
        seekPos = -1;
        actualSeek(pos);
      }
      if (floatViews == null) {
        floatViews = initFloatViews();
      }
      final int remaining = postBuffer.remaining();
      final long bytesRequested = (long) length << 2;
      if (remaining < bytesRequested) {
        dst[offset] = Float.intBitsToFloat(_readInt(remaining));
        for (int i = 1; i < length; ++i) {
          dst[offset + i] = Float.intBitsToFloat(_readInt(postBuffer.remaining()));
        }
      } else {
        final int position = postBuffer.position();
        guard.getFloats(floatViews[position & 0x03].position(position >>> 2), dst, offset, length);
        filePointer += bytesRequested;
        postBuffer.position(position + (int) bytesRequested);
      }
    }

    private LongBuffer[] initLongViews() {
      if (postBuffer.isDirect()) {
        LongBuffer[] template =
            multiLongViews[currentBlockIdx >> COMPRESSION_TO_MAP_TRANSLATE_SHIFT];
        LongBuffer[] ret = new LongBuffer[Long.BYTES];
        for (int i = Long.BYTES - 1; i >= 0; i--) {
          ret[i] = template[i].duplicate();
        }
        return ret;
      } else {
        final LongBuffer[] ret = new LongBuffer[Long.BYTES];
        final ByteBuffer template = postBuffer.duplicate().order(ByteOrder.LITTLE_ENDIAN);
        final int lim = postBuffer.limit();
        for (int i = Long.BYTES - 1; i >= 0; i--) {
          if (i < lim) {
            ret[i] = template.position(i).asLongBuffer();
          } else {
            ret[i] = EMPTY_LONGBUFFER;
          }
        }
        return ret;
      }
    }

    private IntBuffer[] initIntViews() {
      if (postBuffer.isDirect()) {
        IntBuffer[] template = multiIntViews[currentBlockIdx >> COMPRESSION_TO_MAP_TRANSLATE_SHIFT];
        IntBuffer[] ret = new IntBuffer[Integer.BYTES];
        for (int i = Integer.BYTES - 1; i >= 0; i--) {
          ret[i] = template[i].duplicate();
        }
        return ret;
      } else {
        final IntBuffer[] ret = new IntBuffer[Integer.BYTES];
        final ByteBuffer template = postBuffer.duplicate().order(ByteOrder.LITTLE_ENDIAN);
        final int lim = postBuffer.limit();
        for (int i = Integer.BYTES - 1; i >= 0; i--) {
          if (i < lim) {
            ret[i] = template.position(i).asIntBuffer();
          } else {
            ret[i] = EMPTY_INTBUFFER;
          }
        }
        return ret;
      }
    }

    private FloatBuffer[] initFloatViews() {
      if (postBuffer.isDirect()) {
        FloatBuffer[] template =
            multiFloatViews[currentBlockIdx >> COMPRESSION_TO_MAP_TRANSLATE_SHIFT];
        FloatBuffer[] ret = new FloatBuffer[Float.BYTES];
        for (int i = Float.BYTES - 1; i >= 0; i--) {
          ret[i] = template[i].duplicate();
        }
        return ret;
      } else {
        final FloatBuffer[] ret = new FloatBuffer[Float.BYTES];
        final ByteBuffer template = postBuffer.duplicate().order(ByteOrder.LITTLE_ENDIAN);
        final int lim = postBuffer.limit();
        for (int i = Integer.BYTES - 1; i >= 0; i--) {
          if (i < lim) {
            ret[i] = template.position(i).asFloatBuffer();
          } else {
            ret[i] = EMPTY_FLOATBUFFER;
          }
        }
        return ret;
      }
    }

    @Override
    public String readString() throws IOException {
      final long pos = seekPos;
      if (pos != -1) {
        seekPos = -1;
        actualSeek(pos);
      }
      return _readString();
    }

    public String _readString() throws IOException {
      final int length = _readVInt(postBuffer.remaining());
      final byte[] bytes = new byte[length];
      final int left = postBuffer.remaining();
      filePointer += length;
      if (left < length) {
        slowReadBytes(bytes, 0, length, left);
      } else {
        guard.getBytes(postBuffer, bytes, 0, length);
      }
      return new String(bytes, 0, length, StandardCharsets.UTF_8);
    }

    @Override
    public Map<String, String> readMapOfStrings() throws IOException {
      final long pos = seekPos;
      if (pos != -1) {
        seekPos = -1;
        actualSeek(pos);
      }
      final int count = _readVInt(postBuffer.remaining());
      switch (count) {
        case 0:
          return Collections.emptyMap();
        case 1:
          return Collections.singletonMap(_readString(), _readString());
        default:
          final Map<String, String> map =
              count > 10 ? CollectionUtil.newHashMap(count) : new TreeMap<>();
          for (int i = count; i > 0; i--) {
            map.put(_readString(), _readString());
          }
          return Collections.unmodifiableMap(map);
      }
    }

    @Override
    public Set<String> readSetOfStrings() throws IOException {
      final long pos = seekPos;
      if (pos != -1) {
        seekPos = -1;
        actualSeek(pos);
      }
      final int count = _readVInt(postBuffer.remaining());
      switch (count) {
        case 0:
          return Collections.emptySet();
        case 1:
          return Collections.singleton(_readString());
        default:
          final Set<String> set = count > 10 ? CollectionUtil.newHashSet(count) : new TreeSet<>();
          for (int i = count; i > 0; i--) {
            set.add(_readString());
          }
          return Collections.unmodifiableSet(set);
      }
    }

    @Override
    @SuppressWarnings("try")
    public void close() throws IOException {
      try {
        if (mapped == null) return;

        // make local copy, then un-set early
        final ByteBuffer[] bufs = mapped;
        final ByteBuffer[] accessBufs = accessMapped;
        unsetBuffers();

        if (isClone) return;

        synchronized (closed) {
          // use this to reliably signal background loading threads that they should not
          // attempt to load any more blocks. The only lock contention here is (potentially)
          // with single-threaded background loading
          closed[0] = true;
        }

        // tell the guard to invalidate and later unmap the bytebuffers (if supported):
        try (priorityLoad;
            IndexInput fullyLoaded = loaded.get()) {
          compressedGuard.invalidateAndUnmap(bufs);
        } finally {
          guard.invalidateAndUnmap(accessBufs);
        }
      } finally {
        unsetBuffers();
      }
    }

    private void unsetBuffers() {
      accessMapped = null;
      mapped = null;
      currentBlockIdx = -1;
      postBuffer = null;
      floatViews = null;
      intViews = null;
      longViews = null;
    }

    @Override
    public long getFilePointer() {
      return (seekPos == -1 ? filePointer : seekPos) - offset;
    }

    @Override
    public void seek(final long pos) throws IOException {
      seekPos = offset + pos; // defer the actual seek
    }

    @Override
    public long length() {
      return sliceLength;
    }

    @Override
    public IndexInput clone() {
      IndexInput ret;
      try {
        IndexInput fullyLoaded = loaded.get();
        if (fullyLoaded != null) {
          loadedCt.increment();
          ret = fullyLoaded.slice("clone", offset, sliceLength);
        } else if (populated(offset, sliceLength)) {
          populatedCt.increment();
          ret = accessPopulated.slice("clone", offset, sliceLength);
        } else {
          lazyCt.increment();
          ret = new LazyLoadInput("clone", this, 0, sliceLength);
        }
        ret.seek(getFilePointer());
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
      return ret;
    }

    @Override
    public IndexInput slice(String sliceDescription, long offset, long length) throws IOException {
      long absoluteOffset = this.offset + offset;
      IndexInput fullyLoaded = loaded.get();
      if (fullyLoaded != null) {
        loadedCt.increment();
        return fullyLoaded.slice("slice", absoluteOffset, length);
      } else if (populated(absoluteOffset, length)) {
        populatedCt.increment();
        return accessPopulated.slice("slice", absoluteOffset, length);
      } else {
        lazyCt.increment();
        return new LazyLoadInput("slice", this, offset, length);
      }
    }

    @Override
    public void blockUntilLoaded() throws InterruptedException {
      loadedLatch.await();
    }

    /**
     * This must only be called on the initial/canonical lazy input, used to fsync/force output to
     * disk ({@link org.apache.lucene.store.Directory#sync(Collection)} does not work across
     * FileChannel/MappedByteBuffer).
     */
    public void force() {
      MappedByteBuffer[] toForce = (MappedByteBuffer[]) accessMapped;
      if (toForce == null) {
        throw new AlreadyClosedException("already closed");
      }
      synchronized (closed) {
        if (closed[0]) {
          throw new AlreadyClosedException("already closed");
        }
        for (MappedByteBuffer bb : toForce) {
          bb.force();
        }
      }
    }
  }
}
