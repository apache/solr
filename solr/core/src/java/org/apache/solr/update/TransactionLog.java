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
package org.apache.solr.update;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.invoke.MethodHandles;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.lucene.util.BytesRef;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.util.CollectionUtil;
import org.apache.solr.common.util.DataInputInputStream;
import org.apache.solr.common.util.FastInputStream;
import org.apache.solr.common.util.FastOutputStream;
import org.apache.solr.common.util.JavaBinCodec;
import org.apache.solr.common.util.ObjectReleaseTracker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Log Format: List{Operation, Version, ...} ADD, VERSION, DOC DELETE, VERSION, ID_BYTES
 * DELETE_BY_QUERY, VERSION, String
 *
 * <p>TODO: keep two files, one for [operation, version, id] and the other for the actual document
 * data. That way we could throw away document log files more readily while retaining the smaller
 * operation log files longer (and we can retrieve the stored fields from the latest documents from
 * the index).
 *
 * <p>This would require keeping all source fields stored of course.
 *
 * <p>This would also allow to not log document data for requests with commit=true in them (since we
 * know that if the request succeeds, all docs will be committed)
 */
public class TransactionLog implements Closeable {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private static final boolean debug = log.isDebugEnabled();
  private static final boolean trace = log.isTraceEnabled();

  public static final String END_MESSAGE = "SOLR_TLOG_END";

  long id;
  protected Path tlog;
  protected FileChannel channel;
  protected OutputStream os;
  // all accesses to this stream should be synchronized on "this" (The TransactionLog)
  protected FastOutputStream fos;
  protected ChannelInputStreamOpener channelInputStreamOpener;
  int numRecords;
  public boolean isBuffer;

  // we can delete old tlogs since they are currently only used for real-time-get (and in the
  // future, recovery)
  protected volatile boolean deleteOnClose = true;

  protected AtomicInteger refcount = new AtomicInteger(1);
  protected Map<String, Integer> globalStringMap = new HashMap<>();
  protected List<String> globalStringList = new ArrayList<>();

  // write a BytesRef as a byte array
  protected static final JavaBinCodec.ObjectResolver resolver =
      new JavaBinCodec.ObjectResolver() {
        @Override
        public Object resolve(Object o, JavaBinCodec codec) throws IOException {
          if (o instanceof BytesRef) {
            BytesRef br = (BytesRef) o;
            codec.writeByteArray(br.bytes, br.offset, br.length);
            return null;
          }
          // Fallback: we have no idea how to serialize this.  Be noisy to prevent insidious bugs
          throw new SolrException(
              SolrException.ErrorCode.SERVER_ERROR,
              "TransactionLog doesn't know how to serialize "
                  + o.getClass()
                  + "; try implementing ObjectResolver?");
        }
      };

  protected static final OutputStreamOpener OUTPUT_STREAM_OPENER =
      (channel, position) -> Channels.newOutputStream(channel);

  protected static final ChannelInputStreamOpener CHANNEL_INPUT_STREAM_OPENER =
      ChannelFastInputStream::new;

  public class LogCodec extends JavaBinCodec {

    public LogCodec(JavaBinCodec.ObjectResolver resolver) {
      super(resolver);
    }

    @Override
    public void writeExternString(CharSequence s) throws IOException {
      if (s == null) {
        writeTag(NULL);
        return;
      }

      // no need to synchronize globalStringMap - it's only updated before the first record is
      // written to the log
      Integer idx = globalStringMap.get(s.toString());
      if (idx == null) {
        // write a normal string
        writeStr(s);
      } else {
        // write the extern string
        writeTag(EXTERN_STRING, idx);
      }
    }

    @Override
    public CharSequence readExternString(DataInputInputStream fis) throws IOException {
      int idx = readSize(fis);
      if (idx != 0) { // idx != 0 is the index of the extern string
        // no need to synchronize globalStringList - it's only updated before the first record is
        // written to the log
        return globalStringList.get(idx - 1);
      } else { // idx == 0 means it has a string value
        // this shouldn't happen with this codec subclass.
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Corrupt transaction log");
      }
    }

    @Override
    protected Object readObject(DataInputInputStream dis) throws IOException {
      if (UUID == tagByte) {
        return new java.util.UUID(dis.readLong(), dis.readLong());
      }
      return super.readObject(dis);
    }

    @Override
    public boolean writePrimitive(Object val) throws IOException {
      if (val instanceof java.util.UUID) {
        java.util.UUID uuid = (java.util.UUID) val;
        daos.writeByte(UUID);
        daos.writeLong(uuid.getMostSignificantBits());
        daos.writeLong(uuid.getLeastSignificantBits());
        return true;
      }
      return super.writePrimitive(val);
    }
  }

  TransactionLog(Path tlogFile, Collection<String> globalStrings) {
    this(tlogFile, globalStrings, false);
  }

  TransactionLog(Path tlogFile, Collection<String> globalStrings, boolean openExisting) {
    this(tlogFile, globalStrings, openExisting, OUTPUT_STREAM_OPENER, CHANNEL_INPUT_STREAM_OPENER);
  }

  protected TransactionLog(
      Path tlogFile,
      Collection<String> globalStrings,
      boolean openExisting,
      OutputStreamOpener outputStreamOpener,
      ChannelInputStreamOpener channelInputStreamOpener) {
    boolean success = false;
    try {
      this.tlog = tlogFile;
      this.channelInputStreamOpener = channelInputStreamOpener;

      if (debug) {
        log.debug(
            "New TransactionLog file={}, exists={}, size={} openExisting={}",
            tlogFile,
            Files.exists(tlogFile),
            getLogSize(),
            openExisting);
      }

      // Parse tlog id from the filename
      String filename = tlog.getFileName().toString();
      id = Long.parseLong(filename.substring(filename.lastIndexOf('.') + 1));

      if (openExisting) {
        assert Files.exists(tlog) : tlog + " did not exist";

        long start = Files.size(tlog);
        channel = FileChannel.open(tlog, StandardOpenOption.READ, StandardOpenOption.WRITE);
        if (start > 0) {
          readHeader(null);
        }
        os = outputStreamOpener.open(channel, start);
        fos = new FastOutputStream(os, new byte[65536], 0);
        if (start > 0) {
          channel.position(start);
          setWrittenCount(start);
        } else {
          addGlobalStrings(globalStrings);
        }
      } else {
        if (Files.exists(tlog)) {
          log.warn("New transaction log already exists:{} size={}", tlog, Files.size(tlog));
          return;
        }

        channel =
            FileChannel.open(
                tlog,
                StandardOpenOption.READ,
                StandardOpenOption.WRITE,
                StandardOpenOption.CREATE_NEW);
        os = outputStreamOpener.open(channel, 0);
        fos = new FastOutputStream(os, new byte[65536], 0);

        addGlobalStrings(globalStrings);
      }

      success = true;

      assert ObjectReleaseTracker.track(this);

    } catch (IOException e) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e);
    } finally {
      if (!success && channel != null) {
        try {
          channel.close();
        } catch (Exception e) {
          log.error("Error closing tlog file (after error opening)", e);
        }
      }
    }
  }

  // for subclasses
  protected TransactionLog() {}

  /**
   * Sets the counter of written data in the {@link FastOutputStream} view of the log file, to
   * reflect that we aren't starting at the beginning.
   */
  protected void setWrittenCount(long fileStartOffset) throws IOException {
    fos.setWritten(fileStartOffset);
    assert fos.size() == getLogFileSize();
  }

  /** Gets the log file data size. */
  protected long getLogFileSize() throws IOException {
    return channel.size();
  }

  /**
   * Returns the number of records in the log (currently includes the header and an optional
   * commit). Note: currently returns 0 for reopened existing log files.
   */
  public int numRecords() {
    synchronized (this) {
      return this.numRecords;
    }
  }

  public boolean endsWithCommit() throws IOException {
    long size;
    synchronized (this) {
      fos.flush();
      size = fos.size();
    }

    // the end of the file should have the end message (added during a commit) plus a 4 byte size
    byte[] buf = new byte[END_MESSAGE.length()];
    long pos = size - END_MESSAGE.length() - 4;
    if (pos < 0) return false;
    @SuppressWarnings("resource")
    final InputStream is = channelInputStreamOpener.open(channel, pos);
    int n = is.read(buf);
    if (n != buf.length) {
      return false;
    }
    for (int i = 0; i < buf.length; i++) {
      if (buf[i] != END_MESSAGE.charAt(i)) return false;
    }
    return true;
  }

  public long writeData(Object o) {
    @SuppressWarnings("resource")
    final LogCodec codec = new LogCodec(resolver);
    try {
      long pos = fos.size(); // if we had flushed, this should be equal to channel.position()
      codec.init(fos);
      codec.writeVal(o);
      return pos;
    } catch (IOException e) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e);
    }
  }

  @SuppressWarnings({"unchecked"})
  private void readHeader(DataInputInputStream is) throws IOException {
    // read existing header
    is = is != null ? is : channelInputStreamOpener.open(channel, 0);
    @SuppressWarnings("resource")
    final LogCodec codec = new LogCodec(resolver);
    Map<?, ?> header = (Map<?, ?>) codec.unmarshal(is);

    is.readInt(); // skip size

    // needed to read other records

    synchronized (this) {
      globalStringList = (List<String>) header.get("strings");
      globalStringMap = CollectionUtil.newHashMap(globalStringList.size());
      for (int i = 0; i < globalStringList.size(); i++) {
        globalStringMap.put(globalStringList.get(i), i + 1);
      }
    }
  }

  protected void addGlobalStrings(Collection<String> strings) {
    if (strings == null) return;
    int origSize = globalStringMap.size();
    for (String s : strings) {
      Integer idx = null;
      if (origSize > 0) {
        idx = globalStringMap.get(s);
      }
      if (idx != null) continue; // already in list
      globalStringList.add(s);
      globalStringMap.put(s, globalStringList.size());
    }
    assert globalStringMap.size() == globalStringList.size();
  }

  Collection<String> getGlobalStrings() {
    synchronized (this) {
      return new ArrayList<>(globalStringList);
    }
  }

  protected void writeLogHeader(LogCodec codec) throws IOException {
    long pos = fos.size();
    assert pos == 0;

    Map<String, Object> header = new LinkedHashMap<>();
    header.put("SOLR_TLOG", 1); // a magic string + version number
    header.put("strings", globalStringList);
    codec.marshal(header, fos);

    endRecord(pos);
  }

  protected void endRecord(long startRecordPosition) throws IOException {
    fos.writeInt((int) (fos.size() - startRecordPosition));
    numRecords++;
  }

  protected void checkWriteHeader(LogCodec codec, SolrInputDocument optional) throws IOException {

    // Unsynchronized access. We can get away with an unsynchronized access here
    // since we will never get a false non-zero when the position is in fact 0.
    // rollback() is the only function that can reset to zero, and it blocks updates.
    if (fos.size() != 0) return;

    synchronized (this) {
      if (fos.size() != 0) return; // check again while synchronized
      if (optional != null) {
        addGlobalStrings(optional.getFieldNames());
      }
      writeLogHeader(codec);
    }
  }

  int lastAddSize;

  /**
   * Writes an add update command to the transaction log. This is not applicable for in-place
   * updates; use {@link #write(AddUpdateCommand, long)}. (The previous pointer (applicable for
   * in-place updates) is set to -1 while writing the command to the transaction log.)
   *
   * @param cmd The add update command to be written
   * @return Returns the position pointer of the written update command
   * @see #write(AddUpdateCommand, long)
   */
  public long write(AddUpdateCommand cmd) {
    return write(cmd, -1);
  }

  /**
   * Writes an add update command to the transaction log. This should be called only for writing
   * in-place updates, or else pass -1 as the prevPointer.
   *
   * @param cmd The add update command to be written
   * @param prevPointer The pointer in the transaction log which this update depends on (applicable
   *     for in-place updates)
   * @return Returns the position pointer of the written update command
   */
  public long write(AddUpdateCommand cmd, long prevPointer) {
    assert (-1 <= prevPointer && (cmd.isInPlaceUpdate() || (-1 == prevPointer)));

    LogCodec codec = new LogCodec(resolver);
    SolrInputDocument sdoc = cmd.getSolrInputDocument();

    try {
      checkWriteHeader(codec, sdoc);

      // adaptive buffer sizing
      int bufSize = lastAddSize; // unsynchronized access of lastAddSize should be fine
      // at least 256 bytes and at most 1 MB
      bufSize = Math.min(1024 * 1024, Math.max(256, bufSize + (bufSize >> 3) + 256));

      MemOutputStream out = new MemOutputStream(new byte[bufSize]);
      codec.init(out);
      if (cmd.isInPlaceUpdate()) {
        codec.writeTag(JavaBinCodec.ARR, 5);
        codec.writeInt(UpdateLog.UPDATE_INPLACE); // should just take one byte
        codec.writeLong(cmd.getVersion());
        codec.writeLong(prevPointer);
        codec.writeLong(cmd.prevVersion);
        codec.writeSolrInputDocument(cmd.getSolrInputDocument());
      } else {
        codec.writeTag(JavaBinCodec.ARR, 3);
        codec.writeInt(UpdateLog.ADD); // should just take one byte
        codec.writeLong(cmd.getVersion());
        codec.writeSolrInputDocument(cmd.getSolrInputDocument());
      }
      lastAddSize = (int) out.size();

      synchronized (this) {
        long pos = fos.size(); // if we had flushed, this should be equal to channel.position()
        assert pos != 0;

        /*
        System.out.println("###writing at " + pos + " fos.size()=" + fos.size() + " raf.length()=" + raf.length());
        if (pos != fos.size()) {
          throw new RuntimeException("ERROR" + "###writing at " + pos + " fos.size()=" + fos.size() + " raf.length()=" + raf.length());
        }
        */

        out.writeAll(fos);
        endRecord(pos);
        // fos.flushBuffer();  // flush later
        return pos;
      }

    } catch (IOException e) {
      // TODO: reset our file pointer back to "pos", the start of this record.
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Error logging add", e);
    }
  }

  public long writeDelete(DeleteUpdateCommand cmd) {
    LogCodec codec = new LogCodec(resolver);

    try {
      checkWriteHeader(codec, null);

      BytesRef br = cmd.getIndexedId();

      MemOutputStream out = new MemOutputStream(new byte[20 + br.length]);
      codec.init(out);
      codec.writeTag(JavaBinCodec.ARR, 3);
      codec.writeInt(UpdateLog.DELETE); // should just take one byte
      codec.writeLong(cmd.getVersion());
      codec.writeByteArray(br.bytes, br.offset, br.length);

      synchronized (this) {
        long pos = fos.size(); // if we had flushed, this should be equal to channel.position()
        assert pos != 0;
        out.writeAll(fos);
        endRecord(pos);
        // fos.flushBuffer();  // flush later
        return pos;
      }

    } catch (IOException e) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e);
    }
  }

  public long writeDeleteByQuery(DeleteUpdateCommand cmd) {
    LogCodec codec = new LogCodec(resolver);
    try {
      checkWriteHeader(codec, null);

      MemOutputStream out = new MemOutputStream(new byte[20 + (cmd.query.length())]);
      codec.init(out);
      codec.writeTag(JavaBinCodec.ARR, 3);
      codec.writeInt(UpdateLog.DELETE_BY_QUERY); // should just take one byte
      codec.writeLong(cmd.getVersion());
      codec.writeStr(cmd.query);

      synchronized (this) {
        long pos = fos.size(); // if we had flushed, this should be equal to channel.position()
        out.writeAll(fos);
        endRecord(pos);
        // fos.flushBuffer();  // flush later
        return pos;
      }
    } catch (IOException e) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e);
    }
  }

  public long writeCommit(CommitUpdateCommand cmd) {
    LogCodec codec = new LogCodec(resolver);
    synchronized (this) {
      try {
        long pos = fos.size(); // if we had flushed, this should be equal to channel.position()

        if (pos == 0) {
          writeLogHeader(codec);
          pos = fos.size();
        }
        codec.init(fos);
        codec.writeTag(JavaBinCodec.ARR, 3);
        codec.writeInt(UpdateLog.COMMIT); // should just take one byte
        codec.writeLong(cmd.getVersion());
        codec.writeStr(END_MESSAGE); // ensure these bytes are (almost) last in the file

        endRecord(pos);

        fos.flush(); // flush since this will be the last record in a log fill
        assert fos.size() == getLogFileSize();

        return pos;
      } catch (IOException e) {
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e);
      }
    }
  }

  /* This method is thread safe */

  public Object lookup(long pos) {
    // A negative position can result from a log replay (which does not re-log, but does
    // update the version map.  This is OK since the node won't be ACTIVE when this happens.
    if (pos < 0) return null;

    try {
      // make sure any unflushed buffer has been flushed
      synchronized (this) {
        // TODO: optimize this by keeping track of what we have flushed up to
        fos.flush();
        /*
        System.out.println("###flush to " + fos.size() + " raf.length()=" + raf.length() + " pos="+pos);
        if (fos.size() != raf.length() || pos >= fos.size() ) {
          throw new RuntimeException("ERROR" + "###flush to " + fos.size() + " raf.length()=" + raf.length() + " pos="+pos);
        }
        */
      }

      DataInputInputStream is = channelInputStreamOpener.open(channel, pos);
      try (LogCodec codec = new LogCodec(resolver)) {
        return codec.readVal(is);
      }
    } catch (IOException e) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e);
    }
  }

  public void incref() {
    int result = refcount.incrementAndGet();
    if (result <= 1) {
      throw new SolrException(
          SolrException.ErrorCode.SERVER_ERROR, "incref on a closed log: " + this);
    }
  }

  public boolean try_incref() {
    return refcount.incrementAndGet() > 1;
  }

  public void decref() {
    if (refcount.decrementAndGet() == 0) {
      close();
    }
  }

  /** returns the current position in the log file */
  public long position() {
    synchronized (this) {
      return fos.size();
    }
  }

  /**
   * Move to a read-only state, closing and releasing resources while keeping the log available for
   * reads
   */
  public void closeOutput() {}

  public void finish(UpdateLog.SyncLevel syncLevel) {
    if (syncLevel == UpdateLog.SyncLevel.NONE) return;
    try {
      synchronized (this) {
        fos.flush();
      }

      if (syncLevel == UpdateLog.SyncLevel.FSYNC) {
        // Since fsync is outside of synchronized block, we can end up with a partial
        // last record on power failure (which is OK, and does not represent an error...
        // we just need to be aware of it when reading).
        channel.force(true);
      }

    } catch (IOException e) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e);
    }
  }

  @Override
  public void close() {
    try {
      if (debug) {
        log.debug("Closing tlog {}", this);
      }

      synchronized (this) {
        fos.flush();
        fos.close();
      }

      if (deleteOnClose) {
        try {
          Files.deleteIfExists(tlog);
        } catch (IOException e) {
          // TODO: should this class care if a file couldnt be deleted?
          // this just emulates previous behavior, where only SecurityException would be handled.
        }
      }
    } catch (IOException e) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e);
    } finally {
      assert ObjectReleaseTracker.release(this);
    }
  }

  public void forceClose() {
    if (refcount.get() > 0) {
      log.error("Error: Forcing close of {}", this);
      refcount.set(0);
      close();
    }
  }

  @Override
  public String toString() {
    return "tlog{file=" + tlog + " refcount=" + refcount.get() + "}";
  }

  public long getLogSize() {
    if (tlog != null) {
      try {
        return Files.size(tlog);
      } catch (IOException e) {
        log.warn("Could not read tlog length file={}", tlog);
      }
    }
    return 0;
  }

  /**
   * @return the FastOutputStream size
   */
  public synchronized long getLogSizeFromStream() {
    return fos.size();
  }

  /**
   * Returns a reader that can be used while a log is still in use. Currently only *one* LogReader
   * may be outstanding, and that log may only be used from a single thread.
   */
  public LogReader getReader(long startingPos) throws IOException {
    return new LogReader(startingPos);
  }

  public LogReader getSortedReader(long startingPos) throws IOException {
    return new SortedLogReader(startingPos);
  }

  /** Returns a single threaded reverse reader */
  public ReverseReader getReverseReader() throws IOException {
    return new FSReverseReader();
  }

  public class LogReader {
    protected ChannelFastInputStream fis;
    private LogCodec codec = new LogCodec(resolver);

    public LogReader(long startingPos) throws IOException {
      incref();
      fis = channelInputStreamOpener.open(channel, startingPos);
    }

    // for classes that extend
    protected LogReader() {}

    /**
     * Returns the next object from the log, or null if none available.
     *
     * @return The log record, or null if EOF
     * @throws IOException If there is a low-level I/O error.
     */
    public Object next() throws IOException, InterruptedException {
      long pos = fis.position();

      synchronized (TransactionLog.this) {
        if (trace) {
          log.trace("Reading log record.  pos={} currentSize={}", pos, fos.size());
        }

        if (pos >= fos.size()) {
          return null;
        }

        fos.flush();
      }

      if (pos == 0) {
        readHeader(fis);

        // shouldn't currently happen - header and first record are currently written at the same
        // time
        synchronized (TransactionLog.this) {
          if (fis.position() >= fos.size()) {
            return null;
          }
          pos = fis.position();
        }
      }

      Object o = codec.readVal(fis);

      // skip over record size
      int size = fis.readInt();
      assert size == fis.position() - pos - 4;

      return o;
    }

    public void close() {
      decref();
    }

    @Override
    public String toString() {
      synchronized (TransactionLog.this) {
        return "LogReader{"
            + "file="
            + tlog
            + ", position="
            + fis.position()
            + ", end="
            + fos.size()
            + "}";
      }
    }

    // returns best effort current position
    // for info purposes
    public long currentPos() {
      return fis.position();
    }

    // returns best effort current size
    // for info purposes
    public long currentSize() throws IOException {
      return getLogFileSize();
    }
  }

  public class SortedLogReader extends LogReader {
    private long startingPos;
    private boolean inOrder = true;
    private TreeMap<Long, Long> versionToPos;
    Iterator<Long> iterator;

    public SortedLogReader(long startingPos) throws IOException {
      super(startingPos);
      this.startingPos = startingPos;
    }

    @Override
    public Object next() throws IOException, InterruptedException {
      if (versionToPos == null) {
        versionToPos = new TreeMap<>();
        Object o;
        long pos = startingPos;

        long lastVersion = Long.MIN_VALUE;
        while ((o = super.next()) != null) {
          List<?> entry = (List<?>) o;
          long version = (Long) entry.get(UpdateLog.VERSION_IDX);
          version = Math.abs(version);
          versionToPos.put(version, pos);
          pos = currentPos();

          if (version < lastVersion) inOrder = false;
          lastVersion = version;
        }
        fis.seek(startingPos);
      }

      if (inOrder) {
        return super.next();
      } else {
        if (iterator == null) iterator = versionToPos.values().iterator();
        if (!iterator.hasNext()) return null;
        long pos = iterator.next();
        if (pos != currentPos()) fis.seek(pos);
        return super.next();
      }
    }
  }

  public abstract static class ReverseReader {

    /**
     * Returns the next object from the log, or null if none available.
     *
     * @return The log record, or null if EOF
     * @throws IOException If there is a low-level I/O error.
     */
    public abstract Object next() throws IOException;

    /* returns the position in the log file of the last record returned by next() */
    public abstract long position();

    public abstract void close();

    @Override
    public abstract String toString();
  }

  public class FSReverseReader extends ReverseReader {
    ChannelFastInputStream fis;
    private LogCodec codec =
        new LogCodec(resolver) {
          @Override
          public SolrInputDocument readSolrInputDocument(DataInputInputStream dis) {
            // Given that the SolrInputDocument is last in an add record, it's OK to just skip
            // reading it completely.
            return null;
          }
        };

    // length of the next record (the next one closer to the start of the log file)
    int nextLength;
    // where we started reading from last time (so prevPos - nextLength == start of next record)
    long prevPos;

    public FSReverseReader() throws IOException {
      incref();

      long sz;
      synchronized (TransactionLog.this) {
        fos.flush();
        sz = fos.size();
        assert sz == getLogFileSize();
      }

      fis = channelInputStreamOpener.open(channel, 0);
      if (sz >= 4) {
        // readHeader(fis);  // should not be needed
        prevPos = sz - 4;
        fis.seek(prevPos);
        nextLength = fis.readInt();
      }
    }

    /**
     * Returns the next object from the log, or null if none available.
     *
     * @return The log record, or null if EOF
     * @throws IOException If there is a low-level I/O error.
     */
    @Override
    public Object next() throws IOException {
      if (prevPos <= 0) return null;

      long endOfThisRecord = prevPos;

      int thisLength = nextLength;

      long recordStart = prevPos - thisLength; // back up to the beginning of the next record
      prevPos = recordStart - 4; // back up 4 more to read the length of the next record

      if (prevPos <= 0) return null; // this record is the header

      long bufferPos = fis.getBufferPos();
      if (prevPos >= bufferPos) {
        // nothing to do... we're within the current buffer
      } else {
        // Position buffer so that this record is at the end.
        // For small records, this will cause subsequent calls to next() to be within the buffer.
        long seekPos = endOfThisRecord - fis.getBufferSize();
        // seek to the start of the record if it's larger then the block size.
        seekPos = Math.min(seekPos, prevPos);
        seekPos = Math.max(seekPos, 0);
        fis.seek(seekPos);
        fis.peek(); // cause buffer to be filled
      }

      fis.seek(prevPos);
      // this is the length of the *next* record (i.e. closer to the beginning)
      nextLength = fis.readInt();

      // TODO: optionally skip document data
      Object o = codec.readVal(fis);

      // this is only true if we read all the data (and we currently skip reading SolrInputDocument)
      // assert fis.position() == prevPos + 4 + thisLength;

      return o;
    }

    /* returns the position in the log file of the last record returned by next() */
    @Override
    public long position() {
      return prevPos + 4; // skip the length
    }

    @Override
    public void close() {
      decref();
    }

    @Override
    public String toString() {
      synchronized (TransactionLog.this) {
        return "LogReader{"
            + "file="
            + tlog
            + ", position="
            + fis.position()
            + ", end="
            + fos.size()
            + "}";
      }
    }
  }

  public static class ChannelFastInputStream extends FastInputStream {
    protected FileChannel ch;

    public ChannelFastInputStream(FileChannel ch, long chPosition) {
      // super(null, new byte[10],0,0);    // a small buffer size for testing purposes
      super(null);
      this.ch = ch;
      super.readFromStream = chPosition;
    }

    @Override
    public int readWrappedStream(byte[] target, int offset, int len) throws IOException {
      ByteBuffer bb = ByteBuffer.wrap(target, offset, len);
      int ret = ch.read(bb, readFromStream);
      return ret;
    }

    public void seek(long position) throws IOException {
      if (position <= readFromStream && position >= getBufferPos()) {
        // seek within buffer
        pos = (int) (position - getBufferPos());
      } else {
        // long currSize = ch.size();   // not needed - underlying read should handle (unless read
        // never done)
        // if (position > currSize) throw new EOFException("Read past EOF: seeking to " + position +
        // " on file of size " + currSize + " file=" + ch);
        readFromStream = position;
        end = pos = 0;
      }
      assert position() == position;
    }

    /** where is the start of the buffer relative to the whole file */
    public long getBufferPos() {
      return readFromStream - end;
    }

    public int getBufferSize() {
      return buf.length;
    }

    @Override
    public void close() throws IOException {
      ch.close();
    }

    @Override
    public String toString() {
      return "readFromStream="
          + readFromStream
          + " pos="
          + pos
          + " end="
          + end
          + " bufferPos="
          + getBufferPos()
          + " position="
          + position();
    }
  }

  /** Opens {@link OutputStream} from {@link FileChannel}. */
  protected interface OutputStreamOpener {

    /**
     * Opens an {@link OutputStream} to write in a {@link FileChannel}.
     *
     * @param position The initial write position of the {@link OutputStream} view of the {@link
     *     FileChannel}.
     */
    OutputStream open(FileChannel channel, long position) throws IOException;
  }

  /** Opens {@link ChannelFastInputStream} from {@link FileChannel}. */
  protected interface ChannelInputStreamOpener {

    /**
     * Opens a {@link ChannelFastInputStream} to read a {@link FileChannel}.
     *
     * @param position The initial read position of the {@link OutputStream} view of the {@link
     *     FileChannel}.
     */
    ChannelFastInputStream open(FileChannel channel, long position) throws IOException;
  }
}
