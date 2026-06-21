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

import it.unimi.dsi.fastutil.longs.Long2LongAVLTreeMap;
import it.unimi.dsi.fastutil.objects.Object2IntMap;
import it.unimi.dsi.fastutil.objects.Object2IntMaps;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;
import it.unimi.dsi.fastutil.objects.Object2ObjectLinkedOpenHashMap;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import it.unimi.dsi.fastutil.objects.ObjectLists;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.MappedResizeableBuffer;
import org.apache.lucene.util.BytesRef;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.util.*;
import org.eclipse.jetty.io.RuntimeIOException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.lang.invoke.MethodHandles;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 *  Log Format: List{Operation, Version, ...}
 *  ADD, VERSION, DOC
 *  DELETE, VERSION, ID_BYTES
 *  DELETE_BY_QUERY, VERSION, String
 *
 *  TODO: keep two files, one for [operation, version, id] and the other for the actual
 *  document data.  That way we could throw away document log files more readily
 *  while retaining the smaller operation log files longer (and we can retrieve
 *  the stored fields from the latest documents from the index).
 *
 *  This would require keeping all source fields stored of course.
 *
 *  This would also allow to not log document data for requests with commit=true
 *  in them (since we know that if the request succeeds, all docs will be committed)
 *
 */
public class TransactionLog implements Closeable {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private final boolean debug = log.isDebugEnabled();
  private final boolean trace = log.isTraceEnabled();

  public final static String END_MESSAGE = "SOLR_TLOG_END";
  private MappedResizeableBuffer buffer;

  long id;
  volatile File tlogFile;
  RandomAccessFile raf;
  FileChannel channel;
  // OutputStream os;
  DirectMemBufferOutputStream fos;    // all accesses to this stream should be synchronized on "this" (The TransactionLog)

  //GetChannelInputStream cis;

  final ReentrantLock fosLock = new ReentrantLock(true);

  // The memory-mapped tlog buffer (Agrona MappedResizeableBuffer) starts at INITIAL_BUFFER_SIZE and is
  // grown geometrically by ensureCapacity() whenever a record would be written past the current mapping.
  // Agrona's resize() only re-maps the address window (it does NOT change the file length - the existing
  // raf.setLength(fos.size()) calls grow the file), so the file keeps reflecting the logical content size
  // and readers keep bounding themselves by fos.size(). mapLock makes the rare re-map safe against
  // concurrent buffer access: every read/write of the buffer that happens OUTSIDE fosLock takes the shared
  // (read) lock, and the re-map in ensureCapacity (always invoked under fosLock) takes the exclusive
  // (write) lock. Lock order is always fosLock-before-mapLock; no thread holds mapLock while acquiring
  // fosLock, so there is no deadlock.
  static final long INITIAL_BUFFER_SIZE = 6_400_000L;
  private final ReentrantReadWriteLock mapLock = new ReentrantReadWriteLock();
  private final LongAdder numRecords = new LongAdder();
  boolean isBuffer;

  protected volatile boolean deleteOnClose = true;  // we can delete old tlogs since they are currently only used for real-time-get (and in the future, recovery)

  AtomicInteger refcount = new AtomicInteger(1);


  Object2IntMap<String> globalStringMap =  Object2IntMaps.synchronize(new Object2IntOpenHashMap<>());

  List<String> globalStringList = ObjectLists.synchronize(new ObjectArrayList<>());

  // write a BytesRef as a byte array
  static final JavaBinCodec.ObjectResolver resolver = (o, codec) -> {
    if (o instanceof BytesRef) {
      BytesRef br = (BytesRef)o;
      codec.writeByteArray(br.bytes, br.offset, br.length);
      return null;
    }
    // Fallback: we have no idea how to serialize this.  Be noisy to prevent insidious bugs
    throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,
        "TransactionLog doesn't know how to serialize " + o.getClass() + "; try implementing ObjectResolver?");
  };
  private boolean wroteHeader;

  public class LogCodec extends JavaBinCodec {

    public LogCodec(JavaBinCodec.ObjectResolver resolver) {

      super(resolver);
      stdStrings = false;
    }

    @Override
    public void writeExternString(CharSequence s) throws IOException {
      log.trace("tlog writeExternString s={}", s);
      if (s == null) {
        writeTag(NULL);
        return;
      }
      // Always write tlog field names INLINE rather than as extern-string references into
      // globalStringList. The global-string list is only a snapshot taken when the log header
      // is written (and is reset from the header on reopen), but field names are accumulated
      // across records and tlog rollovers, so extern indices can point past the snapshot
      // (IndexOutOfBounds) or at the wrong string. Inline strings make each record fully
      // self-contained, which is what realtime-get, recovery and peersync replay all require.
      writeStr(s);
    }

    @Override
    public CharSequence readExternString(JavaBinInputStream fis) throws IOException {
      int idx = readSize(fis);
      if (idx > 0) {// idx != 0 is the index of the extern string
        //   no need to synchronize globalStringList - it's only updated before the first record is written to the log
        return globalStringList.get(idx - 1);
      } else {// idx == 0 means it has a string value
        //   this shouldn't happen with this codec subclass.
        return readStr(fis, fis.readInt());
        //   throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Corrupt transaction log");
      }
    }

    @Override
    protected Object readObject(JavaBinInputStream dis) throws IOException {
      if (UUID == tagByte) {
        return new java.util.UUID(dis.readLong(), dis.readLong());
      }
      return super.readObject(dis);
    }

    @Override
    public boolean writePrimitive(Object val) throws IOException {
      if (val instanceof java.util.UUID) {
        java.util.UUID uuid = (java.util.UUID) val;
        daos.write(UUID);
        daos.writeLong(uuid.getMostSignificantBits());
        daos.writeLong(uuid.getLeastSignificantBits());
        return true;
      }
      return super.writePrimitive(val);
    }
  }

  TransactionLog(File tlogFile, Collection<String> globalStrings) {
    this(tlogFile, globalStrings, false);
  }

  TransactionLog(File tlogFile, Collection<String> globalStrings, boolean openExisting) {
    boolean success = false;
    try {
      if (debug) {
        log.debug("New TransactionLog file= {}, exists={}, size={} openExisting={}"
            , tlogFile, tlogFile.exists(), tlogFile.length(), openExisting);
      }

      globalStringMap.defaultReturnValue(-1);

      // Parse tlog id from the filename
      String filename = tlogFile.getName();
      id = Long.parseLong(filename.substring(filename.lastIndexOf('.') + 1));

      this.tlogFile = tlogFile;
      raf = new RandomAccessFile(this.tlogFile, "rw");
      long start = raf.length();
      channel = raf.getChannel();
      //os = Channels.newOutputStream(channel);
      buffer = new MappedResizeableBuffer(channel, 0, Math.max(INITIAL_BUFFER_SIZE, start));
      fos = new DirectMemBufferOutputStream(buffer);//new BufferedChannel(channel, 8192);
      if (start > 0) {
        wroteHeader = true;
      }


      // cis = new GetChannelInputStream(channel, Channels.newInputStream(channel));

      if (openExisting) {
        if (start > 0) {
          readHeader(null);
          //       raf.seek(start);
          fos.position((int) start);
          //assert channel.position() == start;
          //  fos.setWritten((int) start);    // reflect that we aren't starting at the beginning
          //  assert fos.size() == channel.size();
        } else {
          addGlobalStrings(globalStrings);
        }
      } else {
        if (start > 0) {
          log.warn("New transaction log already exists:{} size={}", tlogFile, raf.length());
          return;
        }

        //        if (start > 0) {
        //          raf.setLength(0);
        //        }
        addGlobalStrings(globalStrings);
      }

      success = true;

      // TODO: like updatelog, this is currently very hard to nail 1000% as
      // updates can spawn a new one,.l
      // assert ObjectReleaseTracker.track(this);

    } catch (IOException e) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e);
    } finally {
      if (!success && raf != null) {
        try {
          raf.close();
        } catch (Exception e) {
          log.error("Error closing tlog file (after error opening)", e);
        }
      }
    }
  }

  // for subclasses
  protected TransactionLog() {}

  /** Returns the number of records in the log (currently includes the header and an optional commit).
   * Note: currently returns 0 for reopened existing log files.
   */
  public int numRecords() {
    return this.numRecords.intValue();
  }

  public boolean endsWithCommit() throws IOException {
    long size;
    fosLock.lock();
    try {
      // fos.flushBuffer();
      size = fos.length();
    } finally {
      fosLock.unlock();
    }

    // the end of the file should have the end message (added during a commit) plus a 4 byte size
    byte[] buf = new byte[END_MESSAGE.length()];
    long pos = size - END_MESSAGE.length() - 4;
    if (pos < 0) return false;
    // channel.position(pos);
    //final InputStream is = new ChannelFastInputStream(channel, pos);
    // is.position(pos);
    mapLock.readLock().lock();
    try {
      buffer.getBytes(pos, buf);
    } finally {
      mapLock.readLock().unlock();
    }
    for (int i = 0; i < buf.length; i++) {
      if (buf[i] != END_MESSAGE.charAt(i)) return false;
    }
    return true;
  }

  @SuppressWarnings({"unchecked"})
  private void readHeader(DirectMemBufferedInputStream fis) throws IOException {
    log.info("read header fileSize={}", raf.length());
    // read existing header
    fis = fis != null ? fis : new DirectMemBufferedInputStream(buffer, raf.length());
    @SuppressWarnings("resource") final LogCodec codec = new LogCodec(resolver);

    fis.position(0);

    //    fis.flush();

    @SuppressWarnings({"rawtypes"})
    Map header = (Map) codec.unmarshal(fis);

    //  fis.readInt(); // skip size

    // needed to read other records

    fosLock.lock();
    try {
      globalStringList = (List<String>) header.get("strings");
      globalStringMap = new Object2IntOpenHashMap<>(globalStringList.size());
      for (var i = 0; i < globalStringList.size(); i++) {
        globalStringMap.put(globalStringList.get(i), i + 1);
      }
    } finally {
      fosLock.unlock();
    }
  }

  protected void addGlobalStrings(Collection<String> strings) {
    if (strings == null) return;
    fosLock.lock();
    try {
      int origSize = globalStringMap.size();
      for (String s : strings) {
        Integer idx = null;
        if (origSize > 0) {
          idx = globalStringMap.getInt(s);
        }
        if (idx != null) continue;  // already in list
        globalStringList.add(s);
        globalStringMap.put(s, globalStringList.size());
      }
      assert globalStringMap.size() == globalStringList.size();
    } finally {
      fosLock.unlock();
    }
  }

  Collection<String> getGlobalStrings() {
    fosLock.lock();
    try {
      return new ArrayList<>(globalStringList);
    } finally {
      fosLock.unlock();
    }
  }

  @SuppressWarnings({"unchecked"})
  protected void writeLogHeader(LogCodec codec) throws IOException {


    MutableDirectBuffer expandableBuffer1 = ExpandableBuffers.getInstance().acquire(-1, true);
    try {
      //   fos.flush(); // flush since this will be the last record in a log fill
      long pos = raf.length();   // if we had flushed, this should be equal to channel.position()

      if (pos == 0) {
        fos.flush();
        pos = fos.size();
      } else {
        return;
      }


      //    fos.flush();
      //   pos = fos.size();

      //     MutableDirectBuffer expandableBuffer1 = new ExpandableArrayBuffer(32); // MRM TODO:

      ExpandableDirectBufferOutputStream out = new ExpandableDirectBufferOutputStream(expandableBuffer1);
      codec.init(out);
      Map header = new Object2ObjectLinkedOpenHashMap<String, Object>(2, 0.25f);
      header.put("SOLR_TLOG", 1); // a magic string + version number
      header.put("strings", globalStringList);

      log.info("write header={}", header);
      codec.marshal(header, out);
      //codec.writeSInt((int) (pos - fos.size()));

      int lastSize = out.position();


      raf.setLength(fos.length() + out.position() + 4);
      ensureCapacity(pos + lastSize + 4);
      log.info("headerSize={} fileSize={}", out.position(), raf.length());

      //   fos.flushBuffer();
      expandableBuffer1.byteBuffer().position(0 +  expandableBuffer1.wrapAdjustment());
      expandableBuffer1.byteBuffer().limit(out.position() + expandableBuffer1.wrapAdjustment());

      fos.putBytes( pos, expandableBuffer1.byteBuffer(), lastSize);

      fos.putInt((int) (pos + lastSize), lastSize);

      // advance past the header record + its 4-byte size trailer
      fos.position((int) (pos + lastSize + 4));

      numRecords.increment();



    } catch (IOException e) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e);
    } finally {
      ExpandableBuffers.getInstance().release(expandableBuffer1);
    }

  }

  protected void endRecord(long startRecordPosition) throws IOException {
    fos.writeInt((int) (fos.size() - startRecordPosition));
    numRecords.increment();
  }

  protected void  checkWriteHeader(LogCodec codec, SolrInputDocument optional, long pos) throws IOException {
    log.info("check write fileSize={} wroteHeader={} pos={}", raf.length(), wroteHeader, pos);
    // Unsynchronized access. We can get away with an unsynchronized access here
    // since we will never get a false non-zero when the position is in fact 0.
    // rollback() is the only function that can reset to zero, and it blocks updates.
      if (wroteHeader || pos > 0) return;

    fosLock.lock();
    try {
      if (raf.length() > 0) return;  // check again while synchronized
      wroteHeader = true;
      if (optional != null) {
        addGlobalStrings(optional.getFieldNames());
      }
         writeLogHeader(codec);
    } finally {
      fosLock.unlock();
    }
  }

  /**
   * Ensure the memory-mapped buffer covers at least {@code requiredEnd} bytes, growing it geometrically
   * if needed. MUST be called while holding {@link #fosLock} (so only one re-map happens at a time and it
   * is serialized with all the under-lock buffer writes). The actual re-map takes mapLock's write lock so
   * it cannot run while another thread is reading/writing the buffer outside fosLock under mapLock's read
   * lock. Cheap no-op (a single capacity comparison) on the overwhelmingly common path where the record
   * already fits.
   */
  private void ensureCapacity(long requiredEnd) {
    if (requiredEnd <= buffer.capacity()) return;
    long newCap = buffer.capacity();
    if (newCap < INITIAL_BUFFER_SIZE) newCap = INITIAL_BUFFER_SIZE;
    while (newCap < requiredEnd) {
      long doubled = newCap << 1;
      if (doubled <= newCap) { // overflow guard
        newCap = requiredEnd;
        break;
      }
      newCap = doubled;
    }
    mapLock.writeLock().lock();
    try {
      if (requiredEnd > buffer.capacity()) {
        if (log.isInfoEnabled()) {
          log.info("Growing tlog mmap buffer for {} from {} to {} (requiredEnd={})", tlogFile, buffer.capacity(), newCap, requiredEnd);
        }
        buffer.resize(newCap);
      }
    } finally {
      mapLock.writeLock().unlock();
    }
  }

  volatile int lastAddSize;

  /**
   * Writes an add update command to the transaction log. This is not applicable for
   * in-place updates; use {@link #write(AddUpdateCommand, long)}.
   * (The previous pointer (applicable for in-place updates) is set to -1 while writing
   * the command to the transaction log.)
   * @param cmd The add update command to be written
   * @return Returns the position pointer of the written update command
   *
   * @see #write(AddUpdateCommand, long)
   */
  public long write(AddUpdateCommand cmd) {
    return write(cmd, -1);
  }

  /**
   * Writes an add update command to the transaction log. This should be called only for
   * writing in-place updates, or else pass -1 as the prevPointer.
   * @param cmd The add update command to be written
   * @param prevPointer The pointer in the transaction log which this update depends
   * on (applicable for in-place updates)
   * @return Returns the position pointer of the written update command
   */
  public long write(AddUpdateCommand cmd, long prevPointer) {
    assert (-1 <= prevPointer && (cmd.isInPlaceUpdate() || (-1 == prevPointer)));

    LogCodec codec = new LogCodec(resolver);
    SolrInputDocument sdoc = cmd.getSolrInputDocument();
    MutableDirectBuffer expandableBuffer1 = ExpandableBuffers.getInstance().acquire(-1, true);
    try {


      // adaptive buffer sizing
     // int bufSize = lastAddSize;
      // at least 256 bytes and at most 1 MB
      //bufSize = Math.min(1024 * 1024, Math.max(256, bufSize + (bufSize >> 3) + 256));

      try {

        ExpandableDirectBufferOutputStream out = new ExpandableDirectBufferOutputStream(expandableBuffer1);

        codec.init(out);
        if (cmd.isInPlaceUpdate()) {
          codec.writeTag(JavaBinCodec.ARR, 5);
          codec.writeSInt(UpdateLog.UPDATE_INPLACE);  // should just take one byte
          codec.writeLong(cmd.getVersion());
          codec.writeLong(prevPointer);
          codec.writeLong(cmd.prevVersion);
          codec.writeSolrInputDocument(cmd.getSolrInputDocument());
        } else {
          codec.writeTag(JavaBinCodec.ARR, 3);
          codec.writeSInt(UpdateLog.ADD);  // should just take one byte
          codec.writeLong(cmd.getVersion());
          codec.writeSolrInputDocument(cmd.getSolrInputDocument());
        }
        int lastAddSize = (int) (out.position());

        long pos;

        fosLock.lock();
        try {
          checkWriteHeader(codec, sdoc, raf.length());

          pos = fos.size();   // if we had flushed, this should be equal to channel.position()

          // Reserve this record's region [pos, pos+lastAddSize+4) by advancing the write
          // position under the lock, so concurrent writers get distinct, non-overlapping
          // offsets. The actual buffer copy below can then happen outside the lock.
          fos.position((int) (pos + lastAddSize + 4));

          raf.setLength(fos.size());
          ensureCapacity(fos.size());

        } finally {
          fosLock.unlock();
        }
        //   fos.flushBuffer();

        expandableBuffer1.byteBuffer().position(0 +  expandableBuffer1.wrapAdjustment());
        expandableBuffer1.byteBuffer().limit(lastAddSize + expandableBuffer1.wrapAdjustment());

        mapLock.readLock().lock();
        try {
          fos.putBytes( pos, expandableBuffer1.byteBuffer(), lastAddSize);

          fos.putInt((int) (pos + lastAddSize), lastAddSize);
        } finally {
          mapLock.readLock().unlock();
        }

        numRecords.increment();
        //  assert pos != 0;

        /*
         System.out.println("###writing at " + pos + " fos.size()=" + fos.size() + " raf.length()=" + raf.length());
         if (pos != fos.size()) {
         throw new RuntimeException("ERROR" + "###writing at " + pos + " fos.size()=" + fos.size() + " raf.length()=" + raf.length());
         }
         */
        //   ByteBuffer buffer = out.buffer().byteBuffer().asReadOnlyBuffer();
        //   codec.writeInt((int) lastAddSize);
        //   expandableBuffer1.byteBuffer().position(0);
        //   expandableBuffer1.byteBuffer().limit(out.position() + out.buffer().wrapAdjustment());
        //  fos.write(expandableBuffer1.byteBuffer());
        //  numRecords.increment();
        // fos.flushBuffer();

        //   endRecord(pos);
        // fos.flushBuffer();
        return pos;

      } finally {
        ExpandableBuffers.getInstance().release(expandableBuffer1);
      }

    } catch (IOException e) {
      // TODO: reset our file pointer back to "pos", the start of this record.
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Error logging add", e);
    }
  }

  public long writeDelete(DeleteUpdateCommand cmd) {
    LogCodec codec = new LogCodec(resolver);

    try {


      BytesRef br = cmd.getIndexedId();
      MutableDirectBuffer expandableBuffer1 = ExpandableBuffers.getInstance().acquire(-1, true);
      fosLock.lock();
      try {

    //    MutableDirectBuffer expandableBuffer1 = new ExpandableArrayBuffer(20 + (br.length));
        long pos = fos.size();
        checkWriteHeader(codec, null, pos);
        pos = fos.size();   // re-read: checkWriteHeader may have written the log header and advanced fos
        ExpandableDirectBufferOutputStream out = new ExpandableDirectBufferOutputStream(expandableBuffer1);
        codec.init(out);
        codec.writeTag(JavaBinCodec.ARR, 3);
        codec.writeSInt(UpdateLog.DELETE);  // should just take one byte
        codec.writeLong(cmd.getVersion());
        codec.writeByteArray(br.bytes, br.offset, br.length);

        int lastSize = (int) (out.position());

        raf.setLength(raf.length() + out.position() + 4);
        ensureCapacity(pos + lastSize + 4);

        //   fos.flushBuffer();

        expandableBuffer1.byteBuffer().position(0 +  expandableBuffer1.wrapAdjustment());
        expandableBuffer1.byteBuffer().limit(lastSize + expandableBuffer1.wrapAdjustment());

        fos.putBytes(pos, expandableBuffer1.byteBuffer(), lastSize);

        fos.putInt((int) (pos + lastSize), lastSize);

        fos.position((int) (pos + lastSize + 4));

        numRecords.increment();
        //     fos.flushBuffer();  // flush later
        return pos;
      } finally {
        fosLock.unlock();
        ExpandableBuffers.getInstance().release(expandableBuffer1);
      }

    } catch (IOException e) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e);
    }

  }

  public long writeDeleteByQuery(DeleteUpdateCommand cmd) {
    LogCodec codec = new LogCodec(resolver);
    try {

      //   long initSize = fos.size();
      //MutableDirectBuffer expandableBuffer1 = new ExpandableArrayBuffer(20 + (cmd.query.length()));
      MutableDirectBuffer expandableBuffer1 = ExpandableBuffers.getInstance().acquire(-1, true);
      fosLock.lock();

      try {
        ExpandableDirectBufferOutputStream out = new ExpandableDirectBufferOutputStream(expandableBuffer1);
        long pos = fos.length();   // if we had flushed, this should be equal to channel.position()
        codec.init(out);
        codec.writeTag(JavaBinCodec.ARR, 3);
        codec.writeSInt(UpdateLog.DELETE_BY_QUERY);  // should just take one byte
        codec.writeLong(cmd.getVersion());
        codec.writeStr(cmd.query);

        int lastSize = (int) (out.position());
        checkWriteHeader(codec, null, pos);
        pos = fos.size();   // re-read: checkWriteHeader may have written the log header and advanced fos
        raf.setLength(raf.length() +  lastSize + 4);
        ensureCapacity(pos + lastSize + 4);

        //   fos.flushBuffer();

        expandableBuffer1.byteBuffer().position(0 +  expandableBuffer1.wrapAdjustment());
        expandableBuffer1.byteBuffer().limit(out.position() + expandableBuffer1.wrapAdjustment());

        fos.putBytes( pos, expandableBuffer1.byteBuffer(), lastSize);

        fos.putInt((int) (pos + lastSize), lastSize);

        fos.position((int) (pos + lastSize + 4));

        numRecords.increment();
        //  fos.flushBuffer();  // flush later
        return pos;
      } finally {
        fosLock.unlock();

          ExpandableBuffers.getInstance().release(expandableBuffer1);

      }
    } catch (IOException e) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e);
    }

  }


  public long writeCommit(CommitUpdateCommand cmd) {
    LogCodec codec = new LogCodec(resolver);
    fosLock.lock();
    try {
      MutableDirectBuffer expandableBuffer1 = ExpandableBuffers.getInstance().acquire(-1, true);
      try {
     //   fos.flush(); // flush since this will be the last record in a log fill
        long pos = fos.size();   // if we had flushed, this should be equal to channel.position()

        if (pos == 0) {
          writeLogHeader(codec);
        //  fos.flush();
          pos = fos.size();
        }

        //    fos.flush();
        //   pos = fos.size();

       //     MutableDirectBuffer expandableBuffer1 = new ExpandableArrayBuffer(32); // MRM TODO:

        ExpandableDirectBufferOutputStream out = new ExpandableDirectBufferOutputStream(expandableBuffer1);
        codec.init(out);
        codec.writeTag(JavaBinCodec.ARR, 3);
        codec.writeSInt(UpdateLog.COMMIT);  // should just take one byte
        codec.writeLong(cmd.getVersion());
        codec.writeStr(END_MESSAGE);  // ensure these bytes are (almost) last in the file

        int lastSize = (int) (out.position());

        raf.setLength(raf.length() + out.position() + 4);
        ensureCapacity(pos + lastSize + 4);

        //   fos.flushBuffer();
        expandableBuffer1.byteBuffer().position(0 +  expandableBuffer1.wrapAdjustment());
        expandableBuffer1.byteBuffer().limit(out.position() + expandableBuffer1.wrapAdjustment());

        fos.putBytes(pos, expandableBuffer1.byteBuffer(), lastSize);

        fos.putInt((int) (pos + lastSize), lastSize);

        fos.position((int) (pos + lastSize + 4));

        numRecords.increment();

        return pos;
      } catch (IOException e) {
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e);
      } finally {
        ExpandableBuffers.getInstance().release(expandableBuffer1);
      }
    } finally {
      fosLock.unlock();
    }
  }


  /* This method is thread safe */

  public Object lookup(long pos) {
    // A negative position can result from a log replay (which does not re-log, but does
    // update the version map.  This is OK since the node won't be ACTIVE when this happens.
    if (pos < 0) return null;

    try {
      // make sure any unflushed buffer has been flushed
      fosLock.lock();
      try {
        // TODO: optimize this by keeping track of what we have flushed up to
        //fos.flushBuffer();
        /***
         System.out.println("###flushBuffer to " + fos.size() + " raf.length()=" + raf.length() + " pos="+pos);
         if (fos.size() != raf.length() || pos >= fos.size() ) {
         throw new RuntimeException("ERROR" + "###flushBuffer to " + fos.size() + " raf.length()=" + raf.length() + " pos="+pos);
         }
         ***/
      } finally {
        fosLock.unlock();
      }
      // channel.position(pos);
      DirectMemBufferedInputStream fis = new DirectMemBufferedInputStream(buffer, raf.length());

      fis.position(pos);
      mapLock.readLock().lock();
      try (LogCodec codec = new LogCodec(resolver)) {
        return codec.readVal(fis);
      } finally {
        mapLock.readLock().unlock();
      }
    } catch (IOException e) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e);
    }
  }

  public void incref() {
    refcount.updateAndGet(operand -> {
      if (operand == 0) {
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "incref on a closed log: " + this);
      }
      return operand + 1;
    });
  }

  public boolean tryIncref() {
    AtomicBoolean result = new AtomicBoolean();
    refcount.updateAndGet(operand -> {
      if (operand == 0) {
        result.set(false);
        return 0;
      }
      result.set(true);
      return operand + 1;
    });

    return result.get();
  }

  public void decref() {
    refcount.updateAndGet(operand -> {
      if (operand == 0) {
        return 0;
      }
      if (operand == 1) {
        close();
      }
      return  operand - 1;
    });
  }

  /** returns the current position in the log file */
  public long position() {
    fosLock.lock();
    try {
      return fos.size();
    } finally {
      fosLock.unlock();
    }
  }

  /** Move to a read-only state, closing and releasing resources while keeping the log available for reads */
  public void closeOutput() {

  }

  public void finish(UpdateLog.SyncLevel syncLevel) {
    if (syncLevel == UpdateLog.SyncLevel.NONE) return;
    try {
      fosLock.lock();
      try {
        //  fos.flushBuffer();
      } finally {
        fosLock.unlock();
      }

      if (syncLevel == UpdateLog.SyncLevel.FSYNC) {
        // Since fsync is outside of synchronized block, we can end up with a partial
        // last record on power failure (which is OK, and does not represent an error...
        // we just need to be aware of it when reading).
        raf.getFD().sync();
      }

    } catch (IOException e) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e);
    }
  }

  public void close() {
    try {
      if (debug) {
        log.debug("Closing tlog {}", this);
      }

      fosLock.lock();
      try {

        fos.close();

        channel.close();
      } finally {
        fosLock.unlock();
      }

      if (deleteOnClose) {
        try {
          Files.deleteIfExists(tlogFile.toPath());
        } catch (IOException e) {
          // TODO: should this class care if a file couldnt be deleted?
          // this just emulates previous behavior, where only SecurityException would be handled.
        }
      }
    } catch (IOException e) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e);
    } finally {
      assert ObjectReleaseTracker.getInstance().release(this);
    }
  }

  public void forceClose() {
    if (refcount.get() > 0) {
      // Readers (e.g. a LogReplayer mid-read) hold increfs. Closing unmaps the memory-mapped
      // buffer, so unmapping while a reader is active causes a native SIGSEGV in
      // DirectMemBufferedInputStream.read(), not a catchable Java exception. Wait (bounded)
      // for readers to drain — the core close aborts the replayer via AlreadyClosedException
      // and its LogReader.close() decrefs — before releasing the mapping.
      long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
      while (refcount.get() > 0 && System.nanoTime() < deadline) {
        try {
          Thread.sleep(10);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          break;
        }
      }
      if (refcount.get() > 0) {
        log.error("Error: Forcing close of {}", this);
        refcount.set(0);
        close();
      }
    }
  }

  @Override
  public String toString() {
    return "tlog{file=" + tlogFile.toString() + " refcount=" + refcount.get() + " size=" + fos.size() + '}';
  }

  public long getLogSize() {
    if (tlogFile != null) {
      return tlogFile.length();
    }
    return 0;
  }

  /**
   * @return the FastOutputStream size
   */
  public long getLogSizeFromStream() {
    fosLock.lock();
    try {
      return fos.size();
    } finally {
      fosLock.unlock();
    }
  }

  /** Returns a reader that can be used while a log is still in use.
   * Currently only *one* LogReader may be outstanding, and that log may only
   * be used from a single thread. */
  public LogReader getReader(long startingPos) {
    return new LogReader(startingPos);
  }

  public LogReader getSortedReader(long startingPos) {
    return new SortedLogReader(startingPos);
  }

  /** Returns a single threaded reverse reader */
  public ReverseReader getReverseReader() throws IOException {
    return new FSReverseReader();
  }

  public class LogReader {
    protected DirectMemBufferedInputStream fis;
    private final LogCodec codec = new LogCodec(resolver);

    public LogReader(long startingPos) {
      incref();
      fosLock.lock();
      try {
        //fos.flushBuffer();
     //   channel.position(startingPos);
        fis = new DirectMemBufferedInputStream(buffer,  raf.length());

        // fos.flushBuffer();
      } catch (IOException ioException) {
        throw new RuntimeIOException(ioException);
      } finally {
        fosLock.unlock();
      }

    }

    // for classes that extend
    protected LogReader() {}

    /** Returns the next object from the log, or null if none available.
     *
     * @return The log record, or null if EOF
     * @throws IOException If there is a low-level I/O error.
     */
    public Object next() throws IOException, InterruptedException {
      long pos = fis.position();
            fosLock.lock();
            try {
      //        fos.flushBuffer();
      //        pos = fis.position();
      //      //  if (log.isDebugEnabled()) {
      //          log.info("Reading log record.  pos={} currentSize={}", pos, fos.size());
      //      //  }
      //
              if (pos >= fos.size()) {
                return null;
              }

         //     fos.flushBuffer();
            } finally {
              fosLock.unlock();
            }
      if (pos == 0) {
        // The log file begins with a marshalled header record ([version][header map] followed by a
        // 4-byte size trailer, written by writeLogHeader). It must be consumed before the first real
        // record is read: next() reads records with raw readVal(), which would otherwise misinterpret
        // the version-framed header as data and desync the stream (replaying zero records). readHeader
        // also repopulates globalStringList when replaying a log freshly opened from disk.
        fosLock.lock();
        try {
          if (fis.position() >= fos.size()) {
            return null;
          }
          readHeader(fis);   // reads version byte + header map, advancing fis past the header map
          fis.readInt();     // skip the header record's 4-byte size trailer
          pos = fis.position();
          if (pos >= fos.size()) {
            return null;
          }
        } finally {
          fosLock.unlock();
        }
      }

      mapLock.readLock().lock();
      Object o;
      try {
        o = codec.readVal(fis);

        // skip over record size
        int size = fis.readInt();
        //  assert size == fis.position() - pos - 4 : "size=" + size + " pos-4=" + (fis.position() - pos - 4);
      } finally {
        mapLock.readLock().unlock();
      }

      return o;
    }

    public void close() {
      decref();
    }

    @Override
    public String toString() {
      synchronized (TransactionLog.this) {

        return "LogReader{" + "file=" + tlogFile + ", position=" + fis.position() + ", end=" + fos.size() + '}';

      }
    }

    public long currentPos() {

      return fis.position();

    }

    // returns best effort current size
    // for info purposes
    public long currentSize() throws IOException {
      return fos.size();
    }

  }

  public class SortedLogReader extends LogReader {
    private final long startingPos;
    private boolean inOrder = true;
    private Map<Long, Long> versionToPos;
    Iterator<Long> iterator;

    public SortedLogReader(long startingPos) {
      super(startingPos);
      this.startingPos = startingPos;
    }

    @Override
    public Object next() throws IOException, InterruptedException {
      if (versionToPos == null) {
        versionToPos = new Long2LongAVLTreeMap();
        Object o;
        long pos = startingPos;

        long lastVersion = Long.MIN_VALUE;
        while ((o = super.next()) != null) {
          @SuppressWarnings({"rawtypes"})
          List entry = (List) o;
          long version = (Long) entry.get(UpdateLog.VERSION_IDX);
          version = Math.abs(version);
          versionToPos.put(version, pos);
          pos = currentPos();

          if (version < lastVersion) inOrder = false;
          lastVersion = version;
        }
        fis.position(startingPos);
      }

      if (inOrder) {
        return super.next();
      } else {
        if (iterator == null) iterator = versionToPos.values().iterator();
        if (!iterator.hasNext()) return null;
        long pos = iterator.next();
        if (pos != currentPos()) fis.position(pos);
        return super.next();
      }
    }
  }

  public abstract static class ReverseReader {

    /** Returns the next object from the log, or null if none available.
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
    DirectMemBufferedInputStream fis;
    private final LogCodec codec = new LogCodec(resolver) {
      @Override
      public SolrInputDocument readSolrInputDocument(JavaBinInputStream dis, int sz) {
        // Given that the SolrInputDocument is last in an add record, it's OK to just skip
        // reading it completely.
        return null;
      }
    };

    int nextLength;  // length of the next record (the next one closer to the start of the log file)
    long prevPos;    // where we started reading from last time (so prevPos - nextLength == start of next record)

    public FSReverseReader() throws IOException {
      incref();

      long sz;
      fosLock.lock();
      try {
        //  fos.flushBuffer();
        //  channel.position(0);
        fis = new DirectMemBufferedInputStream(buffer,  raf.length());

        sz = fos.size();
        log.info("reverse reader size={} filesz={}", sz, raf.length());
        //  assert sz == channel.size() : "sz:" + sz + " ch:" + channel.size();
      } finally {
        fosLock.unlock();
      }


      if (sz >= 4) {
        // readHeader(fis);  // should not be needed
        prevPos = sz - 4;
        // fis.position(prevPos);
        nextLength = buffer.getInt(prevPos);
        log.info("prevPos={} nextLen={}", prevPos, nextLength);
      }
    }

    /** Returns the next object from the log, or null if none available.
     *
     * @return The log record, or null if EOF
     * @throws IOException If there is a low-level I/O error.
     */
    public Object next() throws IOException {
      if (prevPos <= 0) return null;


      long endOfThisRecord = prevPos;

      int thisLength = nextLength;

      long recordStart = prevPos - thisLength;  // back up to the beginning of the next record

      if (recordStart < 0) return null;

      prevPos = recordStart - 4;  // back up 4 more to read the length of the next record
      log.info("recordStart={} prevPos={} length={}", recordStart, prevPos, thisLength);
        if (prevPos <= 0) return null;  // this record is the header

      //      long bufferPos = fis.getPositionInBuffer();
      //      if (prevPos >= bufferPos) {
      //        // nothing to do... we're within the current buffer
      //      } else {
      //        // Position buffer so that this record is at the end.
      //        // For small records, this will cause subsequent calls to next() to be within the buffer.
      //        long seekPos = endOfThisRecord - fis.getBufferSize();
      //        seekPos = Math.min(seekPos, prevPos); // seek to the start of the record if it's larger then the block size.
      //        seekPos = Math.max(seekPos, 0);
      //        fis.position(seekPos);
      //        fis.peek();  // cause buffer to be filled
      //      }

      fis.position(recordStart);

      mapLock.readLock().lock();
      Object obj;
      try {
        if (prevPos > -1) {
          nextLength = buffer.getInt(prevPos);     // this is the length of the *next* record (i.e. closer to the beginning)
        }

        // TODO: optionally skip document data

        // assert fis.position() == prevPos + 4 + thisLength;  // this is only true if we read all the data (and we currently skip reading SolrInputDocument
        log.info("theNextLen={} readRecAt={}", nextLength, recordStart);

        obj = codec.readVal(fis);
      } finally {
        mapLock.readLock().unlock();
      }
      log.info("obj={}", obj);

      return obj;
    }

    /* returns the position in the log file of the last record returned by next() */
    public long position() {
      return prevPos + 4;  // skip the length
    }

    public void close() {
      decref();
    }

    @Override
    public String toString() {
      fosLock.lock();
      try {
        return "LogReader{" + "file=" + tlogFile + ", position=" + fis.position() + ", end=" + fos.size() + '}';
      } finally {
        fosLock.unlock();
      }
    }


  }

  //  static class ChannelFastInputStream extends SolrInputStream {
  //
  //    public ChannelFastInputStream(FileChannel ch, long chPosition) {
  //      // super(null, new byte[10],0,0);    // a small buffer size for testing purposes
  //  //    super(is, (int) chPosition);
  //      super(Channels.newInputStream(ch));
  //
  //    }

  //    public FileChannel getChannel() {
  //      return ch;
  //    }

  //    @Override
  //    public int readWrappedStream(byte[] target, int offset, int len) throws IOException {
  //      ByteBuffer bb = ByteBuffer.wrap(target, offset, len);
  //      return ch.read(bb, readFromStream);
  //    }
  //
  //    @Override
  //    public void readFully(byte b[], int off, int len) throws IOException {
  //      ByteBuffer bb = ByteBuffer.wrap(b, off, len);
  //      ((FileChannel)((GetChannelInputStream)is).getChannel()).read(bb, position());
  //    }

  /** where is the start of the buffer relative to the whole file */
  //    public long getBufferPos() {
  //      return g;
  //    }
  //
  //    public void seek(long position) throws IOException {
  //      if (position <= readBytes && position >= getPositionInBuffer()) {
  //        // seek within buffer
  //        pos = (int) (position - getPositionInBuffer());
  //      } else {
  //        // long currSize = ch.size();   // not needed - underlying read should handle (unless read never done)
  //        // if (position > currSize) throw new EOFException("Read past EOF: seeking to " + position + " on file of size " + currSize + " file=" + ch);
  //        readBytes = position;
  //        pos = 0;
  //      }
  //      assert position() == position;
  //    }

  //public int getBufferSize() {
  //  return buffer.length;
}
//
//    @Override
//    public void close() throws IOException {
//      super.close();
//    }

//    @Override
//    public void flush() {
//      ((FastBufferedInputStream) is).flush();
//    }

//    @Override
//    public String toString() {
//      try {
//        return "readFromStream=" + readBytes + " pos=" + pos + " bufferPos=" + getPositionInBuffer() + " position=" + position();
//      } catch (IOException e) {
//        throw new RuntimeIOException(e);
//      }
//    }
// }
//}


