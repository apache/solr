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
package org.apache.solr.handler.admin.api;

import static org.apache.solr.handler.ReplicationHandler.ERR_STATUS;
import static org.apache.solr.handler.ReplicationHandler.OK_STATUS;

import jakarta.ws.rs.core.StreamingOutput;
import java.io.IOException;
import java.io.OutputStream;
import java.lang.invoke.MethodHandles;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.regex.Pattern;
import java.util.zip.Adler32;
import java.util.zip.Checksum;
import java.util.zip.DeflaterOutputStream;
import org.apache.commons.io.output.CloseShieldOutputStream;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.index.SegmentCommitInfo;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.RateLimiter;
import org.apache.solr.api.JerseyResource;
import org.apache.solr.client.api.model.FileListResponse;
import org.apache.solr.client.api.model.FileMetaData;
import org.apache.solr.client.api.model.IndexVersionResponse;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.util.FastOutputStream;
import org.apache.solr.core.DirectoryFactory;
import org.apache.solr.core.IndexDeletionPolicyWrapper;
import org.apache.solr.core.SolrCore;
import org.apache.solr.handler.ReplicationHandler;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** A common parent for "replication" (i.e. replication-level) APIs. */
public abstract class ReplicationAPIBase extends JerseyResource {

  public static final String CONF_FILE_SHORT = "cf";
  public static final String TLOG_FILE = "tlogFile";
  public static final String FILE_STREAM = "filestream";
  public static final String STATUS = "status";
  public static final int PACKET_SZ = 1024 * 1024; // 1MB
  public static final String GENERATION = "generation";
  public static final String OFFSET = "offset";
  public static final String LEN = "len";
  public static final String FILE = "file";
  public static final String MAX_WRITE_PER_SECOND = "maxWriteMBPerSec";
  public static final String CHECKSUM = "checksum";
  public static final String COMPRESSION = "compression";
  public static final String POLL_INTERVAL = "pollInterval";
  public static final String INTERVAL_ERR_MSG =
      "The " + POLL_INTERVAL + " must be in this format 'HH:mm:ss'";

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  public static final Pattern INTERVAL_PATTERN = Pattern.compile("(\\d*?):(\\d*?):(\\d*)");
  protected final SolrCore solrCore;
  protected final SolrQueryRequest solrQueryRequest;
  protected final SolrQueryResponse solrQueryResponse;

  public ReplicationAPIBase(
      SolrCore solrCore, SolrQueryRequest solrQueryRequest, SolrQueryResponse solrQueryResponse) {
    this.solrCore = solrCore;
    this.solrQueryRequest = solrQueryRequest;
    this.solrQueryResponse = solrQueryResponse;
  }

  protected IndexVersionResponse doFetchIndexVersion() throws IOException {
    ReplicationHandler replicationHandler =
        (ReplicationHandler) solrCore.getRequestHandler(ReplicationHandler.PATH);
    return replicationHandler.getIndexVersionResponse();
  }

  protected FileListResponse doFetchFileList(long generation) {
    ReplicationHandler replicationHandler =
        (ReplicationHandler) solrCore.getRequestHandler(ReplicationHandler.PATH);
    return getFileList(generation, replicationHandler);
  }

  protected DirectoryFileStream doFetchFile(
      String filePath,
      String dirType,
      String offset,
      String len,
      boolean compression,
      boolean checksum,
      double maxWriteMBPerSec,
      Long gen) {
    DirectoryFileStream dfs;
    if (Objects.equals(dirType, CONF_FILE_SHORT)) {
      dfs =
          new LocalFsConfFileStream(
              filePath, dirType, offset, len, compression, checksum, maxWriteMBPerSec, gen);
    } else if (Objects.equals(dirType, TLOG_FILE)) {
      dfs =
          new LocalFsTlogFileStream(
              filePath, dirType, offset, len, compression, checksum, maxWriteMBPerSec, gen);
    } else {
      dfs =
          new DirectoryFileStream(
              filePath, dirType, offset, len, compression, checksum, maxWriteMBPerSec, gen);
    }
    solrQueryResponse.add(FILE_STREAM, dfs);
    return dfs;
  }

  protected FileListResponse getFileList(long generation, ReplicationHandler replicationHandler) {
    final IndexDeletionPolicyWrapper delPol = solrCore.getDeletionPolicy();
    final FileListResponse filesResponse = new FileListResponse();

    IndexCommit commit = null;
    try {
      if (generation == -1) {
        commit = delPol.getAndSaveLatestCommit();
        if (null == commit) {
          filesResponse.fileList = Collections.emptyList();
          return filesResponse;
        }
      } else {
        try {
          commit = delPol.getAndSaveCommitPoint(generation);
        } catch (IllegalStateException ignored) {
          /* handle this below the same way we handle a return value of null... */
        }
        if (null == commit) {
          // The gen they asked for either doesn't exist or has already been deleted
          reportErrorOnResponse(filesResponse, "invalid index generation", null);
          return filesResponse;
        }
      }
      assert null != commit;

      List<FileMetaData> result = new ArrayList<>();
      Directory dir = null;
      try {
        dir =
            solrCore
                .getDirectoryFactory()
                .get(
                    solrCore.getNewIndexDir(),
                    DirectoryFactory.DirContext.DEFAULT,
                    solrCore.getSolrConfig().indexConfig.lockType);
        SegmentInfos infos = SegmentInfos.readCommit(dir, commit.getSegmentsFileName());
        for (SegmentCommitInfo commitInfo : infos) {
          for (String file : commitInfo.files()) {
            FileMetaData metaData = new FileMetaData();
            metaData.name = file;
            metaData.size = dir.fileLength(file);

            try (final IndexInput in = dir.openInput(file, IOContext.READONCE)) {
              try {
                long checksum = CodecUtil.retrieveChecksum(in);
                metaData.checksum = checksum;
              } catch (Exception e) {
                // TODO Should this trigger a larger error?
                log.warn("Could not read checksum from index file: {}", file, e);
              }
            }
            result.add(metaData);
          }
        }

        // add the segments_N file
        FileMetaData fileMetaData = new FileMetaData();
        fileMetaData.name = infos.getSegmentsFileName();
        fileMetaData.size = dir.fileLength(infos.getSegmentsFileName());
        if (infos.getId() != null) {
          try (final IndexInput in =
              dir.openInput(infos.getSegmentsFileName(), IOContext.READONCE)) {
            try {
              fileMetaData.checksum = CodecUtil.retrieveChecksum(in);
            } catch (Exception e) {
              // TODO Should this trigger a larger error?
              log.warn(
                  "Could not read checksum from index file: {}", infos.getSegmentsFileName(), e);
            }
          }
        }
        result.add(fileMetaData);
      } catch (IOException e) {
        log.error(
            "Unable to get file names for indexCommit generation: {}", commit.getGeneration(), e);
        reportErrorOnResponse(
            filesResponse, "unable to get file names for given index generation", e);
        return filesResponse;
      } finally {
        if (dir != null) {
          try {
            solrCore.getDirectoryFactory().release(dir);
          } catch (IOException e) {
            log.error("Could not release directory after fetching file list", e);
          }
        }
      }
      filesResponse.fileList = new ArrayList<>(result);

      if (replicationHandler.getConfFileNameAlias().size() < 1
          || solrCore.getCoreContainer().isZooKeeperAware()) return filesResponse;
      String includeConfFiles = replicationHandler.getIncludeConfFiles();
      log.debug("Adding config files to list: {}", includeConfFiles);
      // if configuration files need to be included get their details
      filesResponse.confFiles =
          new ArrayList<>(
              replicationHandler.getConfFileInfoFromCache(
                  replicationHandler.getConfFileNameAlias(),
                  replicationHandler.getConfFileInfoCache()));
      filesResponse.status = OK_STATUS;

    } finally {
      if (null != commit) {
        // before releasing the save on our commit point, set a short reserve duration since
        // the main reason remote nodes will ask for the file list is because they are preparing to
        // replicate from us...
        delPol.setReserveDuration(
            commit.getGeneration(), replicationHandler.getReserveCommitDuration());
        delPol.releaseCommitPoint(commit);
      }
    }
    return filesResponse;
  }

  /** This class is used to read and send files in the lucene index */
  protected class DirectoryFileStream implements SolrCore.RawWriter, StreamingOutput {
    protected FastOutputStream fos;

    protected Long indexGen;
    protected IndexDeletionPolicyWrapper delPolicy;

    protected String fileName;
    protected String cfileName;
    protected String tlogFileName;
    protected String sOffset;
    protected String sLen;
    protected final boolean compress;
    protected boolean useChecksum;

    protected long offset = -1;
    protected int len = -1;

    protected Checksum checksum;

    private RateLimiter rateLimiter;

    byte[] buf;

    public DirectoryFileStream(
        String file,
        String dirType,
        String offset,
        String len,
        boolean compression,
        boolean useChecksum,
        double maxWriteMBPerSec,
        Long gen) {
      delPolicy = solrCore.getDeletionPolicy();

      fileName = validateFilenameOrError(file);

      switch (dirType) {
        case CONF_FILE_SHORT:
          cfileName = file;
          break;
        case TLOG_FILE:
          tlogFileName = file;
          break;
        default:
          fileName = file;
          break;
      }

      this.sOffset = offset;
      this.sLen = len;
      this.compress = compression;
      this.useChecksum = useChecksum;
      this.indexGen = gen;
      if (useChecksum) {
        checksum = new Adler32();
      }
      // No throttle if MAX_WRITE_PER_SECOND is not specified
      if (maxWriteMBPerSec == 0) {
        this.rateLimiter = new RateLimiter.SimpleRateLimiter(Double.MAX_VALUE);
      } else {
        this.rateLimiter = new RateLimiter.SimpleRateLimiter(maxWriteMBPerSec);
      }
    }

    // Throw exception on directory traversal attempts
    protected String validateFilenameOrError(String fileName) {
      if (fileName != null) {
        Path filePath = Paths.get(fileName);
        filePath.forEach(
            subpath -> {
              if ("..".equals(subpath.toString())) {
                throw new SolrException(
                    SolrException.ErrorCode.FORBIDDEN, "File name cannot contain ..");
              }
            });
        if (filePath.isAbsolute()) {
          throw new SolrException(SolrException.ErrorCode.FORBIDDEN, "File name must be relative");
        }
        return fileName;
      } else return null;
    }

    protected void initWrite() throws IOException {
      this.offset = (sOffset != null) ? Long.parseLong(sOffset) : -1;
      this.len = (sLen != null) ? Integer.parseInt(sLen) : -1;
      if (fileName == null && cfileName == null && tlogFileName == null) {
        // no filename do nothing
        writeNothingAndFlush();
      }
      buf = new byte[(len == -1 || len > PACKET_SZ) ? PACKET_SZ : len];

      // reserve commit point till write is complete
      if (indexGen != null) {
        delPolicy.saveCommitPoint(indexGen);
      }
    }

    protected void createOutputStream(OutputStream out) {
      // DeflaterOutputStream requires a close call, but don't close the request outputstream
      out = new CloseShieldOutputStream(out);
      if (compress) {
        fos = new FastOutputStream(new DeflaterOutputStream(out));
      } else {
        fos = new FastOutputStream(out);
      }
    }

    protected void extendReserveAndReleaseCommitPoint() {
      ReplicationHandler replicationHandler =
          (ReplicationHandler) solrCore.getRequestHandler(ReplicationHandler.PATH);

      if (indexGen != null) {
        // Reserve the commit point for another 10s for the next file to be to fetched.
        // We need to keep extending the commit reservation between requests so that the replica can
        // fetch all the files correctly.
        delPolicy.setReserveDuration(indexGen, replicationHandler.getReserveCommitDuration());

        // release the commit point as the write is complete
        delPolicy.releaseCommitPoint(indexGen);
      }
    }

    @Override
    public void write(OutputStream out) throws IOException {
      createOutputStream(out);

      IndexInput in = null;
      try {
        initWrite();

        Directory dir = solrCore.withSearcher(searcher -> searcher.getIndexReader().directory());
        in = dir.openInput(fileName, IOContext.READONCE);
        // if offset is mentioned move the pointer to that point
        if (offset != -1) in.seek(offset);

        long filelen = dir.fileLength(fileName);
        long maxBytesBeforePause = 0;

        while (true) {
          offset = offset == -1 ? 0 : offset;
          int read = (int) Math.min(buf.length, filelen - offset);
          in.readBytes(buf, 0, read);

          fos.writeInt(read);
          if (useChecksum) {
            checksum.reset();
            checksum.update(buf, 0, read);
            fos.writeLong(checksum.getValue());
          }
          fos.write(buf, 0, read);
          fos.flush();
          log.debug("Wrote {} bytes for file {}", offset + read, fileName); // nowarn

          // Pause if necessary
          maxBytesBeforePause += read;
          if (maxBytesBeforePause >= rateLimiter.getMinPauseCheckBytes()) {
            rateLimiter.pause(maxBytesBeforePause);
            maxBytesBeforePause = 0;
          }
          if (read != buf.length) {
            writeNothingAndFlush();
            // we close because DeflaterOutputStream requires a close call, but  the request
            // outputstream is protected
            fos.close();
            break;
          }
          offset += read;
          in.seek(offset);
        }
      } catch (IOException e) {
        log.warn(
            "Exception while writing response for params fileName={} cfileName={} tlogFileName={} offset={} len={} compression={} generation={} checksum={}",
            fileName,
            cfileName,
            tlogFileName,
            sOffset,
            sLen,
            compress,
            indexGen,
            useChecksum);
      } finally {
        if (in != null) {
          in.close();
        }
        extendReserveAndReleaseCommitPoint();
      }
    }

    /** Used to write a marker for EOF */
    protected void writeNothingAndFlush() throws IOException {
      fos.writeInt(0);
      fos.flush();
    }
  }

  /** This is used to write files in the conf directory. */
  protected abstract class LocalFsFileStream extends DirectoryFileStream {

    private Path file;

    public LocalFsFileStream(
        String file,
        String dirType,
        String offset,
        String len,
        boolean compression,
        boolean useChecksum,
        double maxWriteMBPerSec,
        Long gen) {
      super(file, dirType, offset, len, compression, useChecksum, maxWriteMBPerSec, gen);
      this.file = this.initFile();
    }

    protected abstract Path initFile();

    @Override
    public void write(OutputStream out) throws IOException {
      createOutputStream(out);
      try {
        initWrite();

        if (Files.isReadable(file)) {
          try (SeekableByteChannel channel = Files.newByteChannel(file)) {
            // if offset is mentioned move the pointer to that point
            if (offset != -1) channel.position(offset);
            ByteBuffer bb = ByteBuffer.wrap(buf);

            while (true) {
              bb.clear();
              long bytesRead = channel.read(bb);
              if (bytesRead <= 0) {
                writeNothingAndFlush();
                // we close because DeflaterOutputStream requires a close call, but the request
                // outputstream is protected
                fos.close();
                break;
              }
              fos.writeInt((int) bytesRead);
              if (useChecksum) {
                checksum.reset();
                checksum.update(buf, 0, (int) bytesRead);
                fos.writeLong(checksum.getValue());
              }
              fos.write(buf, 0, (int) bytesRead);
              fos.flush();
            }
          }
        } else {
          writeNothingAndFlush();
        }
      } catch (IOException e) {
        log.warn(
            "Exception while writing response for params fileName={} cfileName={} tlogFileName={} offset={} len={} compression={} generation={} checksum={}",
            fileName,
            cfileName,
            tlogFileName,
            sOffset,
            sLen,
            compress,
            indexGen,
            useChecksum);
      } finally {
        extendReserveAndReleaseCommitPoint();
      }
    }
  }

  protected class LocalFsTlogFileStream extends LocalFsFileStream {

    public LocalFsTlogFileStream(
        String file,
        String dirType,
        String offset,
        String len,
        boolean compression,
        boolean useChecksum,
        double maxWriteMBPerSec,
        Long gen) {
      super(file, dirType, offset, len, compression, useChecksum, maxWriteMBPerSec, gen);
    }

    @Override
    protected Path initFile() {
      // if it is a tlog file read from tlog directory
      return Path.of(solrCore.getUpdateHandler().getUpdateLog().getTlogDir(), tlogFileName);
    }
  }

  protected class LocalFsConfFileStream extends LocalFsFileStream {

    public LocalFsConfFileStream(
        String file,
        String dirType,
        String offset,
        String len,
        boolean compression,
        boolean useChecksum,
        double maxWriteMBPerSec,
        Long gen) {
      super(file, dirType, offset, len, compression, useChecksum, maxWriteMBPerSec, gen);
    }

    @Override
    protected Path initFile() {
      // if it is a conf file read from config directory
      return solrCore.getResourceLoader().getConfigPath().resolve(cfileName);
    }
  }

  private void reportErrorOnResponse(
      FileListResponse fileListResponse, String message, Exception e) {
    fileListResponse.status = ERR_STATUS;
    fileListResponse.message = message;
    if (e != null) {
      fileListResponse.exception = e;
    }
  }
}
