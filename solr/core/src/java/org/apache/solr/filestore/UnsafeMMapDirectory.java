package org.apache.solr.filestore;

import org.agrona.BufferUtil;
import org.apache.lucene.store.*;
import org.apache.lucene.util.Constants;

import java.io.IOException;
import java.nio.ByteOrder;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Locale;


public class UnsafeMMapDirectory extends NIOFSDirectory {
  private static final UnsafeByteBufferGuard.BufferCleaner CLEANER = (resourceDescription, b) -> BufferUtil.free(b);
  private boolean preload;

  /**
   * Default max chunk size.
   *
   * @see #UnsafeMMapDirectory(Path, LockFactory, int)
   */
  public static final int DEFAULT_MAX_CHUNK_SIZE = Constants.JRE_IS_64BIT ? (1 << 30) : (1 << 28);

  final int chunkSizePower;

  /**
   * Create a new MMapDirectory for the named location. The directory is created at the named
   * location if it does not yet exist.
   *
   * @param path the path of the directory
   * @param lockFactory the lock factory to use
   * @throws IOException if there is a low-level I/O error
   */
  public UnsafeMMapDirectory(Path path, LockFactory lockFactory) throws IOException {
    this(path, lockFactory, DEFAULT_MAX_CHUNK_SIZE);
  }

  /**
   * Create a new MMapDirectory for the named location and {@link FSLockFactory#getDefault()}. The
   * directory is created at the named location if it does not yet exist.
   *
   * @param path the path of the directory
   * @throws IOException if there is a low-level I/O error
   */
  public UnsafeMMapDirectory(Path path) throws IOException {
    this(path, FSLockFactory.getDefault());
  }

  /**
   * Create a new MMapDirectory for the named location and {@link FSLockFactory#getDefault()}. The
   * directory is created at the named location if it does not yet exist.
   *
   * @param path the path of the directory
   * @param maxChunkSize maximum chunk size (default is 1 GiBytes for 64 bit JVMs and 256 MiBytes
   *     for 32 bit JVMs) used for memory mapping.
   * @throws IOException if there is a low-level I/O error
   */
  public UnsafeMMapDirectory(Path path, int maxChunkSize) throws IOException {
    this(path, FSLockFactory.getDefault(), maxChunkSize);
  }

  /**
   * Create a new MMapDirectory for the named location, specifying the maximum chunk size used for
   * memory mapping. The directory is created at the named location if it does not yet exist.
   *
   * <p>Especially on 32 bit platform, the address space can be very fragmented, so large index
   * files cannot be mapped. Using a lower chunk size makes the directory implementation a little
   * bit slower (as the correct chunk may be resolved on lots of seeks) but the chance is higher
   * that mmap does not fail. On 64 bit Java platforms, this parameter should always be {@code 1 <<
   * 30}, as the address space is big enough.
   *
   * <p><b>Please note:</b> The chunk size is always rounded down to a power of 2.
   *
   * @param path the path of the directory
   * @param lockFactory the lock factory to use, or null for the default ({@link
   *     NativeFSLockFactory});
   * @param maxChunkSize maximum chunk size (default is 1 GiBytes for 64 bit JVMs and 256 MiBytes
   *     for 32 bit JVMs) used for memory mapping.
   * @throws IOException if there is a low-level I/O error
   */
  public UnsafeMMapDirectory(Path path, LockFactory lockFactory, int maxChunkSize) throws IOException {
    super(path, lockFactory);
    if (maxChunkSize <= 0) {
      throw new IllegalArgumentException("Maximum chunk size for mmap must be >0");
    }
    this.chunkSizePower = 31 - Integer.numberOfLeadingZeros(maxChunkSize);
    assert this.chunkSizePower >= 0 && this.chunkSizePower <= 30;
  }

  /**
   * Set to {@code true} to ask mapped pages to be loaded into physical memory on init. The behavior
   * is best-effort and operating system dependent.
   *
   * @see MappedByteBuffer#load
   */
  public void setPreload(boolean preload) {
    this.preload = preload;
  }

  /**
   * Returns {@code true} if mapped pages should be loaded.
   *
   * @see #setPreload
   */
  public boolean getPreload() {
    return preload;
  }

  /**
   * Returns the current mmap chunk size.
   *
   * @see #UnsafeMMapDirectory(Path, LockFactory, int)
   */
  public final int getMaxChunkSize() {
    return 1 << chunkSizePower;
  }

  /** Creates an IndexInput for the file with the given name. */
  @Override
  public IndexInput openInput(String name, IOContext context) throws IOException {
    ensureOpen();
    ensureCanRead(name);
    Path path = directory.resolve(name);
    try (FileChannel c = FileChannel.open(path, StandardOpenOption.READ)) {
      final String resourceDescription = "MMapIndexInput(path=\"" + path.toString() + "\")";

      return ByteBufferIndexInput.newInstance(
          resourceDescription,
          map(resourceDescription, c, 0, c.size()),
          c.size(),
          chunkSizePower,
          new UnsafeByteBufferGuard(resourceDescription, CLEANER));
    }
  }

  /** Maps a file into a set of buffers */
  final BulkUnsafeBuffer map(String resourceDescription, FileChannel fc, long offset, long length)
      throws IOException {





      MappedByteBuffer buffer;
      try {
        buffer = fc.map(MapMode.READ_ONLY, offset, length);
        buffer.order(ByteOrder.LITTLE_ENDIAN);
      } catch (IOException ioe) {
        throw convertMapFailedIOException(ioe, resourceDescription, length - offset);
      }
      if (preload) {
        buffer.load();
      }
      BulkUnsafeBuffer buf = new BulkUnsafeBuffer();
      buf.wrap(buffer);



    return buf;
  }

  private IOException convertMapFailedIOException(
      IOException ioe, String resourceDescription, long bufSize) {
    final String originalMessage;
    final Throwable originalCause;
    if (ioe.getCause() instanceof OutOfMemoryError) {
      // nested OOM confuses users, because it's "incorrect", just print a plain message:
      originalMessage = "Map failed";
      originalCause = null;
    } else {
      originalMessage = ioe.getMessage();
      originalCause = ioe.getCause();
    }
    final String moreInfo;
    if (!Constants.JRE_IS_64BIT) {
      moreInfo =
          "MMapDirectory should only be used on 64bit platforms, because the address space on 32bit operating systems is too small. ";
    } else if (Constants.WINDOWS) {
      moreInfo =
          "Windows is unfortunately very limited on virtual address space. If your index size is several hundred Gigabytes, consider changing to Linux. ";
    } else if (Constants.LINUX) {
      moreInfo =
          "Please review 'ulimit -v', 'ulimit -m' (both should return 'unlimited'), and 'sysctl vm.max_map_count'. ";
    } else {
      moreInfo = "Please review 'ulimit -v', 'ulimit -m' (both should return 'unlimited'). ";
    }
    final IOException newIoe =
        new IOException(
            String.format(
                Locale.ENGLISH,
                "%s: %s [this may be caused by lack of enough unfragmented virtual address space "
                    + "or too restrictive virtual memory limits enforced by the operating system, "
                    + "preventing us to map a chunk of %d bytes. %sMore information: "
                    + "http://blog.thetaphi.de/2012/07/use-lucenes-mmapdirectory-on-64bit.html]",
                originalMessage,
                resourceDescription,
                bufSize,
                moreInfo),
            originalCause);
    newIoe.setStackTrace(ioe.getStackTrace());
    return newIoe;
  }





}
