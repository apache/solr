package org.apache.solr.core.backup.repository;

import java.io.IOException;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.store.Directory;
import org.apache.solr.common.util.NamedList;

/**
 * Abstract {@link BackupRepository} implementation providing some logic shared between real
 * implementations.
 */
public abstract class AbstractBackupRepository implements BackupRepository {

  /**
   * Plugin initialization parameter to define whether the {@link BackupRepository} should verify
   * the checksum before copying index files. Defaults to {@code true}.
   *
   * <p>If the checksum cannot be verified in the standard Lucene way ({@link
   * CodecUtil#checkFooter}, then this parameter can be set to false, and the checksum should be
   * verified in a specific way.
   */
  public static final String PARAM_VERIFY_CHECKSUM = "verifyChecksum";

  protected NamedList<?> config;
  protected boolean shouldVerifyChecksum;

  @Override
  public void init(NamedList<?> args) {
    config = args;
    shouldVerifyChecksum = getBooleanConfig(args, PARAM_VERIFY_CHECKSUM, true);
  }

  /**
   * Copies an index file from a specified {@link Directory} to a destination {@link Directory}.
   * Also verifies the checksum unless {@link #PARAM_VERIFY_CHECKSUM} was false in the {@link
   * #init(NamedList)} arguments.
   *
   * @param sourceDir The source directory hosting the file to be copied.
   * @param sourceFileName The name of the file to be copied
   * @param destDir The destination directory.
   * @throws CorruptIndexException in case checksum of the file does not match with precomputed
   *     checksum stored at the end of the file
   */
  @Override
  public void copyIndexFileFrom(
      Directory sourceDir, String sourceFileName, Directory destDir, String destFileName)
      throws IOException {
    if (shouldVerifyChecksum) {
      BackupRepository.super.copyIndexFileFrom(sourceDir, sourceFileName, destDir, destFileName);
    } else {
      copyFileNoChecksum(sourceDir, sourceFileName, destDir, destFileName);
    }
  }

  protected static boolean getBooleanConfig(NamedList<?> args, String param, boolean defaultValue) {
    Object value = args.get(param);
    return value == null ? defaultValue : Boolean.parseBoolean(value.toString());
  }
}
