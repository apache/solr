package org.apache.solr.s3;

import com.adobe.testing.s3mock.testsupport.common.S3MockStarter;
import java.util.Map;

public class S3Mock extends S3MockStarter {

  /** Creates an instance with the default configuration. */
  public S3Mock() {
    super(null);
  }

  public static Builder builder() {
    return new Builder();
  }

  private S3Mock(final Map<String, Object> properties) {
    super(properties);
  }

  public void start() {
    super.start();
  }

  public void stop() {
    super.stop();
  }

  public static class Builder extends S3MockStarter.BaseBuilder<S3Mock> {

    @Override
    public S3Mock build() {
      return new S3Mock(arguments);
    }
  }
}
