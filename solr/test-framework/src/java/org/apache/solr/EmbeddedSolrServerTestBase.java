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
package org.apache.solr;

import com.google.common.io.ByteStreams;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.common.util.ContentStream;
import org.apache.solr.common.util.ContentStreamBase;
import org.apache.solr.common.util.ContentStreamBase.ByteArrayStream;
import org.apache.solr.util.EmbeddedSolrServerTestRule;
import org.apache.solr.util.SolrClientTestRule;
import org.junit.Rule;

public abstract class EmbeddedSolrServerTestBase extends SolrTestCaseJ4 {

  @Rule
  public static SolrClientTestRule solrClientTestRule = new EmbeddedSolrServerTestRule(SolrJettyTestBase.legacyExampleCollection1SolrHome());


  public SolrClient getSolrClient() {
    return solrClientTestRule.getSolrClient();
  }


  public void upload(final String collection, final ContentStream... contents) {
    final Path base = solrClientTestRule.getSolrHome().resolve(collection);
    writeTo(base, contents);
  }

  private void writeTo(final Path base, final ContentStream... contents) {
    try {
      Files.createDirectories(base);

      for (final ContentStream content : contents) {
        final File file = new File(base.toFile(), content.getName());
        file.getParentFile().mkdirs();

        try (OutputStream os = new FileOutputStream(file)) {
          ByteStreams.copy(content.getStream(), os);
        }
      }
    } catch (final IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  public Collection<ContentStream> download(final String collection, final String... names) {
    final Path base = solrClientTestRule.getSolrHome().resolve(collection);
    final List<ContentStream> result = new ArrayList<>();

    if (Files.exists(base)) {
      for (final String name : names) {
        final File file = new File(base.toFile(), name);
        if (file.exists() && file.canRead()) {
          try {
            final ByteArrayOutputStream os = new ByteArrayOutputStream();
            ByteStreams.copy(new FileInputStream(file), os);
            final ByteArrayStream stream =
                new ContentStreamBase.ByteArrayStream(os.toByteArray(), name);
            result.add(stream);
          } catch (final IOException e) {
            throw new RuntimeException(e);
          }
        }
      }
    }

    return result;
  }

}
