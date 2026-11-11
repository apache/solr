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

package org.apache.solr.shell;

import org.junit.Test;

/**
 * Port of {@code test_adminconsole_urls.bats}.
 *
 * <p>Kept close to the original, including the order of the tests and the text each asserts on.
 * Where it differs: each test starts its own node, as the original does, because the last one needs
 * a system property on the command line; the original's repeated {@code curl} calls against one URL
 * (one for the status code, one for the headers, one for the body) collapse into a single {@code
 * http()} request whose status, headers and body are all asserted; and every {@code solr start} and
 * every header assertion also asserts the status, so a 404 cannot pass as a missing header.
 */
public class AdminConsoleUrlsTest extends SolrShellTestCase {

  @Test
  public void assertAbleToLaunchSolrAdminConsole() {
    run("solr start").assertSuccess();

    http("/solr/").assertStatus(200).assertHeaderContains("text/html").assertContains("<html");

    http("/solr/css/angular/chosen.css")
        .assertStatus(200)
        .assertHeaderContains("text/css")
        .assertContains("{");
  }

  @Test
  public void assertAdminConsoleStaticAssetsAreRevalidatedAndVersioned() {
    run("solr start").assertSuccess();

    http("/solr/libs/solr/index.js")
        .assertStatus(200)
        .assertHeaderContains("Cache-Control: no-cache");

    http("/solr/").assertStatus(200).assertNotMatches("(src|href)=\"[^\"]+\\.(js|css)\"");
  }

  @Test
  public void assertCspHeaderContainsCustomConnectSrcUrls() {
    // The original quotes the comma-separated value for the shell; unquoted it is one token on bash
    // and solr.cmd rejoins the pieces cmd splits on commas, so no quoting is needed either way.
    run("solr start -Dsolr.ui.headers.csp.connect-src.urls="
            + "http://example1.com/token,https://example2.com/path/uri1,http://example3.com/oauth2/uri2")
        .assertSuccess();

    http("/solr/")
        .assertStatus(200)
        .assertHeaderContains("http://example1.com/token")
        .assertHeaderContains("https://example2.com/path/uri1")
        .assertHeaderContains("http://example3.com/oauth2/uri2");
  }
}
