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

package org.apache.lucene.gradle;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

/** Checks that ChangesToHtml renders changelog markdown to the expected Changes.html output. */
public class ChangesToHtmlTest {

  @Test
  public void releaseSectionAndJiraItemAreRendered() {
    String html =
        ChangesToHtml.render(
            "[9.9.0] - 2025-07-24\n"
                + "\n"
                + "### Added (1 changes)\n"
                + "- [SOLR-123](https://issues.apache.org/jira/browse/SOLR-123) Add a thing (Jane Doe)\n");

    assertTrue(
        html.contains(
            "<h2><a id=\"v9.9.0\" href=\"javascript:toggleList('v9.9.0')\">"
                + "Release 9.9.0 [2025-07-24]</a></h2>\n"));
    assertTrue(
        html.contains(
            "<a id=\"v9.9.0.added\" href=\"javascript:toggleList('v9.9.0.added')\">Added</a>"
                + "&nbsp;&nbsp;&nbsp;(1)\n"));
    assertTrue(
        html.contains(
            "      <li><a href=\"https://issues.apache.org/jira/browse/SOLR-123\">SOLR-123</a>"
                + ": Add a thing<br /><span class=\"attrib\">(Jane Doe)</span></li>\n"));
    assertTrue(html.endsWith("</body>\n</html>\n\n"));
  }

  @Test
  public void jiraItemWithAuthorIsRendered() {
    assertItem(
        "[SOLR-123](https://issues.apache.org/jira/browse/SOLR-123) Add a thing (Jane Doe)",
        "<a href=\"https://issues.apache.org/jira/browse/SOLR-123\">SOLR-123</a>: Add a thing"
            + "<br /><span class=\"attrib\">(Jane Doe)</span>");
  }

  @Test
  public void plainPullRequestReferenceIsRendered() {
    assertItem(
        "Fix the widget #1234 (Jane Doe)",
        "<a href=\"https://github.com/apache/solr/pull/1234\">PR#1234</a>: Fix the widget"
            + "<br /><span class=\"attrib\">(Jane Doe)</span>");
  }

  @Test
  public void authorsWithGithubHandleAndSecondAuthorAreRendered() {
    assertItem(
        "[SOLR-2](https://issues.apache.org/jira/browse/SOLR-2) Something (Jane Doe @janedoe, John Roe)",
        "<a href=\"https://issues.apache.org/jira/browse/SOLR-2\">SOLR-2</a>: Something"
            + "<br /><span class=\"attrib\">(<a href=\"https://github.com/janedoe\">Jane Doe</a>,"
            + " John Roe)</span>");
  }

  @Test
  public void descriptionIsHtmlEscaped() {
    assertItem(
        "[SOLR-3](https://issues.apache.org/jira/browse/SOLR-3) Handle <b> tags",
        "<a href=\"https://issues.apache.org/jira/browse/SOLR-3\">SOLR-3</a>: Handle &lt;b&gt; tags");
  }

  @Test
  public void itemWithoutIssueOrAuthorIsLinkified() {
    assertItem(
        "Tidy the thing see https://example.com/doc",
        "Tidy the thing see <a href=\"https://example.com/doc\">https://example.com/doc</a>");
  }

  @Test
  public void olderReleasesAreCollapsedFromTheThirdRelease() {
    String html = ChangesToHtml.render("[9.9.0]\n\n[9.8.0]\n\n[9.7.0]\n");

    assertTrue(
        html.contains(
            "<h2><a id=\"v9.8.0\" href=\"javascript:toggleList('v9.8.0')\">Release 9.8.0</a></h2>\n"));
    assertTrue(
        html.contains(
            "<h2><a id=\"older\" href=\"javascript:toggleList('older');\">Older Releases</a></h2>\n"
                + "<div id=\"older.list\">\n"
                + "<h3><a id=\"v9.7.0\" href=\"javascript:toggleList('v9.7.0')\">Release 9.7.0</a></h3>\n"));
    assertTrue(html.endsWith("</div>\n</body>\n</html>\n\n"));
  }

  @Test
  public void collapseRegexEscapesReleaseIds() {
    String html = ChangesToHtml.render("[9.9.0]\n\n[9.8.0]\n");

    assertTrue(
        html.contains(
            "    var newerRegex = new RegExp(\"^(?:v9\\\\.9\\\\.0|v9\\\\.8\\\\.0)\");\n"));
  }

  @Test
  public void emptyChangelogUsesNoneForMissingRelease() {
    String html = ChangesToHtml.render("");

    assertTrue(html.contains("    var newerRegex = new RegExp(\"^(?:trunk)\");\n"));
    assertTrue(html.contains("if (list.id != 'None.list'"));
    assertTrue(html.endsWith("</body>\n</html>\n\n"));
  }

  @Test
  public void preambleMarkdownLinkBecomesHtmlLink() {
    String html =
        ChangesToHtml.render("See [the site](https://solr.apache.org) for details\n\n[9.9.0]\n");

    assertTrue(
        html.contains(
            "<p>See <a href=\"https://solr.apache.org\">the site</a> for details</p>\n\n"));
  }

  @Test
  public void crlfAndLfInputRenderTheSamePage() {
    String lf = "[9.9.0] - 2025-07-24\n\n### Added\n- Plain item\n";

    assertEquals(ChangesToHtml.render(lf), ChangesToHtml.render(lf.replace("\n", "\r\n")));
  }

  private static void assertItem(String item, String expectedHtml) {
    String html = ChangesToHtml.render("[9.9.0]\n\n### Changed\n- " + item + "\n");

    assertTrue(html.contains("      <li>" + expectedHtml + "</li>\n"));
  }
}
