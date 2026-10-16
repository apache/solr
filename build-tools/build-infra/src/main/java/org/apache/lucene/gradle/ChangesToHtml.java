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

import static org.apache.lucene.gradle.PythonCompat.escapeRegex;
import static org.apache.lucene.gradle.PythonCompat.replaceAll;
import static org.apache.lucene.gradle.PythonCompat.strip;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** Renders Solr's CHANGELOG.md as the Changes.html release notes page. */
public final class ChangesToHtml {

  private static final int FLAGS = Pattern.UNICODE_CHARACTER_CLASS | Pattern.UNIX_LINES;

  private static final Pattern RELEASE_PATTERN =
      Pattern.compile("^\\[(\\d+(?:\\.\\d+)*(?:-[a-zA-Z0-9.]+)?)\\](\\s+-\\s+(.+))?$", FLAGS);
  private static final Pattern SECTION_PATTERN =
      Pattern.compile("^###\\s+(\\w+(?:\\s+\\w+)*)\\s*(?:\\(\\d+\\s+changes?\\))?", FLAGS);

  private static final String JIRA_URL_PREFIX = "https://issues.apache.org/jira/browse/";
  private static final String GITHUB_PR_PREFIX = "https://github.com/apache/solr/pull/";

  private static final List<IssuePattern> ISSUE_PATTERNS =
      List.of(
          new IssuePattern(
              Pattern.compile(
                  "\\[([A-Z]+-\\d+)\\]\\(https://issues\\.apache\\.org/jira/browse/\\1\\)", FLAGS),
              JIRA_URL_PREFIX,
              ""),
          new IssuePattern(
              Pattern.compile(
                  "\\[PR#(\\d+)\\]\\(https://github\\.com/apache/solr/pull/\\1\\)", FLAGS),
              GITHUB_PR_PREFIX,
              "PR#"),
          new IssuePattern(
              Pattern.compile(
                  "\\[GITHUB#(\\d+)\\]\\(https://github\\.com/apache/solr/issues/\\1\\)", FLAGS),
              "https://github.com/apache/solr/issues/",
              "GITHUB#"));

  private static final Pattern PLAIN_PR_REFERENCES =
      Pattern.compile("#(\\d+)(?:\\s+#(\\d+))*\\s*(?=\\(|$)", FLAGS);
  private static final Pattern PR_NUMBER = Pattern.compile("#(\\d+)", FLAGS);
  private static final Pattern MARKDOWN_LINK =
      Pattern.compile("\\[([^\\]]+)\\]\\(([^)]+)\\)", FLAGS);
  private static final Pattern GITHUB_HANDLE = Pattern.compile("@([\\w-]+)", FLAGS);
  private static final Pattern AUTHOR_SEPARATOR = Pattern.compile(",\\s*|\\s+and\\s+", FLAGS);
  private static final Pattern LEADING_SEPARATORS = Pattern.compile("^[:\\s]+", FLAGS);
  private static final Pattern JIRA_KEY = Pattern.compile("([A-Z]+-\\d+)", FLAGS);
  private static final Pattern BARE_URL_IN_TEXT =
      Pattern.compile("(?<![\"'>])(https?://[^\\s)]+)", FLAGS);
  private static final Pattern BARE_URL_IN_MARKDOWN =
      Pattern.compile("(?<![\">])(https?://[^\\s)]+)", FLAGS);

  private static final String HEADER_TEMPLATE =
"""
<!--
**********************************************************
** WARNING: This file is generated from CHANGELOG.md by the
**          ChangesToHtml, invoked by the changesToHtml task.
**          Do *not* edit this file!
**********************************************************

****************************************************************************
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
****************************************************************************
-->
<!DOCTYPE html>
<html lang="en">
<head>
  <title>Apache Solr Release Notes</title>
  <link rel="stylesheet" href="ChangesFancyStyle.css" title="Fancy">
  <link rel="alternate stylesheet" href="ChangesSimpleStyle.css" title="Simple">
  <link rel="alternate stylesheet" href="ChangesFixedWidthStyle.css" title="Fixed Width">
  <META http-equiv="Content-Type" content="text/html; charset=UTF-8"/>
  <SCRIPT>
    function toggleList(id) {
      listStyle = document.getElementById(id + '.list').style;
      anchor = document.getElementById(id);
      if (listStyle.display == 'none') {
        listStyle.display = 'block';
        anchor.title = 'Click to collapse';
        location.href = '#' + id;
      } else {
        listStyle.display = 'none';
        anchor.title = 'Click to expand';
      }
      var expandButton = document.getElementById('expand.button');
      expandButton.disabled = false;
      var collapseButton = document.getElementById('collapse.button');
      collapseButton.disabled = false;
    }

    function collapseAll() {
      var unorderedLists = document.getElementsByTagName("ul");
      for (var i = 0; i < unorderedLists.length; i++) {
        if (unorderedLists[i].className != 'bulleted-list')
          unorderedLists[i].style.display = "none";
        else
          unorderedLists[i].style.display = "block";
      }
      var orderedLists = document.getElementsByTagName("ol");
      for (var i = 0; i < orderedLists.length; i++)
        orderedLists[i].style.display = "none";
      var olderList = document.getElementById("older.list");
      if (olderList) olderList.style.display = "none";
      var anchors = document.getElementsByTagName("a");
      for (var i = 0 ; i < anchors.length; i++) {
        if (anchors[i].id != '')
          anchors[i].title = 'Click to expand';
      }
      var collapseButton = document.getElementById('collapse.button');
      collapseButton.disabled = true;
      var expandButton = document.getElementById('expand.button');
      expandButton.disabled = false;
    }

    function expandAll() {
      var unorderedLists = document.getElementsByTagName("ul");
      for (var i = 0; i < unorderedLists.length; i++)
        unorderedLists[i].style.display = "block";
      var orderedLists = document.getElementsByTagName("ol");
      for (var i = 0; i < orderedLists.length; i++)
        orderedLists[i].style.display = "block";
      var olderList = document.getElementById("older.list");
      if (olderList) olderList.style.display = "block";
      var anchors = document.getElementsByTagName("a");
      for (var i = 0 ; i < anchors.length; i++) {
        if (anchors[i].id != '')
          anchors[i].title = 'Click to collapse';
      }
      var expandButton = document.getElementById('expand.button');
      expandButton.disabled = true;
      var collapseButton = document.getElementById('collapse.button');
      collapseButton.disabled = false;
    }

    var newerRegex = new RegExp("@NEWER_VERSION_REGEX@");
    function isOlder(listId) {
      return ! newerRegex.test(listId);
    }

    function escapeMeta(s) {
      return s.replace(/([.*+?^${}()|[\\\\]\\\\\\/])/g, '\\\\\\\\$1');
    }

    function shouldExpand(currentList, currentAnchor, listId) {
      var listName = listId.substring(0, listId.length - 5);
      var parentRegex = new RegExp("^" + escapeMeta(listName) + "\\\\\\\\.");
      return currentList == listId
             || (isOlder(currentAnchor) && listId == 'older.list')
             || parentRegex.test(currentAnchor);
    }

    function collapse() {
      /* Collapse all but the first and second releases. */
      var unorderedLists = document.getElementsByTagName("ul");
      var currentAnchor = location.hash.substring(1);
      var currentList = currentAnchor + ".list";

      for (var i = 0; i < unorderedLists.length; i++) {
        var list = unorderedLists[i];
        /* Collapse the current item, unless either the current item is one of
         * the first two releases, or the current URL has a fragment and the
         * fragment refers to the current item or one of its ancestors.
         */
        if (list.id != '@FIRST_RELID@.list'
            && list.id != '@SECOND_RELID@.list'
            && list.className != 'bulleted-list'
            && (currentAnchor == ''
                || ! shouldExpand(currentList, currentAnchor, list.id))) {
          list.style.display = "none";
        }
      }
      var orderedLists = document.getElementsByTagName("ol");
      for (var i = 0; i < orderedLists.length; i++) {
        var list = orderedLists[i];
        /* Collapse the current item, unless the current URL has a fragment
         * and the fragment refers to the current item or one of its ancestors.
         */
        if (currentAnchor == ''
            || ! shouldExpand(currentList, currentAnchor, list.id)) {
          list.style.display = "none";
        }
      }
      var olderList = document.getElementById("older.list");
      if (olderList) olderList.style.display = "none";
      /* Add "Click to collapse/expand" tooltips to the release/section headings */
      var anchors = document.getElementsByTagName("a");
      for (var i = 0 ; i < anchors.length; i++) {
        var anchor = anchors[i];
        if (anchor.id != '') {
          if (anchor.id == '@FIRST_RELID@' || anchor.id == '@SECOND_RELID@') {
            anchor.title = 'Click to collapse';
          } else {
            anchor.title = 'Click to expand';
          }
        }
      }

      /* Insert "Expand All" and "Collapse All" buttons */
      var buttonsParent = document.getElementById('buttons.parent');
      if (buttonsParent) {
        var expandButton = document.createElement('button');
        expandButton.appendChild(document.createTextNode('Expand All'));
        expandButton.onclick = function() { expandAll(); }
        expandButton.id = 'expand.button';
        buttonsParent.appendChild(expandButton);
        var collapseButton = document.createElement('button');
        collapseButton.appendChild(document.createTextNode('Collapse All'));
        collapseButton.onclick = function() { collapseAll(); }
        collapseButton.id = 'collapse.button';
        buttonsParent.appendChild(collapseButton);
      }
    }

    window.onload = collapse;
  </SCRIPT>
</head>
<body>

<h1>Apache Solr Release Notes</h1>

<div id="buttons.parent"></div>

""";

  private ChangesToHtml() {}

  /** Reads a changelog and writes the rendered page, with LF line endings on every platform. */
  public static void write(Path changelog, Path output) throws IOException {
    String html = render(Files.readString(changelog, StandardCharsets.UTF_8));
    Files.writeString(output, html, StandardCharsets.UTF_8);
  }

  /** Returns the rendered page with LF line endings and a trailing newline. */
  public static String render(String changelog) {
    String text = changelog.replace("\r\n", "\n").replace('\r', '\n');
    Changelog parsed = parse(text);
    return new Generator().generate(parsed.releases, parsed.preamble) + "\n";
  }

  private static Changelog parse(String content) {
    String preamble = null;
    List<Release> releases = new ArrayList<>();
    Release current = null;
    String section = null;
    List<String> items = new ArrayList<>();
    String[] lines = content.split("\n", -1);
    int i = 0;
    while (i < lines.length) {
      String line = lines[i];
      String stripped = strip(line);

      if (stripped.startsWith("<!--") || stripped.startsWith("-->")) {
        i++;
        continue;
      }

      if (current == null && preamble == null && !stripped.isEmpty() && !stripped.startsWith("[")) {
        preamble = stripped;
        i++;
        continue;
      }

      Matcher release = RELEASE_PATTERN.matcher(line);
      if (release.lookingAt()) {
        saveSection(current, section, items);
        if (current != null) {
          releases.add(current);
        }
        String date = release.group(3) == null ? null : strip(release.group(3));
        current = new Release(release.group(1), date);
        section = null;
        items = new ArrayList<>();
        i++;
        continue;
      }

      Matcher sectionMatch = SECTION_PATTERN.matcher(line);
      if (sectionMatch.lookingAt() && current != null) {
        saveSection(current, section, items);
        section = sectionMatch.group(1);
        items = new ArrayList<>();
        i++;
        continue;
      }

      if (line.startsWith("- ") && current != null) {
        StringBuilder item = new StringBuilder(line.substring(2));
        i++;
        while (i < lines.length && !startsItem(lines[i])) {
          String continuation = strip(lines[i]);
          if (!continuation.isEmpty()) {
            item.append(' ').append(continuation);
          }
          i++;
        }
        items.add(item.toString());
        continue;
      }

      i++;
    }
    saveSection(current, section, items);
    if (current != null) {
      releases.add(current);
    }
    return new Changelog(preamble, releases);
  }

  private static boolean startsItem(String line) {
    return line.startsWith("###") || line.startsWith("[") || line.startsWith("- ");
  }

  private static void saveSection(Release release, String section, List<String> items) {
    if (release != null && section != null && !items.isEmpty()) {
      release.sections.add(new Section(section, items));
    }
  }

  static String escapeHtml(String text) {
    return text.replace("<", "&lt;").replace(">", "&gt;");
  }

  private static String formatIssueLink(String urlPrefix, String issueId, String label) {
    return "<a href=\"" + urlPrefix + issueId + "\">" + label + "</a>";
  }

  private static String relid(String version) {
    return ("v" + version).replace(' ', '_').toLowerCase(Locale.ROOT);
  }

  private static String pyStr(String value) {
    return value == null ? "None" : value;
  }

  private static final class Generator {

    private String firstRelid;
    private String secondRelid;

    String generate(List<Release> releases, String preamble) {
      if (!releases.isEmpty()) {
        firstRelid = relid(releases.get(0).version);
      }
      if (releases.size() > 1) {
        secondRelid = relid(releases.get(1).version);
      } else {
        secondRelid = firstRelid;
      }
      return generateHeader(preamble) + generateReleases(releases) + "</body>\n</html>\n";
    }

    private String generateHeader(String preamble) {
      String firstRegex =
          escapeRegex(firstRelid == null || firstRelid.isEmpty() ? "trunk" : firstRelid)
              .replace("\\", "\\\\");
      String secondRegex =
          escapeRegex(secondRelid == null ? "" : secondRelid).replace("\\", "\\\\");

      String newerVersionRegex = "^(?:" + firstRegex;
      if (secondRelid != null && !secondRelid.isEmpty()) {
        newerVersionRegex += "|" + secondRegex;
      }
      newerVersionRegex += ")";

      String html =
          HEADER_TEMPLATE
              .replace("@NEWER_VERSION_REGEX@", newerVersionRegex)
              .replace("@FIRST_RELID@", pyStr(firstRelid))
              .replace("@SECOND_RELID@", pyStr(secondRelid));

      if (preamble != null && !preamble.isEmpty()) {
        html += "<p>" + convertMarkdownLinks(preamble) + "</p>\n\n";
      }
      return html;
    }

    private String generateReleases(List<Release> releases) {
      StringBuilder html = new StringBuilder();
      int relcnt = 0;

      for (Release release : releases) {
        if (release.version == null || release.version.isEmpty()) {
          continue;
        }

        relcnt++;
        if (relcnt == 3) {
          html.append(
              "<h2><a id=\"older\" href=\"javascript:toggleList('older');\">Older Releases</a></h2>\n");
          html.append("<div id=\"older.list\">\n");
        }

        String header = relcnt > 2 ? "h3" : "h2";
        String relid = relid(release.version);

        html.append("<")
            .append(header)
            .append("><a id=\"")
            .append(relid)
            .append("\" href=\"javascript:toggleList('")
            .append(relid)
            .append("')\">Release ")
            .append(escapeHtml(release.version));
        if (release.date != null && !release.date.isEmpty()) {
          html.append(" [").append(escapeHtml(release.date)).append("]");
        }
        html.append("</a></").append(header).append(">\n");
        html.append("<ul id=\"").append(relid).append(".list\">\n");

        for (Section section : release.sections) {
          if (section.name != null && !section.name.isEmpty()) {
            html.append(formatSection(relid, section.name, section.items));
          }
        }

        html.append("</ul>\n");
      }

      if (relcnt > 2) {
        html.append("</div>\n");
      }

      return html.toString();
    }

    private static String formatSection(String relid, String sectionName, List<String> items) {
      String sectid = sectionName.toLowerCase(Locale.ROOT).replace(' ', '_');
      StringBuilder html = new StringBuilder();
      html.append("  <li><a id=\"")
          .append(relid)
          .append('.')
          .append(sectid)
          .append("\" href=\"javascript:toggleList('")
          .append(relid)
          .append('.')
          .append(sectid)
          .append("')\">")
          .append(escapeHtml(sectionName))
          .append("</a>");
      html.append("&nbsp;&nbsp;&nbsp;(").append(items.size()).append(")\n");
      html.append("    <ul id=\"").append(relid).append('.').append(sectid).append(".list\">\n");
      for (String item : items) {
        html.append("      <li>").append(formatChangelogItem(item)).append("</li>\n");
      }
      html.append("    </ul>\n");
      return html.toString();
    }
  }

  private static String formatChangelogItem(String itemText) {
    IssueParts issue = extractIssueFromText(itemText);

    String description;
    List<String> authors;
    if (issue.descriptionHead != null) {
      Authors parsed = extractAuthors(issue.tail);
      authors = parsed.formatted;
      String tailRemainder = strip(parsed.text);
      description = strip(issue.descriptionHead);
      if (!tailRemainder.isEmpty()) {
        description = strip(description + " " + tailRemainder);
      }
    } else {
      Authors parsed = extractAuthors(issue.issueHtml != null ? issue.tail : itemText);
      authors = parsed.formatted;
      description = parsed.text;
    }

    String html;
    if (issue.issueHtml != null) {
      description = strip(LEADING_SEPARATORS.matcher(description).replaceFirst(""));
      html = issue.issueHtml + ": " + escapeHtml(description);
    } else if (authors != null) {
      html = escapeHtml(description);
    } else {
      return linkifyRemainingText(itemText);
    }

    if (authors != null) {
      html += "<br /><span class=\"attrib\">(" + String.join(", ", authors) + ")</span>";
    }
    return html;
  }

  private static String linkifyRemainingText(String text) {
    String result = escapeHtml(text);
    result =
        replaceAll(
            JIRA_KEY,
            result,
            m -> "<a href=\"" + JIRA_URL_PREFIX + m.group(1) + "\">" + m.group(1) + "</a>");
    result =
        replaceAll(
            BARE_URL_IN_TEXT, result, m -> "<a href=\"" + m.group(1) + "\">" + m.group(1) + "</a>");
    return result;
  }

  private static IssueParts extractIssueFromText(String text) {
    List<Issue> matches = findMarkdownIssueMatches(text);
    if (!matches.isEmpty()) {
      int firstStart = matches.get(0).start;
      List<String> parts = new ArrayList<>();
      StringBuilder pieces = new StringBuilder();
      int lastEnd = firstStart;
      for (Issue match : matches) {
        pieces.append(text, lastEnd, match.start);
        parts.add(match.html);
        lastEnd = match.end;
      }
      pieces.append(text.substring(lastEnd));
      return new IssueParts(
          String.join(" ", parts), text.substring(0, firstStart), pieces.toString());
    }

    PlainPr plain = extractPlainPrReferences(text);
    return new IssueParts(plain.html, null, plain.text);
  }

  private static List<Issue> findMarkdownIssueMatches(String text) {
    List<Issue> matches = new ArrayList<>();
    for (IssuePattern pattern : ISSUE_PATTERNS) {
      Matcher matcher = pattern.regex.matcher(text);
      while (matcher.find()) {
        String issueId = matcher.group(1);
        matches.add(
            new Issue(
                matcher.start(),
                matcher.end(),
                formatIssueLink(pattern.urlPrefix, issueId, pattern.labelPrefix + issueId)));
      }
    }
    matches.sort(Comparator.comparingInt(issue -> issue.start));
    return matches;
  }

  private static PlainPr extractPlainPrReferences(String text) {
    Matcher match = PLAIN_PR_REFERENCES.matcher(text);
    if (!match.find()) {
      return new PlainPr(null, text);
    }

    List<String> links = new ArrayList<>();
    Matcher number = PR_NUMBER.matcher(match.group());
    while (number.find()) {
      links.add(formatIssueLink(GITHUB_PR_PREFIX, number.group(1), "PR#" + number.group(1)));
    }

    String textWithout = strip(text.substring(0, match.start()) + text.substring(match.end()));
    return new PlainPr(String.join(", ", links), textWithout);
  }

  private static Authors extractAuthors(String text) {
    List<String> authors = new ArrayList<>();

    int i = text.length() - 1;
    while (i >= 0 && " \t\n\r".indexOf(text.charAt(i)) >= 0) {
      i--;
    }
    if (i < 0 || text.charAt(i) != ')') {
      return new Authors(null, text);
    }

    List<int[]> authorPositions = new ArrayList<>();
    while (i >= 0) {
      if (text.charAt(i) != ')') {
        break;
      }
      int parenDepth = 1;
      int bracketDepth = 0;
      int j = i - 1;
      while (j >= 0 && parenDepth > 0) {
        char c = text.charAt(j);
        if (c == ']') {
          bracketDepth++;
        } else if (c == '[') {
          bracketDepth--;
        } else if (bracketDepth == 0) {
          if (c == ')') {
            parenDepth++;
          } else if (c == '(') {
            parenDepth--;
          }
        }
        j--;
      }
      if (parenDepth != 0) {
        break;
      }

      int startPos = j + 1;
      if (startPos > 0 && text.charAt(startPos - 1) == ']') {
        i = j;
      } else {
        authorPositions.add(0, new int[] {startPos, i});
        i = j;
        while (i >= 0 && " \t\n\r".indexOf(text.charAt(i)) >= 0) {
          i--;
        }
        if (i >= 0 && text.charAt(i) != ')') {
          break;
        }
      }
    }

    if (!authorPositions.isEmpty()) {
      int firstStart = authorPositions.get(0)[0];
      String textWithoutAuthors = strip(text.substring(0, firstStart));

      for (int[] position : authorPositions) {
        String authorContent = text.substring(position[0] + 1, position[1]);
        for (String author : AUTHOR_SEPARATOR.split(authorContent, -1)) {
          String trimmed = strip(author);
          if (!trimmed.isEmpty()) {
            authors.add(formatSingleAuthor(trimmed));
          }
        }
      }

      if (!authors.isEmpty()) {
        return new Authors(authors, textWithoutAuthors);
      }
    }

    return new Authors(null, text);
  }

  private static String formatSingleAuthor(String authorText) {
    String text = strip(authorText);
    Matcher markdownLink = MARKDOWN_LINK.matcher(text);
    Matcher githubMatch = GITHUB_HANDLE.matcher(text);
    boolean hasGithub = githubMatch.find();

    if (markdownLink.find()) {
      String html =
          "<a href=\"" + markdownLink.group(2) + "\">" + escapeHtml(markdownLink.group(1)) + "</a>";
      if (hasGithub) {
        String handle = githubMatch.group(1);
        html += " <a href=\"https://github.com/" + handle + "\">@" + handle + "</a>";
      }
      return html;
    } else if (hasGithub) {
      String handle = githubMatch.group(1);
      String name = strip(text.replace("@" + handle, ""));
      return "<a href=\"https://github.com/" + handle + "\">" + escapeHtml(name) + "</a>";
    } else {
      return escapeHtml(text);
    }
  }

  private static String convertMarkdownLinks(String text) {
    Map<String, String> placeholders = new LinkedHashMap<>();

    String result =
        replaceAll(
            MARKDOWN_LINK,
            text,
            m ->
                protect(
                    placeholders,
                    "<a href=\"" + m.group(2) + "\">" + escapeHtml(m.group(1)) + "</a>"));

    result =
        replaceAll(
            BARE_URL_IN_MARKDOWN,
            result,
            m -> protect(placeholders, "<a href=\"" + m.group(1) + "\">" + m.group(1) + "</a>"));

    result = escapeHtml(result);

    for (Map.Entry<String, String> placeholder : placeholders.entrySet()) {
      result = result.replace(placeholder.getKey(), placeholder.getValue());
    }
    return result;
  }

  private static String protect(Map<String, String> placeholders, String html) {
    String placeholder = "__PLACEHOLDER_" + placeholders.size() + "__";
    placeholders.put(placeholder, html);
    return placeholder;
  }

  private static final class IssuePattern {
    final Pattern regex;
    final String urlPrefix;
    final String labelPrefix;

    IssuePattern(Pattern regex, String urlPrefix, String labelPrefix) {
      this.regex = regex;
      this.urlPrefix = urlPrefix;
      this.labelPrefix = labelPrefix;
    }
  }

  private static final class Issue {
    final int start;
    final int end;
    final String html;

    Issue(int start, int end, String html) {
      this.start = start;
      this.end = end;
      this.html = html;
    }
  }

  private static final class IssueParts {
    final String issueHtml;
    final String descriptionHead;
    final String tail;

    IssueParts(String issueHtml, String descriptionHead, String tail) {
      this.issueHtml = issueHtml;
      this.descriptionHead = descriptionHead;
      this.tail = tail;
    }
  }

  private static final class PlainPr {
    final String html;
    final String text;

    PlainPr(String html, String text) {
      this.html = html;
      this.text = text;
    }
  }

  private static final class Authors {
    final List<String> formatted;
    final String text;

    Authors(List<String> formatted, String text) {
      this.formatted = formatted;
      this.text = text;
    }
  }

  private static final class Changelog {
    final String preamble;
    final List<Release> releases;

    Changelog(String preamble, List<Release> releases) {
      this.preamble = preamble;
      this.releases = releases;
    }
  }

  private static final class Release {
    final String version;
    final String date;
    final List<Section> sections = new ArrayList<>();

    Release(String version, String date) {
      this.version = version;
      this.date = date;
    }
  }

  private static final class Section {
    final String name;
    final List<String> items;

    Section(String name, List<String> items) {
      this.name = name;
      this.items = items;
    }
  }
}
