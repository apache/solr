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
package org.apache.solr.handler.loader;

import java.io.IOException;
import java.io.Reader;
import java.util.Arrays;
import org.apache.solr.common.SolrException;
import org.noggit.JSONParser;

/**
 * A {@link JSONParser} that enforces the newline delimited JSON contract while parsing: every
 * document is a JSON object sitting on a line of its own. Newlines are only whitespace to JSON
 * itself, so the contract is checked here rather than by framing the input into lines, which lets
 * documents stream without ever being materialized as a line.
 *
 * <p>Tracking happens in {@link #fill()}, which sees every character exactly once as it enters the
 * buffer, and in {@link #nextEvent()}, which observes where each top level object starts and ends.
 */
class NDJsonParser extends JSONParser {

  private static final int BUFFER_SIZE = 8192;

  /** Absolute offsets of newlines already seen by {@link #fill()}, not yet reached by parsing. */
  private long[] newlines = new long[16];

  private int head;
  private int tail;

  private boolean lastWasCarriageReturn;

  /** Lines consumed up to {@link #consumedPosition}, 1-based. */
  private long line = 1;

  private long consumedPosition;

  private long documentStartLine = -1;
  private long previousDocumentEndLine = -1;

  NDJsonParser(Reader in) {
    super(in, new char[BUFFER_SIZE]);
  }

  /** The line the parser has reached, 1-based, for error reporting. */
  long getLineNumber() {
    return lineNumberAtCurrentPosition();
  }

  @Override
  public int nextEvent() throws IOException {
    int event = super.nextEvent();
    int level = getLevel();
    if (event == OBJECT_START && level == 1) {
      long startLine = lineNumberAtCurrentPosition();
      if (startLine == previousDocumentEndLine) {
        throw new SolrException(
            SolrException.ErrorCode.BAD_REQUEST,
            "Cannot parse NDJSON at line "
                + startLine
                + ": expected a newline between documents, but found another document on the same"
                + " line");
      }
      documentStartLine = startLine;
    } else if (event == ARRAY_START && level == 1) {
      throw new SolrException(
          SolrException.ErrorCode.BAD_REQUEST,
          "Cannot parse NDJSON at line "
              + lineNumberAtCurrentPosition()
              + ": expected a JSON object, but found an array; NDJSON carries one object per line,"
              + " with no enclosing array");
    } else if (event == OBJECT_END && level == 0) {
      assertOnDocumentLine();
      previousDocumentEndLine = documentStartLine;
      documentStartLine = -1;
    } else if (level == 0 && event != EOF) {
      // A stray value between documents would otherwise be skipped silently
      throw new SolrException(
          SolrException.ErrorCode.BAD_REQUEST,
          "Cannot parse NDJSON at line "
              + lineNumberAtCurrentPosition()
              + ": expected a JSON object, but found "
              + getEventString(event));
    } else if (level > 0 && documentStartLine > 0) {
      assertOnDocumentLine();
    }
    return event;
  }

  /** A document must not span lines; this fires on the first token past an interior newline. */
  private void assertOnDocumentLine() {
    long current = lineNumberAtCurrentPosition();
    if (current != documentStartLine) {
      throw new SolrException(
          SolrException.ErrorCode.BAD_REQUEST,
          "Cannot parse NDJSON at line "
              + documentStartLine
              + ": a document must be on a single line, but this one spans lines "
              + documentStartLine
              + " to "
              + current);
    }
  }

  @Override
  protected void fill() throws IOException {
    super.fill();
    for (int i = 0; i < end; i++) {
      char c = buf[i];
      if (c == '\n' || c == '\r') {
        long position = gpos + i;
        if (!(c == '\n' && lastWasCarriageReturn)) {
          record(position);
        }
        lastWasCarriageReturn = c == '\r';
      } else {
        lastWasCarriageReturn = false;
      }
    }
  }

  private void record(long position) {
    if (tail == newlines.length) {
      if (head > 0) {
        System.arraycopy(newlines, head, newlines, 0, tail - head);
        tail -= head;
        head = 0;
      } else {
        newlines = Arrays.copyOf(newlines, newlines.length * 2);
      }
    }
    newlines[tail++] = position;
  }

  /**
   * The line holding the token {@link #nextEvent()} just returned. The parser's position sits just
   * past that token, so a terminator at the very end of it does not count towards the token's line.
   */
  private long lineNumberAtCurrentPosition() {
    consumeThrough(getPosition() - 1);
    return line;
  }

  /** Counts the newlines at or before {@code position}, which never moves backwards. */
  private void consumeThrough(long position) {
    if (position < consumedPosition) {
      return;
    }
    while (head < tail && newlines[head] <= position) {
      head++;
      line++;
    }
    if (head == tail) {
      head = 0;
      tail = 0;
    }
    consumedPosition = position;
  }
}
