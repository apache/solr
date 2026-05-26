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
package org.apache.solr.security.agent;

import java.io.IOException;
import java.io.Reader;
import java.io.StreamTokenizer;
import java.util.ArrayDeque;
import java.util.Deque;

/**
 * Token stream backed by a {@link StreamTokenizer} for policy file parsing.
 *
 * <p>This file was derived from the OpenSearch project and modified. See {@code NOTICE.txt} for
 * attribution.
 */
class PolicyTokenStream {
  private final StreamTokenizer tokenizer;
  private final Deque<PolicyToken> buffer = new ArrayDeque<>();

  PolicyTokenStream(Reader reader) {
    StreamTokenizer st = new StreamTokenizer(reader);
    st.resetSyntax();
    st.wordChars('a', 'z');
    st.wordChars('A', 'Z');
    st.wordChars('.', '.');
    st.wordChars('0', '9');
    st.wordChars('_', '_');
    st.wordChars('$', '$');
    st.wordChars(128 + 32, 255);
    st.whitespaceChars(0, ' ');
    st.commentChar('/');
    st.quoteChar('\'');
    st.quoteChar('"');
    st.lowerCaseMode(false);
    st.ordinaryChar('/');
    st.slashSlashComments(true);
    st.slashStarComments(true);
    this.tokenizer = st;
  }

  PolicyToken peek() throws IOException {
    if (buffer.isEmpty()) {
      buffer.push(nextToken());
    }
    return buffer.peek();
  }

  PolicyToken consume() throws IOException {
    return buffer.isEmpty() ? nextToken() : buffer.pop();
  }

  boolean isEOF() throws IOException {
    return peek().type() == StreamTokenizer.TT_EOF;
  }

  int line() throws IOException {
    return peek().line();
  }

  private PolicyToken nextToken() throws IOException {
    int type = tokenizer.nextToken();
    String text =
        switch (type) {
          case StreamTokenizer.TT_WORD, '"', '\'' -> tokenizer.sval;
          case StreamTokenizer.TT_EOF -> "<EOF>";
          default -> Character.toString((char) type);
        };
    return new PolicyToken(type, text, tokenizer.lineno());
  }
}
