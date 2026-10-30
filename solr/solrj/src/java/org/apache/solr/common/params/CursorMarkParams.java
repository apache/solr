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
package org.apache.solr.common.params;

/**
 * Parameters and constants used when dealing with cursor based requests across large sorted result
 * sets.
 */
public interface CursorMarkParams {

  /**
   * Param clients should specify indicating that they want a cursor based search. The value must be
   * {@link #CURSOR_MARK_START} for the beginning, {@link #CURSOR_MARK_END} for the end, or a cursor
   * returned through {@link #CURSOR_MARK_NEXT} or {@link #CURSOR_MARK_PREVIOUS} by an earlier
   * search.
   */
  String CURSOR_MARK_PARAM = "cursorMark";

  /**
   * Key used in Solr response to inform the client what the "next" {@link #CURSOR_MARK_PARAM} value
   * should be to continue pagination
   */
  String CURSOR_MARK_NEXT = "nextCursorMark";

  /**
   * Key used in Solr response to inform the client what the "previous" {@link #CURSOR_MARK_PARAM}
   * value should be to go back in pagination
   */
  String CURSOR_MARK_PREVIOUS = "previousCursorMark";

  /**
   * Special value for {@link #CURSOR_MARK_PARAM} indicating that cursor functionality should be
   * used, and a new cursor value should be computed after the last result, but that currently the
   * "first page" of results is being requested
   */
  String CURSOR_MARK_START = "*";

  /**
   * Special value for {@link #CURSOR_MARK_PARAM} indicating that cursor functionality should be
   * used, and a new cursor value should be computed before the first result, but that currently the
   * "last page" of results is being requested
   */
  String CURSOR_MARK_END = "<";
}
