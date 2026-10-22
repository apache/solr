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
package org.apache.solr.search;

/** The result of a search. */
public class QueryResult {

  // Object for back compatibility so that we render true not "true" in json
  private Object partialResults;
  private Object partialResultsDetails;
  private Boolean segmentTerminatedEarly;
  private Boolean terminatedEarly;
  private DocListAndSet docListAndSet;
  private CursorMark nextCursorMark;
  private CursorMark previousCursorMark;
  private Boolean maxHitsTerminatedEarly;
  private Long approximateTotalHits;

  public Object groupedResults; // TODO: currently for testing

  public DocList getDocList() {
    return docListAndSet.docList;
  }

  public void setDocList(DocList list) {
    if (docListAndSet == null) {
      docListAndSet = new DocListAndSet();
    }
    docListAndSet.docList = list;
  }

  public DocSet getDocSet() {
    return docListAndSet.docSet;
  }

  public void setDocSet(DocSet set) {
    if (docListAndSet == null) {
      docListAndSet = new DocListAndSet();
    }
    docListAndSet.docSet = set;
  }

  public boolean isPartialResults() {
    // omitted is equivalent to false/empty for java logic
    return Boolean.parseBoolean(String.valueOf(partialResults));
  }

  public boolean isPartialResultOmitted() {
    return "omitted".equals(partialResults);
  }

  public void setPartialResults(Object partialResults) {
    this.partialResults = partialResults;
  }

  public Object getPartialResultsDetails() {
    return partialResultsDetails;
  }

  public void setPartialResultsDetails(Object partialResultsDetails) {
    this.partialResultsDetails = partialResultsDetails;
  }

  public Boolean getSegmentTerminatedEarly() {
    return segmentTerminatedEarly;
  }

  public void setSegmentTerminatedEarly(Boolean segmentTerminatedEarly) {
    this.segmentTerminatedEarly = segmentTerminatedEarly;
  }

  public void setDocListAndSet(DocListAndSet listSet) {
    docListAndSet = listSet;
  }

  public DocListAndSet getDocListAndSet() {
    return docListAndSet;
  }

  public void setCursorMarkResult(CursorMark previous, CursorMark next) {
    this.previousCursorMark = previous;
    this.nextCursorMark = next;
  }

  public void setCursorMarkNoResults(CursorMark providedCursorMark, int numRequested) {
    if (providedCursorMark != null) {
      if (numRequested == 0) {
        // No docs were requested, so we don't have information on anything other than what the user provided
        if (providedCursorMark.isReverse()) {
          setCursorMarkResult(providedCursorMark, null);
        } else {
          setCursorMarkResult(null, providedCursorMark);
        }
      } else {
        // Documents were requested, but none were returned, so we are either at the beginning or end of the results
        if (providedCursorMark.isReverse()) {
          // If doing a reverse cursorMark, and we have no results, then the nextCursorMark should be "*" as we have reverse-cursed through all documents, and a cursorMark should start from the beginning of the list
          setCursorMarkResult(providedCursorMark, providedCursorMark.initial(false));
        } else {
          // If doing a non-reverse cursorMark, and we have no results, then the previousCursorMark should be "<" as we have cursed through all documents, and a reverse cursorMark should start from the end of the list
          setCursorMarkResult(providedCursorMark.initial(true), providedCursorMark);
        }
      }
    }
  }

  public CursorMark getNextCursorMark() {
    return nextCursorMark;
  }

  public CursorMark getPreviousCursorMark() {
    return previousCursorMark;
  }

  public Boolean getTerminatedEarly() {
    return terminatedEarly;
  }

  public void setTerminatedEarly(boolean terminatedEarly) {
    this.terminatedEarly = terminatedEarly;
  }

  public Boolean getMaxHitsTerminatedEarly() {
    return maxHitsTerminatedEarly;
  }

  public void setMaxHitsTerminatedEarly(Boolean maxHitsTerminatedEarly) {
    this.maxHitsTerminatedEarly = maxHitsTerminatedEarly;
  }

  public Long getApproximateTotalHits() {
    return approximateTotalHits;
  }

  public void setApproximateTotalHits(long approximateTotalHits) {
    this.approximateTotalHits = approximateTotalHits;
  }
}
