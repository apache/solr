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

import java.io.IOException;
import org.apache.solr.handler.component.ResponseBuilder;
import org.apache.solr.handler.component.SearchComponent;

/**
 * A search component that throws {@link QueryLimitsExceededException} (the exception {@code
 * QueryLimits} trips with, a subclass of {@code ExitableDirectoryReader.ExitingReaderException})
 * from {@link #process(ResponseBuilder)} whenever the {@link #THROW_PARAM} request parameter is
 * present. It lets a test drive the {@code SearchHandler} limit catch path deterministically, at
 * whatever position this component occupies in a handler's component list, instead of relying on a
 * wall-clock or CPU limit expiring at just that point.
 */
public class ExitingReaderSearchComponent extends SearchComponent {

  /** Request parameter that arms this component: when present, {@code process} throws. */
  public static final String THROW_PARAM = "throwExitingReaderException";

  @Override
  public void prepare(ResponseBuilder rb) throws IOException {}

  @Override
  public void process(ResponseBuilder rb) throws IOException {
    if (rb.req.getParams().get(THROW_PARAM) != null) {
      throw new QueryLimitsExceededException(
          "test component simulating a query limit tripping in process");
    }
  }

  @Override
  public String getDescription() {
    return "throws QueryLimitsExceededException from process when armed by a request parameter";
  }
}
