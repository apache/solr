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
package org.apache.solr.client.solrj;

import java.io.IOException;
import java.io.Serializable;
import org.apache.solr.common.MapWriter;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.util.NamedList;

/**
 * @since solr 1.3
 */
public abstract class SolrResponse implements Serializable, MapWriter {

  /** make this compatible with earlier versions */
  private static final long serialVersionUID = -7931100103360242645L;

  /** Elapsed time in milliseconds for the request as seen from the client. */
  public abstract long getElapsedTime();

  public abstract void setResponse(NamedList<Object> rsp);

  public abstract void setElapsedTime(long elapsedTime);

  public abstract NamedList<Object> getResponse();

  @Override
  public void writeMap(EntryWriter ew) throws IOException {
    getResponse().writeMap(ew);
  }

  public Exception getException() {
    NamedList<?> exp = (NamedList<?>) getResponse().get("exception");
    if (exp == null) {
      return null;
    }
    Integer rspCode = (Integer) exp.get("rspCode");
    ErrorCode errorCode =
        rspCode != null && rspCode != -1 ? ErrorCode.getErrorCode(rspCode) : ErrorCode.SERVER_ERROR;
    return new SolrException(errorCode, (String) exp.get("msg"));
  }
}
