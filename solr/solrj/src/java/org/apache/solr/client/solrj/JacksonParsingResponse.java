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

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.solr.client.solrj.impl.InputStreamResponseParser;
import org.apache.solr.client.solrj.response.SimpleSolrResponse;
import org.apache.solr.common.util.IOUtils;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.ObjectReleaseTracker;

import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

public class JacksonParsingResponse<T extends Object> extends SimpleSolrResponse {

    private final Class<T> typeParam;

    public JacksonParsingResponse(Class<T> typeParam) {
        this.typeParam = typeParam;
    }

    public T getParsed() {
        // TODO - reuse the ObjectMapper - no reason to recreate each time.
        final NamedList<Object> resp = getResponse();
        final var stream = (InputStream) resp.get("stream");
        try {
            final T parsedVal = new ObjectMapper().readValue(stream, typeParam);
            assert ObjectReleaseTracker.release(stream);
            return parsedVal;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
