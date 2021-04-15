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
package org.apache.solr.blob.client;

/**
 * Specific exception thrown from {@link BlobStorageClient}s when the resource requested is not found (e.g., attempting
 * to fetch a key in S3 that doesn't exist).
 */
public class BlobNotFoundException extends BlobException {

    public BlobNotFoundException(Throwable cause) {
        super(cause);
    }

    public BlobNotFoundException(String message) {
        super(message);
    }

    public BlobNotFoundException(String message, Throwable cause) {
        super(message, cause);
    }
}
