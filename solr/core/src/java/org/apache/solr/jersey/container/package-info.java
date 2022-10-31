/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 * Adapter code used to convert the native Jetty request/response abstractions into the objects
 * required by Jersey
 *
 * <p>Much of the code in this package is inspired or copied from the jersey-container-jetty-http
 * artifact, which greatly simplifies deploying a Jersey application in its own Jetty servlet or
 * servlet filter. Solr cannot currently take advantage of this due to the interconnectedness of its
 * filter logic (i.e. everything running in one massive filter). However, if this is remedied in the
 * future we should be able to get rid of most or all of the adapter code in this package.
 */
package org.apache.solr.jersey.container;
