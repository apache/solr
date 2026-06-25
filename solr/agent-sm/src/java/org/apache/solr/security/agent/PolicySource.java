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

/**
 * Whether a security policy entry came from the default bundled policy ({@code
 * agent-security.policy}) or an operator extension. Standalone top-level type (not nested in {@link
 * PolicyLoader}) because {@link PermittedPath}, {@link PermittedEndpoint}, and {@link
 * ApprovedCallSite} hold it as a field and are injected into bootstrap — a nested type would force
 * {@link PolicyLoader} into bootstrap too.
 */
public enum PolicySource {
  DEFAULT,
  OPERATOR
}
