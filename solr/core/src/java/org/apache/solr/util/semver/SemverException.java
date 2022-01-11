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

// Forked and adapted from https://github.com/vdurmont/semver4j - MIT license
// Copyright (c) 2015-present Vincent DURMONT vdurmont@gmail.com

package org.apache.solr.util.semver;

import org.apache.solr.common.SolrException;

public class SemverException extends SolrException {
  public SemverException(String msg) {
    super(ErrorCode.BAD_REQUEST, msg);
  }

  public SemverException(String msg, Throwable t) {
    super(ErrorCode.BAD_REQUEST, msg, t);
  }
}
