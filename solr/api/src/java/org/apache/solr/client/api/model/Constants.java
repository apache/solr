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

package org.apache.solr.client.api.model;

public class Constants {

  private Constants() {
    /* Private ctor prevents instantiation */
  }

  /** A parameter to specify the name of the backup repository to be used. */
  public static final String BACKUP_REPOSITORY = "repository";

  /** A parameter to specify the location where the backup should be stored. */
  public static final String BACKUP_LOCATION = "location";

  /** Async or not? * */
  public static final String ASYNC = "async";

  /** The name of a collection referenced by an API call */
  public static final String COLLECTION = "collection";

  public static final String COUNT_PROP = "count";

  /** Option to follow aliases when deciding the target of a collection admin command. */
  public static final String FOLLOW_ALIASES = "followAliases";

  /** If you unload a core, delete the index too */
  public static final String DELETE_INDEX = "deleteIndex";

  public static final String DELETE_DATA_DIR = "deleteDataDir";

  public static final String DELETE_INSTANCE_DIR = "deleteInstanceDir";

  public static final String ONLY_IF_DOWN = "onlyIfDown";

  /** The name of the config set to be used for a collection */
  public static final String COLL_CONF = "collection.configName";
}
