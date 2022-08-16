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

package org.apache.solr.jersey.container;

import org.apache.solr.jersey.PermissionName;
import org.apache.solr.security.PermissionNameProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import java.lang.invoke.MethodHandles;

@Path("/someadminpath")
public class SomeAdminResource {
    private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());


    @GET
    @PermissionName(PermissionNameProvider.Name.READ_PERM)
    @Produces(MediaType.TEXT_PLAIN)
    public String helloPlainText() {
        log.info("Made it to SomeAdminResource");

        return "Hello from admin resource!";
    }
}
