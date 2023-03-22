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
const SOLR_HOST_URL = process.env.REACT_APP_SOLR_HOST; 
const SOLR_PORT = process.env.REACT_APP_SOLR_PORT;

export const AuthenticationService = {
    login: async function(username, password) {
        // Note: this direction would only work for basic auth. 
        // Need to do more thinking on other auth classes and dependencies like session storage.
        const response = await fetch(`${SOLR_HOST_URL}:${SOLR_PORT}/solr/admin/authentication`, {
            method: "POST",
            headers: {
            "Content-Type": "application/json",
            },
            body: JSON.stringify({ username, password }),
        });
    
        // If the response is successful, store the JWT in local storage.
        if (response.ok) {
            return true;
            // force users to http://localhost:8983/solr/#/home
        } else {
            return false
        }
    },
  
    logout: function() {
        localStorage.removeItem("jwt");
    },
  
  };
  