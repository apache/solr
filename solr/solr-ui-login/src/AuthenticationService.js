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

export const AuthenticationService = {
    login: async function(username, password) {
        
        /* 
            Note: this direction would only work for basic auth and JWT, 
            can expand to more strategies easily after more thinking. 
        */

        const response = await fetch(`http://0.0.0.0:3001/login`, {
            method: "POST",
            headers: {
            "Content-Type": "application/json",
            "Authorization": "Basic " + btoa(username + ":" + password),
            "Access-Control-Allow-Origin": "*",
            },
            body: JSON.stringify({
                "set-property": {
                    "forwardCredentials": true
                }
            }),
        });
    
        // If the response is successful, store the JWT in local storage.
        if (response.ok) {
            // hard coding the location of the legacy UI until using environment variables
            // const authToken = response.headers.get("Authorization");
            const authToken = response.headers.get("X-Auth-Token");
            document.cookie = `authToken=${authToken}; path=/; SameSite=None; Secure`;
            window.location.href = "http://localhost:8983/solr/#/home";
            // return true;
            // const jwt = await response.json();
            // localStorage.setItem("jwt", jwt);
            
        } else {
            return false
        }
    },
  
    logout: function() {
        localStorage.removeItem("jwt");
    },
  
  };
  