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

        const response = await fetch(`http://localhost:3001/login`, {
            /*
                this is the structure of a Solr Authentication request.
                Ideally each request would be contained in a single service
                file and that service makes a request to a server.js-like
                Express Node backend that then sends the request to Solr.
                This would make it easier to extend.
            */

            method: "POST",
            headers: {
            "Content-Type": "application/json",
            "Authorization": "Basic " + btoa(username + ":" + password),
            "Access-Control-Allow-Origin": "*",
            },
        });
    
        if (response.ok) {
            // hard coding the location of the legacy UI until using environment variables
            // For now will just route to the real login.
            window.location.href = "http://localhost:8983/solr/#/login";
            // return true;
            
        } else {
            return false
        }
    },
    
    // not implemented yet
    logout: function() {
        
        localStorage.removeItem("jwt");
    },
  
  };
  