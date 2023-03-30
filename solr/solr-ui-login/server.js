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

/*
    This is a simple proxy server to handle all requests to the Solr backend.
    It is currently only used to authenticate the user with the legacy UI.

    Over time, this will be expanded to handle more requests and be more, 
    require no manual intervention to start, and it will be secure.

    In the near future, I'll want to grab these environment variables from a start parameter,
    but not befor fixing the security-related depdencies. 
    For now, it will be harcoded above and recommended only for development.
    const SOLR_HOST_URL = process.env.REACT_APP_SOLR_HOST; 
    const SOLR_PORT = process.env.REACT_APP_SOLR_PORT;

    Useful links: 
      https://github.com/apache/solr/blob/33ee6f2dad1122fbc7b065393253d4202e6db8f8/solr/solr-ref-guide/modules/deployment-guide/pages/basic-authentication-plugin.adoc#L24


*/

const express = require('express');
const { createProxyMiddleware } = require("http-proxy-middleware");
const cors = require("cors");
const bodyParser = require("body-parser");
// const cookieParser = require("cookie-parser");

const app = express();
app.use(cors());
app.use(bodyParser.json());
app.use(bodyParser.urlencoded({ extended: true }));
// app.use(cookieParser());
const PORT = process.env.PORT || 3001;
const SOLR_HOST = 'http://localhost:8983';

// Node login API
app.post("/login", async (req, res) => {
  try {
    const solrResponse = await fetch(`${SOLR_HOST}/solr/admin/authentication`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "Authorization": req.headers.authorization,
        "Access-Control-Allow-Origin": "*",
      },
        body: JSON.stringify({
            "set-property": {
                "forwardCredentials": true
            }
        }),
    });

    const solrResponseBody = await solrResponse.json();

    if (solrResponse.ok) {
      res.setHeader("Authorization", req.headers.authorization);
      res.status(200).json(solrResponseBody);
    } else {
      res.status(solrResponse.status).json(solrResponseBody);
    }
  } catch (error) {
    res.status(500).json({ message: "An error occurred while processing the request" });
  }
});

app.use("/", createProxyMiddleware({ target: SOLR_HOST, changeOrigin: true }));

app.listen(PORT, "localhost", () => {
  console.log(`Dev server is running on http://localhost:${PORT}`);
});