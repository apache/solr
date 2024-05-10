<!--
  Licensed to the Apache Software Foundation (ASF) under one or more
  contributor license agreements.  See the NOTICE file distributed with
  this work for additional information regarding copyright ownership.
  The ASF licenses this file to You under the Apache License, Version 2.0
  (the "License"); you may not use this file except in compliance with
  the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
-->

Apache Solr JWT Authentication Plugin
=====================================

Introduction
------------
Solr can support [JSON Web Token](https://en.wikipedia.org/wiki/JSON_Web_Token) (JWT) based 
Bearer authentication with the use of the JWTAuthPlugin.

This allows Solr to assert that a user is already authenticated with an external 
[Identity Provider (IdP)](https://en.wikipedia.org/wiki/Identity_provider) by validating 
that the JWT formatted [access token](https://en.wikipedia.org/wiki/Access_token) 
is digitally signed by the Identity Provider.

The typical use case is to integrate Solr with an [OpenID Connect](https://en.wikipedia.org/wiki/OpenID_Connect) 
enabled Identity Provider.


Getting Started
---------------
Please refer to the Solr Ref Guide at https://solr.apache.org/guide/solr/latest/deployment-guide/jwt-authentication-plugin.html
for more information.

User interface
--------------
A User interface for this module is part of Solr Core.