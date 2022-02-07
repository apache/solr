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
Please refer to the Solr Ref Guide at https://solr.apache.org/guide/jwt-authentication-plugin.html
for more information.

User interface
--------------
A User interface for this module is part of Solr Core.