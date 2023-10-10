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

Apache Solr - AWS Secret Provider
===========================

An implementation of `ZkCredentialsInjector` that pulls Zookeeper credentials from an AWS Secret Manager.

This plugin uses the [default AWS credentials provider chain](https://docs.aws.amazon.com/sdk-for-java/v2/developer-guide/credentials.html), so ensure that your credentials are set appropriately (e.g., via env var, or in `~/.aws/credentials`, etc.).

## Usage

- To enable this feature edit solr.in.sh/cmd and set/add `SOLR_MODULES=aws-secret-provider` 



- Create a secret in AWS SM (for example named *zkCredentialsSecret*) containing the Zookeeper credentials in the following Json format:



```json
    {
        "zkCredentials": [
            {"username": "admin-user", "password": "ADMIN-PASSWORD", "perms": "all"},
            {"username": "readonly-user", "password": "READONLY-PASSWORD", "perms": "read"}
        ]
    }
```


-  Pass the secret name and region trough `solr.in.sh:`

```shell
# Settings for ZK ACL
SOLR_ZK_CREDS_AND_ACLS="-DzkACLProvider=org.apache.solr.common.cloud.DigestZkACLProvider \
-DzkCredentialsProvider=org.apache.solr.common.cloud.DigestZkCredentialsProvider \
-DzkCredentialsInjector=org.apache.solr.secret.zk.AWSSecretManagerCredentialsInjector \
-DzkSecretCredentialSecretName=zkCredentialsSecret \
-DzkCredentialsAWSSecretRegion=us-west-2"
SOLR_OPTS="$SOLR_OPTS $SOLR_ZK_CREDS_AND_ACLS"
```

- For Windows edit `solr.in.cmd`:

```bat
REM Settings for ZK ACL
set SOLR_ZK_CREDS_AND_ACLS=-DzkACLProvider=org.apache.solr.common.cloud.DigestZkACLProvider ^
 -DzkCredentialsProvider=org.apache.solr.common.cloud.DigestZkCredentialsProvider ^
 -DzkCredentialsInjector=org.apache.solr.secret.zk.AWSSecretManagerCredentialsInjector ^
 -DzkSecretCredentialSecretName=zkCredentialsSecret ^
 -DzkCredentialsAWSSecretRegion=us-west-2
set SOLR_OPTS=%SOLR_OPTS% %SOLR_ZK_CREDS_AND_ACLS%
```


- Do the same for zkcli.*

zkcli.sh


```shell
# Settings for ZK ACL
SOLR_ZK_CREDS_AND_ACLS="-DzkACLProvider=org.apache.solr.common.cloud.DigestZkACLProvider \
  -DzkCredentialsProvider=org.apache.solr.common.cloud.DigestZkCredentialsProvider \ 
  -DzkCredentialsInjector=org.apache.solr.secret.zk.AWSSecretManagerCredentialsInjector \
  -DzkSecretCredentialSecretName=zkCredentialsSecret \
  -DzkCredentialsAWSSecretRegion=us-west-2"
```

zkcli.bat

```bat
REM Settings for ZK ACL
set SOLR_ZK_CREDS_AND_ACLS=-DzkACLProvider=org.apache.solr.common.cloud.DigestZkACLProvider ^
 -DzkCredentialsProvider=org.apache.solr.common.cloud.DigestZkCredentialsProvider ^
 -DzkCredentialsInjector=org.apache.solr.secret.zk.AWSSecretManagerCredentialsInjector ^
 -DzkSecretCredentialSecretName=zkCredentialsSecret ^
 -DzkCredentialsAWSSecretRegion=us-west-2
```

- Restart Solr.