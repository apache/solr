Apache Solr - AWS Secret Provider
===========================

An implementation of `SecretCredentialsProvider` that pulls Zookeeper credentials from an AWS Secret Manager.

This plugin uses the [default AWS credentials provider chain](https://docs.aws.amazon.com/sdk-for-java/v2/developer-guide/credentials.html), so ensure that your credentials are set appropriately (e.g., via env var, or in `~/.aws/credentials`, etc.).

## Usage

- To enable this feature copy the jar files in `modules/aws-secret-provider/lib` to `SOLR_INSTALL/server/solr-webapp/webapp/WEB-INF/lib/` and add follow the below steps before restarting Solr.

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
SOLR_ZK_CREDS_AND_ACLS="-DzkACLProvider=org.apache.solr.common.cloud.acl.DigestZkACLProvider \
-DzkCredentialsProvider=org.apache.solr.common.cloud.acl.DigestZkCredentialsProvider \
-DzkCredentialsInjector=org.apache.solr.common.cloud.acl.SecretCredentialInjector
-DzkSecretCredentialsProvider=org.apache.solr.secret.zk.AWSSecretCredentialsProvider
-DzkSecretCredentialSecretName=zkCredentialsSecret
-DzkCredentialsAWSSecretRegion=us-west-2"
SOLR_OPTS="$SOLR_OPTS $SOLR_ZK_CREDS_AND_ACLS"
```

- For Windows edit `solr.in.cmd`:

```bat
REM Settings for ZK ACL
set SOLR_ZK_CREDS_AND_ACLS=-DzkACLProvider=org.apache.solr.common.cloud.acl.DigestZkACLProvider ^
 -DzkCredentialsProvider=org.apache.solr.common.cloud.acl.DigestZkCredentialsProvider ^
 -DzkCredentialsInjector=org.apache.solr.common.cloud.acl.SecretCredentialInjector ^
 -DzkSecretCredentialsProvider=org.apache.solr.secret.zk.AWSSecretCredentialsProvider ^
 -DzkSecretCredentialSecretName=zkCredentialsSecret ^
 -DzkCredentialsAWSSecretRegion=us-west-2
set SOLR_OPTS=%SOLR_OPTS% %SOLR_ZK_CREDS_AND_ACLS%
```


- Do the same for zkcli.*

zkcli.sh


```shell
# Settings for ZK ACL
SOLR_ZK_CREDS_AND_ACLS="-DzkACLProvider=org.apache.solr.common.cloud.acl.DigestZkACLProvider \
  -DzkCredentialsProvider=org.apache.solr.common.cloud.acl.DigestZkCredentialsProvider \
  -DzkCredentialsInjector=org.apache.solr.common.cloud.acl.SecretCredentialInjector
  -DzkSecretCredentialsProvider=org.apache.solr.secret.zk.AWSSecretCredentialsProvider
  -DzkSecretCredentialSecretName=zkCredentialsSecret
  -DzkCredentialsAWSSecretRegion=us-west-2"
```

zkcli.bat

```bat
REM Settings for ZK ACL
set SOLR_ZK_CREDS_AND_ACLS=-DzkACLProvider=org.apache.solr.common.cloud.acl.DigestZkACLProvider ^
 -DzkCredentialsProvider=org.apache.solr.common.cloud.acl.DigestZkCredentialsProvider ^
 -DzkCredentialsInjector=org.apache.solr.common.cloud.acl.SecretCredentialInjector ^
 -DzkSecretCredentialsProvider=org.apache.solr.secret.zk.AWSSecretCredentialsProvider ^
 -DzkSecretCredentialSecretName=zkCredentialsSecret ^
 -DzkCredentialsAWSSecretRegion=us-west-2
```

- Restart Solr.