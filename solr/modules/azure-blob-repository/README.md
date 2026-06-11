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

# Apache Solr Azure Blob Storage Backup Repository

A backup repository implementation for storing Solr backups in Azure Blob Storage.

## Prerequisites

- Azure Storage Account with a blob container (must already exist)
- Network access to Azure Blob Storage (HTTPS port 443)

Enable the module:
```bash
export SOLR_MODULES=azure-blob-repository
```

## Configuration

Add to `solr.xml`:

```xml
<backup>
  <repository name="azure_blob" class="org.apache.solr.azureblob.AzureBlobBackupRepository" default="false">
    <str name="azure.blob.container.name">YOUR_CONTAINER_NAME</str>
    <!-- Authentication options below -->
  </repository>
</backup>
```

## Authentication Methods

### Connection String (Development)

```xml
<str name="azure.blob.connection.string">DefaultEndpointsProtocol=https;AccountName=...;AccountKey=...;EndpointSuffix=core.windows.net</str>
```

### Account Name + Account Key

```xml
<str name="azure.blob.endpoint">https://YOUR_ACCOUNT.blob.core.windows.net</str>
<str name="azure.blob.account.name">YOUR_ACCOUNT</str>
<str name="azure.blob.account.key">YOUR_ACCOUNT_KEY</str>
```

### SAS Token (Production)

Generate a SAS token with permissions: Read, Write, Delete, List, Add, Create (`sp=rwdlac`) and resource types: Service, Container, Object (`srt=sco`).

```xml
<str name="azure.blob.endpoint">https://YOUR_ACCOUNT.blob.core.windows.net</str>
<str name="azure.blob.sas.token">sv=2024-11-04&amp;ss=b&amp;srt=sco&amp;sp=rwdlac&amp;...</str>
```

Note: Escape `&` as `&amp;` in XML.

### Azure Identity (Production - Recommended)

Uses Azure AD authentication. Requires "Storage Blob Data Contributor" role on the storage account.

```xml
<str name="azure.blob.endpoint">https://YOUR_ACCOUNT.blob.core.windows.net</str>
<!-- No credentials needed when using Managed Identity or Azure CLI -->
```

For Service Principal, add:
```xml
<str name="azure.blob.tenant.id">YOUR_TENANT_ID</str>
<str name="azure.blob.client.id">YOUR_CLIENT_ID</str>
<str name="azure.blob.client.secret">YOUR_CLIENT_SECRET</str>
```

Or set environment variables: `AZURE_TENANT_ID`, `AZURE_CLIENT_ID`, `AZURE_CLIENT_SECRET`.

## Known Limitations

Azure Identity authentication (Service Principal, Managed Identity, `DefaultAzureCredential`) does not work when Solr is started with the Java `SecurityManager` enabled. The Azure Identity SDK relies on `doPrivileged` patterns that fail under Solr's default security policy; see the [upstream issue](https://github.com/Azure/azure-sdk-for-java/issues/37464) for details. Note that the `SecurityManager` is deprecated for removal in modern JDKs, but Solr still enables it by default via `SOLR_SECURITY_MANAGER_ENABLED=true`.

Workaround: set `SOLR_SECURITY_MANAGER_ENABLED=false` (in `solr.in.sh` / `solr.in.cmd`, or as an environment variable) before starting Solr. The Connection String, Account Key, and SAS Token authentication methods are unaffected and work with the `SecurityManager` enabled.

## Usage

```bash
# Backup
curl "http://localhost:8983/solr/admin/collections?action=BACKUP&name=my-backup&collection=my-collection&repository=azure_blob&location=/"

# Restore
curl "http://localhost:8983/solr/admin/collections?action=RESTORE&name=my-backup&collection=my-collection&repository=azure_blob&location=/"

# List backups
curl "http://localhost:8983/solr/admin/collections?action=LISTBACKUP&name=my-backup&repository=azure_blob&location=/"

# Delete a specific backup
curl "http://localhost:8983/solr/admin/collections?action=DELETEBACKUP&name=my-backup&backupId=0&repository=azure_blob&location=/"
```

## Troubleshooting

**403 Forbidden**: Check SAS token permissions (`srt=sco`, `sp=rwdlac`) or RBAC role assignment.

**Signature did not match**: Ensure `&` is escaped as `&amp;` in XML and no whitespace in token.

**DefaultAzureCredential failed**: Run `az login` or verify service principal credentials.
