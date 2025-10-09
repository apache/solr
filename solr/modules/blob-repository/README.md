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

Apache Solr - Azure Blob Storage Repository
===========================================

This Azure Blob Storage repository is a backup repository implementation designed to provide backup/restore functionality to Azure Blob Storage.

## Quick Start

**Choose your authentication method:**

- 🚀 **Local Development?** → Use **Connection String** (simplest)
- 🔐 **Production on Azure VM/AKS?** → Use **Managed Identity** (most secure)
- 🏢 **Production elsewhere?** → Use **Service Principal** or **SAS Token**
- 🧪 **Testing?** → Use **Azure CLI** (no config changes)

**Prerequisites:**
- Azure Storage Account with a blob container
- Container must already exist (e.g., `solr-backup`)
- Solr blob-repository module enabled
- Network access to Azure Blob Storage (HTTPS port 443)

## Prerequisites

Before configuring authentication, ensure you have:

1. **Azure Storage Account** - Created and accessible
2. **Blob Container** - Must already exist in your storage account
   ```bash
   # Create container using Azure CLI
   az storage container create \
     --name solr-backup \
     --account-name YOUR_ACCOUNT_NAME
   ```
3. **Solr Module** - Enable blob-repository module:
   ```bash
   export SOLR_MODULES=blob-repository
   ./bin/solr start
   ```
4. **Network Access** - Solr can reach Azure Blob Storage (HTTPS port 443)

Optional (depending on authentication method):
- **Azure CLI** installed and configured (`az login`)
- **RBAC Permissions** for Azure Identity methods
- **SAS Token** or **Account Keys** from Azure Portal

## Authentication Options

The Azure Blob Storage backup repository supports four authentication methods. Choose the one that best fits your security requirements and deployment environment.

### 1. Connection String

The simplest authentication method using a full connection string.

#### Configuration in solr.xml:
```xml
<backup>
  <repository name="blob" class="org.apache.solr.blob.BlobBackupRepository" default="false">
    <str name="blob.container.name">YOUR_CONTAINER_NAME</str>
    <str name="blob.connection.string">DefaultEndpointsProtocol=https;AccountName=YOUR_ACCOUNT_NAME;AccountKey=YOUR_ACCOUNT_KEY;EndpointSuffix=core.windows.net</str>
  </repository>
</backup>
```

**Note:** This method is simple but exposes the account key in configuration. Not recommended for production environments.

### 2. Account Name + Key

Separates the account credentials from the endpoint configuration.

#### Configuration in solr.xml:
```xml
<backup>
  <repository name="blob" class="org.apache.solr.blob.BlobBackupRepository" default="false">
    <str name="blob.container.name">YOUR_CONTAINER_NAME</str>
    <str name="blob.endpoint">https://YOUR_ACCOUNT_NAME.blob.core.windows.net</str>
    <str name="blob.account.name">YOUR_ACCOUNT_NAME</str>
    <str name="blob.account.key">YOUR_ACCOUNT_KEY</str>
  </repository>
</backup>
```

**Note:** Similar to connection string, this exposes the account key. Use with caution in production.

### 3. SAS Token (Recommended for Production)

**Important:** The SAS token must be configured with proper permissions to work correctly.

#### Required SAS Token Configuration:
- **Allowed services:** Blob
- **Allowed resource types:** Service, Container, Object (`srt=sco`)
- **Allowed permissions:** Read, Write, Delete, List, Add, Create (`sp=rwdlac` minimum)
- **Protocol:** HTTPS only
- **Expiry:** Set appropriate expiration time (e.g., 1 year)

#### Generating SAS Token (Azure Portal):
1. Navigate to your Storage Account
2. Click "Shared access signature" (left menu under "Security + networking")
3. Configure:
   - Allowed services: ☑ Blob
   - Allowed resource types: ☑ Service, ☑ Container, ☑ Object
   - Allowed permissions: ☑ Read, ☑ Write, ☑ Delete, ☑ List, ☑ Add, ☑ Create
   - Start/Expiry time: Set your desired validity period
   - Allowed protocols: HTTPS only
4. Click "Generate SAS and connection string"
5. Copy the **SAS token** (remove the leading `?` if present)

#### Generating SAS Token (Azure CLI):
```bash
az storage account generate-sas \
  --account-name YOUR_ACCOUNT_NAME \
  --services b \
  --resource-types sco \
  --permissions rwdlac \
  --expiry 2026-12-31T23:59:59Z \
  --https-only \
  --output tsv
```

#### Configuration in solr.xml:
```xml
<backup>
  <repository name="blob" class="org.apache.solr.blob.BlobBackupRepository" default="false">
    <str name="blob.container.name">YOUR_CONTAINER_NAME</str>
    <str name="blob.endpoint">https://YOUR_ACCOUNT_NAME.blob.core.windows.net</str>
    <str name="blob.sas.token">sv=2024-11-04&amp;ss=b&amp;srt=sco&amp;sp=rwdlac&amp;se=2026-12-31T23:59:59Z&amp;st=2025-01-01T00:00:00Z&amp;spr=https&amp;sig=YOUR_SIGNATURE</str>
  </repository>
</backup>
```

**Note:** In XML, `&` characters in the SAS token must be escaped as `&amp;`. The container must already exist in Azure Blob Storage before using it with Solr.

#### Why SAS Token?
- ✅ Time-limited access (automatically expires)
- ✅ Scoped permissions (can restrict to specific operations)
- ✅ Revocable without rotating account keys
- ✅ No account key exposure in configuration
- ✅ Can restrict to specific IP addresses

### 4. Azure Identity (Best for Production)

Uses Azure Active Directory (Entra ID) for authentication. Provides enterprise-grade security with **no credentials in configuration files**.

Azure Identity supports three authentication methods:
- **Azure CLI** - For local development
- **Service Principal** - For automation and CI/CD
- **Managed Identity** - For Azure VMs/AKS (no credentials needed)

---

#### Option A: Azure CLI (Local Development)

Best for local development and testing. Uses your Azure login credentials.

**Prerequisites:**
- Azure CLI installed and logged in (`az login`)
- User account has "Storage Blob Data Contributor" role

**Grant permissions:**
```bash
# Get your user's Object ID
USER_OBJECT_ID=$(az ad signed-in-user show --query id -o tsv)

# Grant Storage Blob Data Contributor role
az role assignment create \
  --role "Storage Blob Data Contributor" \
  --assignee $USER_OBJECT_ID \
  --scope /subscriptions/YOUR_SUBSCRIPTION_ID/resourceGroups/YOUR_RESOURCE_GROUP/providers/Microsoft.Storage/storageAccounts/YOUR_ACCOUNT_NAME
```

**Configuration in solr.xml:**
```xml
<backup>
  <repository name="blob" class="org.apache.solr.blob.BlobBackupRepository" default="false">
    <str name="blob.container.name">YOUR_CONTAINER_NAME</str>
    <str name="blob.endpoint">https://YOUR_ACCOUNT_NAME.blob.core.windows.net</str>
    <!-- No credentials needed - uses Azure CLI token -->
  </repository>
</backup>
```

---

#### Option B: Service Principal (Automation/CI-CD)

Best for automation, CI/CD pipelines, and production deployments outside of Azure.

**Create Service Principal:**
```bash
az ad sp create-for-rbac \
  --name "solr-backup-sp" \
  --role "Storage Blob Data Contributor" \
  --scopes /subscriptions/YOUR_SUBSCRIPTION_ID/resourceGroups/YOUR_RESOURCE_GROUP/providers/Microsoft.Storage/storageAccounts/YOUR_ACCOUNT_NAME

# Output:
# {
#   "appId": "CLIENT_ID",
#   "password": "CLIENT_SECRET",
#   "tenant": "TENANT_ID"
# }
```

**Configuration in solr.xml:**
```xml
<backup>
  <repository name="blob" class="org.apache.solr.blob.BlobBackupRepository" default="false">
    <str name="blob.container.name">YOUR_CONTAINER_NAME</str>
    <str name="blob.endpoint">https://YOUR_ACCOUNT_NAME.blob.core.windows.net</str>
    <str name="blob.tenant.id">YOUR_TENANT_ID</str>
    <str name="blob.client.id">YOUR_CLIENT_ID</str>
    <str name="blob.client.secret">YOUR_CLIENT_SECRET</str>
  </repository>
</backup>
```

**Alternative: Environment Variables**

Instead of putting credentials in solr.xml, you can use environment variables:
```bash
export AZURE_TENANT_ID="your-tenant-id"
export AZURE_CLIENT_ID="your-client-id"
export AZURE_CLIENT_SECRET="your-client-secret"
```

Then solr.xml only needs:
```xml
<backup>
  <repository name="blob" class="org.apache.solr.blob.BlobBackupRepository" default="false">
    <str name="blob.container.name">YOUR_CONTAINER_NAME</str>
    <str name="blob.endpoint">https://YOUR_ACCOUNT_NAME.blob.core.windows.net</str>
    <!-- Credentials from environment variables -->
  </repository>
</backup>
```

---

#### Option C: Managed Identity (Azure VM/AKS)

Best for production workloads running on Azure infrastructure. **Most secure** - no credentials at all!

**Enable Managed Identity:**
```bash
# For Azure VM
az vm identity assign \
  --name YOUR_VM_NAME \
  --resource-group YOUR_RESOURCE_GROUP

# Get the managed identity principal ID
PRINCIPAL_ID=$(az vm identity show \
  --name YOUR_VM_NAME \
  --resource-group YOUR_RESOURCE_GROUP \
  --query principalId -o tsv)

# Grant Storage Blob Data Contributor role
az role assignment create \
  --role "Storage Blob Data Contributor" \
  --assignee $PRINCIPAL_ID \
  --scope /subscriptions/YOUR_SUBSCRIPTION_ID/resourceGroups/YOUR_RESOURCE_GROUP/providers/Microsoft.Storage/storageAccounts/YOUR_ACCOUNT_NAME
```

**Configuration in solr.xml:**
```xml
<backup>
  <repository name="blob" class="org.apache.solr.blob.BlobBackupRepository" default="false">
    <str name="blob.container.name">YOUR_CONTAINER_NAME</str>
    <str name="blob.endpoint">https://YOUR_ACCOUNT_NAME.blob.core.windows.net</str>
    <!-- No credentials needed - Azure handles authentication automatically -->
  </repository>
</backup>
```

---

#### Why Use Azure Identity?

**Security Benefits:**
- ✅ **Zero secrets** in configuration files
- ✅ **Automatic credential rotation** via Azure AD
- ✅ **Fine-grained RBAC** access control
- ✅ **Full audit logging** via Azure AD
- ✅ **Compliance-friendly** (SOC 2, ISO 27001, etc.)
- ✅ **Token-based** authentication (short-lived tokens)

**Operational Benefits:**
- ✅ **No credential management** overhead
- ✅ **Works across environments** (dev, staging, prod)
- ✅ **Integrates with Azure services** seamlessly
- ✅ **Supports multiple identities** (users, service principals, managed identities)

**Performance:**
- Slightly slower than key-based auth (~5-10 seconds overhead for token acquisition)
- Negligible for large backups (overhead is constant, not proportional to data size)
- Well worth the security benefits

## Authentication Comparison

| Method | Security | Setup | Best For | Credentials in Config | Production |
|--------|----------|-------|----------|----------------------|------------|
| Connection String | ⚠️ Low | ⭐ Simple | Development | ❌ Full account key | ❌ Dev only |
| Account Key | ⚠️ Low | ⭐ Simple | Development | ❌ Full account key | ⚠️ Caution |
| **SAS Token** | ✅ Good | ⭐⭐ Medium | **Production** | ⚠️ Time-limited token | ✅ **Recommended** |
| Azure Identity (CLI) | ✅ Excellent | ⭐⭐ Medium | Local Dev/Test | ✅ None (uses login) | ✅ Dev/Test |
| **Azure Identity (SP)** | ✅ Excellent | ⭐⭐⭐ Complex | **CI/CD/Production** | ⚠️ Scoped credentials | ✅ **Recommended** |
| Azure Identity (MI) | ✅✅ Best | ⭐⭐⭐ Complex | **Azure VMs/AKS** | ✅ **None** | ✅✅ **Best** |

## Troubleshooting

### SAS Token Issues

**Error: "Failed to check existence" or "403 Forbidden"**

This usually means the SAS token lacks required permissions. Verify:
1. ✅ Resource types include: **Service, Container, and Object** (`srt=sco`)
   - ❌ Wrong: `srt=c` (container only)
   - ✅ Correct: `srt=sco` (service, container, object)
2. ✅ Permissions include at least: **Read, Write, Delete, List, Add, Create** (`sp=rwdlac`)
3. ✅ Token has not expired
4. ✅ `&` characters are escaped as `&amp;` in XML
5. ✅ Container already exists in Azure Blob Storage

**Error: "Signature did not match"**

1. Check that `&` characters are properly escaped as `&amp;` in solr.xml
2. Ensure no extra whitespace or line breaks in the token
3. Remove the leading `?` from the token if present
4. Verify the token was copied completely

### Azure Identity Issues

**Error: "403 Forbidden" or "AuthorizationFailed"**

This means your identity lacks the required permissions. Verify:

1. ✅ **Azure CLI:** You're logged in with `az login`
2. ✅ **RBAC Role:** Identity has "Storage Blob Data Contributor" role
3. ✅ **Scope:** Role is assigned at the correct scope (storage account level)
4. ✅ **Token:** For CLI, run `az account get-access-token --resource https://storage.azure.com/` to verify token

**Check role assignment:**
```bash
# List all role assignments for the storage account
az role assignment list \
  --scope /subscriptions/YOUR_SUBSCRIPTION_ID/resourceGroups/YOUR_RESOURCE_GROUP/providers/Microsoft.Storage/storageAccounts/YOUR_ACCOUNT_NAME \
  --query "[].{Principal:principalName, Role:roleDefinitionName}" -o table
```

**Error: "DefaultAzureCredential failed to retrieve token"**

This means the credential chain couldn't find valid credentials. Check:

1. **Azure CLI:** Ensure `az login` is successful and not expired
2. **Service Principal:** Verify environment variables or solr.xml credentials are correct
3. **Managed Identity:** Ensure it's enabled on the VM/AKS and has permissions
4. **Token expiry:** Azure CLI tokens expire - re-run `az login` if needed

**Performance slower than expected:**

Azure Identity adds ~5-10 seconds overhead for token acquisition. This is normal and expected:
- First operation: ~10-15 seconds (token acquisition)
- Subsequent operations: ~5 seconds (token refresh)
- For large backups (GB/TB), this overhead is negligible

## Usage

Once you've configured authentication in `solr.xml`, you can use standard Solr backup/restore commands.

### Create a Backup

```bash
# Create a backup of a collection
curl "http://localhost:8983/solr/admin/collections?action=BACKUP&name=my-backup&collection=my-collection&repository=blob&location=/"

# Example response:
# {
#   "responseHeader": {"status": 0, "QTime": 1234},
#   "response": {
#     "collection": "my-collection",
#     "backupId": 1,
#     "indexFileCount": 156,
#     "indexSizeMB": 245.5
#   }
# }
```

**Parameters:**
- `name` - Backup name (used for restore)
- `collection` - Source collection to backup
- `repository` - Repository name from solr.xml (e.g., `blob`)
- `location` - Path in blob container (use `/` for root, or `/backups/` for subdirectory)

### Restore from Backup

```bash
# Restore a backup to a new or existing collection
curl "http://localhost:8983/solr/admin/collections?action=RESTORE&name=my-backup&collection=my-collection-restored&repository=blob&location=/"

# Example response:
# {
#   "responseHeader": {"status": 0, "QTime": 567},
#   "success": {...}
# }
```

**Parameters:**
- `name` - Backup name to restore
- `collection` - Target collection name (can be different from original)
- `repository` - Repository name from solr.xml
- `location` - Same path used during backup

### List Backups

```bash
# List all backups at a location
curl "http://localhost:8983/solr/admin/collections?action=LISTBACKUP&name=my-backup&repository=blob&location=/"

# Example response:
# {
#   "responseHeader": {"status": 0},
#   "backups": [
#     {"backupId": 1, "indexFileCount": 156, "indexSizeMB": 245.5},
#     {"backupId": 2, "indexFileCount": 158, "indexSizeMB": 247.1}
#   ]
# }
```

### Delete a Backup

```bash
# Delete a specific backup
curl "http://localhost:8983/solr/admin/collections?action=DELETEBACKUP&name=my-backup&backupId=1&repository=blob&location=/"
```

**Note:** The `location` parameter should be `/` (root of container) or a subdirectory path like `/backups/`. The path must not have a trailing slash except for root.

### Best Practices

1. **Naming Convention:** Use descriptive backup names with timestamps
   ```bash
   curl "...&name=my-collection-2025-10-08&..."
   ```

2. **Regular Testing:** Periodically test restore operations
   ```bash
   # Restore to a test collection
   curl "...&collection=my-collection-test&..."
   ```

3. **Multiple Backups:** Keep multiple backup versions
   ```bash
   # Backups are versioned automatically (backupId)
   curl "...action=LISTBACKUP..." # View all versions
   ```

4. **Monitor Progress:** Use Solr admin UI or check logs
   ```bash
   tail -f $SOLR_HOME/logs/solr.log | grep -i backup
   ```
