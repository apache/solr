Apache Solr - Blob Repository
=============================

The Blob repository is a backup repository implementation designed to provide backup/restore functionality to remote blob store providers, like S3, GCS, ABS.

Current implementation only supports S3.

# S3 Storage

Add this to your `solr.xml`:

    <backup>
        <repository name="s3" class="org.apache.solr.s3.S3BackupRepository" default="false">
            <str name="blob.store.provider.type">S3</str>
            <str name="blob.store.bucket.name">BUCKET_NAME</str>
            <str name="blob.store.region">us-west-2</str>
        </repository>
    </backup>

This plugin uses the [default AWS credentials provider chain](https://docs.aws.amazon.com/sdk-for-java/v1/developer-guide/credentials.html), so ensure that your credentials are set appropriately (e.g., via env var, or in `~/.aws/credentials`, etc.).

## Testing locally

To run / test locally, first spin up S3Mock:

    docker run -p 9090:9090 --env initialBuckets=TEST_BUCKET --env root=/data -v /tmp/s3:/data -t adobe/s3mock

Add this to your `solr.xml`:

    <backup>
        <repository name="s3" class="org.apache.solr.s3.S3BackupRepository" default="false">
            <str name="blob.store.provider.type">S3Mock</str>
            <str name="blob.store.bucket.name">TEST_BUCKET</str>
        </repository>
    </backup>
