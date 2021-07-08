Apache Solr - S3 Repository
===========================

This S3 repository is a backup repository implementation designed to provide backup/restore functionality to Amazon S3.

# Getting Started

Add this to your `solr.xml`:

    <backup>
        <repository name="s3" class="org.apache.solr.s3.S3BackupRepository" default="false">
            <str name="blob.s3.bucket.name">BUCKET_NAME</str>
            <str name="blob.s3.region">us-west-2</str>
        </repository>
    </backup>

This plugin uses the [default AWS credentials provider chain](https://docs.aws.amazon.com/sdk-for-java/v1/developer-guide/credentials.html), so ensure that your credentials are set appropriately (e.g., via env var, or in `~/.aws/credentials`, etc.).

## Testing locally

To run / test locally, first spin up S3Mock:

    mkdir /tmp/s3
    docker run -p 9090:9090 --env initialBuckets=TEST_BUCKET --env root=/data -v /tmp/s3:/data -t adobe/s3mock

Add this to your `solr.xml`:

    <backup>
        <repository name="s3" class="org.apache.solr.s3.S3BackupRepository" default="false">
            <str name="blob.s3.bucket.name">TEST_BUCKET</str>
            <str name="blob.s3.mock">true</str>
        </repository>
    </backup>

Start Solr, and create a collection (e.g., "foo"). Then hit the following URL, which will take a backup and persist it in S3Mock under the name `test`:

http://localhost:8983/solr/admin/collections?action=BACKUP&repository=s3&location=s3:/&collection=foo&name=test

To restore from that backup, hit this URL, which will create a new collection `bar` with the contents of the backup `test` you just made: 

http://localhost:8983/solr/admin/collections?action=RESTORE&repository=s3&location=s3:/&name=test&collection=bar
