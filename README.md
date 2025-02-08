# Beggar

Beggar is a simple S3 interface emulating the S3 API.  It is designed to be used in an environment where a real S3 implementation is not available.  It is not intended to be a full implementation of the S3 API, but rather a subset of the API that is useful for majority of use cases.

This application features storing the actual data on the local file system or NAS.  While the item metadata is stored in Postgres Database.

## Goals

- Provide a simple S3 interface
- Be easy to deploy
- Be easy to use
- Be easy to maintain
- Must be able to run in a container
- Fast and consume minimal resources
- High performance
- Minimal access control

## Non Goals

- Full S3 API implementation
- No replication
- No encryption
- No versioning
- No lifecycle policies


## Usage

### Environment Variables

The minimum environment variable required for **development** is `DATABASE_URL`.  The `DATABASE_URL` is the connection string to the Postgres database.

```bash
# Start the server
$ beggar --access-key $ACCESS_KEY --secret-key $SECRET_KEY --port $PORT $DATA_DIR
```

Where:
- `ACCESS_KEY` is the access key to use for authentication
- `SECRET_KEY` is the secret key to use for authentication
- `PORT` is the port to listen on
- `DATA_DIR` is the directory to store the data, ex: `/data/beggar`

### Configuration

The configurations are placed in the directory `./config` and are loaded in the following order:

1. `default.yaml` - Optional
2. `local.yaml` - Optional
3. `application.yaml` - Required

The configurations are them merged in the order above.  The `local.yaml` is useful for local development and should not be checked into source control.  It is also preferrable to separate the credentials from the default configuration.

### Schema migration

The application used `sqlx` for database access and `migrate` for schema migration.  The schema migration is done using the `sqlx migrate` tool.  The schema migration files are placed in the `./migrations` directory.  The schema migration is done automatically when the application starts.

During **development** is is best to run the migration manually using the following command:

```bash
sqlx migrate run --source ./migrations
```

### Accessing using the aws cli

#### Put Object

```bash
aws s3api put-object --profile dev --checksum-algorithm SHA256 --bucket test-bucket --key hack/temp.json --body openapi.yaml --metadata '{"source": "app1"}' --no-cli-pager
```
Sample output

```json
{
    "ETag": "106044d0f81c0c96956c39fc8abcf5f7",
    "ChecksumSHA256": "LtS1aChFv/xwGBc+5rU8JK+54qrEBIMaUuhnkK04lVs="
}
```

#### Get Object

```bash
aws s3api get-object --profile dev --bucket test-bucket --key hack/temp.json --no-cli-pager /tmp/copy-openapi.yaml
```

Sample output:

```json
{
    "LastModified": "2025-02-07T23:47:34+00:00",
    "ContentLength": 841452,
    "ETag": "106044d0f81c0c96956c39fc8abcf5f7",
    "ChecksumSHA256": "LtS1aChFv/xwGBc+5rU8JK+54qrEBIMaUuhnkK04lVs=",
    "Metadata": {
        "source": "app1"
    }
}
```
#### List Objects V2

```bash
aws s3api list-objects-v2 --profile dev --bucket test-bucket --prefix hack  --no-cli-pager
```

Sample output:

```json
{
    "Contents": [
        {
            "Key": "hack/temp.json",
            "LastModified": "2025-02-07T23:47:34.975000+00:00",
            "ETag": "106044d0f81c0c96956c39fc8abcf5f7",
            "Size": 841452
        }
    ],
    "RequestCharged": null,
    "Prefix": "hack"
}
```

#### Head object

```bash
aws s3api head-object --profile dev --bucket test-bucket --key hack/temp.json --no-cli-pager
```

Sample output:

```json
{
    "LastModified": "2025-02-07T23:47:34+00:00",
    "ContentLength": 841452,
    "ETag": "106044d0f81c0c96956c39fc8abcf5f7",
    "ContentType": "application/octet-stream",
    "Metadata": {
        "source": "app1"
    }
}

```

#### List buckets

```bash 
aws s3api list-buckets --profile dev --prefix test-buckets  --no-cli-pager

Sample output:
```
```json
{
    "Buckets": [
        {
            "Name": "test-bucket",
            "CreationDate": "2025-02-07T01:10:47.826000+00:00"
        }
    ],
    "Owner": null,
    "Prefix": null
}
```

#### Head bucket

```bash
aws s3api head-bucket --profile dev --bucket test-bucket --key hack/temp.json --no-cli-pager
```

No output is shown, but if the bucket exists, the command will return a 200 status code.


#### Create multipart upload

```bash
aws s3api create-multipart-upload  --profile dev --bucket test-buckets --key large.zip --metadata '{"source": "app2"}'  --no-cli-pager
```

Sample output:

```json
{
    "Bucket": "test-buckets",
    "Key": "large.zip",
    "UploadId": "339a0963-b299-4c9c-9577-f45e996a524a"
}

```

#### Upload part

##### Split manually the file

```bash
split -b 5M -a 5 --numeric-suffixes  JetBrainsMono.zip JetBrainsMono.zip.
```
Note: The above command will split the file into 5MB parts. Suffixed with a number.
The split files will be named `JetBrainsMono.zip.00000`, `JetBrainsMono.zip.00001`, `JetBrainsMono.zip.00002`, etc.



```bash

aws s3api upload-part --profile dev --bucket test-buckets --key large.zip --part-number 1 --upload-id 339a0963-b299-4c9c-9577-f45e996a524a --body JetBrainsMono.zip.00000 --no-cli-pager
aws s3api upload-part --profile dev --bucket test-buckets --key large.zip --part-number 2 --upload-id 339a0963-b299-4c9c-9577-f45e996a524a --body JetBrainsMono.zip.00001 --no-cli-pager
aws s3api upload-part --profile dev --bucket test-buckets --key large.zip --part-number 3 --upload-id 339a0963-b299-4c9c-9577-f45e996a524a --body JetBrainsMono.zip.00002 --no-cli-pager
aws s3api upload-part --profile dev --bucket test-buckets --key large.zip --part-number 4 --upload-id 339a0963-b299-4c9c-9577-f45e996a524a --body JetBrainsMono.zip.00003 --no-cli-pager
aws s3api upload-part --profile dev --bucket test-buckets --key large.zip --part-number 5 --upload-id 339a0963-b299-4c9c-9577-f45e996a524a --body JetBrainsMono.zip.00004 --no-cli-pager
aws s3api upload-part --profile dev --bucket test-buckets --key large.zip --part-number 6 --upload-id 339a0963-b299-4c9c-9577-f45e996a524a --body JetBrainsMono.zip.00005 --no-cli-pager
aws s3api upload-part --profile dev --bucket test-buckets --key large.zip --part-number 7 --upload-id 339a0963-b299-4c9c-9577-f45e996a524a --body JetBrainsMono.zip.00006 --no-cli-pager
aws s3api upload-part --profile dev --bucket test-buckets --key large.zip --part-number 8 --upload-id 339a0963-b299-4c9c-9577-f45e996a524a --body JetBrainsMono.zip.00007 --no-cli-pager
aws s3api upload-part --profile dev --bucket test-buckets --key large.zip --part-number 9 --upload-id 339a0963-b299-4c9c-9577-f45e996a524a --body JetBrainsMono.zip.00008 --no-cli-pager
aws s3api upload-part --profile dev --bucket test-buckets --key large.zip --part-number 10 --upload-id 339a0963-b299-4c9c-9577-f45e996a524a --body JetBrainsMono.zip.00009 --no-cli-pager
aws s3api upload-part --profile dev --bucket test-buckets --key large.zip --part-number 11 --upload-id 339a0963-b299-4c9c-9577-f45e996a524a --body JetBrainsMono.zip.00010 --no-cli-pager
aws s3api upload-part --profile dev --bucket test-buckets --key large.zip --part-number 12 --upload-id 339a0963-b299-4c9c-9577-f45e996a524a --body JetBrainsMono.zip.00011 --no-cli-pager
aws s3api upload-part --profile dev --bucket test-buckets --key large.zip --part-number 13 --upload-id 339a0963-b299-4c9c-9577-f45e996a524a --body JetBrainsMono.zip.00012 --no-cli-pager
aws s3api upload-part --profile dev --bucket test-buckets --key large.zip --part-number 14 --upload-id 339a0963-b299-4c9c-9577-f45e996a524a --body JetBrainsMono.zip.00013 --no-cli-pager
aws s3api upload-part --profile dev --bucket test-buckets --key large.zip --part-number 15 --upload-id 339a0963-b299-4c9c-9577-f45e996a524a --body JetBrainsMono.zip.00014 --no-cli-pager
aws s3api upload-part --profile dev --bucket test-buckets --key large.zip --part-number 16 --upload-id 339a0963-b299-4c9c-9577-f45e996a524a --body JetBrainsMono.zip.00015 --no-cli-pager
aws s3api upload-part --profile dev --bucket test-buckets --key large.zip --part-number 17 --upload-id 339a0963-b299-4c9c-9577-f45e996a524a --body JetBrainsMono.zip.00016 --no-cli-pager
aws s3api upload-part --profile dev --bucket test-buckets --key large.zip --part-number 18 --upload-id 339a0963-b299-4c9c-9577-f45e996a524a --body JetBrainsMono.zip.00017 --no-cli-pager
aws s3api upload-part --profile dev --bucket test-buckets --key large.zip --part-number 19 --upload-id 339a0963-b299-4c9c-9577-f45e996a524a --body JetBrainsMono.zip.00018 --no-cli-pager
aws s3api upload-part --profile dev --bucket test-buckets --key large.zip --part-number 20 --upload-id 339a0963-b299-4c9c-9577-f45e996a524a --body JetBrainsMono.zip.00019 --no-cli-pager
aws s3api upload-part --profile dev --bucket test-buckets --key large.zip --part-number 21 --upload-id 339a0963-b299-4c9c-9577-f45e996a524a --body JetBrainsMono.zip.00020 --no-cli-pager
```

Sample Output:

Each command will return a JSON output with the ETag of the uploaded part.

```json
{
    "ETag": "\"c54003fb4dad33b877513290e06deec4\""
}
```
#### List Parts

```bash
 aws s3api list-parts --profile dev --bucket test-buckets --key large.zip --upload-id 339a0963-b299-4c9c-9577-f45e996a524a --no-cli-pager
 ```

 Sample Output:

 ```json
 {
    "Parts": [
        {
            "PartNumber": 1,
            "LastModified": "2025-02-08T03:32:42.675000+00:00",
            "Size": 5242880
        },
        {
            "PartNumber": 2,
            "LastModified": "2025-02-08T03:33:13.645000+00:00",
            "Size": 5242880
        },
        {
            "PartNumber": 3,
            "LastModified": "2025-02-08T03:33:16.934000+00:00",
            "Size": 5242880
        },
        {
            "PartNumber": 4,
            "LastModified": "2025-02-08T03:33:20.344000+00:00",
            "Size": 5242880
        },
        {
            "PartNumber": 5,
            "LastModified": "2025-02-08T03:33:23.671000+00:00",
            "Size": 5242880
        },
        {
            "PartNumber": 6,
            "LastModified": "2025-02-08T03:33:26.972000+00:00",
            "Size": 5242880
        },
        {
            "PartNumber": 7,
            "LastModified": "2025-02-08T03:33:30.399000+00:00",
            "Size": 5242880
        },
        {
            "PartNumber": 8,
            "LastModified": "2025-02-08T03:33:33.871000+00:00",
            "Size": 5242880
        },
        {
            "PartNumber": 9,
            "LastModified": "2025-02-08T03:33:37.204000+00:00",
            "Size": 5242880
        },
        {
            "PartNumber": 10,
            "LastModified": "2025-02-08T03:33:40.500000+00:00",
            "Size": 5242880
        },
        {
            "PartNumber": 11,
            "LastModified": "2025-02-08T03:33:43.770000+00:00",
            "Size": 5242880
        },
        {
            "PartNumber": 12,
            "LastModified": "2025-02-08T03:33:47.008000+00:00",
            "Size": 5242880
        },
        {
            "PartNumber": 13,
            "LastModified": "2025-02-08T03:33:50.229000+00:00",
            "Size": 5242880
        },
        {
            "PartNumber": 14,
            "LastModified": "2025-02-08T03:33:53.477000+00:00",
            "Size": 5242880
        },
        {
            "PartNumber": 15,
            "LastModified": "2025-02-08T03:33:56.748000+00:00",
            "Size": 5242880
        },
        {
            "PartNumber": 16,
            "LastModified": "2025-02-08T03:33:59.931000+00:00",
            "Size": 5242880
        },
        {
            "PartNumber": 17,
            "LastModified": "2025-02-08T03:34:03.272000+00:00",
            "Size": 5242880
        },
        {
            "PartNumber": 18,
            "LastModified": "2025-02-08T03:34:06.533000+00:00",
            "Size": 5242880
        },
        {
            "PartNumber": 19,
            "LastModified": "2025-02-08T03:34:09.853000+00:00",
            "Size": 5242880
        },
        {
            "PartNumber": 20,
            "LastModified": "2025-02-08T03:34:13.131000+00:00",
            "Size": 5242880
        },
        {
            "PartNumber": 21,
            "LastModified": "2025-02-08T03:34:16.053000+00:00",
            "Size": 374905
        }
    ],
    "ChecksumAlgorithm": null,
    "Initiator": null,
    "Owner": null,
    "StorageClass": null,
    "ChecksumType": null
}

 ```