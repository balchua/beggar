# Beggar

![Rust CI](https://github.com/your-username/beggar/workflows/Rust%20CI/badge.svg)
[![codecov](https://codecov.io/gh/your-username/beggar/branch/main/graph/badge.svg)](https://codecov.io/gh/your-username/beggar)

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

## Development

### Prerequisites
- Rust stable
- PostgreSQL

### Setup
```bash
# Clone the repository
git clone https://github.com/your-username/beggar.git
cd beggar

# Build the project
cargo build

# Run tests
cargo test
```

### Build
To build for production release use the following

```bash
cargo build --release
```

### Test

#### Code Coverage

This project uses [`cargo-llvm-cov`](https://github.com/taiki-e/cargo-llvm-cov) to generate code coverage reports. Code coverage reports are automatically generated during CI runs and uploaded to [Codecov](https://codecov.io/gh/your-username/beggar).

To generate coverage reports locally:

```bash
# Install cargo-llvm-cov if you don't have it
cargo install cargo-llvm-cov

# Generate HTML and LCov report
cargo llvm-cov test --html --ignore-filename-regex="(error|main)\.rs"
cargo llvm-cov report --lcov --output-path lcov.info
```

#### VSCode Integration

Install [Coverage Gutters](https://marketplace.visualstudio.com/items?itemName=ryanluker.vscode-coverage-gutters) VSCode extension to see code coverage in the editor.

```bash
code --install-extension ryanluker.vscode-coverage-gutters
```

### Database Setup
```bash
# Set up the database
export DATABASE_URL=postgres://username:password@localhost:5432/beggar_db
```

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

### SQLx Offline Mode

This project uses SQLx offline mode for CI, which allows building and verifying SQL queries without requiring a live database connection. This is helpful for CI environments and for development when you don't have access to the database.

To update the SQLx offline data:

```bash
# Make sure you have a database connection available
export DATABASE_URL=postgres://username:password@localhost:5432/beggar_db

# Run migrations to ensure the schema is up to date
sqlx migrate run

# Generate offline data
cargo sqlx prepare --merged
```

The generated `.sqlx` directory contains the prepared SQL queries, which should be committed to the repository.

To build using offline mode:

```bash
# Set SQLX_OFFLINE environment variable
export SQLX_OFFLINE=true

# Now you can build without a database connection
cargo build
```

In CI, the `SQLX_OFFLINE` environment variable is set to `true` by default to use the prepared queries.

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
aws s3api create-multipart-upload  --profile dev --bucket test-buckets --key crictl --metadata '{"source": "app2"}'  --no-cli-pager
```

Sample output:

```json
{
    "Bucket": "test-buckets",
    "Key": "crictl",
    "UploadId": "624be2ac-073a-452d-95d9-60c838877232"
}

```

#### Upload part

##### Split manually the file

```bash
split -b 5M -a 5 --numeric-suffixes crictl split/crictl.
```
Note: The above command will split the file into 5MB parts. Suffixed with a number.
The split files will be named `crictl.00000`, `crictl.00001`, `crictl.00002`, etc.



```bash

aws s3api upload-part --profile dev --bucket test-buckets --key crictl --part-number 1 --upload-id 624be2ac-073a-452d-95d9-60c838877232 --body crictl.00000 --no-cli-pager
aws s3api upload-part --profile dev --bucket test-buckets --key crictl --part-number 2 --upload-id 624be2ac-073a-452d-95d9-60c838877232 --body crictl.00001 --no-cli-pager
aws s3api upload-part --profile dev --bucket test-buckets --key crictl --part-number 3 --upload-id 624be2ac-073a-452d-95d9-60c838877232 --body crictl.00002 --no-cli-pager
aws s3api upload-part --profile dev --bucket test-buckets --key crictl --part-number 4 --upload-id 624be2ac-073a-452d-95d9-60c838877232 --body crictl.00003 --no-cli-pager
aws s3api upload-part --profile dev --bucket test-buckets --key crictl --part-number 5 --upload-id 624be2ac-073a-452d-95d9-60c838877232 --body crictl.00004 --no-cli-pager
aws s3api upload-part --profile dev --bucket test-buckets --key crictl --part-number 6 --upload-id 624be2ac-073a-452d-95d9-60c838877232 --body crictl.00005 --no-cli-pager
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
 aws s3api list-parts --profile dev --bucket test-buckets --key crictl --upload-id 624be2ac-073a-452d-95d9-60c838877232 --no-cli-pager
 ```

 Sample Output:

 ```json
 {
    "Parts": [
        {
            "PartNumber": 1,
            "LastModified": "2025-02-08T07:13:25.301000+00:00",
            "ETag": "75754efcbf4bed4512c000cb1fa14833",
            "Size": 5242880
        },
        {
            "PartNumber": 2,
            "LastModified": "2025-02-08T07:13:28.252000+00:00",
            "ETag": "afad18abd66efd67b147ec620520da36",
            "Size": 5242880
        },
        {
            "PartNumber": 3,
            "LastModified": "2025-02-08T07:13:31.196000+00:00",
            "ETag": "1dd93451a2c69c9d043ca4ce52f6ff40",
            "Size": 5242880
        },
        {
            "PartNumber": 4,
            "LastModified": "2025-02-08T07:13:34.135000+00:00",
            "ETag": "6345246af82f58e48edd52cbc489ce0a",
            "Size": 5242880
        },
        {
            "PartNumber": 5,
            "LastModified": "2025-02-08T07:13:37.102000+00:00",
            "ETag": "2829d3fcdab531ee21df8fb223ec1267",
            "Size": 5242880
        },
        {
            "PartNumber": 6,
            "LastModified": "2025-02-08T07:13:40.045000+00:00",
            "ETag": "c7d9dbed17bc9c11f9ede32494fac6c1",
            "Size": 5011228
        }
    ],
    "ChecksumAlgorithm": null,
    "Initiator": null,
    "Owner": null,
    "StorageClass": null,
    "ChecksumType": null
}

 ```

 #### Complete Multipart Upload

Check the sample [file](./sample_complete_multipart.json) for the sample JSON to use.

 ```bash
 aws s3api complete-multipart-upload --profile dev --bucket test-buckets --key crictl --upload-id 624be2ac-073a-452d-95d9-60c838877232 --multipart-upload file://sample_complete_multipart.json --no-cli-pager
 ```

Sample Output:

```json
{
    "Bucket": "test-buckets",
    "Key": "crictl",
    "ETag": "808a27eb31b920d66efce3df2ff394de"
}
```

 #### Abort Multipart Upload

 ```bash
 aws s3api abort-multipart-upload --profile dev --bucket test-buckets --key crictl --upload-id 624be2ac-073a-452d-95d9-60c838877232 --no-cli-pager
 ```

## License

<!-- Add your license information here -->
