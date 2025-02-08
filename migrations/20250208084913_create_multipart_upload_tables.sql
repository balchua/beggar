CREATE TABLE IF NOT EXISTS multipart_upload (
    upload_id VARCHAR(255) NOT NULL,
    bucket VARCHAR(50) NOT NULL,
    key VARCHAR(255) NOT NULL,
    metadata TEXT NOT NULL,
    last_modified TIMESTAMP NOT NULL,
    access_key VARCHAR(255) NOT NULL,
    PRIMARY KEY (upload_id),
    UNIQUE(upload_id, bucket, key)
 );


 CREATE TABLE IF NOT EXISTS multipart_upload_part(
    upload_id VARCHAR(255) NOT NULL,
    part_number INTEGER NOT NULL,
    last_modified TIMESTAMP NOT NULL,
    md5 VARCHAR(255) NOT NULL,
    -- The length of this field is the size of bucket and key together
    data_location VARCHAR(310) NOT NULL,
    PRIMARY KEY (upload_id, part_number),
    FOREIGN KEY (upload_id) REFERENCES multipart_upload(upload_id) ON DELETE CASCADE
 );

 

 