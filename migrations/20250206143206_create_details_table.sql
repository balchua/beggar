CREATE TABLE IF NOT EXISTS s3_item_detail (
    bucket VARCHAR(50) NOT NULL,
    key VARCHAR(255) NOT NULL,
    metadata TEXT NOT NULL,
    internal_info TEXT NOT NULL,
    last_modified TIMESTAMP NOT NULL,
    md5 VARCHAR(255) NOT NULL,
    PRIMARY KEY (bucket, key),
    -- The length of this field is the size of bucket and key together
    data_location VARCHAR(310) NOT NULL
 );

 