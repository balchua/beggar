use crate::storage_backend::InternalInfo;

use stdx::default::default;

pub fn modify_internal_info(
    info: &mut serde_json::Map<String, serde_json::Value>,
    checksum: &s3s::dto::Checksum,
) {
    if let Some(checksum_crc32) = &checksum.checksum_crc32 {
        info.insert(
            "checksum_crc32".to_owned(),
            serde_json::Value::String(checksum_crc32.clone()),
        );
    }
    if let Some(checksum_crc32c) = &checksum.checksum_crc32c {
        info.insert(
            "checksum_crc32c".to_owned(),
            serde_json::Value::String(checksum_crc32c.clone()),
        );
    }
    if let Some(checksum_sha1) = &checksum.checksum_sha1 {
        info.insert(
            "checksum_sha1".to_owned(),
            serde_json::Value::String(checksum_sha1.clone()),
        );
    }
    if let Some(checksum_sha256) = &checksum.checksum_sha256 {
        info.insert(
            "checksum_sha256".to_owned(),
            serde_json::Value::String(checksum_sha256.clone()),
        );
    }
}

pub fn from_internal_info(info: &InternalInfo) -> s3s::dto::Checksum {
    let mut ans: s3s::dto::Checksum = default();
    if let Some(checksum_crc32) = info.get("checksum_crc32") {
        ans.checksum_crc32 = Some(checksum_crc32.as_str().unwrap().to_owned());
    }
    if let Some(checksum_crc32c) = info.get("checksum_crc32c") {
        ans.checksum_crc32c = Some(checksum_crc32c.as_str().unwrap().to_owned());
    }
    if let Some(checksum_sha1) = info.get("checksum_sha1") {
        ans.checksum_sha1 = Some(checksum_sha1.as_str().unwrap().to_owned());
    }
    if let Some(checksum_sha256) = info.get("checksum_sha256") {
        ans.checksum_sha256 = Some(checksum_sha256.as_str().unwrap().to_owned());
    }
    ans
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_modify_internal_info() {
        let mut info: serde_json::Map<String, serde_json::Value> = serde_json::Map::new();
        let checksum = s3s::dto::Checksum {
            checksum_crc32: Some("crc32".to_string()),
            checksum_crc32c: Some("crc32c".to_string()),
            checksum_sha1: Some("sha1".to_string()),
            checksum_sha256: Some("sha256".to_string()),
        };

        modify_internal_info(&mut info, &checksum);

        assert_eq!(info.get("checksum_crc32"), Some(&json!("crc32")));
        assert_eq!(info.get("checksum_crc32c"), Some(&json!("crc32c")));
        assert_eq!(info.get("checksum_sha1"), Some(&json!("sha1")));
        assert_eq!(info.get("checksum_sha256"), Some(&json!("sha256")));
    }

    #[test]
    fn test_from_internal_info() {
        let info: InternalInfo = serde_json::from_str(
            r#"{
            "checksum_crc32": "crc32",
            "checksum_crc32c": "crc32c",
            "checksum_sha1": "sha1",
            "checksum_sha256": "sha256"
        }"#,
        )
        .unwrap();

        let checksum = from_internal_info(&info);

        assert_eq!(checksum.checksum_crc32, Some("crc32".to_string()));
        assert_eq!(checksum.checksum_crc32c, Some("crc32c".to_string()));
        assert_eq!(checksum.checksum_sha1, Some("sha1".to_string()));
        assert_eq!(checksum.checksum_sha256, Some("sha256".to_string()));
    }

    #[test]
    fn test_from_internal_info_missing_fields() {
        let info: InternalInfo = serde_json::from_str(r"{}").unwrap();
        let checksum = from_internal_info(&info);

        assert_eq!(checksum.checksum_crc32, None);
        assert_eq!(checksum.checksum_crc32c, None);
        assert_eq!(checksum.checksum_sha1, None);
        assert_eq!(checksum.checksum_sha256, None);
    }
}
