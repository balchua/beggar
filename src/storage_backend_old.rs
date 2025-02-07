use crate::error::Result;
use crate::DataStore;

#[derive(Debug)]
pub struct StorageBackend<T: DataStore> {
    pub datastore: T,
}

impl<T: DataStore> StorageBackend<T> {
    pub fn new(datastore: T) -> Self {
        Self { datastore }
    }

    pub async fn save_s3_item_detail(&self) -> Result<()> {
        self.datastore.save_s3_item_detail().await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use mockall::mock;
    use mockall::predicate::*;

    mock! {
        pub TestDataStore {}
        #[async_trait]
        impl DataStore for TestDataStore {
            async fn save_s3_item_detail(&self) -> Result<()>;
        }
    }

    #[tokio::test]
    async fn test_save_s3_item_detail() {
        let mut mock_ds = MockTestDataStore::new();
        mock_ds
            .expect_save_s3_item_detail()
            .times(1)
            .returning(|| Ok(()));

        let backend = StorageBackend::new(mock_ds);
        let result = backend.save_s3_item_detail().await;
        assert!(result.is_ok());
    }
}
