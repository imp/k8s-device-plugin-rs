use super::*;

#[derive(Debug)]
pub struct RegistrationClient {
    inner: v1beta1::RegistrationClient<Channel>,
}

impl RegistrationClient {
    pub async fn new(path: String) -> tonic::Result<Self> {
        v1beta1::RegistrationClient::connect(path)
            .await
            .map(|inner| Self { inner })
            .map_err(|err| tonic::Status::from_error(Box::new(err)))
    }

    pub async fn register(&mut self, endpoint: &str, resource_name: &str) -> tonic::Result<()> {
        let version = v1beta1::VERSION.to_string();
        let endpoint = endpoint.to_string();
        let resource_name = resource_name.to_string();

        let request = v1beta1::RegisterRequest {
            version,
            endpoint,
            resource_name,
            options: None,
        };

        self.inner.register(request).await?;
        Ok(())
    }
}
