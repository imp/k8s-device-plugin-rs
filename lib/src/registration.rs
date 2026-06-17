use super::*;
use hyper_util::rt::TokioIo;
use std::path::PathBuf;

use tokio::net::UnixStream;
use tonic::transport::Endpoint;
use tonic::transport::Uri;
use tower::service_fn;

#[derive(Debug)]
pub struct RegistrationClient {
    inner: v1beta1::RegistrationClient<Channel>,
}

impl RegistrationClient {
    pub async fn new(path: String) -> tonic::Result<Self> {
        let socket_path = PathBuf::from(path);
        let connector_path = socket_path.clone();

        // The URI host is ignored for Unix Domain Socket transport.
        let endpoint = Endpoint::try_from("http://[::]:50051").map_err(|err| {
            tonic::Status::internal(format!(
                "failed to build kubelet registration endpoint: {err}"
            ))
        })?;

        let channel = endpoint
            .connect_with_connector(service_fn(move |_: Uri| {
                let path = connector_path.clone();
                async move { UnixStream::connect(path).await.map(TokioIo::new) }
            }))
            .await
            .map_err(|err| {
                tonic::Status::unavailable(format!(
                    "failed to connect to kubelet registration socket {}: {err}",
                    socket_path.display()
                ))
            })?;

        Ok(Self {
            inner: v1beta1::RegistrationClient::new(channel),
        })
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

#[cfg(test)]
mod tests {
    use super::*;
    use k8s_device_plugin_test::registration::start_mock_registration_server;
    use tempfile::TempDir;

    #[tokio::test]
    async fn registration_client_connects_over_unix_socket() {
        let server = start_mock_registration_server(None);

        let mut client = RegistrationClient::new(server.socket_path())
            .await
            .expect("connect registration client");
        client
            .register("plugin.sock", "example.com/device")
            .await
            .expect("register call over unix socket");

        let requests = server.collected_requests().await;
        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0].endpoint, "plugin.sock");
        assert_eq!(requests[0].version, v1beta1::VERSION);
        assert_eq!(requests[0].resource_name, "example.com/device");
        assert!(requests[0].options.is_none());

        server.shutdown();
    }

    #[tokio::test]
    async fn registration_client_reports_socket_path_on_connect_failure() {
        let dir = TempDir::new().expect("create temp dir");
        let path_str = dir
            .path()
            .join("missing.sock")
            .to_string_lossy()
            .into_owned();

        let status = RegistrationClient::new(path_str.clone())
            .await
            .expect_err("connect should fail for missing socket");

        assert_eq!(status.code(), tonic::Code::Unavailable);
        assert!(
            status
                .message()
                .contains("failed to connect to kubelet registration socket")
        );
        assert!(status.message().contains(&path_str));
    }

    #[tokio::test]
    async fn registration_client_surfaces_server_registration_error() {
        let server = start_mock_registration_server(Some((
            tonic::Code::FailedPrecondition,
            "registration rejected",
        )));

        let mut client = RegistrationClient::new(server.socket_path())
            .await
            .expect("connect registration client");

        let status = client
            .register("plugin.sock", "example.com/device")
            .await
            .expect_err("register should fail when server rejects");

        assert_eq!(status.code(), tonic::Code::FailedPrecondition);
        assert!(status.message().contains("registration rejected"));

        server.shutdown();
    }
}
