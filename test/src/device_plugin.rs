use std::path::Path;
use std::path::PathBuf;

use hyper_util::rt::TokioIo;
use k8s_device_plugin_proto::v1beta1;
use tokio::net::UnixStream;
use tokio_stream::StreamExt as _;
use tonic::transport::Channel;
use tonic::transport::Endpoint;
use tonic::transport::Uri;
use tower::service_fn;

#[derive(Debug)]
pub struct MockDevicePluginClient {
    inner: v1beta1::DevicePluginClient<Channel>,
}

impl MockDevicePluginClient {
    /// Connect to a device plugin Unix socket at `path`, mirroring the kubelet side.
    pub async fn connect(path: impl AsRef<Path>) -> tonic::Result<Self> {
        let socket_path = PathBuf::from(path.as_ref());
        let connector_path = socket_path.clone();

        let endpoint = Endpoint::try_from("http://[::]:50051").map_err(|err| {
            tonic::Status::internal(format!("failed to build device plugin endpoint: {err}"))
        })?;

        let channel = endpoint
            .connect_with_connector(service_fn(move |_: Uri| {
                let path = connector_path.clone();
                async move { UnixStream::connect(path).await.map(TokioIo::new) }
            }))
            .await
            .map_err(|err| {
                tonic::Status::unavailable(format!(
                    "failed to connect to device plugin socket {}: {err}",
                    socket_path.display()
                ))
            })?;

        Ok(Self {
            inner: v1beta1::DevicePluginClient::new(channel),
        })
    }

    /// Call ListAndWatch and return the first response emitted by the plugin.
    pub async fn list_and_watch_once(&mut self) -> tonic::Result<v1beta1::ListAndWatchResponse> {
        let mut stream = self
            .inner
            .list_and_watch(tonic::Request::new(v1beta1::Empty {}))
            .await?
            .into_inner();
        stream.next().await.ok_or_else(|| {
            tonic::Status::data_loss("ListAndWatch stream ended without a response")
        })?
    }

    /// Call Allocate with a single container request containing the given device IDs.
    pub async fn allocate(
        &mut self,
        device_ids: &[&str],
    ) -> tonic::Result<v1beta1::AllocateResponse> {
        let request = v1beta1::AllocateRequest {
            container_requests: vec![v1beta1::ContainerAllocateRequest {
                devices_ids: device_ids.iter().map(|s| s.to_string()).collect(),
            }],
        };
        self.inner
            .allocate(tonic::Request::new(request))
            .await
            .map(|r| r.into_inner())
    }

    /// Call GetDevicePluginOptions.
    pub async fn get_device_plugin_options(
        &mut self,
    ) -> tonic::Result<v1beta1::DevicePluginOptions> {
        self.inner
            .get_device_plugin_options(tonic::Request::new(v1beta1::Empty {}))
            .await
            .map(|r| r.into_inner())
    }
}
