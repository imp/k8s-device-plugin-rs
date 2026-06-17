use std::collections::HashMap;
use std::fmt;
use std::fs;
use std::io;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use tokio::net::UnixListener;
use tokio::task::JoinHandle;
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::wrappers::UnixListenerStream;
use tonic::transport;
use tonic::transport::Channel;

use k8s_device_plugin_proto as proto;
pub use proto::v1beta1;

pub use health::Health;
pub use permissions::DevicePermissions;
pub use registration::RegistrationClient;

mod health;
mod permissions;
mod registration;

#[derive(Debug, Clone)]
pub struct DevicePath {
    pub host_path: PathBuf,
    pub container_path: PathBuf,
    pub permissions: DevicePermissions,
}

impl From<DevicePath> for v1beta1::DeviceSpec {
    fn from(path: DevicePath) -> Self {
        Self {
            host_path: path.host_path.to_string_lossy().into_owned(),
            container_path: path.container_path.to_string_lossy().into_owned(),
            permissions: path.permissions.to_string(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct Device {
    pub id: String,
    pub health: Health,
    pub paths: Vec<DevicePath>,
}

impl Device {
    fn to_proto(&self) -> v1beta1::Device {
        v1beta1::Device {
            id: self.id.clone(),
            health: self.health.to_string(),
            topology: None,
        }
    }

    fn to_device_specs(&self) -> Vec<v1beta1::DeviceSpec> {
        self.paths
            .iter()
            .map(|p| v1beta1::DeviceSpec {
                host_path: p.host_path.to_string_lossy().into_owned(),
                container_path: p.container_path.to_string_lossy().into_owned(),
                permissions: p.permissions.to_string(),
            })
            .collect()
    }
}

#[derive(Clone, Debug)]
pub struct DevicePlugin {
    endpoint: String,
    resource_name: String,
    service: Arc<DevicePluginService>,
}

impl DevicePlugin {
    pub fn new(resource_name: &str, service: DevicePluginService) -> Self {
        let socket_name = sanitize_socket_name(resource_name);
        let resource_name = resource_name.to_string();
        let endpoint = String::from(v1beta1::DEVICE_PLUGIN_PATH) + &socket_name;
        let service = Arc::new(service);
        Self {
            endpoint,
            resource_name,
            service,
        }
    }

    pub async fn run(&self) -> io::Result<()> {
        let mut server_handle = self.spawn_server()?;
        loop {
            // Subscribe before registering so a fast kubelet disconnect is never missed.
            let kubelet_gone = self.service.kubelet_gone.notified();
            self.register_with_retry().await?;
            tokio::select! {
                result = &mut server_handle => {
                    return result
                        .map_err(io::Error::other)?
                        .map_err(io::Error::other);
                }
                _ = kubelet_gone => {
                    // Kubelet disconnected; loop back to re-register.
                }
            }
        }
    }

    async fn register_with_retry(&self) -> io::Result<()> {
        self.try_register(Self::kubelet_socket_path(), 10, Duration::from_secs(1))
            .await
    }

    async fn try_register(
        &self,
        kubelet_socket: String,
        max_attempts: u32,
        initial_delay: Duration,
    ) -> io::Result<()> {
        let mut delay = initial_delay;
        for attempt in 1..=max_attempts {
            match self.register_at(kubelet_socket.clone()).await {
                Ok(()) => return Ok(()),
                Err(err) if attempt < max_attempts => {
                    eprintln!("Registration attempt {attempt}/{max_attempts} failed: {err}");
                    tokio::time::sleep(delay).await;
                    delay = (delay * 2).min(Duration::from_secs(30));
                }
                Err(err) => return Err(io::Error::other(err)),
            }
        }
        unreachable!()
    }

    fn spawn_server(&self) -> io::Result<JoinHandle<Result<(), transport::Error>>> {
        let incoming: UnixListenerStream = self.setup_listener()?;
        let svc = self.service();
        let router = transport::Server::builder().add_service(svc);
        let handle = tokio::spawn(router.serve_with_incoming(incoming));
        Ok(handle)
    }

    pub async fn register(&self) -> tonic::Result<()> {
        self.register_at(Self::kubelet_socket_path()).await
    }

    async fn register_at(&self, kubelet_socket: String) -> tonic::Result<()> {
        RegistrationClient::new(kubelet_socket)
            .await?
            .register(self.registration_endpoint(), &self.resource_name)
            .await
    }

    fn registration_endpoint(&self) -> &str {
        Path::new(&self.endpoint)
            .file_name()
            .and_then(|file_name| file_name.to_str())
            .unwrap_or(&self.endpoint)
    }

    fn kubelet_socket_path() -> String {
        String::from(v1beta1::DEVICE_PLUGIN_PATH) + v1beta1::KUBELET_SOCKET
    }

    fn setup_listener(&self) -> io::Result<UnixListenerStream> {
        if fs::exists(&self.endpoint)? {
            fs::remove_file(&self.endpoint)?;
        }
        UnixListener::bind(&self.endpoint).map(UnixListenerStream::new)
    }

    fn service(&self) -> v1beta1::DevicePluginServer<DevicePluginService> {
        let inner = Arc::clone(&self.service);
        v1beta1::DevicePluginServer::from_arc(inner)
    }
}

#[derive(Debug)]
pub struct DevicePluginService {
    devices: HashMap<String, Device>,
    kubelet_gone: Arc<tokio::sync::Notify>,
}

impl DevicePluginService {
    pub fn new(devices: HashMap<String, Device>) -> Self {
        Self {
            devices,
            kubelet_gone: Arc::new(tokio::sync::Notify::new()),
        }
    }
}

#[tonic::async_trait]
impl v1beta1::DevicePlugin for DevicePluginService {
    type ListAndWatchStream = ReceiverStream<tonic::Result<v1beta1::ListAndWatchResponse>>;

    async fn get_device_plugin_options(
        &self,
        _request: tonic::Request<v1beta1::Empty>,
    ) -> tonic::Result<tonic::Response<v1beta1::DevicePluginOptions>> {
        Ok(tonic::Response::new(v1beta1::DevicePluginOptions::default()))
    }

    async fn list_and_watch(
        &self,
        _request: tonic::Request<v1beta1::Empty>,
    ) -> tonic::Result<tonic::Response<Self::ListAndWatchStream>> {
        let (tx, rx) = tokio::sync::mpsc::channel(1);

        let devices: Vec<_> = self.devices.values().map(Device::to_proto).collect();
        let kubelet_gone = Arc::clone(&self.kubelet_gone);
        tokio::spawn(async move {
            let response = v1beta1::ListAndWatchResponse { devices };
            let _ = tx.send(Ok(response)).await;
            // Wait until kubelet closes the stream (tx receiver dropped)
            tx.closed().await;
            kubelet_gone.notify_one();
        });

        Ok(tonic::Response::new(Self::ListAndWatchStream::new(rx)))
    }

    async fn get_preferred_allocation(
        &self,
        _request: tonic::Request<v1beta1::PreferredAllocationRequest>,
    ) -> tonic::Result<tonic::Response<v1beta1::PreferredAllocationResponse>> {
        let status = tonic::Status::unimplemented("GetPreferredAllocation not implemented");
        Err(status)
    }

    async fn allocate(
        &self,
        request: tonic::Request<v1beta1::AllocateRequest>,
    ) -> tonic::Result<tonic::Response<v1beta1::AllocateResponse>> {
        let container_responses = request
            .into_inner()
            .container_requests
            .into_iter()
            .map(|container_request| {
                let devices = container_request
                    .devices_ids
                    .iter()
                    .map(|id| {
                        self.devices
                            .get(id)
                            .ok_or_else(|| {
                                tonic::Status::not_found(format!("device {id} not found"))
                            })
                            .map(Device::to_device_specs)
                    })
                    .collect::<tonic::Result<Vec<_>>>()?
                    .into_iter()
                    .flatten()
                    .collect::<Vec<_>>();

                Ok(v1beta1::ContainerAllocateResponse {
                    devices,
                    ..Default::default()
                })
            })
            .collect::<tonic::Result<Vec<_>>>()?;

        Ok(tonic::Response::new(v1beta1::AllocateResponse {
            container_responses,
        }))
    }

    async fn pre_start_container(
        &self,
        _request: tonic::Request<v1beta1::PreStartContainerRequest>,
    ) -> tonic::Result<tonic::Response<v1beta1::PreStartContainerResponse>> {
        Err(tonic::Status::unimplemented(
            "PreStartContainer not implemented",
        ))
    }
}

fn sanitize_socket_name(name: &str) -> String {
    name.replace(invalid_char, "_")
}

fn invalid_char(c: char) -> bool {
    !(c.is_ascii_alphanumeric() || c == '-' || c == '_')
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio_stream::StreamExt;

    #[test]
    fn kubelet_socket_path() {
        let endpoint = DevicePlugin::kubelet_socket_path();
        assert_eq!(endpoint, "/var/lib/kubelet/device-plugins/kubelet.sock");
    }

    #[test]
    fn register_uses_socket_filename_as_endpoint() {
        let plugin = DevicePlugin::new("example.com/device", make_service());

        assert_eq!(
            plugin.endpoint,
            "/var/lib/kubelet/device-plugins/example_com_device"
        );
        assert_eq!(plugin.registration_endpoint(), "example_com_device");
    }

    #[test]
    fn device_to_proto() {
        let device = Device {
            id: "dev-0".to_string(),
            health: Health::Healthy,
            paths: vec![],
        };
        let proto = device.to_proto();
        assert_eq!(proto.id, "dev-0");
        assert_eq!(proto.health, v1beta1::HEALTHY);
    }

    #[test]
    fn device_to_device_specs() {
        let device = Device {
            id: "dev-0".to_string(),
            health: Health::Healthy,
            paths: vec![DevicePath {
                host_path: PathBuf::from("/dev/mydev0"),
                container_path: PathBuf::from("/dev/mydev0"),
                permissions: DevicePermissions::rdwr(),
            }],
        };
        let specs = device.to_device_specs();
        assert_eq!(specs.len(), 1);
        assert_eq!(specs[0].host_path, "/dev/mydev0");
        assert_eq!(specs[0].container_path, "/dev/mydev0");
        assert_eq!(specs[0].permissions, "rw");
    }

    fn make_service() -> DevicePluginService {
        let device = Device {
            id: "dev-0".to_string(),
            health: Health::Healthy,
            paths: vec![DevicePath {
                host_path: PathBuf::from("/dev/mydev0"),
                container_path: PathBuf::from("/dev/mydev0"),
                permissions: DevicePermissions::rdwr(),
            }],
        };
        DevicePluginService::new(HashMap::from([("dev-0".to_string(), device)]))
    }

    #[tokio::test]
    async fn list_and_watch_sends_initial_device_list() {
        use v1beta1::DevicePlugin as _;

        let service = make_service();
        let mut stream = service
            .list_and_watch(tonic::Request::new(v1beta1::Empty {}))
            .await
            .unwrap()
            .into_inner();

        let response = stream.next().await.unwrap().unwrap();
        assert_eq!(response.devices.len(), 1);
        assert_eq!(response.devices[0].id, "dev-0");
        assert_eq!(response.devices[0].health, v1beta1::HEALTHY);
    }

    #[tokio::test]
    async fn allocate_known_device() {
        use v1beta1::DevicePlugin as _;

        let service = make_service();
        let request = tonic::Request::new(v1beta1::AllocateRequest {
            container_requests: vec![v1beta1::ContainerAllocateRequest {
                devices_ids: vec!["dev-0".to_string()],
            }],
        });

        let response = service.allocate(request).await.unwrap().into_inner();
        assert_eq!(response.container_responses.len(), 1);
        assert_eq!(response.container_responses[0].devices.len(), 1);
        assert_eq!(
            response.container_responses[0].devices[0].host_path,
            "/dev/mydev0"
        );
    }

    #[tokio::test]
    async fn allocate_unknown_device_returns_not_found() {
        use v1beta1::DevicePlugin as _;

        let service = make_service();
        let request = tonic::Request::new(v1beta1::AllocateRequest {
            container_requests: vec![v1beta1::ContainerAllocateRequest {
                devices_ids: vec!["does-not-exist".to_string()],
            }],
        });

        let status = service.allocate(request).await.unwrap_err();
        assert_eq!(status.code(), tonic::Code::NotFound);
    }

    #[tokio::test]
    async fn try_register_succeeds_on_first_attempt() {
        use k8s_device_plugin_test::registration::start_mock_registration_server;

        let server = start_mock_registration_server(None);
        let plugin = DevicePlugin::new("example.com/device", make_service());

        plugin
            .try_register(server.socket_path(), 3, Duration::from_millis(1))
            .await
            .expect("registration should succeed");

        let requests = server.collected_requests().await;
        assert_eq!(requests.len(), 1);
        server.shutdown();
    }

    #[tokio::test]
    async fn try_register_gives_up_after_max_attempts() {
        use k8s_device_plugin_test::registration::start_mock_registration_server;

        let server =
            start_mock_registration_server(Some((tonic::Code::Unavailable, "kubelet down")));
        let plugin = DevicePlugin::new("example.com/device", make_service());

        let err = plugin
            .try_register(server.socket_path(), 3, Duration::from_millis(1))
            .await
            .expect_err("should give up after max attempts");

        assert!(err.to_string().contains("kubelet down"));
        server.shutdown();
    }

    #[tokio::test]
    async fn try_register_retries_until_success() {
        use k8s_device_plugin_test::registration::start_mock_registration_server_with_failures;

        let server = start_mock_registration_server_with_failures(2);
        let plugin = DevicePlugin::new("example.com/device", make_service());

        plugin
            .try_register(server.socket_path(), 3, Duration::from_millis(1))
            .await
            .expect("should succeed after 2 failures");

        // Only the successful attempt is recorded (failures don't push to requests).
        let requests = server.collected_requests().await;
        assert_eq!(requests.len(), 1);
        server.shutdown();
    }
}
