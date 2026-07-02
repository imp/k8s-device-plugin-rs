use std::fmt;
use std::fs;
use std::io;
use std::path::Path;
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

pub use k8s_device_plugin_core::AllocationError;
pub use k8s_device_plugin_core::ContainerAllocation;
pub use k8s_device_plugin_core::Device;
pub use k8s_device_plugin_core::DeviceAllocator;
pub use k8s_device_plugin_core::DeviceDiscovery;
pub use k8s_device_plugin_core::DevicePath;
pub use k8s_device_plugin_core::DevicePermissions;
pub use k8s_device_plugin_core::Health;
pub use k8s_device_plugin_core::K8sDevicePlugin;
pub use registration::RegistrationClient;

mod registration;

fn device_to_proto(device: &Device) -> v1beta1::Device {
    v1beta1::Device {
        id: device.id.clone(),
        health: device.health.to_string(),
        topology: None,
    }
}

fn device_path_to_spec(path: &DevicePath) -> v1beta1::DeviceSpec {
    v1beta1::DeviceSpec {
        host_path: path.host_path.to_string_lossy().into_owned(),
        container_path: path.container_path.to_string_lossy().into_owned(),
        permissions: path.permissions.to_string(),
    }
}

#[derive(Clone, Debug)]
pub struct DevicePlugin {
    endpoint: String,
    resource_name: String,
    service: Arc<DevicePluginService>,
    kubelet_socket: String,
}

impl DevicePlugin {
    pub fn new(resource_name: &str, service: DevicePluginService) -> Self {
        let socket_name = sanitize_socket_name(resource_name);
        let resource_name = resource_name.to_string();
        let endpoint = String::from(v1beta1::DEVICE_PLUGIN_PATH) + &socket_name;
        let service = Arc::new(service);
        let kubelet_socket = Self::kubelet_socket_path();
        Self {
            endpoint,
            resource_name,
            service,
            kubelet_socket,
        }
    }

    #[cfg(test)]
    fn for_test(
        resource_name: &str,
        service: DevicePluginService,
        endpoint: String,
        kubelet_socket: String,
    ) -> Self {
        Self {
            endpoint,
            resource_name: resource_name.to_string(),
            service: Arc::new(service),
            kubelet_socket,
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
        self.try_register(self.kubelet_socket.clone(), 10, Duration::from_secs(1))
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
        self.register_at(self.kubelet_socket.clone()).await
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

pub struct DevicePluginService {
    plugin: Box<dyn K8sDevicePlugin>,
    kubelet_gone: Arc<tokio::sync::Notify>,
}

impl DevicePluginService {
    pub fn new<P: K8sDevicePlugin + 'static>(plugin: P) -> Self {
        Self {
            plugin: Box::new(plugin),
            kubelet_gone: Arc::new(tokio::sync::Notify::new()),
        }
    }
}

impl fmt::Debug for DevicePluginService {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DevicePluginService")
            .finish_non_exhaustive()
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

        let devices: Vec<_> = self
            .plugin
            .discover()
            .await
            .iter()
            .map(device_to_proto)
            .collect();
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
        let mut container_responses = Vec::new();
        for container_request in request.into_inner().container_requests {
            let allocation = self
                .plugin
                .allocate(&container_request.devices_ids)
                .await
                .map_err(|err| tonic::Status::not_found(err.to_string()))?;
            let devices = allocation
                .device_paths
                .iter()
                .map(device_path_to_spec)
                .collect();
            container_responses.push(v1beta1::ContainerAllocateResponse {
                devices,
                ..Default::default()
            });
        }

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
    use std::path::PathBuf;

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
    fn converts_device_to_proto() {
        let device = Device {
            id: "dev-0".to_string(),
            health: Health::Healthy,
            paths: vec![],
        };
        let proto = device_to_proto(&device);
        assert_eq!(proto.id, "dev-0");
        assert_eq!(proto.health, v1beta1::HEALTHY);
    }

    #[test]
    fn converts_device_path_to_spec() {
        let path = DevicePath {
            host_path: PathBuf::from("/dev/mydev0"),
            container_path: PathBuf::from("/dev/mydev0"),
            permissions: DevicePermissions::rdwr(),
        };
        let spec = device_path_to_spec(&path);
        assert_eq!(spec.host_path, "/dev/mydev0");
        assert_eq!(spec.container_path, "/dev/mydev0");
        assert_eq!(spec.permissions, "rw");
    }

    struct StaticDevicePlugin(Vec<Device>);

    #[tonic::async_trait]
    impl DeviceDiscovery for StaticDevicePlugin {
        async fn discover(&self) -> Vec<Device> {
            self.0.clone()
        }
    }

    #[tonic::async_trait]
    impl DeviceAllocator for StaticDevicePlugin {
        async fn allocate(
            &self,
            device_ids: &[String],
        ) -> Result<ContainerAllocation, AllocationError> {
            let mut device_paths = Vec::new();
            for id in device_ids {
                let device = self
                    .0
                    .iter()
                    .find(|d| &d.id == id)
                    .ok_or_else(|| AllocationError::DeviceNotFound(id.clone()))?;
                device_paths.extend(device.paths.iter().cloned());
            }
            Ok(ContainerAllocation { device_paths })
        }
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
        DevicePluginService::new(StaticDevicePlugin(vec![device]))
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

    #[tokio::test]
    async fn run_reregisters_after_kubelet_disconnects() {
        use k8s_device_plugin_test::device_plugin::MockDevicePluginClient;
        use k8s_device_plugin_test::registration::start_mock_registration_server;
        use tempfile::TempDir;

        let registration_server = start_mock_registration_server(None);
        let plugin_dir = TempDir::new().expect("create temp dir for plugin socket");
        let endpoint = plugin_dir
            .path()
            .join("plugin.sock")
            .to_string_lossy()
            .into_owned();

        let plugin = DevicePlugin::for_test(
            "example.com/device",
            make_service(),
            endpoint.clone(),
            registration_server.socket_path(),
        );

        let run_handle = tokio::spawn(async move { plugin.run().await });

        wait_for_request_count(&registration_server, 1).await;

        // Connect as the kubelet and read the initial device list. The
        // client-side stream is dropped when this call returns, which the
        // plugin detects as the kubelet going away.
        let mut client = MockDevicePluginClient::connect(&endpoint)
            .await
            .expect("connect to plugin socket");
        client
            .list_and_watch_once()
            .await
            .expect("initial device list");

        wait_for_request_count(&registration_server, 2).await;

        let requests = registration_server.collected_requests().await;
        assert_eq!(requests.len(), 2);
        assert_eq!(requests[1].resource_name, "example.com/device");
        assert_eq!(requests[1].endpoint, "plugin.sock");

        run_handle.abort();
        registration_server.shutdown();
    }

    async fn wait_for_request_count(
        server: &k8s_device_plugin_test::registration::MockRegistrationServer,
        count: usize,
    ) {
        for _ in 0..200 {
            if server.collected_requests().await.len() >= count {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        panic!("timed out waiting for {count} registration request(s)");
    }
}
