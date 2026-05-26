use std::collections::HashMap;
use std::fs;
use std::io;
use std::path::PathBuf;
use std::sync::Arc;

use tokio::task::JoinHandle;
use tonic::Result;
use tonic::transport;
use tonic::transport::Channel;

pub mod v1beta1 {
    use std::fmt;

    #[derive(Debug, Clone)]
    pub enum Health {
        Healthy,
        Unhealthy,
    }

    impl Health {
        pub fn as_str(&self) -> &'static str {
            match self {
                Self::Healthy => HEALTHY,
                Self::Unhealthy => UNHEALTHY,
            }
        }
    }

    impl fmt::Display for Health {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            self.as_str().fmt(f)
        }
    }

    #[derive(Debug, Clone, Copy)]
    pub struct DevicePermissions {
        pub read: bool,
        pub write: bool,
        pub mknod: bool,
    }

    impl DevicePermissions {
        pub fn as_str(&self) -> &'static str {
            match (self.read, self.write, self.mknod) {
                (true, true, true) => "rwm",
                (true, true, false) => "rw",
                (true, false, true) => "rm",
                (true, false, false) => "r",
                (false, true, true) => "wm",
                (false, true, false) => "w",
                (false, false, true) => "m",
                (false, false, false) => "",
            }
        }
    }

    impl fmt::Display for DevicePermissions {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            self.as_str().fmt(f)
        }
    }

    pub const HEALTHY: &str = "Healthy";
    pub const UNHEALTHY: &str = "Unhealthy";
    pub const VERSION: &str = "v1beta1";

    #[cfg(not(windows))]
    pub const DEVICE_PLUGIN_PATH: &str = "/var/lib/kubelet/device-plugins/";
    #[cfg(windows)]
    pub const DEVICE_PLUGIN_PATH: &str = "\\var\\lib\\kubelet\\device-plugins\\";

    pub const KUBELET_SOCKET: &str = "kubelet.sock";

    // const KUBELET_PRE_START_CONTAINER_RPC_TIMEOUT_IN_SECS: u64 = 30;

    pub use device_plugin_server::DevicePlugin;
    pub use device_plugin_server::DevicePluginServer;
    pub use device_plugin_server::SERVICE_NAME;
    pub use registration_client::RegistrationClient;

    tonic::include_proto!("v1beta1");
}

#[derive(Debug, Clone)]
pub struct DevicePath {
    pub host_path: PathBuf,
    pub container_path: PathBuf,
    pub permissions: v1beta1::DevicePermissions,
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
    pub health: v1beta1::Health,
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
    service: Arc<DevicePluginService>,
}

impl DevicePlugin {
    pub fn new(service: DevicePluginService) -> Self {
        let service = Arc::new(service);
        Self { service }
    }

    pub async fn start(&self, socket_name: &str, resource_name: &str) -> tonic::Result<()> {
        let endpoint = String::from(v1beta1::DEVICE_PLUGIN_PATH) + socket_name;
        loop {
            self.service.kubelet_gone.notify_waiters();
            let handle = self
                .spawn_server(&endpoint)
                .await
                .map_err(|e| tonic::Status::internal(e.to_string()))?;
            Self::register(&endpoint, resource_name).await?;
            // Block until list_and_watch client disconnects — that signals a kubelet restart
            self.service.kubelet_gone.notified().await;
            handle.abort();

            // let this = self.clone();
            // let name = socket_name.to_string();
            // let bound = Arc::new(tokio::sync::Notify::new());
            // let bound_notify = Arc::clone(&bound);
            // let handle = tokio::spawn(async move { this.serve(&name, bound_notify).await });

            // // Wait until the socket is bound and ready to accept connections
            // bound.notified().await;

            // Self::register(socket_name, resource_name).await?;

            // // Block until list_and_watch client disconnects — that signals a kubelet restart
            // self.service.kubelet_gone.notified().await;

            // handle.abort();
        }
    }

    async fn spawn_server(
        &self,
        socket_name: &str,
    ) -> io::Result<JoinHandle<Result<(), transport::Error>>> {
        use tokio::net::UnixListener;
        use tokio_stream::wrappers::UnixListenerStream;

        let endpoint = String::from(v1beta1::DEVICE_PLUGIN_PATH) + socket_name;
        ensure_no_file(&endpoint)?;
        let uds = UnixListener::bind(endpoint).map(UnixListenerStream::new)?;

        let svc = self.service();
        let router = transport::Server::builder().add_service(svc);
        let handle = tokio::spawn(router.serve_with_incoming(uds));

        Ok(handle)
    }

    pub async fn register(
        endpoint: &str,
        resource_name: &str,
    ) -> tonic::Result<tonic::Response<v1beta1::Empty>> {
        let version = v1beta1::VERSION.to_string();
        let endpoint = endpoint.to_string();
        let resource_name = resource_name.to_string();

        let request = v1beta1::RegisterRequest {
            version,
            endpoint,
            resource_name,
            options: None,
        };

        Self::registration_client().await?.register(request).await
    }

    async fn registration_client() -> tonic::Result<v1beta1::RegistrationClient<Channel>> {
        v1beta1::RegistrationClient::connect(Self::kubelet_socket_path())
            .await
            .map_err(|err| tonic::Status::from_error(Box::new(err)))
    }

    fn kubelet_socket_path() -> String {
        String::from(v1beta1::DEVICE_PLUGIN_PATH) + v1beta1::KUBELET_SOCKET
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
    type ListAndWatchStream =
        tokio_stream::wrappers::ReceiverStream<Result<v1beta1::ListAndWatchResponse>>;

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

fn ensure_no_file(path: &str) -> io::Result<()> {
    if fs::exists(path)? {
        fs::remove_file(path)?;
    }
    Ok(())
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
    fn health_as_str() {
        assert_eq!(v1beta1::Health::Healthy.as_str(), v1beta1::HEALTHY);
        assert_eq!(v1beta1::Health::Unhealthy.as_str(), v1beta1::UNHEALTHY);
    }

    #[test]
    fn health_display() {
        assert_eq!(v1beta1::Health::Healthy.to_string(), v1beta1::HEALTHY);
        assert_eq!(v1beta1::Health::Unhealthy.to_string(), v1beta1::UNHEALTHY);
    }

    #[test]
    fn device_permissions_as_str() {
        assert_eq!(
            v1beta1::DevicePermissions {
                read: true,
                write: true,
                mknod: true
            }
            .as_str(),
            "rwm"
        );
        assert_eq!(
            v1beta1::DevicePermissions {
                read: true,
                write: true,
                mknod: false
            }
            .as_str(),
            "rw"
        );
        assert_eq!(
            v1beta1::DevicePermissions {
                read: true,
                write: false,
                mknod: false
            }
            .as_str(),
            "r"
        );
        assert_eq!(
            v1beta1::DevicePermissions {
                read: false,
                write: false,
                mknod: false
            }
            .as_str(),
            ""
        );
    }

    #[test]
    fn device_to_proto() {
        let device = Device {
            id: "dev-0".to_string(),
            health: v1beta1::Health::Healthy,
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
            health: v1beta1::Health::Healthy,
            paths: vec![DevicePath {
                host_path: PathBuf::from("/dev/mydev0"),
                container_path: PathBuf::from("/dev/mydev0"),
                permissions: v1beta1::DevicePermissions {
                    read: true,
                    write: true,
                    mknod: false,
                },
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
            health: v1beta1::Health::Healthy,
            paths: vec![DevicePath {
                host_path: PathBuf::from("/dev/mydev0"),
                container_path: PathBuf::from("/dev/mydev0"),
                permissions: v1beta1::DevicePermissions {
                    read: true,
                    write: true,
                    mknod: false,
                },
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
}
