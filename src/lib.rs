use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use tonic::transport;
use tonic::transport::Channel;
use tonic::Result;

pub mod v1beta1 {
    use std::fmt;
    #[derive(Debug, Clone)]
    pub enum Health {
        Healthy,
        Unhealthy,
    }

    impl Health {
        pub fn as_str(&self) -> &str {
            match self {
                Health::Healthy   => HEALTHY,
                Health::Unhealthy => UNHEALTHY,
            }
        }
    }

    impl fmt::Display for Health {
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

    tonic::include_proto!("v1beta1");

    pub use device_plugin_server::DevicePlugin;
    pub use device_plugin_server::DevicePluginServer;
    pub use device_plugin_server::SERVICE_NAME;
    pub use registration_client::RegistrationClient;
}

#[derive(Debug, Clone, Copy)]
pub struct DevicePermissions {
    pub read: bool,
    pub write: bool,
    pub mknod: bool,
}

impl DevicePermissions {
    fn as_str(self) -> String {
        let mut s = String::with_capacity(3);
        if self.read  { s.push('r'); }
        if self.write { s.push('w'); }
        if self.mknod { s.push('m'); }
        s
    }
}

#[derive(Debug, Clone)]
pub struct DevicePath {
    pub host_path: PathBuf,
    pub container_path: PathBuf,
    pub permissions: DevicePermissions,
}

#[derive(Debug, Clone)]
pub struct Device {
    pub id: String,
    pub health: v1beta1::Health,
    pub paths: Vec<DevicePath>,
}

impl Device {
    fn to_proto(&self) -> v1beta1::Device {
        v1beta1::Device { id: self.id.clone(), health: self.health.to_string(), topology: None }
    }

    fn to_device_specs(&self) -> Vec<v1beta1::DeviceSpec> {
        self.paths
            .iter()
            .map(|p| v1beta1::DeviceSpec {
                host_path: p.host_path.to_string_lossy().into_owned(),
                container_path: p.container_path.to_string_lossy().into_owned(),
                permissions: p.permissions.as_str(),
            })
            .collect()
    }
}

#[derive(Debug)]
pub struct DevicePlugin {
    service: Arc<DevicePluginService>,
}

impl DevicePlugin {
    pub fn new(service: DevicePluginService) -> Self {
        let service = Arc::new(service);
        Self { service }
    }

    #[cfg(unix)]
    pub async fn serve(&self, socket_name: &str) -> tonic::Result<()> {
        use tokio::net::UnixListener;
        use tokio_stream::wrappers::UnixListenerStream;

        let endpoint = String::from(v1beta1::DEVICE_PLUGIN_PATH) + socket_name;

        if Path::new(&endpoint).exists() {
            fs::remove_file(&endpoint).map_err(|e| tonic::Status::internal(e.to_string()))?;
        }

        let uds = UnixListener::bind(endpoint)
            .map(UnixListenerStream::new)
            .map_err(|e| tonic::Status::internal(e.to_string()))?;

        let inner = self.service.clone();
        let service = v1beta1::DevicePluginServer::from_arc(inner);
        transport::Server::builder()
            .add_service(service)
            .serve_with_incoming(uds)
            .await
            .map_err(|e| tonic::Status::from_error(Box::new(e)))
    }

    pub async fn register(endpoint: &str, resource_name: &str) -> tonic::Result<()> {
        let version = v1beta1::VERSION.to_string();
        let endpoint = endpoint.to_string();
        let resource_name = resource_name.to_string();

        let request = v1beta1::RegisterRequest {
            version,
            endpoint,
            resource_name,
            options: None,
        };

        Self::registration_client().await?.register(request).await?;

        Ok(())
    }

    async fn registration_client() -> tonic::Result<v1beta1::RegistrationClient<Channel>> {
        v1beta1::RegistrationClient::connect(Self::kubelet_socket_path())
            .await
            .map_err(|err| tonic::Status::from_error(Box::new(err)))
    }

    fn kubelet_socket_path() -> String {
        String::from(v1beta1::DEVICE_PLUGIN_PATH) + v1beta1::KUBELET_SOCKET
    }
}

#[derive(Debug)]
pub struct DevicePluginService {
    devices: HashMap<String, Device>,
}

impl DevicePluginService {
    pub fn new(devices: HashMap<String, Device>) -> Self {
        Self { devices }
    }
}

#[tonic::async_trait]
impl v1beta1::DevicePlugin for DevicePluginService {
    type ListAndWatchStream = tokio_stream::wrappers::ReceiverStream<
        Result<v1beta1::ListAndWatchResponse>,
    >;

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
        tokio::spawn(async move {
            let response = v1beta1::ListAndWatchResponse { devices };
            let _ = tx.send(Ok(response)).await;
        });

        Ok(tonic::Response::new(
            tokio_stream::wrappers::ReceiverStream::new(rx),
        ))
    }

    async fn get_preferred_allocation(
        &self,
        _request: tonic::Request<v1beta1::PreferredAllocationRequest>,
    ) -> tonic::Result<tonic::Response<v1beta1::PreferredAllocationResponse>> {
        Err(tonic::Status::unimplemented(
            "GetPreferredAllocation not implemented",
        ))
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
                            .ok_or_else(|| tonic::Status::not_found(format!("device {id} not found")))
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
        assert_eq!(v1beta1::Health::Healthy.as_str(),   v1beta1::HEALTHY);
        assert_eq!(v1beta1::Health::Unhealthy.as_str(), v1beta1::UNHEALTHY);
    }

    #[test]
    fn health_display() {
        assert_eq!(v1beta1::Health::Healthy.to_string(),   v1beta1::HEALTHY);
        assert_eq!(v1beta1::Health::Unhealthy.to_string(), v1beta1::UNHEALTHY);
    }

    #[test]
    fn device_permissions_as_str() {
        assert_eq!(DevicePermissions { read: true,  write: true,  mknod: true  }.as_str(), "rwm");
        assert_eq!(DevicePermissions { read: true,  write: true,  mknod: false }.as_str(), "rw");
        assert_eq!(DevicePermissions { read: true,  write: false, mknod: false }.as_str(), "r");
        assert_eq!(DevicePermissions { read: false, write: false, mknod: false }.as_str(), "");
    }

    #[test]
    fn device_to_proto() {
        let device = Device {
            id: "dev-0".to_string(),
            health: v1beta1::Health::Healthy,
            paths: vec![],
        };
        let proto = device.to_proto();
        assert_eq!(proto.id,     "dev-0");
        assert_eq!(proto.health, v1beta1::HEALTHY);
    }

    #[test]
    fn device_to_device_specs() {
        let device = Device {
            id: "dev-0".to_string(),
            health: v1beta1::Health::Healthy,
            paths: vec![
                DevicePath {
                    host_path:      PathBuf::from("/dev/mydev0"),
                    container_path: PathBuf::from("/dev/mydev0"),
                    permissions:    DevicePermissions { read: true, write: true, mknod: false },
                },
            ],
        };
        let specs = device.to_device_specs();
        assert_eq!(specs.len(), 1);
        assert_eq!(specs[0].host_path,      "/dev/mydev0");
        assert_eq!(specs[0].container_path, "/dev/mydev0");
        assert_eq!(specs[0].permissions,    "rw");
    }

    fn make_service() -> DevicePluginService {
        let device = Device {
            id: "dev-0".to_string(),
            health: v1beta1::Health::Healthy,
            paths: vec![DevicePath {
                host_path:      PathBuf::from("/dev/mydev0"),
                container_path: PathBuf::from("/dev/mydev0"),
                permissions:    DevicePermissions { read: true, write: true, mknod: false },
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
        assert_eq!(response.devices[0].id,     "dev-0");
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
        assert_eq!(response.container_responses[0].devices[0].host_path, "/dev/mydev0");
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
