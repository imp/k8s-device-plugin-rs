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

/// Compares two device snapshots by content, ignoring order, so a backend whose
/// `discover()` doesn't return devices in a stable order (e.g. backed by a
/// `HashMap`) doesn't trigger spurious `ListAndWatch` updates every poll.
fn devices_equal_ignoring_order(a: &[Device], b: &[Device]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut a_sorted = a.iter().collect::<Vec<_>>();
    let mut b_sorted = b.iter().collect::<Vec<_>>();
    a_sorted.sort_by(|x, y| x.id.cmp(&y.id));
    b_sorted.sort_by(|x, y| x.id.cmp(&y.id));
    a_sorted == b_sorted
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

    #[tracing::instrument(skip(self), fields(resource_name = %self.resource_name))]
    pub async fn run(&self) -> io::Result<()> {
        let mut server_handle = self.spawn_server()?;
        loop {
            // Subscribe before registering so a fast kubelet disconnect is never missed.
            let kubelet_gone = self.service.kubelet_gone.notified();
            if let Err(err) = self.register_with_retry().await {
                tracing::error!(%err, "registration permanently failed; shutting down");
                // Registration is permanently exhausted: abort the spawned server task
                // so it doesn't keep serving RPCs against a listener nobody can reach.
                server_handle.abort();
                return Err(err);
            }
            tracing::info!("registered with kubelet");
            tokio::select! {
                result = &mut server_handle => {
                    return result
                        .map_err(io::Error::other)?
                        .map_err(io::Error::other);
                }
                _ = kubelet_gone => {
                    tracing::warn!("kubelet disconnected; re-registering");
                }
            }
        }
    }

    async fn register_with_retry(&self) -> io::Result<()> {
        self.try_register(self.kubelet_socket.clone(), 10, Duration::from_secs(1))
            .await
    }

    #[tracing::instrument(skip(self, kubelet_socket), fields(resource_name = %self.resource_name))]
    async fn try_register(
        &self,
        kubelet_socket: String,
        max_attempts: u32,
        initial_delay: Duration,
    ) -> io::Result<()> {
        if max_attempts == 0 {
            return Err(io::Error::other("max_attempts must be at least 1"));
        }
        let mut delay = initial_delay;
        for attempt in 1..=max_attempts {
            match self.register_at(kubelet_socket.clone()).await {
                Ok(()) => return Ok(()),
                Err(err) if attempt < max_attempts => {
                    tracing::warn!(attempt, max_attempts, %err, "registration attempt failed; retrying");
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

/// Default interval at which [`DevicePluginService`] re-polls [`DeviceDiscovery::discover`]
/// to detect health/state changes while a `ListAndWatch` stream is open.
const DEFAULT_POLL_INTERVAL: Duration = Duration::from_secs(5);

pub struct DevicePluginService {
    plugin: Arc<dyn K8sDevicePlugin>,
    kubelet_gone: Arc<tokio::sync::Notify>,
    poll_interval: Duration,
}

impl DevicePluginService {
    pub fn new<P: K8sDevicePlugin + 'static>(plugin: P) -> Self {
        Self {
            plugin: Arc::new(plugin),
            kubelet_gone: Arc::new(tokio::sync::Notify::new()),
            poll_interval: DEFAULT_POLL_INTERVAL,
        }
    }

    /// Overrides the default interval at which device state is re-polled for
    /// `ListAndWatch` updates.
    #[must_use]
    pub fn with_poll_interval(mut self, poll_interval: Duration) -> Self {
        self.poll_interval = poll_interval;
        self
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
        Ok(tonic::Response::new(v1beta1::DevicePluginOptions {
            pre_start_required: self.plugin.pre_start_required(),
            get_preferred_allocation_available: self.plugin.preferred_allocation_available(),
        }))
    }

    #[tracing::instrument(skip(self, _request))]
    async fn list_and_watch(
        &self,
        _request: tonic::Request<v1beta1::Empty>,
    ) -> tonic::Result<tonic::Response<Self::ListAndWatchStream>> {
        let (tx, rx) = tokio::sync::mpsc::channel(1);

        let mut last_devices = self.plugin.discover().await;
        let response = v1beta1::ListAndWatchResponse {
            devices: last_devices.iter().map(device_to_proto).collect(),
        };
        let _ = tx.send(Ok(response)).await;

        let plugin = Arc::clone(&self.plugin);
        let kubelet_gone = Arc::clone(&self.kubelet_gone);
        let poll_interval = self.poll_interval;
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    () = tokio::time::sleep(poll_interval) => {
                        let devices = plugin.discover().await;
                        if !devices_equal_ignoring_order(&devices, &last_devices) {
                            tracing::debug!(device_count = devices.len(), "device state changed; pushing update");
                            let response = v1beta1::ListAndWatchResponse {
                                devices: devices.iter().map(device_to_proto).collect(),
                            };
                            last_devices = devices;
                            if tx.send(Ok(response)).await.is_err() {
                                break;
                            }
                        }
                    }
                    () = tx.closed() => break,
                }
            }
            kubelet_gone.notify_one();
        });

        Ok(tonic::Response::new(Self::ListAndWatchStream::new(rx)))
    }

    #[tracing::instrument(skip(self, request))]
    async fn get_preferred_allocation(
        &self,
        request: tonic::Request<v1beta1::PreferredAllocationRequest>,
    ) -> tonic::Result<tonic::Response<v1beta1::PreferredAllocationResponse>> {
        let mut container_responses = Vec::new();
        for container_request in request.into_inner().container_requests {
            let size = usize::try_from(container_request.allocation_size).map_err(|_| {
                tonic::Status::invalid_argument("allocation_size must be non-negative")
            })?;
            let device_ids = self
                .plugin
                .preferred_allocation(
                    &container_request.available_device_i_ds,
                    &container_request.must_include_device_i_ds,
                    size,
                )
                .await
                .inspect_err(|err| tracing::warn!(%err, "preferred_allocation hook failed"))
                .map_err(|err| tonic::Status::failed_precondition(err.to_string()))?;
            container_responses.push(v1beta1::ContainerPreferredAllocationResponse {
                device_i_ds: device_ids,
            });
        }

        Ok(tonic::Response::new(v1beta1::PreferredAllocationResponse {
            container_responses,
        }))
    }

    #[tracing::instrument(skip(self, request))]
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
                .inspect_err(|err| tracing::warn!(%err, "allocate failed"))
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

    #[tracing::instrument(skip(self, request))]
    async fn pre_start_container(
        &self,
        request: tonic::Request<v1beta1::PreStartContainerRequest>,
    ) -> tonic::Result<tonic::Response<v1beta1::PreStartContainerResponse>> {
        let device_ids = request.into_inner().devices_ids;
        self.plugin
            .pre_start_container(&device_ids)
            .await
            .inspect_err(|err| tracing::warn!(%err, "pre_start_container hook failed"))
            .map_err(|err| tonic::Status::failed_precondition(err.to_string()))?;
        Ok(tonic::Response::new(v1beta1::PreStartContainerResponse {}))
    }
}

/// Linux's `sockaddr_un.sun_path` is 108 bytes including the NUL terminator;
/// stay one byte under that as a conservative, portable budget.
const MAX_SOCKET_PATH_LEN: usize = 107;

/// Derives a filesystem-safe, collision-resistant socket name from a resource name.
///
/// Sanitization alone is not injective (e.g. "acme.com/gpu" and "acme_com/gpu" both
/// sanitize to "acme_com_gpu"), so a deterministic hash of the *original* name is
/// appended to guarantee distinct resource names never collide on the same path.
/// The human-readable sanitized part is truncated (never the hash) if needed so the
/// full endpoint path — including [`v1beta1::DEVICE_PLUGIN_PATH`] — never exceeds
/// the platform's Unix socket path limit.
fn sanitize_socket_name(name: &str) -> String {
    let sanitized = name.replace(invalid_char, "_");
    let suffix = format!("-{:016x}", fnv1a64(name.as_bytes()));
    let budget = MAX_SOCKET_PATH_LEN
        .saturating_sub(v1beta1::DEVICE_PLUGIN_PATH.len())
        .saturating_sub(suffix.len());
    // `sanitized` is guaranteed pure ASCII (invalid_char maps everything else to
    // '_'), so counting chars is equivalent to counting bytes here.
    let truncated = sanitized.chars().take(budget).collect::<String>();
    truncated + &suffix
}

fn invalid_char(c: char) -> bool {
    !(c.is_ascii_alphanumeric() || c == '-' || c == '_')
}

fn fnv1a64(bytes: &[u8]) -> u64 {
    const OFFSET_BASIS: u64 = 0xcbf29ce484222325;
    const PRIME: u64 = 0x0000_0100_0000_01b3;
    bytes.iter().fold(OFFSET_BASIS, |hash, &b| {
        (hash ^ u64::from(b)).wrapping_mul(PRIME)
    })
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

        let expected_prefix = "/var/lib/kubelet/device-plugins/example_com_device-";
        assert!(
            plugin.endpoint.starts_with(expected_prefix),
            "endpoint {} should start with {expected_prefix}",
            plugin.endpoint
        );
        assert!(
            plugin
                .registration_endpoint()
                .starts_with("example_com_device-")
        );
    }

    #[test]
    fn sanitize_socket_name_is_deterministic() {
        assert_eq!(
            sanitize_socket_name("example.com/device"),
            sanitize_socket_name("example.com/device")
        );
    }

    #[test]
    fn sanitize_socket_name_does_not_collide_across_distinct_names() {
        // These two names sanitize to the same "acme_com_gpu" prefix but must not
        // collide once the disambiguating hash suffix is applied.
        assert_ne!(
            sanitize_socket_name("acme.com/gpu"),
            sanitize_socket_name("acme_com/gpu")
        );
    }

    #[test]
    fn sanitize_socket_name_keeps_full_endpoint_within_socket_path_limit() {
        let long_name = "example.com/a-very-long-custom-accelerator-resource-name-that-keeps-going";
        let socket_name = sanitize_socket_name(long_name);
        let endpoint_len = v1beta1::DEVICE_PLUGIN_PATH.len() + socket_name.len();

        assert!(
            endpoint_len <= MAX_SOCKET_PATH_LEN,
            "endpoint length {endpoint_len} exceeds the socket path limit of {MAX_SOCKET_PATH_LEN}"
        );
        // The disambiguating hash suffix must survive truncation intact.
        assert!(socket_name.ends_with(&format!("-{:016x}", fnv1a64(long_name.as_bytes()))));
    }

    #[test]
    fn devices_equal_ignoring_order_treats_reordered_devices_as_equal() {
        let a = vec![
            make_device("dev-0", Health::Healthy),
            make_device("dev-1", Health::Healthy),
        ];
        let b = vec![
            make_device("dev-1", Health::Healthy),
            make_device("dev-0", Health::Healthy),
        ];

        assert!(devices_equal_ignoring_order(&a, &b));
    }

    #[test]
    fn devices_equal_ignoring_order_detects_real_changes() {
        let a = vec![make_device("dev-0", Health::Healthy)];
        let b = vec![make_device("dev-0", Health::Unhealthy)];

        assert!(!devices_equal_ignoring_order(&a, &b));
    }

    #[tokio::test]
    async fn list_and_watch_does_not_repeat_reordered_but_unchanged_devices() {
        use v1beta1::DevicePlugin as _;

        let devices = Arc::new(std::sync::Mutex::new(vec![
            make_device("dev-0", Health::Healthy),
            make_device("dev-1", Health::Healthy),
        ]));
        let service = DevicePluginService::new(DynamicDevicePlugin(Arc::clone(&devices)))
            .with_poll_interval(Duration::from_millis(5));

        let mut stream = service
            .list_and_watch(tonic::Request::new(v1beta1::Empty {}))
            .await
            .unwrap()
            .into_inner();

        stream.next().await.unwrap().unwrap();

        // Same devices, different order: must not be treated as a change.
        *devices.lock().unwrap() = vec![
            make_device("dev-1", Health::Healthy),
            make_device("dev-0", Health::Healthy),
        ];

        let second = tokio::time::timeout(Duration::from_millis(50), stream.next()).await;
        assert!(
            second.is_err(),
            "no update should be pushed when devices are merely reordered"
        );
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

    impl K8sDevicePlugin for StaticDevicePlugin {}

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

    struct DynamicDevicePlugin(Arc<std::sync::Mutex<Vec<Device>>>);

    impl K8sDevicePlugin for DynamicDevicePlugin {}

    #[tonic::async_trait]
    impl DeviceDiscovery for DynamicDevicePlugin {
        async fn discover(&self) -> Vec<Device> {
            self.0.lock().unwrap().clone()
        }
    }

    #[tonic::async_trait]
    impl DeviceAllocator for DynamicDevicePlugin {
        async fn allocate(
            &self,
            _device_ids: &[String],
        ) -> Result<ContainerAllocation, AllocationError> {
            Ok(ContainerAllocation::default())
        }
    }

    fn make_device(id: &str, health: Health) -> Device {
        Device {
            id: id.to_string(),
            health,
            paths: vec![],
        }
    }

    #[tokio::test]
    async fn list_and_watch_pushes_update_when_devices_change() {
        use v1beta1::DevicePlugin as _;

        let devices = Arc::new(std::sync::Mutex::new(vec![make_device(
            "dev-0",
            Health::Healthy,
        )]));
        let service = DevicePluginService::new(DynamicDevicePlugin(Arc::clone(&devices)))
            .with_poll_interval(Duration::from_millis(5));

        let mut stream = service
            .list_and_watch(tonic::Request::new(v1beta1::Empty {}))
            .await
            .unwrap()
            .into_inner();

        let first = stream.next().await.unwrap().unwrap();
        assert_eq!(first.devices[0].health, v1beta1::HEALTHY);

        *devices.lock().unwrap() = vec![make_device("dev-0", Health::Unhealthy)];

        let second = stream.next().await.unwrap().unwrap();
        assert_eq!(second.devices[0].health, v1beta1::UNHEALTHY);
    }

    #[tokio::test]
    async fn list_and_watch_does_not_repeat_unchanged_devices() {
        use v1beta1::DevicePlugin as _;

        let devices = Arc::new(std::sync::Mutex::new(vec![make_device(
            "dev-0",
            Health::Healthy,
        )]));
        let service = DevicePluginService::new(DynamicDevicePlugin(Arc::clone(&devices)))
            .with_poll_interval(Duration::from_millis(5));

        let mut stream = service
            .list_and_watch(tonic::Request::new(v1beta1::Empty {}))
            .await
            .unwrap()
            .into_inner();

        stream.next().await.unwrap().unwrap();

        let second = tokio::time::timeout(Duration::from_millis(50), stream.next()).await;
        assert!(
            second.is_err(),
            "no update should be pushed while devices are unchanged"
        );
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

    struct FullFeaturedPlugin;

    #[tonic::async_trait]
    impl DeviceDiscovery for FullFeaturedPlugin {
        async fn discover(&self) -> Vec<Device> {
            vec![]
        }
    }

    #[tonic::async_trait]
    impl DeviceAllocator for FullFeaturedPlugin {
        async fn allocate(
            &self,
            _device_ids: &[String],
        ) -> Result<ContainerAllocation, AllocationError> {
            Ok(ContainerAllocation::default())
        }
    }

    #[tonic::async_trait]
    impl K8sDevicePlugin for FullFeaturedPlugin {
        fn pre_start_required(&self) -> bool {
            true
        }

        async fn pre_start_container(&self, device_ids: &[String]) -> Result<(), AllocationError> {
            if device_ids.iter().any(|id| id == "broken") {
                return Err(AllocationError::HookFailed("device reset failed".into()));
            }
            Ok(())
        }

        fn preferred_allocation_available(&self) -> bool {
            true
        }

        async fn preferred_allocation(
            &self,
            available_device_ids: &[String],
            _must_include_device_ids: &[String],
            size: usize,
        ) -> Result<Vec<String>, AllocationError> {
            Ok(available_device_ids.iter().take(size).cloned().collect())
        }
    }

    #[tokio::test]
    async fn get_device_plugin_options_reports_defaults_when_hooks_unimplemented() {
        use v1beta1::DevicePlugin as _;

        let service = make_service();
        let options = service
            .get_device_plugin_options(tonic::Request::new(v1beta1::Empty {}))
            .await
            .unwrap()
            .into_inner();

        assert!(!options.pre_start_required);
        assert!(!options.get_preferred_allocation_available);
    }

    #[tokio::test]
    async fn get_device_plugin_options_reports_enabled_hooks() {
        use v1beta1::DevicePlugin as _;

        let service = DevicePluginService::new(FullFeaturedPlugin);
        let options = service
            .get_device_plugin_options(tonic::Request::new(v1beta1::Empty {}))
            .await
            .unwrap()
            .into_inner();

        assert!(options.pre_start_required);
        assert!(options.get_preferred_allocation_available);
    }

    #[tokio::test]
    async fn pre_start_container_default_is_a_no_op() {
        use v1beta1::DevicePlugin as _;

        let service = make_service();
        let request = tonic::Request::new(v1beta1::PreStartContainerRequest {
            devices_ids: vec!["dev-0".to_string()],
        });

        service.pre_start_container(request).await.unwrap();
    }

    #[tokio::test]
    async fn pre_start_container_surfaces_hook_failure() {
        use v1beta1::DevicePlugin as _;

        let service = DevicePluginService::new(FullFeaturedPlugin);
        let request = tonic::Request::new(v1beta1::PreStartContainerRequest {
            devices_ids: vec!["broken".to_string()],
        });

        let status = service.pre_start_container(request).await.unwrap_err();
        assert_eq!(status.code(), tonic::Code::FailedPrecondition);
    }

    #[tokio::test]
    async fn get_preferred_allocation_default_is_unavailable() {
        use v1beta1::DevicePlugin as _;

        let service = make_service();
        let request = tonic::Request::new(v1beta1::PreferredAllocationRequest {
            container_requests: vec![v1beta1::ContainerPreferredAllocationRequest {
                available_device_i_ds: vec!["dev-0".to_string()],
                must_include_device_i_ds: vec![],
                allocation_size: 1,
            }],
        });

        let status = service.get_preferred_allocation(request).await.unwrap_err();
        assert_eq!(status.code(), tonic::Code::FailedPrecondition);
    }

    #[tokio::test]
    async fn get_preferred_allocation_returns_chosen_devices() {
        use v1beta1::DevicePlugin as _;

        let service = DevicePluginService::new(FullFeaturedPlugin);
        let request = tonic::Request::new(v1beta1::PreferredAllocationRequest {
            container_requests: vec![v1beta1::ContainerPreferredAllocationRequest {
                available_device_i_ds: vec!["dev-0".to_string(), "dev-1".to_string()],
                must_include_device_i_ds: vec![],
                allocation_size: 1,
            }],
        });

        let response = service
            .get_preferred_allocation(request)
            .await
            .unwrap()
            .into_inner();

        assert_eq!(response.container_responses.len(), 1);
        assert_eq!(response.container_responses[0].device_i_ds, vec!["dev-0"]);
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
