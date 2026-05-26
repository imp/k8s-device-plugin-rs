pub mod v1beta1 {

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
