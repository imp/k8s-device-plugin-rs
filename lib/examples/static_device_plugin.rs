//! The fastest way to stand up a device plugin: a fixed device list, no
//! trait implementation required. See `StaticDevicePlugin` in the `lib`
//! crate for what this buys you (and what it doesn't -- no custom discovery,
//! no optional hooks).
//!
//! Run with:
//!
//! ```bash
//! cargo run --example static_device_plugin
//! ```

use std::path::PathBuf;

use k8s_device_plugin_lib::Device;
use k8s_device_plugin_lib::DevicePath;
use k8s_device_plugin_lib::DevicePermissions;
use k8s_device_plugin_lib::DevicePlugin;
use k8s_device_plugin_lib::DevicePluginService;
use k8s_device_plugin_lib::Health;
use k8s_device_plugin_lib::StaticDevicePlugin;

const RESOURCE_NAME: &str = "example.com/widget";

#[tokio::main]
async fn main() -> std::io::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    let devices = vec![Device {
        id: "widget-0".to_string(),
        health: Health::Healthy,
        paths: vec![DevicePath {
            host_path: PathBuf::from("/dev/widget0"),
            container_path: PathBuf::from("/dev/widget0"),
            permissions: DevicePermissions::rdwr(),
        }],
    }];

    let service = DevicePluginService::new(StaticDevicePlugin::new(devices));
    let plugin = DevicePlugin::new(RESOURCE_NAME, service);

    tracing::info!(
        resource_name = RESOURCE_NAME,
        "starting static device plugin"
    );
    plugin.run().await
}
