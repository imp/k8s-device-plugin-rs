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

use k8s_device_plugin_lib::Device;
use k8s_device_plugin_lib::DevicePlugin;
use k8s_device_plugin_lib::DevicePluginService;
use k8s_device_plugin_lib::StaticDevicePlugin;

const RESOURCE_NAME: &str = "example.com/widget";

#[tokio::main]
async fn main() -> std::io::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    let devices = vec![Device::rdwr("widget-0", "/dev/widget0")];
    let service = DevicePluginService::new(StaticDevicePlugin::new(devices));
    let plugin = DevicePlugin::new(RESOURCE_NAME, service);

    tracing::info!(
        resource_name = RESOURCE_NAME,
        "starting static device plugin"
    );
    plugin.run().await
}
