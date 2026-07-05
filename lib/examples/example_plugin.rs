//! Minimal device plugin backend demonstrating the k8s-device-plugin-lib framework.
//!
//! Registers two in-memory "widget" devices, and opts into the `PreStartContainer`
//! and `GetPreferredAllocation` hooks so kubelet exercises the full RPC surface.
//!
//! This must run somewhere kubelet's device-plugin registration socket is
//! reachable (e.g. inside a kind/minikube node, or as a DaemonSet) — it will
//! retry registration and eventually fail if there is no kubelet to register
//! with. Run with:
//!
//! ```bash
//! cargo run --example example_plugin
//! ```

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Mutex;

use k8s_device_plugin_lib::AllocationError;
use k8s_device_plugin_lib::ContainerAllocation;
use k8s_device_plugin_lib::Device;
use k8s_device_plugin_lib::DeviceAllocator;
use k8s_device_plugin_lib::DeviceDiscovery;
use k8s_device_plugin_lib::DevicePath;
use k8s_device_plugin_lib::DevicePermissions;
use k8s_device_plugin_lib::DevicePlugin;
use k8s_device_plugin_lib::DevicePluginService;
use k8s_device_plugin_lib::Health;
use k8s_device_plugin_lib::K8sDevicePlugin;

const RESOURCE_NAME: &str = "example.com/widget";

/// Fake backend exposing two in-memory "widget" devices.
struct ExampleWidgetPlugin {
    devices: Mutex<Vec<Device>>,
}

impl ExampleWidgetPlugin {
    fn new() -> Self {
        let devices = vec![
            make_device("widget-0", Health::Healthy),
            make_device("widget-1", Health::Healthy),
        ];
        Self {
            devices: Mutex::new(devices),
        }
    }
}

fn make_device(id: &str, health: Health) -> Device {
    Device {
        id: id.to_string(),
        health,
        paths: vec![DevicePath {
            host_path: PathBuf::from(format!("/dev/{id}")),
            container_path: PathBuf::from(format!("/dev/{id}")),
            permissions: DevicePermissions::rdwr(),
        }],
    }
}

#[tonic::async_trait]
impl DeviceDiscovery for ExampleWidgetPlugin {
    async fn discover(&self) -> Vec<Device> {
        self.devices.lock().unwrap().clone()
    }
}

#[tonic::async_trait]
impl DeviceAllocator for ExampleWidgetPlugin {
    async fn allocate(
        &self,
        device_ids: &[String],
    ) -> Result<ContainerAllocation, AllocationError> {
        let devices = self.devices.lock().unwrap();
        let mut device_paths = Vec::new();
        for id in device_ids {
            let device = devices
                .iter()
                .find(|device| &device.id == id)
                .ok_or_else(|| AllocationError::DeviceNotFound(id.clone()))?;
            device_paths.extend(device.paths.iter().cloned());
        }
        // Real devices often need more than a /dev node -- e.g. an env var
        // pointing the workload at which devices it was given.
        let envs = HashMap::from([("EXAMPLE_WIDGET_DEVICES".to_string(), device_ids.join(","))]);
        Ok(ContainerAllocation {
            device_paths,
            envs,
            ..Default::default()
        })
    }
}

#[tonic::async_trait]
impl K8sDevicePlugin for ExampleWidgetPlugin {
    fn pre_start_required(&self) -> bool {
        true
    }

    async fn pre_start_container(&self, device_ids: &[String]) -> Result<(), AllocationError> {
        tracing::info!(?device_ids, "pre-start hook: resetting devices");
        Ok(())
    }

    fn preferred_allocation_available(&self) -> bool {
        true
    }

    async fn preferred_allocation(
        &self,
        available_device_ids: &[String],
        must_include_device_ids: &[String],
        size: usize,
    ) -> Result<Vec<String>, AllocationError> {
        let mut chosen = must_include_device_ids
            .iter()
            .take(size)
            .cloned()
            .collect::<Vec<_>>();
        for id in available_device_ids {
            if chosen.len() >= size {
                break;
            }
            if !chosen.contains(id) {
                chosen.push(id.clone());
            }
        }
        Ok(chosen)
    }
}

#[tokio::main]
async fn main() -> std::io::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    let service = DevicePluginService::new(ExampleWidgetPlugin::new());
    let plugin = DevicePlugin::new(RESOURCE_NAME, service);

    tracing::info!(
        resource_name = RESOURCE_NAME,
        "starting example device plugin"
    );
    plugin.run().await
}
