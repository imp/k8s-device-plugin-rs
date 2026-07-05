use std::collections::HashMap;

use async_trait::async_trait;

mod device;
mod health;
mod permissions;

pub use device::Device;
pub use device::DevicePath;
pub use device::HostMount;
pub use health::Health;
pub use permissions::DevicePermissions;

/// Enumerates the devices a plugin backend currently knows about.
#[async_trait]
pub trait DeviceDiscovery: Send + Sync {
    /// List all devices currently known to this backend, including their health.
    async fn discover(&self) -> Vec<Device>;
}

/// Resolves container allocation artifacts for a set of requested device IDs.
#[async_trait]
pub trait DeviceAllocator: Send + Sync {
    async fn allocate(&self, device_ids: &[String])
    -> Result<ContainerAllocation, AllocationError>;
}

/// Artifacts to attach to a container as a result of an `Allocate` call.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct ContainerAllocation {
    pub device_paths: Vec<DevicePath>,
    pub mounts: Vec<HostMount>,
    pub envs: HashMap<String, String>,
    pub annotations: HashMap<String, String>,
    /// Fully qualified CDI device names, e.g. `"vendor.com/gpu=gpudevice1"`.
    pub cdi_devices: Vec<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum AllocationError {
    #[error("device not found: {0}")]
    DeviceNotFound(String),
    #[error("preferred allocation is not available for this plugin")]
    PreferredAllocationUnavailable,
    #[error("hook failed: {0}")]
    HookFailed(String),
}

/// Full framework abstraction a device plugin backend implements.
///
/// `pre_start_container` and `preferred_allocation` are optional hooks with safe
/// defaults. Override a hook, and its matching availability flag, only if the
/// backend needs kubelet to call it.
#[async_trait]
pub trait K8sDevicePlugin: DeviceDiscovery + DeviceAllocator {
    /// Whether kubelet must call `pre_start_container` before starting each
    /// container. Defaults to `false`.
    fn pre_start_required(&self) -> bool {
        false
    }

    /// Runs before kubelet starts a container using the given device IDs.
    /// The default implementation does nothing.
    async fn pre_start_container(&self, _device_ids: &[String]) -> Result<(), AllocationError> {
        Ok(())
    }

    /// Whether this backend implements `preferred_allocation`. Defaults to `false`.
    fn preferred_allocation_available(&self) -> bool {
        false
    }

    /// Chooses `size` preferred device IDs from `available_device_ids` for one
    /// container request, including every ID in `must_include_device_ids`.
    /// Only called when `preferred_allocation_available` returns `true`.
    async fn preferred_allocation(
        &self,
        _available_device_ids: &[String],
        _must_include_device_ids: &[String],
        _size: usize,
    ) -> Result<Vec<String>, AllocationError> {
        Err(AllocationError::PreferredAllocationUnavailable)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct StaticPlugin(Vec<Device>);

    impl K8sDevicePlugin for StaticPlugin {}

    #[async_trait]
    impl DeviceDiscovery for StaticPlugin {
        async fn discover(&self) -> Vec<Device> {
            self.0.clone()
        }
    }

    #[async_trait]
    impl DeviceAllocator for StaticPlugin {
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
            Ok(ContainerAllocation {
                device_paths,
                ..Default::default()
            })
        }
    }

    fn make_plugin() -> StaticPlugin {
        StaticPlugin(vec![Device {
            id: "dev-0".to_string(),
            health: Health::Healthy,
            paths: vec![DevicePath {
                host_path: "/dev/dev0".into(),
                container_path: "/dev/dev0".into(),
                permissions: DevicePermissions::rdwr(),
            }],
        }])
    }

    async fn use_backend<P: K8sDevicePlugin>(plugin: &P) -> Vec<Device> {
        plugin.discover().await
    }

    #[tokio::test]
    async fn discover_is_usable_through_k8s_device_plugin_bound() {
        let plugin = make_plugin();
        let devices = use_backend(&plugin).await;
        assert_eq!(devices.len(), 1);
        assert_eq!(devices[0].id, "dev-0");
    }

    #[tokio::test]
    async fn allocate_resolves_known_device() {
        let plugin = make_plugin();
        let allocation = plugin
            .allocate(&["dev-0".to_string()])
            .await
            .expect("device is known");
        assert_eq!(allocation.device_paths.len(), 1);
    }

    #[tokio::test]
    async fn allocate_reports_unknown_device() {
        let plugin = make_plugin();
        let err = plugin
            .allocate(&["missing".to_string()])
            .await
            .expect_err("device is unknown");
        assert_eq!(err, AllocationError::DeviceNotFound("missing".to_string()));
        assert_eq!(err.to_string(), "device not found: missing");
    }
}
