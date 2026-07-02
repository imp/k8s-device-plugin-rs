use std::fmt;

use async_trait::async_trait;

mod device;
mod health;
mod permissions;

pub use device::Device;
pub use device::DevicePath;
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
#[derive(Clone, Debug, Default)]
pub struct ContainerAllocation {
    pub device_paths: Vec<DevicePath>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AllocationError {
    DeviceNotFound(String),
}

impl fmt::Display for AllocationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::DeviceNotFound(id) => write!(f, "device not found: {id}"),
        }
    }
}

impl std::error::Error for AllocationError {}

/// Full framework abstraction a device plugin backend implements.
pub trait K8sDevicePlugin: DeviceDiscovery + DeviceAllocator {}

impl<T: DeviceDiscovery + DeviceAllocator> K8sDevicePlugin for T {}

#[cfg(test)]
mod tests {
    use super::*;

    struct StaticPlugin(Vec<Device>);

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
            Ok(ContainerAllocation { device_paths })
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
