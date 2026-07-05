use async_trait::async_trait;

use crate::AllocationError;
use crate::ContainerAllocation;
use crate::Device;
use crate::DeviceAllocator;
use crate::DeviceDiscovery;
use crate::K8sDevicePlugin;

/// A [`K8sDevicePlugin`] backed by a fixed, in-memory device list -- a shared
/// test double for exercising `discover()`/`allocate()` without a real backend.
#[derive(Debug)]
pub struct StaticPlugin(pub Vec<Device>);

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
                .find(|device| &device.id == id)
                .ok_or_else(|| AllocationError::DeviceNotFound(id.clone()))?;
            device_paths.extend(device.paths.iter().cloned());
        }
        Ok(ContainerAllocation {
            device_paths,
            ..Default::default()
        })
    }
}
