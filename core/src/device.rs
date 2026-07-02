use std::path::PathBuf;

use crate::DevicePermissions;
use crate::Health;

#[derive(Clone, Debug)]
pub struct DevicePath {
    pub host_path: PathBuf,
    pub container_path: PathBuf,
    pub permissions: DevicePermissions,
}

#[derive(Clone, Debug)]
pub struct Device {
    pub id: String,
    pub health: Health,
    pub paths: Vec<DevicePath>,
}
