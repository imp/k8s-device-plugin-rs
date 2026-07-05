use std::path::PathBuf;

use crate::DevicePermissions;
use crate::Health;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DevicePath {
    pub host_path: PathBuf,
    pub container_path: PathBuf,
    pub permissions: DevicePermissions,
}

/// A host directory or file to bind-mount into the container, as opposed to a
/// device special file (see [`DevicePath`]).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HostMount {
    pub host_path: PathBuf,
    pub container_path: PathBuf,
    pub read_only: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Device {
    pub id: String,
    pub health: Health,
    pub paths: Vec<DevicePath>,
}
