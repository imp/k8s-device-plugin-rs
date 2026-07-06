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

impl Device {
    pub fn new(id: impl Into<String>) -> Self {
        let id = id.into();
        let health = Health::Healthy;
        let paths = vec![];
        Self { id, health, paths }
    }

    pub fn rdwr(id: impl Into<String>, path: impl Into<PathBuf>) -> Self {
        let id = id.into();
        let health = Health::Healthy;
        let paths = vec![DevicePath::rdwr(path)];
        Self { id, health, paths }
    }

    pub fn health(self, health: Health) -> Self {
        Self { health, ..self }
    }
}

impl DevicePath {
    pub fn rdwr(path: impl Into<PathBuf>) -> Self {
        let host_path = path.into();
        let container_path = host_path.clone();
        let permissions = DevicePermissions::rdwr();
        Self {
            host_path,
            container_path,
            permissions,
        }
    }
}
