use std::fmt;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DevicePermissions {
    pub read: bool,
    pub write: bool,
    pub mknod: bool,
}

impl DevicePermissions {
    pub fn rdonly() -> Self {
        Self {
            read: true,
            write: false,
            mknod: false,
        }
    }

    pub fn rdwr() -> Self {
        Self {
            read: true,
            write: true,
            mknod: false,
        }
    }

    pub fn as_str(&self) -> &'static str {
        match (self.read, self.write, self.mknod) {
            (true, true, true) => "rwm",
            (true, true, false) => "rw",
            (true, false, true) => "rm",
            (true, false, false) => "r",
            (false, true, true) => "wm",
            (false, true, false) => "w",
            (false, false, true) => "m",
            (false, false, false) => "",
        }
    }
}

impl fmt::Display for DevicePermissions {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.as_str().fmt(f)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn device_permissions_as_str() {
        assert_eq!(
            DevicePermissions {
                read: true,
                write: true,
                mknod: true
            }
            .as_str(),
            "rwm"
        );
        assert_eq!(
            DevicePermissions {
                read: true,
                write: true,
                mknod: false
            }
            .as_str(),
            "rw"
        );
        assert_eq!(
            DevicePermissions {
                read: true,
                write: false,
                mknod: false
            }
            .as_str(),
            "r"
        );
        assert_eq!(
            DevicePermissions {
                read: false,
                write: false,
                mknod: false
            }
            .as_str(),
            ""
        );
    }
}
