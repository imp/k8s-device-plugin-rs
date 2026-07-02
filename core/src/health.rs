use std::fmt;

#[derive(Clone, Copy, Debug)]
pub enum Health {
    Healthy,
    Unhealthy,
}

impl Health {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Healthy => "Healthy",
            Self::Unhealthy => "Unhealthy",
        }
    }
}

impl fmt::Display for Health {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.as_str().fmt(f)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn health_as_str() {
        assert_eq!(Health::Healthy.as_str(), "Healthy");
        assert_eq!(Health::Unhealthy.as_str(), "Unhealthy");
    }

    #[test]
    fn health_display() {
        assert_eq!(Health::Healthy.to_string(), "Healthy");
        assert_eq!(Health::Unhealthy.to_string(), "Unhealthy");
    }
}
