use super::*;

#[derive(Debug, Clone)]
pub enum Health {
    Healthy,
    Unhealthy,
}

impl Health {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Healthy => v1beta1::HEALTHY,
            Self::Unhealthy => v1beta1::UNHEALTHY,
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
        assert_eq!(Health::Healthy.as_str(), v1beta1::HEALTHY);
        assert_eq!(Health::Unhealthy.as_str(), v1beta1::UNHEALTHY);
    }

    #[test]
    fn health_display() {
        assert_eq!(Health::Healthy.to_string(), v1beta1::HEALTHY);
        assert_eq!(Health::Unhealthy.to_string(), v1beta1::UNHEALTHY);
    }
}
