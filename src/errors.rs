use thiserror::Error;

#[derive(Debug, Error)]
pub enum TransportError {
    #[error("retryable transport error: {message}")]
    Retryable { message: String },
    #[error("terminal transport error: {message}")]
    Terminal { message: String },
}

impl TransportError {
    pub fn retryable<M: Into<String>>(message: M) -> Self {
        Self::Retryable {
            message: message.into(),
        }
    }

    pub fn terminal<M: Into<String>>(message: M) -> Self {
        Self::Terminal {
            message: message.into(),
        }
    }

    pub fn is_retryable(&self) -> bool {
        matches!(self, Self::Retryable { .. })
    }
}

#[derive(Debug, Error)]
pub enum EngineError {
    #[error("invalid request: {message}")]
    InvalidRequest { message: String },
    #[error("i/o error: {message}")]
    Io { message: String },
}

impl EngineError {
    pub fn invalid<M: Into<String>>(message: M) -> Self {
        Self::InvalidRequest {
            message: message.into(),
        }
    }

    pub fn io<M: Into<String>>(message: M) -> Self {
        Self::Io {
            message: message.into(),
        }
    }

    pub fn errno(&self) -> i32 {
        match self {
            Self::InvalidRequest { .. } => libc::EINVAL,
            Self::Io { .. } => libc::EIO,
        }
    }
}
