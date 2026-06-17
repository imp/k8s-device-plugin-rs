use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::AtomicU32;
use std::sync::atomic::Ordering;

use k8s_device_plugin_proto::v1beta1;
use tempfile::TempDir;
use tokio::net::UnixListener;
use tokio::sync::Mutex;
use tokio::task::JoinHandle;
use tokio_stream::wrappers::UnixListenerStream;
use tonic::transport;

#[derive(Debug, Default)]
pub struct FakeRegistration {
    pub requests: Arc<Mutex<Vec<v1beta1::RegisterRequest>>>,
    /// Always-fail response: returns this status for every request.
    pub failure: Option<(tonic::Code, String)>,
    /// Countdown: fail this many times with `Unavailable`, then succeed.
    pub fail_count: Arc<AtomicU32>,
}

#[tonic::async_trait]
impl v1beta1::Registration for FakeRegistration {
    async fn register(
        &self,
        request: tonic::Request<v1beta1::RegisterRequest>,
    ) -> tonic::Result<tonic::Response<v1beta1::Empty>> {
        if let Some((code, message)) = &self.failure {
            return Err(tonic::Status::new(*code, message.clone()));
        }
        let remaining = self.fail_count.load(Ordering::SeqCst);
        if remaining > 0 {
            self.fail_count.fetch_sub(1, Ordering::SeqCst);
            return Err(tonic::Status::unavailable("transient registration failure"));
        }
        self.requests.lock().await.push(request.into_inner());
        Ok(tonic::Response::new(v1beta1::Empty {}))
    }
}

#[derive(Debug)]
pub struct MockRegistrationServer {
    // Kept alive so the temp dir (and its socket) are removed on drop.
    _socket_dir: TempDir,
    socket_path: PathBuf,
    requests: Arc<Mutex<Vec<v1beta1::RegisterRequest>>>,
    server_handle: JoinHandle<Result<(), transport::Error>>,
}

impl MockRegistrationServer {
    pub fn socket_path(&self) -> String {
        self.socket_path.to_string_lossy().into_owned()
    }

    pub async fn collected_requests(&self) -> Vec<v1beta1::RegisterRequest> {
        self.requests.lock().await.clone()
    }

    pub fn shutdown(self) {
        self.server_handle.abort();
        // _socket_dir is dropped here, removing the temp directory and socket.
    }
}

pub fn start_mock_registration_server(
    failure: Option<(tonic::Code, &str)>,
) -> MockRegistrationServer {
    let fake = FakeRegistration {
        failure: failure.map(|(code, message)| (code, message.to_string())),
        ..Default::default()
    };
    start_server(fake)
}

/// Start a mock registration server that rejects the first `fail_count` requests
/// with `Unavailable`, then accepts all subsequent ones.
pub fn start_mock_registration_server_with_failures(fail_count: u32) -> MockRegistrationServer {
    let fake = FakeRegistration {
        fail_count: Arc::new(AtomicU32::new(fail_count)),
        ..Default::default()
    };
    start_server(fake)
}

fn start_server(fake: FakeRegistration) -> MockRegistrationServer {
    let socket_dir = TempDir::new().expect("create temp dir for registration socket");
    let socket_path = socket_dir.path().join(v1beta1::KUBELET_SOCKET);
    let requests = Arc::clone(&fake.requests);

    let listener = UnixListener::bind(&socket_path).expect("bind unix socket");
    let incoming = UnixListenerStream::new(listener);
    let server = transport::Server::builder().add_service(v1beta1::RegistrationServer::new(fake));
    let server_handle = tokio::spawn(server.serve_with_incoming(incoming));

    MockRegistrationServer {
        _socket_dir: socket_dir,
        socket_path,
        requests,
        server_handle,
    }
}
