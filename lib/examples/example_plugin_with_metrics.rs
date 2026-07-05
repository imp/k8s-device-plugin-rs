//! Demonstrates turning the `#[tracing::instrument]` spans that
//! `DevicePluginService` wraps every RPC handler in (see `lib/src/lib.rs`) into
//! Prometheus metrics, via a small custom `tracing_subscriber::Layer`.
//!
//! This is deliberately independent of the framework itself — `RpcMetricsLayer`
//! below is plain `tracing_subscriber` + `prometheus` code that would work
//! against *any* `#[tracing::instrument]`-wrapped spans, not just this crate's.
//!
//! Run with:
//!
//! ```bash
//! cargo run --example example_plugin_with_metrics
//! ```
//!
//! Then, while it's running against a real kubelet: `curl http://127.0.0.1:9184/metrics`

use std::path::PathBuf;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Instant;

use k8s_device_plugin_lib::AllocationError;
use k8s_device_plugin_lib::ContainerAllocation;
use k8s_device_plugin_lib::Device;
use k8s_device_plugin_lib::DeviceAllocator;
use k8s_device_plugin_lib::DeviceDiscovery;
use k8s_device_plugin_lib::DevicePath;
use k8s_device_plugin_lib::DevicePermissions;
use k8s_device_plugin_lib::DevicePlugin;
use k8s_device_plugin_lib::DevicePluginService;
use k8s_device_plugin_lib::Health;
use k8s_device_plugin_lib::K8sDevicePlugin;
use prometheus_exporter::prometheus::HistogramOpts;
use prometheus_exporter::prometheus::HistogramVec;
use prometheus_exporter::prometheus::IntCounterVec;
use prometheus_exporter::prometheus::Opts;
use prometheus_exporter::prometheus::register;
use tracing::span;
use tracing_subscriber::Layer;
use tracing_subscriber::layer::Context;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::registry::LookupSpan;
use tracing_subscriber::util::SubscriberInitExt;

const RESOURCE_NAME: &str = "example.com/widget";

/// Span names this layer records metrics for — every `#[tracing::instrument]`
/// site in `DevicePlugin`/`DevicePluginService` (see `lib/src/lib.rs`).
const TRACKED_SPANS: &[&str] = &[
    "run",
    "try_register",
    "list_and_watch",
    "allocate",
    "pre_start_container",
    "get_preferred_allocation",
];

struct RpcMetrics {
    calls: IntCounterVec,
    duration: HistogramVec,
}

impl RpcMetrics {
    fn register() -> Self {
        let calls = IntCounterVec::new(
            Opts::new(
                "device_plugin_rpc_calls_total",
                "Total number of device plugin RPC handler invocations",
            ),
            &["rpc"],
        )
        .expect("valid metric definition");
        register(Box::new(calls.clone())).expect("register calls counter");

        let duration = HistogramVec::new(
            HistogramOpts::new(
                "device_plugin_rpc_duration_seconds",
                "Device plugin RPC handler duration in seconds",
            ),
            &["rpc"],
        )
        .expect("valid metric definition");
        register(Box::new(duration.clone())).expect("register duration histogram");

        Self { calls, duration }
    }
}

/// Records when a tracked span started, stashed in the span's extensions.
struct SpanStart(Instant);

/// A `tracing_subscriber::Layer` that turns span open/close pairs into a
/// Prometheus call counter and duration histogram, labeled by span name.
struct RpcMetricsLayer {
    metrics: Arc<RpcMetrics>,
}

impl<S> Layer<S> for RpcMetricsLayer
where
    S: tracing::Subscriber + for<'a> LookupSpan<'a>,
{
    fn on_new_span(&self, _attrs: &span::Attributes<'_>, id: &span::Id, ctx: Context<'_, S>) {
        let Some(span) = ctx.span(id) else {
            return;
        };
        if TRACKED_SPANS.contains(&span.name()) {
            span.extensions_mut().insert(SpanStart(Instant::now()));
        }
    }

    fn on_close(&self, id: span::Id, ctx: Context<'_, S>) {
        let Some(span) = ctx.span(&id) else {
            return;
        };
        let rpc = span.name();
        let Some(&SpanStart(start)) = span.extensions().get::<SpanStart>() else {
            return;
        };
        self.metrics.calls.with_label_values(&[rpc]).inc();
        self.metrics
            .duration
            .with_label_values(&[rpc])
            .observe(start.elapsed().as_secs_f64());
    }
}

/// Fake backend exposing a single in-memory "widget" device.
struct ExampleWidgetPlugin {
    devices: Mutex<Vec<Device>>,
}

impl ExampleWidgetPlugin {
    fn new() -> Self {
        let devices = vec![Device {
            id: "widget-0".to_string(),
            health: Health::Healthy,
            paths: vec![DevicePath {
                host_path: PathBuf::from("/dev/widget-0"),
                container_path: PathBuf::from("/dev/widget-0"),
                permissions: DevicePermissions::rdwr(),
            }],
        }];
        Self {
            devices: Mutex::new(devices),
        }
    }
}

#[tonic::async_trait]
impl DeviceDiscovery for ExampleWidgetPlugin {
    async fn discover(&self) -> Vec<Device> {
        self.devices.lock().unwrap().clone()
    }
}

#[tonic::async_trait]
impl DeviceAllocator for ExampleWidgetPlugin {
    async fn allocate(
        &self,
        device_ids: &[String],
    ) -> Result<ContainerAllocation, AllocationError> {
        let devices = self.devices.lock().unwrap();
        let mut device_paths = Vec::new();
        for id in device_ids {
            let device = devices
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

impl K8sDevicePlugin for ExampleWidgetPlugin {}

#[tokio::main]
async fn main() -> std::io::Result<()> {
    let metrics = Arc::new(RpcMetrics::register());

    tracing_subscriber::registry()
        .with(tracing_subscriber::fmt::layer())
        .with(RpcMetricsLayer { metrics })
        .init();

    let binding = "127.0.0.1:9184".parse().expect("valid socket address");
    prometheus_exporter::start(binding).expect("failed to start Prometheus exporter");
    tracing::info!("Prometheus metrics available at http://127.0.0.1:9184/metrics");

    let service = DevicePluginService::new(ExampleWidgetPlugin::new());
    let plugin = DevicePlugin::new(RESOURCE_NAME, service);
    plugin.run().await
}
