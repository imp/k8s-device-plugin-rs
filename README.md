# k8s-device-plugin-rs

A Rust framework for writing Kubernetes [device plugins](https://kubernetes.io/docs/concepts/extend-kubernetes/compute-storage-net/device-plugins/) — the gRPC protocol kubelet uses to discover, health-check, and allocate custom hardware (GPUs, FPGAs, NICs, and similar) to pods.

You implement one trait describing *your* device backend; the framework handles kubelet registration, the retry/re-registration lifecycle, and the `DevicePlugin` gRPC service itself.

## Architecture

| Crate | Role |
|---|---|
| `core` | Backend-facing abstractions: `DeviceDiscovery`, `DeviceAllocator`, `K8sDevicePlugin`, and the `Device`/`DevicePermissions`/`AllocationError` types. No gRPC or async runtime specifics — this is what a backend implements against. |
| `proto` | Generated bindings for the `v1beta1` device-plugin gRPC protocol (via `tonic`/`prost`), plus the vendored `k8s.io/kubelet` proto submodule. |
| `lib` | The framework itself: `DevicePlugin` (registration + lifecycle) and `DevicePluginService` (the gRPC service adapter that drives a `K8sDevicePlugin` backend). |
| `test` | Shared test-only helpers (mock kubelet registration server, mock device-plugin client) used by `lib`'s integration tests. |
| `example` | A complete, deployable plugin built on `StaticDevicePlugin`, with a `Dockerfile` and K8s manifests — see [Deploying a real plugin](#deploying-a-real-plugin). |

## Quickstart

### The fast path: a fixed device list

If your devices are known up front and don't need custom discovery or allocation logic, `StaticDevicePlugin` needs no trait implementation at all — just a `Vec<Device>`. It re-checks that each device's host path still exists on disk before every `Allocate` call, so unplugged hardware fails cleanly instead of handing kubelet a stale path:

```rust
use k8s_device_plugin_lib::{Device, DevicePlugin, DevicePluginService, StaticDevicePlugin};

#[tokio::main]
async fn main() -> std::io::Result<()> {
    let devices = vec![Device::rdwr("widget-0", "/dev/widget0")];

    let service = DevicePluginService::new(StaticDevicePlugin::new(devices));
    let plugin = DevicePlugin::new("example.com/widget", service);
    plugin.run().await
}
```

That's the whole plugin. Reach for a custom backend (below) once you need dynamic discovery, the optional hooks, or allocation logic beyond "does this path exist."

### Custom backends

Implement `DeviceDiscovery` and `DeviceAllocator` for your backend type, then opt into `K8sDevicePlugin` (a marker trait with optional, default-implemented hooks — see below):

```rust
use k8s_device_plugin_lib::{
    AllocationError, ContainerAllocation, Device, DeviceAllocator, DeviceDiscovery,
    DevicePlugin, DevicePluginService, K8sDevicePlugin,
};

struct MyBackend { /* ... */ }

#[tonic::async_trait]
impl DeviceDiscovery for MyBackend {
    async fn discover(&self) -> Vec<Device> {
        // Return every device this backend currently knows about, with health.
        vec![]
    }
}

#[tonic::async_trait]
impl DeviceAllocator for MyBackend {
    async fn allocate(&self, device_ids: &[String]) -> Result<ContainerAllocation, AllocationError> {
        // Resolve device_ids to the host/container paths + permissions to mount.
        Ok(ContainerAllocation::default())
    }
}

impl K8sDevicePlugin for MyBackend {}

#[tokio::main]
async fn main() -> std::io::Result<()> {
    let service = DevicePluginService::new(MyBackend { /* ... */ });
    let plugin = DevicePlugin::new("example.com/my-device", service);
    plugin.run().await
}
```

`plugin.run()` registers with kubelet, serves the gRPC service over a Unix socket under `/var/lib/kubelet/device-plugins/`, re-registers automatically if kubelet restarts, and re-polls `discover()` on an interval (default 5s, override with `DevicePluginService::with_poll_interval`) so `ListAndWatch` reports health/inventory changes as they happen.

A complete, runnable version of this — including the optional hooks below — lives in [`lib/examples/example_plugin.rs`](lib/examples/example_plugin.rs):

```bash
cargo run --example example_plugin
```

This needs a reachable kubelet device-plugin registration socket to actually register (e.g. inside a kind/minikube node, or as a DaemonSet); outside that environment it fails fast with a clear I/O error instead of hanging.

### Optional hooks

`K8sDevicePlugin` provides two optional, default-implemented hooks beyond discovery and allocation. Override the hook *and* its matching availability flag together — the framework reports the flags to kubelet via `GetDevicePluginOptions`:

```rust
impl K8sDevicePlugin for MyBackend {
    fn pre_start_required(&self) -> bool {
        true
    }

    async fn pre_start_container(&self, device_ids: &[String]) -> Result<(), AllocationError> {
        // e.g. reset the device before kubelet starts the container.
        Ok(())
    }

    fn preferred_allocation_available(&self) -> bool {
        true
    }

    async fn preferred_allocation(
        &self,
        available_device_ids: &[String],
        must_include_device_ids: &[String],
        size: usize,
    ) -> Result<Vec<String>, AllocationError> {
        // Choose `size` device IDs from `available_device_ids`, including
        // every ID in `must_include_device_ids`.
        Ok(vec![])
    }
}
```

Leave both hooks at their defaults (`false` / no-op / unavailable) if your device doesn't need a pre-start step or non-arbitrary allocation choice.

### Allocation artifacts beyond device paths

`ContainerAllocation` isn't limited to `device_paths` (`/dev` nodes) — it also carries `mounts` (extra host bind-mounts, e.g. shared libraries), `envs` (environment variables, e.g. a `*_VISIBLE_DEVICES`-style variable), `annotations`, and `cdi_devices` (fully qualified [CDI](https://github.com/container-orchestrated-devices/container-device-interface) device names). All four default to empty, so use `..Default::default()` when you only need a subset:

```rust
Ok(ContainerAllocation {
    device_paths,
    envs: HashMap::from([("MY_VISIBLE_DEVICES".to_string(), device_ids.join(","))]),
    ..Default::default()
})
```

## Deploying a real plugin

[`example/`](example/) is a complete, deployable device plugin built on `StaticDevicePlugin` — its own workspace crate with a `Dockerfile` (multi-stage, distroless static runtime) and a minimal K8s manifest set (`Namespace` + `DaemonSet` + `kustomization.yaml`) under `example/k8s/`. It's configured entirely through env vars (`RESOURCE_NAME`, `DEVICE_PATHS`), so forking it means editing the DaemonSet YAML, not the source. See [`example/README.md`](example/README.md) for the build/push/deploy walkthrough.

## Observability

The framework emits structured [`tracing`](https://docs.rs/tracing) events and spans — never raw `println!`/`eprintln!` — covering the registration lifecycle and every RPC handler. It doesn't install a subscriber itself (see `obs-library-facade`); your binary chooses one. The example plugin installs a plain formatting subscriber filtered by `RUST_LOG`:

```bash
RUST_LOG=info cargo run --example example_plugin
```

Because RPC handlers are wrapped in `#[tracing::instrument]` spans, any subscriber `Layer` you attach (a Prometheus exporter, `tracing-opentelemetry`, etc.) gets call counts and latencies for `Allocate`, `ListAndWatch`, `PreStartContainer`, and `GetPreferredAllocation` for free, without the framework needing its own metrics API.

[`lib/examples/example_plugin_with_metrics.rs`](lib/examples/example_plugin_with_metrics.rs) shows this concretely: a small, framework-independent `RpcMetricsLayer` turns those spans into a Prometheus `IntCounterVec` and `HistogramVec`, served over a real `/metrics` endpoint:

```bash
cargo run --example example_plugin_with_metrics
# then, while it's running: curl http://127.0.0.1:9184/metrics
```

## Local validation

This repo uses [`mise`](https://mise.jdx.dev/) to mirror the CI checklist locally:

```bash
mise run ci     # fmt --check, clippy -D warnings, test (via cargo-nextest)
```

Or individually: `mise run fmt`, `mise run clippy`, `mise run test`.

Building the `proto` crate requires `protoc` (the Protobuf compiler) on `PATH`.

## Issue tracking

Work is tracked locally with [beads_rust](https://github.com/Dicklesworthstone/beads_rust) (`br`/`brr`) — see `AGENTS.md` for the workflow. `.beads/` is git-ignored; issue state is local-only and not part of this repository.
