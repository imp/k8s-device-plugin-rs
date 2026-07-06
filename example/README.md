# k8s-device-plugin-example

A real, deployable device plugin built on `StaticDevicePlugin` -- meant as a
base to fork, not to run as-is against real hardware. It's configured
entirely through environment variables (see `src/main.rs`):

| Env var | Default | Meaning |
|---|---|---|
| `RESOURCE_NAME` | `example.com/widget` | The extended resource kubelet advertises for this plugin. |
| `DEVICE_PATHS` | *(empty)* | Comma-separated host device paths, e.g. `/dev/widget0,/dev/widget1`. Each device's ID is derived from the path's file name. |
| `RUST_LOG` | *(unset)* | Standard `tracing-subscriber` env filter, e.g. `info` or `k8s_device_plugin_lib=debug`. |

## Build

Run from the **repository root** (the build context needs the workspace
`Cargo.toml`/`Cargo.lock` and the `proto/kubelet` git submodule):

```bash
git submodule update --init
docker build -f example/Dockerfile -t <registry>/k8s-device-plugin-example:<tag> .
docker push <registry>/k8s-device-plugin-example:<tag>
```

For multi-arch images, use `docker buildx build --platform linux/amd64,linux/arm64 ...`
instead of `docker build` -- see the comments at the top of `Dockerfile`.

## Deploy

Point `example/k8s/kustomization.yaml` at your image, then apply:

```bash
cd example/k8s
kustomize edit set image k8s-device-plugin-example=<registry>/k8s-device-plugin-example:<tag>
cd ../..
kubectl apply -k example/k8s/
```

Edit `example/k8s/daemonset.yaml`'s `RESOURCE_NAME`/`DEVICE_PATHS` env vars
(and the commented-out `nodeSelector`/`tolerations`) to match your actual
hardware and the nodes that have it before deploying for real.

Verify:

```bash
kubectl -n k8s-device-plugin-example get pods
kubectl describe node <node-name> | grep example.com/widget
```

## What this doesn't cover

- **RBAC**: none needed -- device plugins talk to kubelet over a local Unix
  socket under `/var/lib/kubelet/device-plugins/`, never the API server.
- **Custom discovery/health-checking**: `StaticDevicePlugin` re-checks that
  each device's host path still exists before every `Allocate` call, but
  doesn't otherwise probe hardware health. If you need that, implement
  `K8sDevicePlugin` directly against your backend instead (see the root
  [`README.md`](../README.md)) and swap it in here.
