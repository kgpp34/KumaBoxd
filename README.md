<p align="center">
  <img src="assets/logo.png" alt="KumaBoxd logo" width="144">
</p>

<h1 align="center">KumaBoxd</h1>
<p align="center">Node-local sandbox orchestration for KumaBox.</p>

KumaBoxd manages ready-to-use microVM sandboxes on a single Linux node. Built on
[KumaBox](https://github.com/kgpp34/KumaBox), it maintains warm pools from golden
snapshots and exposes a local HTTP API for sandbox allocation and leases.

## Responsibilities

- **Templates:** register existing golden snapshots with pinned snapshot and image identities.
- **Warm pools:** maintain a configured number of ready sandboxes, with identity, networking and guest readiness prepared before allocation.
- **Leases:** allocate sandboxes with idempotent claims, renew leases, and reclaim sandboxes on release or expiry.
- **Capacity:** reserve VM, vCPU, memory and storage budgets shared across pools.
- **Recovery:** reconcile inventory and leases after restart, monitor VM health, and replenish depleted pools.

Claims allocate an existing ready sandbox. Preparation and replenishment run in
the background. Used sandboxes are destroyed after release or expiry and replaced
with fresh instances from the template.

## Architecture

```text
Local API clients
       |
       | HTTP
       v
   KumaBoxd
   Templates / warm pools / leases / resource budgets
       |
       | HTTP/2 over Unix socket
       v
   KumaBox runtime
   VM lifecycle / images / snapshots / storage / networking / guest operations
       |
       v
   Cloud Hypervisor + guest
```

KumaBoxd owns pool inventory and allocation state; KumaBox owns the underlying
VMs and runtime resources. The services communicate through the runtime RPC API.

The node API binds to loopback. Node state is stored locally in embedded
[bbolt](https://github.com/etcd-io/bbolt), without an external database service.
KumaBoxd runs alongside a KumaBox runtime on a Linux host with KVM support.

## License

[MIT](LICENSE)
