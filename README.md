<p align="center">
  <img src="assets/logo.png" alt="KumaBoxd logo" width="144">
</p>

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

## Benchmarks

Measured on November 1, 2026: Xeon E3-1245 v2 (4 cores / 8 threads), 32 GiB RAM,
SATA SSD RAID1, XFS, Linux 7.0.0-34 and Cloud Hypervisor v54 with mmap restore.
Each pool VM has 2 vCPUs, 512 MiB RAM, a 10 GiB writable disk and one eBPF NIC.

| Durability | Claim load | p50 (ms) | p95 (ms) | Samples |
| --- | --- | ---: | ---: | ---: |
| `sync` (default) | Serial | 0.891 | 1.745 | 480 |
| `sync` (default) | 16 concurrent | 3.609 | 6.484 | 480 |
| `buffered` | Serial | 0.230 | 0.543 | 480 |
| `buffered` | 16 concurrent | 1.714 | 3.526 | 480 |

Each row pools 480 request samples from three interleaved rounds, with 10 batches
of 16 claims per round. Every batch starts with 16 ready VMs; refill is excluded.
Timing includes request encoding, loopback HTTP/1.1 and response decoding.
VM preparation and subsequent guest commands are outside the claim measurement.
Claimed VMs were verified with `/bin/true` after claim timing stopped.

`sync` acknowledges a synchronous bbolt commit. `buffered` acknowledges a journal
append to the OS cache and checkpoints asynchronously; host failure can lose
recently acknowledged changes. These are different durability guarantees.

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
