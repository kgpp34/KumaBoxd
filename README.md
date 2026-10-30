<p align="center">
  <img src="assets/logo.png" alt="KumaBoxd — 熊骑士徽标" width="360">
</p>

<h1 align="center">KumaBoxd</h1>
<p align="center">A node agent for KumaBox sandboxes.</p>

KumaBoxd 是构建在 [KumaBox](https://github.com/kgpp34/KumaBox) runtime 之上的节点服务，用于管理本机的沙箱模板、预热池、租约和容量。未来的控制面负责选择节点，KumaBoxd 负责在选中的节点上交付沙箱。

**当前处于早期开发阶段。** 已实现服务入口、健康检查、优雅退出、typed runtime client，以及节点本地模板的注册、查询和持久化。Warm pool、claim/release 和租约尚未实现，HTTP API 目前不能领取沙箱。

## 职责与架构

```mermaid
flowchart TD
    Control[未来的控制面 / Scheduler] -->|选择节点，提交本地请求| Node[KumaBoxd]
    Node --> Templates[节点本地模板注册]
    Node --> Pool[Warm pool · 规划中]
    Node --> Leases[租约与本地容量 · 规划中]
    Node -->|HTTP/2 · Unix socket| Runtime[KumaBox daemon]
    CLI[KumaBox CLI] -->|本地 RPC| Runtime
    Runtime --> VMM[VMM / 磁盘 / eBPF 网络]
    VMM --> Guest[Guest init + agent]
```

| 层 | 负责什么 |
| --- | --- |
| KumaBox runtime | 镜像、VM 生命周期、磁盘、快照、网络、guest 操作及这些资源的状态一致性 |
| KumaBoxd | 节点本地模板注册；后续增加预热库存、租约、本机容量与这些业务对象的恢复 |
| 未来控制面 | 集群节点发现、全局调度、租户与集群策略 |

KumaBoxd 复用 runtime 的本地 RPC。它不导入 KumaBox 的实现包，不打开 runtime 的 SQLite，也不通过启动 CLI 子进程驱动 VM。两者各自管理自己的进程生命周期；停止 KumaBoxd 不会关闭 runtime 或已有沙箱。

runtime 适配器对齐 KumaBox 提交 `360c562` 的 `/rpc/v1/` 协议，提供健康检查、快照和镜像查询、clone、沙箱查询与列表、流式命令输出、stop 和 remove。它属于节点内部适配层，不是公开的通用 SDK。连接和 HTTP/2 transport 复用；变更操作不会自动重试。终态错误保留 runtime 错误码与 committed 字段；丢失终态应答时返回 OutcomeUnknownError，调用方应按原身份查询后再决定如何处理。

## 本地运行

需要 Go 1.24.4 或更高版本。编译与单元测试不需要 KVM、root 或运行中的 VM；真正管理沙箱的 runtime 需要另行部署在 Linux/KVM 节点上。

```bash
make build
./bin/kumaboxd --help
./bin/kumaboxd --state-dir /tmp/kumaboxd-dev --runtime-socket /run/kumabox/kumabox.sock
```

另一终端中执行：

```bash
curl -i http://127.0.0.1:8088/healthz
curl -i http://127.0.0.1:8088/readyz
```

| 接口 | 返回与含义 |
| --- | --- |
| `GET /healthz` | `200 {"status":"ok"}`：节点 HTTP 服务存活 |
| `GET /readyz` | `200 {"status":"ready"}`：runtime 的 `node.info` 已成功应答 |
| `GET /readyz` | `503 {"status":"not_ready"}`：runtime 不可达、超时或响应无效 |

没有运行 runtime 时，`/healthz` 仍返回 200，`/readyz` 返回 503。这里的 readiness 只检查 runtime RPC 可用性，不代表有可领取的库存、资源足够或某台 guest 已就绪。

HTTP 当前仅允许绑定 loopback IP，默认 `127.0.0.1:8088`；远程 API 和认证尚未实现。日志输出到 stderr，错误详情不放入健康响应。按 Ctrl+C 或发送 SIGTERM 会取消探测并在有限时间内关闭 HTTP 服务。

### 配置

显式参数优先于环境变量，环境变量优先于默认值。

| 参数 | 环境变量 | 默认值 |
| --- | --- | --- |
| `--listen` | `KUMABOXD_LISTEN` | `127.0.0.1:8088` |
| `--runtime-socket` | `KUMABOXD_RUNTIME_SOCKET` | `/run/kumabox/kumabox.sock` |
| `--state-dir` | `KUMABOXD_STATE_DIR` | `/var/lib/kumaboxd` |
| `--operation-timeout` | — | `2m` |
| `--probe-timeout` | — | `2s` |
| `--shutdown-timeout` | — | `5s` |
| `--version` | — | 打印构建版本并退出 |

进程需要连接 runtime socket 及写入 state-dir 的权限。连接使用常驻、可复用的 HTTP/2 transport，不按探测重复打开数据库，也不会在连接失败时退回直接操作本地状态。

## 节点本地模板

注册现有 golden，不重新构建镜像。请求中的快照名会被解析为不可变 ID，并查询对应镜像的摘要、平台和启动协议。完整 guest 配置保留在记录中，避免后续字段在中间层丢失。

```bash
curl -i http://127.0.0.1:8088/v1/templates \
  -H 'Content-Type: application/json' \
  -d '{"name":"python","snapshot":"my-golden"}'

curl http://127.0.0.1:8088/v1/templates/python
curl 'http://127.0.0.1:8088/v1/templates?limit=50'
```

- 首次注册返回 `201`；相同绑定重试返回 `200`，保留首次时间。
- 同名绑定其他快照或配置返回 `409`。名称须匹配 `[a-z0-9][a-z0-9._-]{0,62}`。
- 列表使用 `limit`（1–200，默认 50）和 `after` 游标，响应提供 `next_cursor`。
- 注册需要 runtime 可用；读取已注册记录不需要 runtime 在线。
- 状态保存到独立的 `state-dir/node.db`，采用 bbolt 同步事务和进程独占文件锁；一个进程只打开一次。它是嵌入式库，不是 etcd 服务，没有中央数据库依赖。
- 注册验证元数据，不代替一次成功的原生快照恢复，也不会阻止 runtime 用户直接删除原快照。补池仍须确认制品存在、运行时兼容和 guest 就绪。

这是**节点上的本地注册**。未来全局模板定义及制品来源，会与每台节点上的本地快照副本分开建模。本阶段不把本地 UUID 当作整个集群都能访问的快照文件。

## 下一阶段：模板与 warm pool

以下是实现边界，不是已上线能力。

```text
准备模板
  固定 image / golden / VM shape / guest 与 VMM 兼容版本
      ↓
后台补池
  runtime clone → guest ready → 完成独立身份初始化 → 标记 Ready
      ↓
Claim
  本地容量校验 → 原子预留 Ready VM → 持久化租约与访问绑定 → 返回
      ↓
Release / TTL 到期
  使租约失效 → 回收访问与 VM → 确认销毁 → 异步补充新的 VM
```

- **模板**：使用不可变的 image/golden 标识，池键包含资源规格与兼容信息，避免不同启动协议的快照混用。
- **Warm pool**：每个池有明确的目标库存和资源预算；补池是后台任务，缺货行为通过 API 明确定义。
- **租约**：claim/release/renew 需要幂等键和持久化状态。第一版销毁用过的 VM，再从 golden 补充，不把用过的 guest 直接交给下一个用户。
- **重启恢复**：KumaBoxd 恢复自己的模板、库存和租约，向 runtime 查询实际资源后收敛；runtime 继续负责 VMM、TAP 和磁盘的底层恢复。
- **性能验证**：分别记录有库存的 claim、无库存的真实 clone、guest 首次执行命令和网络可用时间，不把后台准备成本藏进指标。

先跑通单节点闭环和有网卡的并发验证，再接远程控制面与调度。KumaBoxd 的 Go runtime client 服务于节点内部调用；面向用户的 SDK 仍以 Python、TypeScript 为主要方向。

## 工程结构

```text
cmd/kumaboxd/           参数入口、依赖组装、信号处理
internal/config/       配置读取与校验
internal/node/         节点 HTTP 服务与生命周期
internal/template/     本地模板模型、校验和注册流程
internal/metadata/     节点事务存储适配器
internal/runtimeclient/  KumaBox RPC 适配器
assets/logo.png        项目徽标原图
.github/workflows/     Linux / macOS 检查
```

按实际职责增加包，不为尚未实现的能力建立空接口或空目录。接口由使用方定义；资源释放和超时保持显式。

```bash
make verify
make race
make lint              # golangci-lint v2.13.2
make benchmark         # 本地存储基线，不代表 VM claim 延迟
```

测试覆盖真实 Unix socket 上的 HTTP/2 请求格式、连接复用、取消、无效响应，以及节点服务的健康语义和退出流程。另有持久化重启恢复、并发注册唯一性、取消、名称冲突、HTTP 输入边界和错误映射测试。

真实 VM 验收位于 `test/e2e`，默认不运行。它需要已有的、带一张 eBPF 网卡的 golden，guest 需有 `ip`、`getent`、`curl` 并能访问 example.com：

```bash
sudo env KUMABOXD_E2E_SOCKET=/run/kumabox/kumabox.sock \
  KUMABOXD_E2E_SNAPSHOT=my-ebpf-golden \
  go test -tags integration -v ./test/e2e
```

测试通过 typed client 克隆唯一命名的 VM，检查命令执行、DNS、HTTPS，验证模板的注册、重试、列表和节点服务重启恢复，最后停止并删除该测试克隆。输入的 golden 不会被删除。测试中的单次耗时不作为性能基准。

存储基线保留同步落盘，不关闭 fsync；bbolt 是否满足将来的领取延迟目标，需要在对应主机和租约负载上进一步实测。

本地设计草稿和实验报告可放在 `docs/`，该目录被 Git 忽略；需要随代码审阅的当前架构与开发说明放在本 README。

## License

[MIT](LICENSE) · Copyright (c) 2026 YoungLH
