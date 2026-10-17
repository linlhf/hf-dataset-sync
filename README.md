# hf-dataset-sync

可复用的 Codex Skill，附带 **HF 官方 dataset 仓库 / HF Bucket → 有界本地缓存 → 阿里云 OSS 内网** 的可执行流水线。

保留 SQLite 任务状态、官方 HTTP / 原生 Xet 下载、独立校验进程、独立大文件上传进程、小文件上传名额、429 冷却、源 SHA/Git blob/Xet 哈希校验、OSS CRC 与禁止覆盖。普通 transient 错误有上限重试；授权、源变化或对象冲突不会假报成功。

## 安装与调用

将仓库克隆为 `~/.codex/skills/hf-dataset-sync`，不要覆盖已存在目录。新会话中使用：

> 使用 $hf-dataset-sync，把 HF 数据集同步到指定 OSS 前缀；先给我完整清单和容量。

脚本使用 Python 3.11–3.13 和 `uv sync --locked` 创建隔离环境，通常在实际下载服务器运行，不在本机堆放大数据。

```bash
uv run scripts/task.py --help
PYTHONPATH=scripts uv run python -m unittest discover -s tests -v
```

[SKILL.md](SKILL.md) 是代理入口；[运行手册](references/operations.md) 包含完整命令与恢复边界；[性能说明](references/performance.md) 包含测速、资源使用和验证口径。

## 范围

- 此版本支持 dataset + HTTP、dataset + Xet、Bucket + Xet，以及内网 OSS。没有 Web UI、MCP 服务、自动建代理或定时任务。
- Bucket 用 `plan --bucket ORG/NAME [--prefix PATH/]` 枚举，`init` 自动选择 Xet；dataset 在 `init --mode xet` 开启 Xet。非 Xet 存储的小文件仍由官方 SDK 用 HTTP 获取，实际传输计数可查。
- Bucket 没有 Git commit 快照：记录逐对象 Xet 哈希，下载前检查源变化，下载后重新计算原生 Xet 哈希；不把卡片仓库当实际数据。
- Xet 统一使用 profile 的 `proxy`；多节点复用本地负载均衡入口。初始化 Xet 任务会将 `download_proxies` 收敛为该入口，HTTP 模式才支持逐文件多入口分流。
- 不自动猜测 LeRobot 重复版本，不附带任何真实凭据、私有节点或下载清单。
- 0.2 依赖锁定 HF SDK 1.33.0 / hf-xet 1.7.0；升级需重新测试 HTTPX 路由、限流、实际 Xet 和恢复，不能直接 `pip install -U`。
- 环境 profile 参数是测速起点，不是容量要求或固定最优值；多任务共享账号额度需要协调请求预算。

## 安全

凭据从执行进程的 `HF_TOKEN`、`OSS_ACCESS_KEY_ID`、`OSS_ACCESS_KEY_SECRET` 获取。使用已有凭据管理器或受保护的服务环境，禁止把它们写进 Git、命令参数或日志。运行目录必须是本任务独占、受信且不可被其他用户改写的持久化路径。

本工具 0.1 创建的 dataset 任务保持身份算法兼容，初始化时只增补可空的 Xet 字段；不支持导入其他旧脚本的任务数据库。任务绑定源清单与目标身份；不修改已有生产任务。

恢复会复用已校验的本地文件和已验证的 OSS 对象；HTTP 保留 SDK 下载断点，Xet 未完成文件可能重新重组，不保证逐字节断点恢复。未进行 TB 级 Xet 稳定性验证，也不保证历史吞吐。
