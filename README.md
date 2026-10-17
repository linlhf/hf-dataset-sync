# hf-dataset-sync

可复用的 Codex Skill，附带 **HF 官方 dataset 仓库 → 有界本地缓存 → 阿里云 OSS 内网** 的可执行流水线。

保留 SQLite 断点、官方 SDK 下载、独立校验进程、独立大文件上传进程、小文件上传名额、429 冷却、源 SHA/Git blob 校验、OSS CRC 与禁止覆盖。普通 transient 错误有上限重试；授权或对象冲突不会假报成功。

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

- 此版本支持官方普通 dataset 仓库及内网 OSS。没有 Web UI、MCP 服务、自动建代理或定时任务。
- HF Bucket 和 Xet 尚未接入此执行器，不把卡片仓库当实际 Bucket 数据。
- 不自动猜测 LeRobot 重复版本，不附带任何真实凭据、私有节点或下载清单。
- 依赖锁定到原流程验证的版本；升级 HF SDK 需重新测试 HTTP hook/限流/续传，不能直接 `pip install -U`。
- 环境 profile 参数是测速起点，不是容量要求或固定最优值；多任务共享账号额度需要协调请求预算。

## 安全

凭据从执行进程的 `HF_TOKEN`、`OSS_ACCESS_KEY_ID`、`OSS_ACCESS_KEY_SECRET` 获取。使用已有凭据管理器或受保护的服务环境，禁止把它们写进 Git、命令参数或日志。运行目录必须是本任务独占、受信且不可被其他用户改写的持久化路径。

此版本没有导入旧任务数据库的自动迁移。新任务绑定源清单与目标身份；不修改已有生产任务。
