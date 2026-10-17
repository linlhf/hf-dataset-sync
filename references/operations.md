# 运行手册

## 准备一次环境，下次复用

使用用户现有 SSH 别名和凭据索引；严格校验指纹。读 `whoami; hostname; pwd`，以及 `nproc`、`free -h`、`df -h`、已有进程和服务。已有 `iostat`/`sar` 时使用，不为状态检查额外安装。检查目标桶地域、可用内网路由、缓存盘余量和现有负载。

在执行服务器安装本仓库，运行 `uv sync --locked`。不要升级系统 Python。将 `examples/profile.json` 复制到仓库外的私有环境配置目录，修改 endpoint、缓存预算和代理；示例的北京 endpoint 不是用户实际桶默认值。profile 可供多个新任务复用，但任务目录不得共享。

profile 可附加 `ssh_alias` 供代理选择已有主机；脚本不会自行 SSH 或重新配置节点。先查本地 `~/.config/hf-dataset-sync/profiles/`，复用前核对别名、路由和当前资源；只传递该任务需要的无秘密配置到执行主机。

代理使用已存在的 HTTP loopback 入口，例如 `http://127.0.0.1:1080`。HTTP 模式的 `download_proxies` 可以列多个入口按在途源文件字节分流，`proxy` 为回退入口。Xet 的 Rust 网络层使用进程级代理：`init --mode xet` 将多个下载入口收敛为 `[proxy]`；多节点应把 `proxy` 指向已配置的本地负载均衡器。空字符串表示直连，只支持无内嵌凭据的本地代理。Hysteria 的 UDP 是入口后面的传输层，本执行器本身不安装或设置 Hysteria。OSS 使用 `trust_env=False` 的独立 session，不经过代理。

凭据先查已有索引和管理器。解析已有名称/字段后，将 HF Token 和 OSS AK/SK 直接导入环境（例如通过 `vault env`，或 `export HF_TOKEN="$(vault get 已确认名称 已确认字段)"`），禁止输出值。服务使用受保护环境文件或管理器注入；进程环境对同权限用户可见。不要把 secrets 写到仓库。当前执行器只支持 AK/SK，不支持 STS 自动刷新。

## 清单与任务

以下是命令模板，参数必须换成用户已确认的实际值；任务目录使用持久化数据盘。

```bash
uv run scripts/task.py plan --repo ORG/DATASET --output /data/plans/dataset.json
uv run scripts/task.py init --manifest /data/plans/dataset.json \
  --profile /data/profiles/environment.json \
  --destination oss://BUCKET/datasets/DATASET/ --task /data/tasks/DATASET
uv run scripts/sync.py /data/tasks/DATASET
```

dataset 的 `init` 可加 `--mode xet`，使用官方 `hf_hub_download` 的原生 Xet 实现；普通 Git 小文件仍走 HTTP。Bucket 使用另一组官方接口：

```bash
uv run scripts/task.py plan --bucket ORG/BUCKET --prefix data/ \
  --output /data/plans/bucket.json
uv run scripts/task.py init --manifest /data/plans/bucket.json \
  --profile /data/profiles/environment.json \
  --destination oss://BUCKET/datasets/BUCKET_SOURCE/ --task /data/tasks/BUCKET_SOURCE
uv run scripts/sync.py /data/tasks/BUCKET_SOURCE
```

Bucket 默认 Xet，拒绝 `--mode http`；不设 `--prefix` 才枚举整桶。源路径原样追加到 OSS 目标前缀，`--prefix data/` 不会剥掉路径中的 `data/`。使用 `list_bucket_tree` 递归分页；记录路径、大小和原生 Xet 哈希，manifest 的 `revision` 是清单摘要，**不是 Git revision 或原子快照**，Bucket 不接受 `--revision`。分页期间上游仍可变化，最终完成只针对所选清单；再次 `plan` 生成新清单并比较才能判断更新。

下载前 `get_bucket_paths_info` 核对当前哈希和大小，再将 `BucketFile` 交给 `download_bucket_files`；缺失/变化标记 `SourceChanged` 并 blocked，不自动接受替换内容。原生 `hf_xet.hash_files` 校验下载结果，Xet 哈希不能用 SHA-256 替代。

dataset 的 `plan` 固定解析后的 commit SHA，并发枚举顶层目录、完整分页；两种源的清单 API 请求均共享每秒 1 次节奏。API 错误会失败退出，不发布部分清单；按错误类别/冷却处理后重试，不以部分扫描替代全量。输出文件禁止覆盖。原始仓库链接里的 revision/subfolder 必须显式传 `--revision`、`--include`，不能擅自下载 main 或全库。

`--include`/`--exclude` 可重复使用，采用区分大小写的 fnmatch 模式；它们是用户确认的路径选择，不是语义去重。排除项完整记录在 manifest，默认全部保留。若选择新版，需要额外保存版本映射及覆盖证据；不能只看文件名。空选择、重复路径、路径穿越、未知摘要、统计不一致会拒绝。

`init` 只创建目录和配置，不开始网络传输。若用户要求先审查，到此停止。启动需要环境中三个凭据变量。保持一任务一进程，使用已有 Supervisor/systemd 管理本任务进程组，绝不重启共享服务；自动重启只适合意外退出，认证/冲突阻塞时必须暂停并报告。服务用户必须能访问受保护环境。

## 暂停、恢复与失败

通过本任务的服务管理器停止整个进程组，检查子进程退出，保留 cache、state.sqlite3、durable、manifest 和 settings。默认 TERM 中断是可恢复停止，不表示完成；不要单杀父进程留下子上传器。

恢复使用相同的 `sync.py TASK_DIRECTORY`，它会恢复中断阶段并复用已校验缓存和 OSS multipart checkpoint。HTTP 支持续传；Xet 未完成文件可能重新重组，不保证字节级续传。只调整性能参数，不改源类型/repo/revision/目标/清单。无损扩容优先；本版 worker 修改需本任务重启，先说明在途重试成本。SQLite 每 5 分钟做备份，停止/完成时亦备份。0.1 本工具的任务身份保持兼容，数据库增补可空的 `xet_hash` 字段；不接受没有 identity 的其他旧数据库，禁止用旧备份覆盖新进度。

401/403 不换凭据身份绕过权限；报告实际授权失败。目标冲突不覆盖。429 读取 Retry-After/RateLimit 冷却。每文件累计达到 max_attempts 后 blocked，保留失败项；排查解决后，经用户授权才能在任务停止时定点重置该文件状态与计数（先做一致性备份），不批量清空失败表。

## 状态与对账

```bash
uv run scripts/task.py status --task /data/tasks/DATASET --seconds 60
uv run scripts/task.py verify --task /data/tasks/DATASET
```

`status` 无远端写入；60 秒样本报告下载进度及 verified 完成增量。Xet 可能预分配整文件，因此只统计完成重组的字节，不把预分配文件大小当实际下载量；`download_metric` 会说明口径。状态需与服务进程及日志时间核对，旧 status.json 不是活跃证据。

`verify` 对照选定清单、数据库（含 Bucket 的 Xet 哈希）、整个 OSS 前缀的对象存在性与大小，报告多余对象但不删除。未全部 verified 或有缺失/不一致时退出码 2；权限/网络错误退出码 1。全量对账不重新哈希所有 OSS 对象。源 SHA/Git/Xet 及上传 CRC 校验发生在流水线内；已有匹配元数据的对象可复用。dataset 元数据不匹配时读取校验；Bucket 必须匹配 `xet:<hash>` 和源/服务端 CRC 元数据，否则报冲突，不覆盖、不下载整个旧对象来猜测来源。

新 revision 使用新清单/差异报告，由用户确定覆盖策略；现有目标的不同内容默认冲突。没有自动轮询上游、自动删除旧对象或自动监控功能。
