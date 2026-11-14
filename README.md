# 遥测恢复演练台（FastAPI + PostgreSQL 16）

数仓工程师误删遥测记录后的恢复演练环境：在**原库持续入仓不受影响**的前提下，
从**物理基线备份 + WAL 归档**真实重放，精确停在所选**命名恢复点**，拉起一份
**独立核查库**，用于核对误删前数据。

- 恢复 = 复制 `pg_basebackup` 物理基线 + `restore_command` 从 WAL 归档重放 +
  `recovery_target_name` 精确停止 + `recovery_target_action=promote`，**不是 SQL 重建样例**。
- 只有三重证据齐备才开放结果（仅凭数据库可连接不算成功）：
  1. 日志出现 `recovery stopping at restore point "<所选恢复点>"`
  2. 日志出现 `archive recovery complete`
  3. `pg_is_in_recovery() = false`（已 promote）
- 全站同时只允许一份恢复；归档缺段 / 未到达目标 → 失败，进入失败收尾。
  失败残库不开放查询，必须回收后才允许重试。

## 回收安全模型（停库报错/超时不再“假成功”）

回收（手动回收、失败收尾、API 重启遗留清理）一律遵循同一套规则，修复了
“停库命令报错或超时后接口仍返回成功并接纳新恢复、旧实例却还活着”的问题：

1. **先独立确认实例停止，才允许删除副本、释放恢复名额。**
   停库结论由三路独立证据裁决，绝不只凭 `pg_ctl stop` 的返回码：
   - `postmaster.pid` 中 PID 对应的进程是否仍存活（`/proc/<pid>/cmdline` 确认是 postgres）
   - `pg_ctl status` 返回码
   - 恢复端口 5433 是否仍在监听
2. **停库命令报错/超时 ≠ 实例已停。** 命令失败后继续多轮独立探测：
   - 命令报错但实例实际已停 → 照常完成清理（`stop_error_dead` 演示）；
   - 实例仍存活或证据不一致无法确认 → **保留副本目录与恢复名额占用**，
     状态置 `RECLAIM_FAILED`，拒绝结果查询，网页显示未回收原因与三路探测证据，可一键重试。
3. **状态持久化**到 `/recovery/state.json`。API 重启后不跳过任何未清完的副本：
   磁盘上的 `run_*` 目录（含无主遗留目录）全部重新走“停止确认 → 删除”流程；
   无法确认停止则继续保留并占用。
4. **停库/回收在后台线程执行**，等待停库期间状态页与所有只读接口保持可用；
   清理完成前 `POST /api/recover` 一律 409（另有“无遗留目录 + 端口未占用”物理门禁）。

### 可控的停库故障演示（网页单选，或 `POST /api/fault`）

| stop_mode | 表现 | 预期结果 |
|---|---|---|
| `ok` | 正常停库 | 确认停止 → 删副本 → 释放名额 |
| `stop_error` | 停库命令直接报错，实例保持运行 | `RECLAIM_FAILED`：目录/名额保留、拒绝查询、可重试 |
| `stop_timeout` | 停库命令挂起，超过 `STOP_TIMEOUT` 被整组 SIGKILL，实例仍运行 | 同上（原因显示为超时） |
| `stop_error_dead` | 命令报错，但实例此前已真实停止 | 独立探测确认已停，回收照常完成 |

故障只作用于 `pg_ctl stop` 这一条命令（包装脚本 `app/pg_ctl_wrapper.sh`），
`start/status` 等子命令原样透传；实例是否真停始终由应用侧探测裁决。

## 架构

```
┌────────────┐   WAL 归档    ┌──────────────┐
│  primary   │ ───────────▶ │  wal_archive │（命名卷）
│ 遥测源(原库)│              └──────┬───────┘
│  :5432     │   pg_basebackup     │ restore_command
└─────▲──────┘ ───────────▶ ┌─────┴──────────────┐
      │ 持续写入             │  basebackup/base   │
┌─────┴──────┐              └─────┬──────────────┘
│  writer    │                    │ 复制副本 + recovery.signal
│ 心跳遥测    │              ┌─────▼──────────────┐
└────────────┘              │  app (FastAPI)     │
                            │  · 恢复编排/状态机  │
                            │  · 内嵌 PG16 恢复实例│
                            │    127.0.0.1:5433  │
                            │  · 浏览器 :8080     │
                            └────────────────────┘
```

## 快速开始

```bash
docker compose up --build -d
docker compose logs -f setup     # 等待出现 “[setup] 完成。”
# 浏览器打开 http://localhost:8080
```

可选：直接核查原库 `psql postgresql://postgres:postgres@localhost:5432/telemetry`

## 演练流程

1. **恢复到误删前**：选择 `rp_before_delete` → 启动恢复 → 观察 WAL 逐段重放 →
   READY 后核对：总行数 168、误删集（WX-03/temperature）7 行、
   最新 recorded_at `2026-11-12 11:00:00+08`。可逐行查询恢复库记录。
2. **回收**当前恢复库 → 选择 `rp_after_delete` → 核对：总行数 185、
   误删集 0 行、含误删后批次2（最新 recorded_at `2026-11-13 09:00:00+08`）。
   两个恢复点的差异即“误删事件”的影响范围。
3. **恢复失败演示**：勾选“注入恢复故障：归档缺段”→ 启动恢复 → 恢复在缺失段处停下，
   postgres 报 `recovery ended before configured recovery target was reached` →
   页面显示失败并执行失败收尾（先独立确认停库再删残库）；若实例仍存活则转入
   “回收未完成”，保留目录、拒绝查询，网页可重试回收。
4. **停库故障演示**：在“停库故障注入”面板选择 `stop_error` 或 `stop_timeout` →
   对一份 READY/FAILED 恢复点“回收” → 页面显示 `RECLAIM_FAILED`、未回收原因与三路
   探测证据，恢复名额仍被占用、新恢复被 409 拒绝、结果查询被拒；切回 `ok`（或选
   `stop_error_dead` 验证“报错但实际已停”）后点“重试回收”即可完成清理。
5. 全程观察左上角原库面板：行数持续增长，证明原库入仓不受影响；
   恢复点之后产生的 `WX-LIVE` 心跳不会出现在恢复库。

## 虚构遥测数据与两个恢复点的差异

| 批次 | 内容 | 行数 |
|---|---|---|
| 基线（备份前） | 6 设备 × 4 指标 × 5 时点 | 120 |
| 批次1（备份后、rp1 前） | 6 设备 × 4 指标 × 2 时点 | +48 |
| **误删**（rp1 后） | `DELETE WHERE device_id='WX-03' AND metric='temperature'` | −7 |
| 批次2（误删后、rp2 前） | 6 设备 × 4 指标 × 1 时点 | +24 |
| 心跳（rp2 后持续） | writer 每 2 秒 1 行 `WX-LIVE/heartbeat` | 持续增长 |

| 核对项 | rp_before_delete | rp_after_delete |
|---|---|---|
| 遥测总行数 | 168 | 185 |
| 误删集行数（WX-03/temperature） | 7 | 0 |
| 最新 recorded_at | 2026-11-12 11:00:00+08 | 2026-11-13 09:00:00+08 |

清单 `/basebackup/manifest.json` 由 setup 以实际 SQL 计数生成，页面“预期”列以此为准。

## 目录

```
docker-compose.yml         # 四服务编排：primary / setup / writer / app（含停库安全参数）
primary/initdb/00_init.sh  # 复制账号 + pg_hba
setup/setup.sh             # 造数、pg_basebackup、两个命名恢复点、归档完备性校验、清单
writer/writer.sh           # 持续入仓心跳
app/Dockerfile             # postgres:16 + python3 + fastapi
app/main.py                # 恢复编排状态机（单恢复锁、目标确认、三路停库探测、
                           #   失败收尾/手动回收/启动对账、状态持久化、查询门禁）
app/pg_ctl_wrapper.sh      # pg_ctl 包装：注入停库报错/超时（stop 之外的子命令透传）
app/restore_command.sh     # restore_command（支持注入“归档缺段”故障）
app/static/index.html      # 演练台页面（停库故障面板、未回收原因与探测证据、重试回收）
```

## HTTP 接口

| 方法/路径 | 说明 |
|---|---|
| `GET /api/status` | 状态机、事件、日志尾部、停库故障模式、磁盘遗留目录、端口占用 |
| `POST /api/recover` | 发起恢复；非 IDLE / 有遗留副本 / 端口被占用时 409 拒绝 |
| `POST /api/reclaim` | 后台回收（停止确认 → 删副本 → 释放名额），立即返回，进度看状态页 |
| `GET/POST /api/fault` | 读取/设置停库故障模式（ok / stop_error / stop_timeout / stop_error_dead） |
| `GET /api/recovered/summary`、`/api/recovered/rows` | 结果查询，仅 READY 放行，其余状态（含 RECLAIM_FAILED）409 |

## 重置

```bash
docker compose down -v && docker compose up --build -d
```

## 说明

- 恢复实例只监听 app 容器内 `127.0.0.1:5433`，不对外暴露；结果查询一律经过
  应用层 READY 门禁（失败残库不可查）。
- 演示环境使用弱口令（postgres/postgres、replicator/replicator），仅限本地演练。
- setup 幂等：已存在基线备份与清单时直接跳过；如需重新生成请先 `down -v`。
