"""
遥测恢复演练台（FastAPI）

核心流程（真实物理恢复，非 SQL 重建）：
  1. 复制 pg_basebackup 物理基线备份到独立运行目录
  2. 写入恢复配置：restore_command 指向 WAL 归档、recovery_target_name 精确停在
     所选命名恢复点、recovery_target_action=promote
  3. 以独立 postgres 实例（容器内 127.0.0.1:5433）重放 WAL
  4. 三重证据确认“准确到达目标”后才开放结果：
       - 日志出现 recovery stopping at restore point "<所选恢复点>"
       - 日志出现 archive recovery complete
       - SQL 查询 pg_is_in_recovery() = false（已 promote）
     仅凭数据库可连接一律不算成功
  5. 归档缺段 / 未到达目标 -> FAILED：进入失败收尾（见下），残库不开放查询
  6. 全站同时只允许一份恢复（状态机 + 锁 + 物理端口/目录双重门禁）

回收安全模型（本版本修复重点）：
  * 任何副本目录的删除都必须先“独立确认所属 postgres 实例已经停止”
    （postmaster.pid 进程存活探测 + pg_ctl status + 恢复端口监听三路证据）。
  * 停库命令报错或超时 != 实例已停：命令失败后仍继续独立探测；命令报错但实例
    实际已停时，回收照常完成。
  * 实例仍存活或无法确认时：保留副本目录与恢复名额占用，状态置 RECLAIM_FAILED，
    结果查询一律拒绝，网页展示未回收原因与证据，可一键重试回收。
  * 状态持久化到 /recovery/state.json；API 重启后不跳过任何未清完的副本：
    遗留目录一律走同一套“停止确认 -> 删除”流程，无法确认则继续保留并占用。
  * 停止/回收在后台线程执行，等待停库期间状态页与所有只读接口照常可用，
    清理完成前不允许发起新恢复。
"""
import json
import os
import re
import shutil
import signal
import socket
import subprocess
import threading
import time
from contextlib import asynccontextmanager
from datetime import datetime
from pathlib import Path

import psycopg
from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import FileResponse
from psycopg.rows import dict_row
from pydantic import BaseModel

PG_BIN = os.environ.get("PG_BIN", "/usr/lib/postgresql/16/bin")
PRIMARY_DSN = os.environ.get(
    "PRIMARY_DSN",
    "host=primary port=5432 dbname=telemetry user=postgres password=postgres",
)
BASEBACKUP_DIR = Path(os.environ.get("BASEBACKUP_DIR", "/basebackup/base"))
MANIFEST_PATH = Path(os.environ.get("MANIFEST_PATH", "/basebackup/manifest.json"))
RUNS_DIR = Path(os.environ.get("RUNS_DIR", "/recovery"))
STATE_FILE = RUNS_DIR / "state.json"
FAULT_DIR = RUNS_DIR / "faults"
STOP_FAULT_FILE = Path(
    os.environ.get("STOP_FAULT_FILE", str(FAULT_DIR / "stop_mode"))
)
RESTORE_SCRIPT = os.environ.get("RESTORE_SCRIPT", "/opt/app/restore_command.sh")
PG_CTL_WRAPPER = os.environ.get("PG_CTL_WRAPPER", "/opt/app/pg_ctl_wrapper.sh")
RECOVERY_PORT = int(os.environ.get("RECOVERY_PORT", "5433"))
RECOVERY_DB = os.environ.get("RECOVERY_DB", "telemetry")
RECOVERY_USER = os.environ.get("RECOVERY_USER", "postgres")
RECOVERY_PASSWORD = os.environ.get("RECOVERY_PASSWORD", "postgres")
RECOVERY_TIMEOUT = int(os.environ.get("RECOVERY_TIMEOUT", "300"))
# 停库命令本身的硬超时（超时后杀掉命令，但实例是否真停仍以独立探测为准）
STOP_TIMEOUT = int(os.environ.get("STOP_TIMEOUT", "15"))
# pg_ctl stop -w 内部等待上限
STOP_WAIT_SECONDS = int(os.environ.get("STOP_WAIT_SECONDS", "10"))
# 停库命令返回后，独立探测确认的重试次数与间隔
STOP_CONFIRM_TRIES = int(os.environ.get("STOP_CONFIRM_TRIES", "6"))
STOP_CONFIRM_INTERVAL = float(os.environ.get("STOP_CONFIRM_INTERVAL", "2"))
STATIC_DIR = Path(__file__).resolve().parent / "static"

IDLE, PREPARING, RECOVERING, VERIFYING, READY, FAILED = (
    "IDLE", "PREPARING", "RECOVERING", "VERIFYING", "READY", "FAILED",
)
# 回收进行中 / 回收未完成（实例仍存活或状态无法确认，目录与名额保留，可重试）
RECLAIMING, RECLAIM_FAILED = "RECLAIMING", "RECLAIM_FAILED"
TERMINAL_RECLAIM_STATES = (READY, FAILED, RECLAIM_FAILED)
VALID_STOP_MODES = ("ok", "stop_error", "stop_timeout", "stop_error_dead")


def now_iso() -> str:
    return datetime.now().astimezone().isoformat(timespec="seconds")


class Manager:
    """全站单恢复状态机。"""

    def __init__(self):
        self.lock = threading.RLock()
        self.state = IDLE
        self.run = None

    def event(self, msg: str):
        print(f"[recovery] {msg}", flush=True)
        with self.lock:
            if self.run is not None:
                self.run["events"].append({"ts": now_iso(), "msg": msg})
                self.persist_locked()

    def persist_locked(self):
        """把当前状态机原子落盘，API 重启后可据此继续未完成的回收。"""
        try:
            RUNS_DIR.mkdir(parents=True, exist_ok=True)
            tmp = STATE_FILE.with_suffix(".json.tmp")
            tmp.write_text(
                json.dumps({"state": self.state, "run": self.run},
                           ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
            os.replace(tmp, STATE_FILE)
        except Exception as e:
            print(f"[recovery] 状态持久化失败: {e}", flush=True)


mgr = Manager()


def load_manifest() -> dict:
    if not MANIFEST_PATH.exists():
        raise HTTPException(503, "setup 尚未完成：未找到 /basebackup/manifest.json")
    try:
        return json.loads(MANIFEST_PATH.read_text(encoding="utf-8"))
    except Exception as e:
        raise HTTPException(500, f"清单解析失败: {e}")


def recovered_dsn(extra: str = "") -> str:
    return (
        f"host=127.0.0.1 port={RECOVERY_PORT} dbname={RECOVERY_DB} "
        f"user={RECOVERY_USER} password={RECOVERY_PASSWORD} connect_timeout=2 {extra}"
    )


def run_cmd(args, timeout=120) -> subprocess.CompletedProcess:
    return subprocess.run(args, capture_output=True, text=True, timeout=timeout)


def pg_ctl(args: list[str], timeout: int = 120) -> subprocess.CompletedProcess:
    """所有 pg_ctl 调用统一走包装脚本，以便可控演示停库报错/超时（其余子命令透传）。"""
    return run_cmd(
        ["gosu", "postgres", PG_CTL_WRAPPER, *args],
        timeout=timeout,
    )


def pg_ctl_stop(pgdata: Path) -> tuple[bool, str]:
    """
    执行停库命令。返回 (命令是否干净成功, 命令问题描述)。
    命令失败/超时不代表实例仍存活——调用方必须再做独立探测确认。
    """
    cmd = [
        PG_CTL_WRAPPER, "-D", str(pgdata),
        "stop", "-m", "immediate", "-w", "-t", str(STOP_WAIT_SECONDS),
    ]
    try:
        # 独立进程组，超时可整组杀掉（演示 stop_timeout 时包装脚本正 sleep 挂起）
        proc = subprocess.Popen(
            ["gosu", "postgres", *cmd],
            stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            text=True, start_new_session=True,
        )
        try:
            out, err = proc.communicate(timeout=STOP_TIMEOUT)
        except subprocess.TimeoutExpired:
            try:
                os.killpg(proc.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            try:
                proc.communicate(timeout=10)
            except Exception:
                pass
            return False, f"停库命令超过 {STOP_TIMEOUT}s 未返回（超时，命令已被强杀）"
        if proc.returncode != 0:
            detail = (err or out or "").strip().splitlines()
            tail = detail[-1] if detail else f"退出码 {proc.returncode}"
            return False, f"停库命令报错（退出码 {proc.returncode}）：{tail}"
        return True, ""
    except Exception as e:
        return False, f"停库命令执行异常：{e}"


# ---------------------------------------------------------------------------
# 实例存活独立探测：postmaster.pid 进程 + pg_ctl status + 端口监听 三路证据
# ---------------------------------------------------------------------------
def _pid_is_postgres(pid: int) -> bool | None:
    """pid 是否属于一个存活的 postgres 进程。无法判断返回 None。"""
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True  # 进程存在但无权发信号，视为存活
    except OSError:
        return False
    try:
        cmdline = Path(f"/proc/{pid}/cmdline").read_bytes().replace(b"\x00", b" ")
        return b"postgres" in cmdline
    except OSError:
        return None


def _port_listening(port: int) -> bool:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.settimeout(1)
        return s.connect_ex(("127.0.0.1", port)) == 0


def probe_instance(pgdata: Path) -> tuple[str, dict]:
    """
    返回 (state, evidence)，state ∈ {"alive", "stopped", "unknown"}。
    安全原则：任何“正在运行”的正向证据都判 alive；无法确认时不轻易判 stopped。
    """
    ev: dict = {"pgdata": str(pgdata)}
    pidfile = pgdata / "postmaster.pid"
    pid_alive = None
    if pidfile.exists():
        try:
            pid = int(pidfile.read_text().splitlines()[0].strip())
            ev["postmaster_pid"] = pid
            pid_alive = _pid_is_postgres(pid)
            ev["pid_process_alive"] = pid_alive
        except Exception as e:
            ev["pid_process_alive"] = None
            ev["pid_read_error"] = str(e)
    else:
        ev["postmaster_pid"] = None
        ev["pid_process_alive"] = None

    try:
        st = pg_ctl(["-D", str(pgdata), "status"], timeout=15)
        ev["pg_ctl_status_rc"] = st.returncode
    except Exception as e:
        ev["pg_ctl_status_rc"] = None
        ev["pg_ctl_status_error"] = str(e)

    try:
        ev["port_listening"] = _port_listening(RECOVERY_PORT)
    except Exception as e:
        ev["port_listening"] = None
        ev["port_check_error"] = str(e)

    # 任一正向证据 -> 实例存活
    if pid_alive is True or ev.get("pg_ctl_status_rc") == 0 or ev["port_listening"] is True:
        return "alive", ev

    # 无 pid 文件、status 明确报“未运行”、端口无监听 -> 确认已停
    if not pidfile.exists() and ev.get("pg_ctl_status_rc") not in (0, None) \
            and not ev["port_listening"]:
        return "stopped", ev

    # 残留 pid 文件但对应进程已不在，且 status/端口都否认运行 -> 确认已停（脏 pid 文件）
    if pidfile.exists() and pid_alive is False \
            and ev.get("pg_ctl_status_rc") not in (0, None) \
            and not ev["port_listening"]:
        ev["note"] = "postmaster.pid 为残留文件，对应进程已不存在"
        return "stopped", ev

    return "unknown", ev


def stop_and_confirm(pgdata: Path) -> dict:
    """
    停库并独立确认。绝不只凭 pg_ctl 的返回码下结论：
      - 命令报错/超时后继续探测；实例确实已停则视为成功（满足“命令报错但实际已停可继续清理”）
      - 最终仍存活或无法确认 -> stopped=False，调用方必须保留目录与占用
    """
    if not pgdata.exists():
        return {"stopped": True, "reason": "", "note": "副本数据目录已不存在，视为已停止",
                "evidence": [], "command_issue": ""}

    state, ev = probe_instance(pgdata)
    if state == "stopped":
        return {"stopped": True, "reason": "", "note": "停库前独立探测已确认实例未在运行",
                "evidence": [ev], "command_issue": ""}

    ok, issue = pg_ctl_stop(pgdata)
    evidence = []
    for i in range(STOP_CONFIRM_TRIES):
        state, ev = probe_instance(pgdata)
        ev["probe_round"] = i + 1
        evidence.append(ev)
        if state == "stopped":
            note = "停库后独立探测确认实例已停止"
            if not ok:
                note += f"（停库命令虽报错/超时，但实例实际已停止：{issue}）"
            return {"stopped": True, "reason": "", "note": note,
                    "evidence": evidence, "command_issue": "" if ok else issue}
        time.sleep(STOP_CONFIRM_INTERVAL)

    state, ev = probe_instance(pgdata)
    ev["probe_round"] = "final"
    evidence.append(ev)
    if state == "alive":
        reason = "停库流程结束后独立探测仍确认实例存活，保留副本目录与恢复名额"
    else:
        reason = "无法确认实例是否已停止（证据不一致），按存活处理以策安全，保留副本目录与恢复名额"
    if issue:
        reason += f"；{issue}"
    return {"stopped": False, "reason": reason, "note": "",
            "evidence": evidence, "command_issue": issue}


# ---------------------------------------------------------------------------
# 回收 / 失败收尾：只有确认实例停止后才删除副本、释放恢复名额
# ---------------------------------------------------------------------------
def reclaim_run(run: dict):
    """完整回收：停止确认 -> 删除副本目录 -> 释放名额（IDLE）。供 API 与启动对账共用。"""
    rundir = Path(run["rundir"])
    pgdata = rundir / "pgdata"
    with mgr.lock:
        mgr.state = RECLAIMING
        mgr.run = run
        mgr.persist_locked()
    mgr.event(f"开始回收副本 {rundir.name}：先独立确认实例停止，确认前不会删除目录")

    if not rundir.exists():
        mgr.event("副本目录已不存在，直接释放恢复名额")
        with mgr.lock:
            mgr.state = IDLE
            mgr.run = None
            mgr.persist_locked()
        return

    res = stop_and_confirm(pgdata)
    if not res["stopped"]:
        mgr.event("回收未完成：" + res["reason"])
        with mgr.lock:
            run["reclaim_error"] = res["reason"]
            run["reclaim_evidence"] = res["evidence"][-3:]
            run["reclaim_command_issue"] = res["command_issue"]
            run["reclaimed_at"] = None
            mgr.state = RECLAIM_FAILED
            mgr.persist_locked()
        return
    if res["note"]:
        mgr.event(res["note"])

    try:
        shutil.rmtree(rundir)
    except Exception as e:
        mgr.event(f"副本目录删除失败，保留占用：{e}")
        with mgr.lock:
            run["reclaim_error"] = f"实例已确认停止，但副本目录删除失败：{e}"
            run["reclaim_evidence"] = res["evidence"][-3:]
            run["reclaimed_at"] = None
            mgr.state = RECLAIM_FAILED
            mgr.persist_locked()
        return

    mgr.event("副本已确认停止并删除，恢复名额已释放")
    with mgr.lock:
        run["cleaned"] = True
        run["reclaimed_at"] = now_iso()
        mgr.state = IDLE
        mgr.run = None
        mgr.persist_locked()


def finalize_failed(run: dict, msg: str, pgdata: Path):
    """
    恢复失败收尾：记录失败原因后执行停止确认。
      - 确认已停 -> 删除残库目录；保留 FAILED 状态展示原因，用户点“回收/重置”后释放名额
      - 仍存活/无法确认 -> RECLAIM_FAILED：目录与名额保留，拒绝查询，网页可重试回收
    """
    mgr.event("失败：" + msg)
    with mgr.lock:
        run["error"] = msg
        run["finished_at"] = now_iso()
        mgr.state = FAILED
        mgr.persist_locked()

    rundir = Path(run["rundir"])
    if not rundir.exists():
        run["cleaned"] = True
        with mgr.lock:
            mgr.persist_locked()
        return

    res = stop_and_confirm(pgdata)
    if not res["stopped"]:
        mgr.event("失败收尾未完成：" + res["reason"])
        with mgr.lock:
            run["reclaim_error"] = res["reason"]
            run["reclaim_evidence"] = res["evidence"][-3:]
            run["reclaim_command_issue"] = res["command_issue"]
            mgr.state = RECLAIM_FAILED
            mgr.persist_locked()
        return
    if res["note"]:
        mgr.event(res["note"])
    try:
        shutil.rmtree(rundir)
        run["cleaned"] = True
        mgr.event("失败残库已确认停止并删除；点击“回收/重置”后可再次发起恢复")
    except Exception as e:
        with mgr.lock:
            run["reclaim_error"] = f"实例已确认停止，但残库目录删除失败：{e}"
            run["reclaim_evidence"] = res["evidence"][-3:]
            mgr.state = RECLAIM_FAILED
            mgr.persist_locked()
        return
    with mgr.lock:
        mgr.persist_locked()


def do_recovery(run: dict):
    rundir = Path(run["rundir"])
    pgdata = rundir / "pgdata"
    logfile = rundir / "postgres.log"
    rp = run["restore_point"]
    try:
        # ---- 1. 复制物理基线备份 ----
        mgr.event(f"复制物理基线备份 {BASEBACKUP_DIR} -> {pgdata}")
        shutil.copytree(BASEBACKUP_DIR, pgdata, symlinks=True)

        # ---- 2. 写入恢复配置（WAL 归档重放 + 命名恢复点精确停止） ----
        missing = run.get("missing_segment") or ""
        restore_cmd = f"{RESTORE_SCRIPT} %f %p" + (f" {missing}" if missing else "")
        if missing:
            mgr.event(f"注入归档缺段故障：段 {missing} 将在 restore_command 中缺失")
        with open(pgdata / "postgresql.auto.conf", "a", encoding="utf-8") as f:
            f.write("\n# ---- recovery drill ----\n")
            f.write(f"port = {RECOVERY_PORT}\n")
            f.write("listen_addresses = '127.0.0.1'\n")
            f.write("unix_socket_directories = '/tmp'\n")
            f.write("archive_mode = off\n")
            f.write(f"restore_command = '{restore_cmd}'\n")
            f.write(f"recovery_target_name = '{rp}'\n")
            f.write("recovery_target_action = 'promote'\n")
            f.write("hot_standby = on\n")
            f.write("default_transaction_read_only = on\n")
        (pgdata / "recovery.signal").touch()
        mgr.event(f"恢复配置就绪：recovery_target_name='{rp}'，restore_command 指向 WAL 归档")

        run_cmd(["chown", "-R", "postgres:postgres", str(pgdata)])
        run_cmd(["chown", "postgres:postgres", str(rundir)])
        run_cmd(["chmod", "700", str(pgdata)])

        # ---- 3. 启动恢复实例，开始重放 WAL ----
        mgr.event("启动恢复实例，开始从 WAL 归档重放")
        with mgr.lock:
            mgr.state = RECOVERING
            mgr.persist_locked()
        r = pg_ctl(
            ["-D", str(pgdata), "-l", str(logfile), "-w", "-t", "120", "start"],
            timeout=150,
        )
        if r.returncode != 0:
            finalize_failed(run,
                            f"恢复实例未能进入服务状态（pg_ctl 退出码 {r.returncode}）。"
                            "通常为归档缺段或未到达目标恢复点，详见日志。",
                            pgdata)
            return
        poll_until_ready(run, pgdata, logfile, rp)
    except Exception as e:
        finalize_failed(run, f"恢复过程异常：{e}", pgdata)


# 轮询连接在恢复早期会被拒，这类连接级 FATAL 是良性的，不能据此判失败
BENIGN_FATALS = (
    "the database system is not yet accepting connections",
    "the database system is starting up",
    "the database system is in recovery mode",
)


def poll_until_ready(run: dict, pgdata: Path, logfile: Path, rp: str):
    deadline = time.time() + RECOVERY_TIMEOUT
    target_re = re.compile(r'recovery stopping at restore point "%s"' % re.escape(rp))
    while time.time() < deadline:
        log = tail_file(logfile, n=400, max_bytes=524288)
        if "recovery ended before configured recovery target was reached" in log:
            finalize_failed(run, "未到达目标恢复点：WAL 重放提前结束（归档缺段或目标不在归档中）。", pgdata)
            return
        # 非良性 FATAL 需进程确认退出后才判失败（避免连接期误报）
        fatals = [f for f in re.findall(r"FATAL:\s+(.+)", log)
                  if not any(b in f for b in BENIGN_FATALS)]
        if fatals:
            time.sleep(2)
            st = pg_ctl(["-D", str(pgdata), "status"])
            if st.returncode != 0:
                finalize_failed(run, f"恢复实例报错：{fatals[-1].strip()}", pgdata)
                return
        # 热备模式下可在重放期间只读连接，用于展示进度；可连接绝不等于成功
        try:
            with psycopg.connect(recovered_dsn(), autocommit=True) as conn:
                in_rec, lsn, ts = conn.execute(
                    "SELECT pg_is_in_recovery(), pg_last_wal_replay_lsn(), "
                    "pg_last_xact_replay_timestamp()"
                ).fetchone()
            run["replay_lsn"] = str(lsn) if lsn is not None else None
            run["replay_ts"] = str(ts) if ts is not None else None
            if not in_rec:
                # 已 promote —— 必须再核对日志中的目标落点证据
                with mgr.lock:
                    mgr.state = VERIFYING
                    mgr.persist_locked()
                mgr.event("实例已提升（promote），核对目标恢复点落点证据…")
                log = tail_file(logfile, n=400, max_bytes=524288)
                if target_re.search(log) and "archive recovery complete" in log:
                    gather_results(run)
                    with mgr.lock:
                        mgr.state = READY
                        run["finished_at"] = now_iso()
                        mgr.persist_locked()
                    mgr.event(f"已确认到达目标恢复点 {rp}，结果开放查询")
                else:
                    finalize_failed(run, "实例已提升但日志缺少目标恢复点落点记录，拒绝开放结果。", pgdata)
                return
        except psycopg.OperationalError:
            pass
        except Exception as e:
            print(f"[recovery] poll error: {e}", flush=True)
        # 进程异常退出且未就绪 -> 失败
        st = pg_ctl(["-D", str(pgdata), "status"])
        if st.returncode != 0:
            log = tail_file(logfile, n=400, max_bytes=524288)
            if "database system is ready to accept connections" not in log:
                finalize_failed(run, "恢复实例进程退出且未报告就绪。", pgdata)
                return
        time.sleep(1.5)
    finalize_failed(run, f"超过 {RECOVERY_TIMEOUT}s 仍未到达目标恢复点。", pgdata)


def gather_results(run: dict):
    manifest = load_manifest()
    rpinfo = next(r for r in manifest["restore_points"] if r["name"] == run["restore_point"])
    ds = manifest["deleted_set"]
    with psycopg.connect(recovered_dsn(), autocommit=True) as conn:
        total = conn.execute("SELECT count(*) FROM telemetry_events").fetchone()[0]
        ds_rows = conn.execute(
            "SELECT count(*) FROM telemetry_events WHERE device_id=%s AND metric=%s",
            (ds["device_id"], ds["metric"]),
        ).fetchone()[0]
        mn, mx = conn.execute(
            "SELECT min(recorded_at), max(recorded_at) FROM telemetry_events"
        ).fetchone()
        # 与清单预期值在数据库内做精确比较，避免时区字符串格式差异造成误判
        mx_match = conn.execute(
            "SELECT max(recorded_at) IS NOT DISTINCT FROM %s::timestamptz "
            "FROM telemetry_events",
            (rpinfo["max_recorded_at"],),
        ).fetchone()[0]
        matrix = conn.execute(
            "SELECT device_id, metric, count(*) FROM telemetry_events "
            "GROUP BY 1,2 ORDER BY 1,2"
        ).fetchall()
    checks = [
        {"label": "遥测总行数", "actual": total,
         "expected": rpinfo["expected_total"], "ok": total == rpinfo["expected_total"]},
        {"label": f"误删集行数（{ds['device_id']} / {ds['metric']}）", "actual": ds_rows,
         "expected": rpinfo["deleted_set_rows"], "ok": ds_rows == rpinfo["deleted_set_rows"]},
        {"label": "最新 recorded_at", "actual": str(mx),
         "expected": rpinfo["max_recorded_at"], "ok": bool(mx_match)},
    ]
    run["result"] = {
        "restore_point": rpinfo["name"],
        "total": total,
        "deleted_set_rows": ds_rows,
        "min_recorded_at": str(mn),
        "max_recorded_at": str(mx),
        "matrix": [{"device_id": d, "metric": m, "count": c} for d, m, c in matrix],
        "expected": rpinfo,
        "checks": checks,
    }


def tail_file(path: Path, n=80, max_bytes=65536) -> str:
    try:
        with open(path, "rb") as f:
            f.seek(0, 2)
            size = f.tell()
            f.seek(max(0, size - max_bytes))
            data = f.read().decode("utf-8", "replace")
        return "\n".join(data.splitlines()[-n:])
    except Exception:
        return ""


# ---------------------------------------------------------------------------
# 启动对账：API 重启后绝不跳过未清完的副本，全部走“停止确认 -> 删除”同一套流程
# ---------------------------------------------------------------------------
def _load_saved_state():
    if not STATE_FILE.exists():
        return
    try:
        payload = json.loads(STATE_FILE.read_text(encoding="utf-8"))
        if isinstance(payload, dict) and payload.get("state"):
            mgr.state = payload["state"]
            mgr.run = payload.get("run")
            print(f"[recovery] 载入持久化状态：{mgr.state}", flush=True)
    except Exception as e:
        print(f"[recovery] 持久化状态读取失败，按空状态启动: {e}", flush=True)


def _synth_orphan_run(d: Path) -> dict:
    return {
        "id": d.name,
        "restore_point": "(遗留副本)",
        "inject_fault": False,
        "missing_segment": None,
        "started_at": None,
        "finished_at": None,
        "rundir": str(d),
        "events": [{"ts": now_iso(),
                    "msg": f"API 重启时发现磁盘遗留副本 {d.name}，纳入停止确认回收流程"}],
        "replay_lsn": None,
        "replay_ts": None,
        "error": "API 重启后发现的遗留副本，未完成清理",
        "result": None,
        "cleaned": False,
    }


def startup_reconcile():
    """同步重建内存状态后，后台串行处理所有遗留副本（不阻塞状态页）。"""
    with mgr.lock:
        _load_saved_state()
        RUNS_DIR.mkdir(parents=True, exist_ok=True)
        FAULT_DIR.mkdir(parents=True, exist_ok=True)
        run_dirs = sorted(p for p in RUNS_DIR.glob("run_*") if p.is_dir())
        saved_rundir = Path(mgr.run["rundir"]) if mgr.run and mgr.run.get("rundir") else None

        orphan_dirs = [d for d in run_dirs if d != saved_rundir]
        saved_on_disk = saved_rundir is not None and saved_rundir.exists()

        # 对已保存任务做非破坏性探测，刷新证据；决定哪些需要后台回收
        deferred_reclaim = False
        if mgr.run is not None and saved_on_disk:
            pgdata = saved_rundir / "pgdata"
            state, ev = probe_instance(pgdata)
            mgr.run["last_probe"] = ev
            if mgr.state in (PREPARING, RECOVERING, VERIFYING, RECLAIMING):
                mgr.run["error"] = (mgr.run.get("error") or "") + \
                    f" API 重启中断了 {mgr.state} 流程，需重新停止确认后回收。"
                deferred_reclaim = True
            elif mgr.state == READY:
                if state != "alive":
                    mgr.state = FAILED
                    mgr.run["error"] = "API 重启后探测恢复实例未存活，副本保留待回收"
                    mgr.persist_locked()
            # FAILED / RECLAIM_FAILED：保留现场与占用，等待用户在网页重试回收
        mgr.persist_locked()

    def worker():
        # 1) 先处理无主遗留目录：一个都不能跳过
        for d in orphan_dirs:
            mgr.event(f"处理遗留副本 {d.name}")
            reclaim_run(_synth_orphan_run(d))
        # 2) 再处理被重启打断的已保存任务
        if deferred_reclaim:
            with mgr.lock:
                run = mgr.run
            if run is not None:
                reclaim_run(run)

    threading.Thread(target=worker, daemon=True).start()


@asynccontextmanager
async def lifespan(app: FastAPI):
    startup_reconcile()
    yield


app = FastAPI(title="遥测恢复演练台", lifespan=lifespan,
              docs_url=None, redoc_url=None, openapi_url=None)


class RecoverRequest(BaseModel):
    restore_point: str
    inject_fault: bool = False


class FaultRequest(BaseModel):
    stop_mode: str


@app.get("/", include_in_schema=False)
def index():
    return FileResponse(STATIC_DIR / "index.html")


@app.get("/api/health")
def api_health():
    return {"ok": True}


@app.get("/api/manifest")
def api_manifest():
    return load_manifest()


@app.get("/api/primary/stats")
def api_primary_stats():
    """原库实时状态：证明演练期间原库持续入仓、不受影响。"""
    try:
        with psycopg.connect(PRIMARY_DSN, autocommit=True) as conn:
            total, latest = conn.execute(
                "SELECT count(*), max(recorded_at) FROM telemetry_events"
            ).fetchone()
            live = conn.execute(
                "SELECT count(*) FROM telemetry_events WHERE device_id='WX-LIVE'"
            ).fetchone()[0]
            arch = conn.execute(
                "SELECT last_archived_wal, last_archived_time, archived_count, failed_count "
                "FROM pg_stat_archiver"
            ).fetchone()
        return {
            "reachable": True,
            "total": total,
            "live_rows": live,
            "latest_recorded_at": str(latest),
            "archiver": {
                "last_archived_wal": arch[0],
                "last_archived_time": str(arch[1]),
                "archived_count": arch[2],
                "failed_count": arch[3],
            },
        }
    except Exception as e:
        return {"reachable": False, "error": str(e)}


def _read_stop_mode() -> str:
    try:
        mode = STOP_FAULT_FILE.read_text(encoding="utf-8").strip()
        return mode if mode in VALID_STOP_MODES else "ok"
    except OSError:
        return "ok"


@app.get("/api/fault")
def api_get_fault():
    return {"stop_mode": _read_stop_mode(), "valid_modes": VALID_STOP_MODES}


@app.post("/api/fault")
def api_set_fault(req: FaultRequest):
    if req.stop_mode not in VALID_STOP_MODES:
        raise HTTPException(400, f"未知停库故障模式: {req.stop_mode}")
    FAULT_DIR.mkdir(parents=True, exist_ok=True)
    STOP_FAULT_FILE.write_text(req.stop_mode, encoding="utf-8")
    os.chmod(STOP_FAULT_FILE, 0o644)
    mgr.event(f"停库故障模式切换为：{req.stop_mode}")
    return {"ok": True, "stop_mode": req.stop_mode}


@app.post("/api/recover")
def api_recover(req: RecoverRequest):
    manifest = load_manifest()
    rp = next((r for r in manifest["restore_points"] if r["name"] == req.restore_point), None)
    if rp is None:
        raise HTTPException(400, f"未知恢复点: {req.restore_point}")
    if not BASEBACKUP_DIR.exists():
        raise HTTPException(500, "物理基线备份不存在，无法恢复")
    with mgr.lock:
        if mgr.state != IDLE:
            hint = ""
            if mgr.state == RECLAIM_FAILED:
                hint = "存在未回收副本（实例仍存活或无法确认），请先在网页重试回收。"
            elif mgr.state == RECLAIMING:
                hint = "上一副本正在停库回收中，清理完成前不能发起新恢复。"
            elif mgr.state == FAILED:
                hint = "请先回收失败残库。"
            else:
                hint = "请先回收当前恢复库。"
            raise HTTPException(409, f"当前状态为 {mgr.state}：全站同时只允许一份恢复。{hint}")
        # 物理门禁：磁盘上不得残留副本目录，恢复端口不得被占用
        leftovers = [p.name for p in RUNS_DIR.glob("run_*") if p.is_dir()]
        if leftovers:
            raise HTTPException(
                409, f"磁盘存在未清理副本目录 {leftovers}，拒绝接纳新恢复，请先完成回收。"
            )
        if _port_listening(RECOVERY_PORT):
            raise HTTPException(
                409, f"恢复端口 {RECOVERY_PORT} 仍被存活实例占用，拒绝接纳新恢复。"
            )
        run_id = time.strftime("run_%Y%m%d_%H%M%S_") + req.restore_point
        rundir = RUNS_DIR / run_id
        rundir.mkdir(parents=True, exist_ok=False)
        mgr.run = {
            "id": run_id,
            "restore_point": req.restore_point,
            "inject_fault": req.inject_fault,
            "missing_segment": manifest.get("first_wal_after_backup") if req.inject_fault else None,
            "started_at": now_iso(),
            "finished_at": None,
            "rundir": str(rundir),
            "events": [],
            "replay_lsn": None,
            "replay_ts": None,
            "error": None,
            "result": None,
            "cleaned": False,
            "reclaim_error": None,
            "reclaim_evidence": None,
            "reclaim_command_issue": None,
        }
        mgr.state = PREPARING
        mgr.event(
            f"创建恢复任务 {run_id}，目标恢复点 {req.restore_point}"
            + ("（注入归档缺段故障）" if req.inject_fault else "")
        )
    threading.Thread(target=do_recovery, args=(mgr.run,), daemon=True).start()
    return {"ok": True, "run_id": run_id}


@app.get("/api/status")
def api_status():
    with mgr.lock:
        state = mgr.state
        run = dict(mgr.run) if mgr.run else None
    log_tail = ""
    if run:
        log_tail = tail_file(Path(run["rundir"]) / "postgres.log", 80)
    leftovers = [p.name for p in RUNS_DIR.glob("run_*") if p.is_dir()]
    return {
        "state": state,
        "run": run,
        "log_tail": log_tail,
        "stop_fault_mode": _read_stop_mode(),
        "leftover_dirs": leftovers,
        "recovery_port_in_use": _port_listening(RECOVERY_PORT),
    }


@app.post("/api/reclaim")
def api_reclaim():
    """
    回收（手动重试入口）：后台执行“停止确认 -> 删除副本 -> 释放名额”。
    立即返回，进度经 /api/status 展示；等待停库期间状态页照常可用。
    """
    with mgr.lock:
        if mgr.state == RECLAIMING:
            raise HTTPException(409, "回收正在进行中，请等待停库确认完成")
        if mgr.state not in TERMINAL_RECLAIM_STATES or mgr.run is None:
            raise HTTPException(409, f"当前状态 {mgr.state} 无需回收")
        run = mgr.run
    threading.Thread(target=reclaim_run, args=(dict(run),), daemon=True).start()
    return {"ok": True, "async": True, "msg": "回收已在后台开始，可在状态页观察停止确认进度"}


def require_ready() -> dict:
    """结果开放门禁：仅 READY 放行；FAILED / RECLAIM_FAILED 等一律拒绝（残库/未回收不可查）。"""
    with mgr.lock:
        if mgr.state != READY or mgr.run is None or mgr.run.get("result") is None:
            if mgr.state == RECLAIM_FAILED:
                raise HTTPException(
                    409, "副本尚未回收（实例仍存活或无法确认停止），结果查询已拒绝；请先重试回收。"
                )
            raise HTTPException(409, "恢复库未确认到达目标恢复点，结果不开放查询")
        return mgr.run


@app.get("/api/recovered/summary")
def api_recovered_summary():
    run = require_ready()
    return run["result"]


@app.get("/api/recovered/rows")
def api_recovered_rows(
    device_id: str | None = None,
    metric: str | None = None,
    limit: int = Query(50, ge=1, le=500),
):
    require_ready()
    for v in (device_id, metric):
        if v is not None and not re.fullmatch(r"[A-Za-z0-9_\-]{1,40}", v):
            raise HTTPException(400, "非法过滤参数")
    sql = ("SELECT id, device_id, site, metric, value, recorded_at, ingested_at "
           "FROM telemetry_events")
    conds, params = [], []
    if device_id:
        conds.append("device_id = %s")
        params.append(device_id)
    if metric:
        conds.append("metric = %s")
        params.append(metric)
    if conds:
        sql += " WHERE " + " AND ".join(conds)
    sql += " ORDER BY recorded_at DESC, id DESC LIMIT %s"
    params.append(limit)
    try:
        with psycopg.connect(
            recovered_dsn("options='-c default_transaction_read_only=on'"),
            autocommit=True,
            row_factory=dict_row,
        ) as conn:
            rows = conn.execute(sql, params).fetchall()
    except psycopg.OperationalError as e:
        raise HTTPException(503, f"恢复库暂不可达: {e}")
    for r in rows:
        r["recorded_at"] = str(r["recorded_at"])
        r["ingested_at"] = str(r["ingested_at"])
    return {"rows": rows}
