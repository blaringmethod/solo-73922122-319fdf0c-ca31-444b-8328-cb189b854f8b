#!/bin/bash
# =============================================================================
# pg_ctl 包装脚本：所有 pg_ctl 调用统一经此入口。
#   - 非 stop 子命令：原样透传给真实 pg_ctl（start/status/... 行为完全不变）
#   - stop 子命令：读取 /recovery/faults/stop_mode 故障模式，可控演示
#       ok               正常停库（默认）
#       stop_error       命令直接报错且实例保持运行（模拟“停库报错但旧实例还活着”）
#       stop_timeout     命令挂起直到被上层超时杀掉，实例保持运行（模拟停库超时）
#       stop_error_dead  命令报错，但实例此前已自行退出（模拟“命令报错但实际已停”，
#                        应用凭独立探测确认已停后仍应能继续清理）
# 注意：真实是否停止由应用侧三路独立探测（postmaster.pid 进程 / pg_ctl status /
#       端口监听）裁决，本脚本只控制“命令表现”，不改变裁决依据。
# =============================================================================
set -u

REAL_PGCTL="/usr/lib/postgresql/16/bin/pg_ctl"
MODE_FILE="${STOP_FAULT_FILE:-/recovery/faults/stop_mode}"
mode="ok"
[ -r "$MODE_FILE" ] && mode="$(cat "$MODE_FILE" 2>/dev/null || echo ok)"

# 兼容两种参数顺序：pg_ctl [opts] stop ...  /  pg_ctl stop [opts] ...
sub=""
for a in "$@"; do
  case "$a" in
    start|stop|restart|reload|status|promote|logrotate|kill) sub="$a"; break;;
  esac
done

if [ "$sub" != "stop" ]; then
  exec "$REAL_PGCTL" "$@"
fi

case "$mode" in
  stop_error)
    echo "pg_ctl_wrapper: [注入] 停库命令报错（模拟），实例保持运行" >&2
    exit 1
    ;;
  stop_timeout)
    echo "pg_ctl_wrapper: [注入] 停库命令挂起（模拟超时），实例保持运行" >&2
    # 前台长睡眠，等待上层 STOP_TIMEOUT 后整组 SIGKILL；实例完全不受影响
    exec sleep 300
    ;;
  stop_error_dead)
    # 真实执行一次 immediate stop（实例真的会停），然后返回非零，
    # 制造“命令报错但实例实际已停”的场景
    "$REAL_PGCTL" "$@"
    rc=$?
    echo "pg_ctl_wrapper: [注入] 停库命令报错（模拟），但实例实际已停止 (真实 rc=$rc)" >&2
    exit 1
    ;;
  ok|"")
    exec "$REAL_PGCTL" "$@"
    ;;
  *)
    echo "pg_ctl_wrapper: 未知故障模式 $mode，按正常停库处理" >&2
    exec "$REAL_PGCTL" "$@"
    ;;
esac
