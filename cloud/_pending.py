"""跨档待办台账（pending ledger）：让「上档没做完的帖」能在下一档被继续处理。

## 为什么需要
分片因时间预算截断时，会把剩余帖子 id 写进 `work/unfinished/unfinished_<源>_<片>.json`。
这个文件只随当前 run 的 artifact 到 merge，**run 一结束就没了**。

后果（2026-11-26 实测确认）：
  · merge 只能补抓「列表行」→ 无正文、无图片、无 OCR → 空壳帖
  · 下一档 plan 完全不知道这些帖没做完
  · → 帖子永久残缺，用户要求的「依次类推到第 5 档」无法闭环

## 本模块职责
把 unfinished 提升为**跨档持久台账**：
  · merge 结束时：把「本档未完成」写成 state/pending_<源>.json
  · 下一档 plan 开跑时：读回来，并入 union_ids 强制处理
  · 处理完：从台账里删掉

## 文件位置
`<state_dir>/pending_<源>.json`，随 state 目录一起进数据仓（publish 已会推 state/）。

## 结构
{
  "source": "tuku",
  "period": 330,
  "updatedAt": "2026-11-26T14:00:00+08:00",
  "items": {
    "43047443": {"reason": "capacity", "tries": 1, "firstSeen": "2026-11-26T13:00:00+08:00"},
    "43047444": {"reason": "sigterm",  "tries": 2, "firstSeen": "2026-11-26T12:00:00+08:00",
                 "stuck": true}
  }
}

## 清理规则
· 换期（period 不同）→ 整个台账重置（新期是新帖，旧期待办无意义）
· 处理成功（出现在产物且缓存完整）→ 从 items 里删掉
· 连续 3 档都做不完 → 保留但打 `stuck: true`，merge 日志如实报告
  （不静默丢，也不无限重试）
"""
from __future__ import annotations

import json
import time
from pathlib import Path

#: 连续多少档做不完就标记 stuck（如实报告，不再当作「进行中」）
STUCK_TRIES = 3

#: 单源台账最多记多少条（防失控增长；超出按「最久未处理」丢弃最老的）
MAX_ITEMS = 5000


def _beijing_now() -> str:
    """北京时间的 ISO 字符串（与项目其它地方口径一致）。"""
    from datetime import datetime, timedelta, timezone

    return datetime.now(timezone(timedelta(hours=8))).strftime("%Y-%m-%dT%H:%M:%S+08:00")


def pending_path(state_dir: Path | str, source: str) -> Path:
    return Path(state_dir) / f"pending_{source}.json"


def load_pending(state_dir: Path | str, source: str, period: int = 0) -> dict:
    """读台账。返回 {"period": int, "items": {aid(str): {...}}}。

    · 文件不存在 / 损坏 → 返回空台账（不能让台账把主流程搞挂）
    · 期号不一致 → 返回空台账（换期即作废）
    """
    p = pending_path(state_dir, source)
    try:
        data = json.loads(p.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return {"period": int(period or 0), "items": {}}
    if not isinstance(data, dict):
        return {"period": int(period or 0), "items": {}}

    got_period = int(data.get("period") or 0)
    want_period = int(period or 0)
    if want_period and got_period and got_period != want_period:
        # 换期了 → 旧台账整体作废
        return {"period": want_period, "items": {}}

    items = data.get("items")
    if not isinstance(items, dict):
        items = {}
    # 只保留合法条目（aid 能转 int、值是 dict）
    clean: dict[str, dict] = {}
    for k, v in items.items():
        try:
            int(k)
        except (TypeError, ValueError):
            continue
        clean[str(k)] = v if isinstance(v, dict) else {}
    return {"period": want_period or got_period, "items": clean}


def save_pending(
    state_dir: Path | str,
    source: str,
    period: int,
    items: dict,
) -> dict:
    """写台账（原子替换）。返回写进去的台账 dict。"""
    p = pending_path(state_dir, source)
    p.parent.mkdir(parents=True, exist_ok=True)

    # 限制条目数：按 firstSeen 最旧的先丢（保新弃旧，避免台账无限增长）
    if len(items) > MAX_ITEMS:
        def _key(kv):
            return str((kv[1] or {}).get("firstSeen") or "")

        kept = sorted(items.items(), key=_key)[-MAX_ITEMS:]
        items = dict(kept)

    payload = {
        "source": source,
        "period": int(period or 0),
        "updatedAt": _beijing_now(),
        "count": len(items),
        "stuckCount": sum(1 for v in items.values() if (v or {}).get("stuck")),
        "items": items,
    }
    tmp = p.with_suffix(".tmp")
    tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(p)
    return payload


def merge_pending(
    state_dir: Path | str,
    source: str,
    period: int,
    newly_failed: dict[int, str] | None = None,
    done_ids: set[int] | None = None,
) -> dict:
    """把新一档的失败/完成情况并进台账。返回更新后的台账。

    参数
    ----
    newly_failed : {aid: reason}  本档确认没做完的（来自 unfinished 收集）
    done_ids     : 本档已经完整处理完的 id（从台账里删掉）

    逻辑
    ----
      ① 先读旧台账（同期）
      ② done_ids 里的从 items 删除（做完了）
      ③ newly_failed 里的：
           · 已在台账 → tries += 1，刷新 reason；tries >= STUCK_TRIES → stuck=True
           · 不在台账 → 新增，tries=1
      ④ 写回
    """
    old = load_pending(state_dir, source, period)
    items: dict[str, dict] = dict(old.get("items") or {})

    for aid in done_ids or set():
        items.pop(str(int(aid)), None)

    now = _beijing_now()
    for aid, reason in (newly_failed or {}).items():
        k = str(int(aid))
        prev = items.get(k) or {}
        tries = int(prev.get("tries") or 0) + 1
        items[k] = {
            "reason": str(reason or "unknown"),
            "tries": tries,
            "firstSeen": str(prev.get("firstSeen") or now),
            "lastSeen": now,
        }
        if tries >= STUCK_TRIES:
            items[k]["stuck"] = True

    return save_pending(state_dir, source, period, items)


def pending_ids(state_dir: Path | str, source: str, period: int) -> list[int]:
    """给 plan 用：拿本期待补的 id 列表（不含已 stuck 的）。"""
    led = load_pending(state_dir, source, period)
    out: list[int] = []
    for k, v in (led.get("items") or {}).items():
        if (v or {}).get("stuck"):
            continue
        try:
            out.append(int(k))
        except (TypeError, ValueError):
            continue
    return sorted(out)


def stuck_ids(state_dir: Path | str, source: str, period: int) -> list[int]:
    """已连续 STUCK_TRIES 档做不完的 id（如实上报，不再当进行中）。"""
    led = load_pending(state_dir, source, period)
    out: list[int] = []
    for k, v in (led.get("items") or {}).items():
        if (v or {}).get("stuck"):
            try:
                out.append(int(k))
            except (TypeError, ValueError):
                continue
    return sorted(out)


def _ts() -> str:
    return time.strftime("%H:%M:%S")
