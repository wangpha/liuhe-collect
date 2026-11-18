"""预算模型（A''）：命中率 → 有效秒/次 → 转移决策。

## 为什么需要这个模块（2026-11-19 实测演进）
1. 旧探针（合成图）低估 4.8 倍：合成图 OCR 后 rows=0，rec 阶段不跑
2. 改用 CPU 型号映射后，发现「纯 OCR 值」与 `[diag] 秒/次` 量纲不同（AMD 差 2.3 倍）
3. 试图统一乘 `END_TO_END_FACTOR=2.34` → 被 20 片实测推翻：
   AMD 需 ×2.3，但 Intel 6975P-C 需 ×0.6（真实图兜底测出的 1.56 反而是高估）
4. 最终口径：用 20 片实测的「端到端秒/次」中位做基准，再用**当场测得的缓存命中率**换算

## 核心公式
    hit_rate  = 当场测（数 .cache/ocr/*.json ÷ 本片计划图数）
    miss_cost = (E2E_HISTORY - HIT_RATE_HISTORY × HIT_COST) / (1 - HIT_RATE_HISTORY)
    effective = miss_cost × (1 - hit_rate) + HIT_COST × hit_rate

## HIT_RATE_HISTORY 是 PENDING 假设值（必读）
`HIT_RATE_HISTORY = 0.74` **不是独立实测**，它与 `miss_cost` 构成**循环依赖**：
    1.04（实测 E2E）+ 0.74（假设）→ 反推 miss_cost = 3.99
    3.99（反推值）  + 0.74（假设）→ 再算 effective 当然得回 1.04（数学恒成立）
所以它**无法自我证伪**。真实命中率要等分片上报 `ocrCacheHit/Miss`（任务 4）才能拿到。

**误差方向（安全侧）**：真实 hit_rate < 0.74 → miss_cost 被高估 → effective 高估
→ 容量低估 → **过度转移**（可能让 merge 变忙，但不会让分片超时）。
例：真实 0.50 时 miss_cost 应为 2.08，我们按 3.99 算 → 高估 1.92 倍。

**校准时机**：任务 4 上线后第一轮，用真实 `ocrHitRate` 重算；
误差 >30% → 回写 `RUNNER_CPU_E2E_SECONDS` 或 `HIT_RATE_HISTORY`；≤30% → 标注「已校准」。

## 冷启动为什么不需要额外处理
冷启动时 `hit_rate` 是**当场测的 0**（.cache/ocr 空）→ `effective = miss_cost × 1 = 3.99`，
公式本身正确。`HIT_RATE_HISTORY` 只影响 miss_cost 的反推值，不影响冷启动的 effective 计算。
冷启动真正的风险是**物理下限**（1097 图 × 3.99 = 73 分钟 > 容量），由 `decide_transfer` 的
`shortfall` 明确告警，不靠公式掩盖。
"""
from __future__ import annotations

import os
from pathlib import Path

#: 命中缓存一次的固定开销（实测）
HIT_COST: float = 0.005

#: **PENDING 假设值**（见模块 docstring 的循环依赖说明）：最近一轮实测的缓存命中率
HIT_RATE_HISTORY: float = 0.74

#: 容量利用率：留 10% 余量给收尾（打包/上传），避免撞上 job 硬超时
USABLE_RATIO: float = 0.9

#: GitHub runner 池 CPU → **端到端秒/次**（2026-11-19 run 44358492008 的 20 片实测中位）
RUNNER_CPU_E2E_SECONDS: dict[str, float] = {
    "AMD EPYC 7763": 1.040,       # 5 片：1.018/1.043/1.055/1.202/1.275
    "AMD EPYC 9V74": 0.960,       # 3 片：0.846/0.964/1.144
    "Intel Xeon 6975P-C": 0.950,  # 11 片：0.541-1.186（本轮不比 AMD 慢）
    "AMD EPYC 9V45": 1.000,       # 无本轮样本，按 AMD 均值
    "Intel Xeon 8370C": 1.200,    # 无本轮样本
}

#: 未知 CPU 的保守端到端值（慢机上限量级）
UNKNOWN_CPU_E2E_SECONDS: float = 1.500


def normalize_cpu_name(raw: str) -> str:
    """归一化 CPU 型号名，便于与表做子串匹配。

    实测坑（2026-11-19）：`/proc/cpuinfo` 给 `Intel(R) Xeon(R) 6975P-C`，
    表里写 `Intel Xeon 6975P-C` → 直接 `in` 匹配失败 → 11 片 Intel 白走兜底探针。
    """
    t = str(raw or "").replace("(R)", " ").replace("(TM)", " ").replace("(tm)", " ")
    t = t.replace("CPU ", " ").replace("Processor", " ")
    return " ".join(t.split())


def count_cached_ocr(cache_dir: Path | str) -> int:
    """数 `<cache_dir>/ocr/*.json` 的个数（`cache_dir` 就是 `.../.cache` 目录）。

    层2 缓存（`work/ocr-cache/ocr`）在「分片预热」之前已经 `cp -n` 铺回这里，
    所以分片启动时数到的就是可命中的上限。
    """
    ocr_dir = Path(cache_dir) / "ocr"
    if not ocr_dir.exists():
        return 0
    return sum(1 for p in ocr_dir.glob("*.json") if p.is_file())


def _resolve_e2e(cpu_model: str, e2e_table: dict[str, float] | None,
                 unknown_e2e: float | None) -> tuple[float, bool, str]:
    table = RUNNER_CPU_E2E_SECONDS if e2e_table is None else e2e_table
    unknown = UNKNOWN_CPU_E2E_SECONDS if unknown_e2e is None else unknown_e2e
    norm = normalize_cpu_name(cpu_model)
    for model, sec in table.items():
        if normalize_cpu_name(model) in norm:
            return float(sec), True, model
    return float(unknown), False, ""


def estimate_effective_seconds(
    cpu_model: str,
    cached_count: int,
    total_images: int,
    *,
    e2e_table: dict[str, float] | None = None,
    unknown_e2e: float | None = None,
    hit_rate_history: float | None = None,
    hit_cost: float | None = None,
) -> dict:
    """估算本片「有效秒/次」和命中率。

    ## miss_cost 的来源（必读，用户 2026-11-19 条件①）
        miss_cost = (RUNNER_CPU_E2E_SECONDS[cpu] - HIT_RATE_HISTORY × HIT_COST)
                    / (1 - HIT_RATE_HISTORY)

    ⚠️ `HIT_RATE_HISTORY = 0.74` 是 **PENDING 假设值，不是独立实测**：
       - 它和 miss_cost 是**循环依赖**（1.04 + 0.74 → 3.99，再验证 1.04 当然成立）
       - 真实 hit_rate 要等「分片上报 ocrCacheHit/Miss」（任务 4）才能拿到
       - 误差方向：真实 hit_rate < 0.74 → miss_cost 高估 → effective 高估
                   → 容量低估 → **过度转移（安全侧）**
       - 校准时机：任务 4 上线后第一轮，用真实 ocrHitRate 重算

    返回 dict：`effective` / `hit_rate` / `miss_cost` / `e2e_history` / `cpu_hit` / `cpu_matched`
    """
    hist = HIT_RATE_HISTORY if hit_rate_history is None else float(hit_rate_history)
    hcost = HIT_COST if hit_cost is None else float(hit_cost)

    total = max(0, int(total_images))
    cached = max(0, int(cached_count))
    hit_rate = min(1.0, cached / total) if total > 0 else 0.0

    e2e, cpu_hit, matched = _resolve_e2e(cpu_model, e2e_table, unknown_e2e)
    denom = max(1e-6, 1.0 - hist)
    miss_cost = max(0.01, (e2e - hist * hcost) / denom)

    if total <= 0:
        effective = 0.0
    else:
        effective = miss_cost * (1.0 - hit_rate) + hcost * hit_rate

    # 条件③：合理范围自检 —— cache_dir 指错（如指到 out_dir 而非 .cache）时立刻抓
    assert 0.0 <= hit_rate <= 1.0, f"hit_rate 越界: {hit_rate}"
    assert 0.01 <= miss_cost <= 30.0, f"miss_cost 越界: {miss_cost}"
    assert 0.001 <= effective <= 30.0 or effective == 0.0, f"effective 越界: {effective}"

    return {
        "effective": effective,
        "hit_rate": hit_rate,
        "miss_cost": miss_cost,
        "e2e_history": e2e,
        "cpu_hit": cpu_hit,
        "cpu_matched": matched,
    }


def decide_transfer(
    total_images: int,
    effective_seconds: float,
    *,
    shard_timeout_s: float = 1500.0,
    merge_timeout_s: float = 1500.0,
    usable_ratio: float | None = None,
) -> dict:
    """决定本片转移多少图给 merge（A'' 三段式）。

    ## 核心原则（用户 2026-11-19 裁决）
    ① 分片用满 `job_timeout × 0.9` 的量 —— 最大化本片产出，同时**永不被 kill**
       （旧逻辑用软预算 1080s，只发挥 72% 的 timeout → 分片少干 2.3 倍的活）
    ② merge 也用满 `merge_timeout × 0.9` —— 最大化补抓能力
    ③ 两部分加起来还不够 → **明确告警「物理下限」**，不掩盖
       （被 kill 时 `unfinished` 已写，merge 至少有数据可补；不转移才是「什么都没写就死」）

    返回 dict：`transfer` / `shortfall` / `shard_capacity` / `merge_capacity` / `reason`
    """
    ratio = USABLE_RATIO if usable_ratio is None else float(usable_ratio)
    eff = max(0.0, float(effective_seconds))
    total = max(0, int(total_images))

    if eff <= 0:
        # 无有效秒/次（如 total=0 或环境异常）→ 不做裁剪，全部自己处理
        return {"transfer": 0, "shortfall": 0, "shard_capacity": total,
                "merge_capacity": 0, "reason": "无有效秒/次，不做预算裁剪"}

    shard_capacity = int(shard_timeout_s * ratio / eff)
    if total <= shard_capacity:
        return {"transfer": 0, "shortfall": 0, "shard_capacity": shard_capacity,
                "merge_capacity": int(merge_timeout_s * ratio / eff),
                "reason": "容量足够，全部分片自己处理"}

    transferable = total - shard_capacity
    merge_capacity = int(merge_timeout_s * ratio / eff)
    transfer = min(transferable, merge_capacity)
    shortfall = transferable - transfer
    if shortfall == 0:
        reason = "正常转移（分片+merge 都能按时完成）"
    else:
        reason = (f"物理下限：{shortfall} 图无人处理"
                  f"（{shortfall * eff / 60:.1f} 分钟的工作量），整轮会失败")
    return {
        "transfer": transfer,
        "shortfall": shortfall,
        "shard_capacity": shard_capacity,
        "merge_capacity": merge_capacity,
        "reason": reason,
    }


def timeouts_from_env() -> tuple[float, float]:
    """从环境变量读分片/merge 的 job timeout（秒），默认 1500（25 分）。"""
    shard = float(os.environ.get("LIUHE_SHARD_JOB_TIMEOUT", "1500") or 1500)
    merge = float(os.environ.get("LIUHE_MERGE_JOB_TIMEOUT", "1500") or 1500)
    return shard, merge
