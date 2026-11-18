"""A'' 预算模型单测（任务 1：先让它全 RED）。

## 被测接口（cloud/budget.py，任务 2 实现）
    count_cached_ocr(cache_dir) -> int
    estimate_effective_seconds(cpu_model, cached_count, total_images, ...) -> dict
    decide_transfer(total_images, effective_seconds, ...) -> dict
    normalize_cpu_name(raw) -> str

## 用户裁决的三个条件（2026-11-19）
① miss_cost 的假设来源写进 docstring（循环依赖 + 误差方向 + 校准时机）
② 任务 4 上线后第一轮必须用真实 hit_rate 重算（误差 >30% 则回写表）
③ 加合理范围自检断言（hit_rate 0-1 / miss_cost 0.01-30 / effective 0.001-30）
"""
from __future__ import annotations

import sys
from pathlib import Path

import pytest

sys.path.insert(0, r"D:\qode工作区")

from cloud.budget import (  # noqa: E402
    HIT_COST,
    HIT_RATE_HISTORY,
    USABLE_RATIO,
    count_cached_ocr,
    decide_transfer,
    estimate_effective_seconds,
    normalize_cpu_name,
)


# ============================================================
# ① 命中率 → effective 的三种边界
# ============================================================
def test_hit_rate_zero_returns_miss_cost():
    """hit_rate=0（冷启动）→ effective 必须等于 miss_cost。"""
    r = estimate_effective_seconds("AMD EPYC 7763", 0, 1000)
    assert r["hit_rate"] == pytest.approx(0.0)
    assert r["effective"] == pytest.approx(r["miss_cost"], rel=1e-9)


def test_hit_rate_full_returns_hit_cost():
    """hit_rate=1（全命中）→ effective 必须等于 HIT_COST。"""
    r = estimate_effective_seconds("AMD EPYC 7763", 1000, 1000)
    assert r["hit_rate"] == pytest.approx(1.0)
    assert r["effective"] == pytest.approx(HIT_COST, rel=1e-6)


def test_hit_rate_half():
    """hit_rate=0.5 → effective = (miss_cost + HIT_COST) / 2。"""
    r = estimate_effective_seconds("AMD EPYC 7763", 500, 1000)
    assert r["hit_rate"] == pytest.approx(0.5)
    expect = (r["miss_cost"] + HIT_COST) / 2
    assert r["effective"] == pytest.approx(expect, rel=1e-9)


# ============================================================
# ② miss_cost 从 E2E 表反推
# ============================================================
def test_miss_cost_backcalc_from_e2e():
    """AMD 7763 的 E2E 表值 1.04 + 历史命中率 0.74 → miss_cost ≈ 3.99。

    反推式：miss = (e2e - hist×hit_cost) / (1 - hist)
            = (1.04 - 0.74×0.005) / 0.26 = (1.04 - 0.0037) / 0.26 = 3.9858
    """
    r = estimate_effective_seconds("AMD EPYC 7763", 0, 1000)
    assert r["e2e_history"] == pytest.approx(1.04)
    assert r["miss_cost"] == pytest.approx(3.9858, abs=0.02)


def test_unknown_cpu_uses_default():
    """未知 CPU → 用 UNKNOWN_CPU_E2E_SECONDS=1.5，且 cpu_hit=False。"""
    r = estimate_effective_seconds("Intel(R) Xeon(R) 9999Z", 0, 1000)
    assert r["cpu_hit"] is False
    assert r["e2e_history"] == pytest.approx(1.5)


def test_cpu_normalize_strips_R_TM():
    """Intel(R) Xeon(R) 6975P-C 必须能命中表里的 Intel Xeon 6975P-C（真 bug 回归）。"""
    assert "Xeon" in normalize_cpu_name("Intel(R) Xeon(R) 6975P-C")
    assert "(R)" not in normalize_cpu_name("Intel(R) Xeon(R) 6975P-C")
    r = estimate_effective_seconds("Intel(R) Xeon(R) 6975P-C", 0, 1000)
    assert r["cpu_hit"] is True
    assert r["e2e_history"] == pytest.approx(0.95)


# ============================================================
# ③ 转移决策（A''：容量用 job timeout × 0.9）
# ============================================================
def test_no_transfer_when_capacity_sufficient():
    """容量够 → transfer=0, shortfall=0。"""
    # 1500s × 0.9 / 0.4 = 3375 图容量 > 1000 图
    d = decide_transfer(1000, 0.4)
    assert d["transfer"] == 0
    assert d["shortfall"] == 0
    assert "自己处理" in d["reason"]


def test_normal_transfer_when_merge_can_absorb():
    """分片装不下、merge 能吸收 → shortfall=0 且 transfer==transferable。"""
    # effective=2.0：分片容量 675、merge 容量 675；总数 1200 → transferable 525 < 675
    d = decide_transfer(1200, 2.0)
    assert d["shard_capacity"] == int(1500 * USABLE_RATIO / 2.0)
    assert d["merge_capacity"] == int(1500 * USABLE_RATIO / 2.0)
    assert d["shortfall"] == 0
    assert d["transfer"] == 1200 - d["shard_capacity"]


def test_shortfall_when_both_capacities_exceeded():
    """分片+merge 都装不下 → transfer==merge_capacity 且 shortfall>0。"""
    # effective=4.0：容量各 337；总数 2000 → transferable 1663 > 337
    d = decide_transfer(2000, 4.0)
    assert d["transfer"] == d["merge_capacity"]
    assert d["shortfall"] == 2000 - d["shard_capacity"] - d["merge_capacity"]
    assert d["shortfall"] > 0
    assert "物理下限" in d["reason"]


def test_shard_uses_job_timeout_not_soft_budget():
    """容量必须用 job timeout × 0.9（1350s），不是旧的软预算 1080s。"""
    d = decide_transfer(10**6, 1.0)          # 必然转移
    assert d["shard_capacity"] == 1350       # 1500 × 0.9，不是 1080
    assert d["shard_capacity"] != 1080


# ============================================================
# ④ 条件 ③：合理范围自检断言（cache_dir 指错时立刻抓到）
# ============================================================
def test_assert_hit_rate_out_of_range():
    """cached > total 时 hit_rate 会被截到 1.0（不越界）。"""
    r = estimate_effective_seconds("AMD EPYC 7763", 9999, 1000)
    assert 0.0 <= r["hit_rate"] <= 1.0


def test_miss_cost_and_effective_in_range():
    """所有已知 CPU 的 miss_cost / effective 都必须在合理区间内。"""
    for cpu in ("AMD EPYC 7763", "AMD EPYC 9V74", "Intel Xeon 6975P-C",
                "AMD EPYC 9V45", "Intel Xeon 8370C", "未知型号 X"):
        for cached in (0, 500, 1000):
            r = estimate_effective_seconds(cpu, cached, 1000)
            assert 0.01 <= r["miss_cost"] <= 30.0
            assert 0.001 <= r["effective"] <= 30.0


def test_zero_total_images_is_safe():
    """total_images<=0 → effective=0（调用方据此跳过预算），不除零。"""
    r = estimate_effective_seconds("AMD EPYC 7763", 0, 0)
    assert r["effective"] == pytest.approx(0.0)
    assert r["hit_rate"] == pytest.approx(0.0)


# ============================================================
# ⑤ count_cached_ocr（数 .cache/ocr/*.json）
# ============================================================
def test_count_cached_ocr(tmp_path: Path):
    """数 <cache_dir>/ocr/*.json；不存在时返回 0。"""
    cache = tmp_path / ".cache"
    assert count_cached_ocr(cache) == 0            # 目录不存在
    (cache / "ocr").mkdir(parents=True)
    assert count_cached_ocr(cache) == 0            # 目录空
    for i in range(3):
        (cache / "ocr" / f"{i}.json").write_text("{}", encoding="utf-8")
    (cache / "ocr" / "not_a_json.txt").write_text("x", encoding="utf-8")
    assert count_cached_ocr(cache) == 3            # 只数 *.json


def test_hit_rate_history_constant_is_documented():
    """条件①：HIT_RATE_HISTORY 是 PENDING 假设值，必须能在模块 docstring 里找到说明。"""
    import cloud.budget as b

    doc = (b.__doc__ or "") + (estimate_effective_seconds.__doc__ or "")
    assert "PENDING" in doc, "docstring 必须标注 HIT_RATE_HISTORY 是待校准假设"
    assert "循环依赖" in doc, "docstring 必须说明循环依赖"
    assert HIT_RATE_HISTORY == pytest.approx(0.74)
