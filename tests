"""阶段 1 单测：`detect_period_safe` —— 期号判定的 8 种情况（v5 任务书第六部分·阶段 1）。

## 为什么需要「第 1 页唯一性 + 台账锁定」
现有 `detect_period` 的三源探测各有一套四重校验，但缺两个能力：
  1. **过渡期混期**：新期刚开时，第 1 页可能同时出现 321 和 322 —— 取 max 会提前换期
  2. **20:50 那轮必须锁定台账期号**：不能因为源站刚发了几帖新期就换期，否则
     当晚数据被拆成两期，产物不完整

## 8 种情况（任务书给定）
  1. 单一期号 + 无台账 → 建立基线
  2. 单一期号 + 一致   → 正常
  3. 单一期号 + 更大   → 换期
  4. 单一期号 + 更小   → 报警中止
  5. 混期   + 有台账   → 用台账
  6. 混期   + 无台账   → 取最大
  7. 空页   + 有台账   → 用台账
  8. 空页   + 无台账   → 中止
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

sys.path.insert(0, r"D:\qode工作区")

from cloud._period import PeriodAbort, detect_period_safe  # noqa: E402


def _ledger(tmp_path: Path, period: int | None) -> Path:
    p = tmp_path / "current_period_dahkg.json"
    if period is None:
        return p
    p.write_text(json.dumps({"period": period, "source": "dahkg"}), encoding="utf-8")
    return p


def _rows(*periods):
    return [{"periods": p} for p in periods]


# ---------------- 情况 1：单一期号 + 无台账 → 建立基线 ----------------
def test_case1_single_no_ledger_establishes_baseline(tmp_path):
    got = detect_period_safe("dahkg", _ledger(tmp_path, None), log=lambda m: None,
                             first_page_rows=_rows(322, 322, 322))
    assert got["period"] == 322
    assert got["evidence"]["reason"] == "首轮建立基线"


# ---------------- 情况 2：单一期号 + 一致 → 正常 ----------------
def test_case2_single_consistent(tmp_path):
    got = detect_period_safe("dahkg", _ledger(tmp_path, 322), log=lambda m: None,
                             first_page_rows=_rows(322, 322))
    assert got["period"] == 322
    assert got["evidence"]["reason"] == "与台账一致"


# ---------------- 情况 3：单一期号 + 更大 → 换期 ----------------
def test_case3_single_greater_switches_period(tmp_path):
    got = detect_period_safe("dahkg", _ledger(tmp_path, 321), log=lambda m: None,
                             first_page_rows=_rows(322, 322))
    assert got["period"] == 322
    assert "换期" in got["evidence"]["reason"] and "321" in got["evidence"]["reason"]


# ---------------- 情况 4：单一期号 + 更小 → 报警中止 ----------------
def test_case4_single_smaller_aborts(tmp_path):
    with pytest.raises(PeriodAbort) as ei:
        detect_period_safe("dahkg", _ledger(tmp_path, 322), log=lambda m: None,
                           first_page_rows=_rows(321))
    assert "回退" in str(ei.value)


# ---------------- 情况 5：混期 + 有台账 → 用台账 ----------------
def test_case5_mixed_uses_ledger(tmp_path):
    got = detect_period_safe("dahkg", _ledger(tmp_path, 321), log=lambda m: None,
                             first_page_rows=_rows(321, 322, 321))
    assert got["period"] == 321, "混期且有台账时必须用台账，不能被新期带走"
    assert "混期" in got["evidence"]["reason"]


# ---------------- 情况 6：混期 + 无台账 → 取最大 ----------------
def test_case6_mixed_no_ledger_takes_max(tmp_path):
    got = detect_period_safe("dahkg", _ledger(tmp_path, None), log=lambda m: None,
                             first_page_rows=_rows(321, 322, 321))
    assert got["period"] == 322
    assert "取最大" in got["evidence"]["reason"]


# ---------------- 情况 7：空页 + 有台账 → 用台账 ----------------
def test_case7_empty_page_uses_ledger(tmp_path):
    got = detect_period_safe("dahkg", _ledger(tmp_path, 321), log=lambda m: None,
                             first_page_rows=[])
    assert got["period"] == 321
    assert "无期号" in got["evidence"]["reason"]


# ---------------- 情况 8：空页 + 无台账 → 中止 ----------------
def test_case8_empty_page_no_ledger_aborts(tmp_path):
    with pytest.raises(PeriodAbort):
        detect_period_safe("dahkg", _ledger(tmp_path, None), log=lambda m: None,
                           first_page_rows=[])


# ---------------- 附加：返回结构必须带 display / caliber / evidence ----------------
def test_returns_display_and_caliber(tmp_path):
    got = detect_period_safe("dahkg", _ledger(tmp_path, None), log=lambda m: None,
                             first_page_rows=_rows(322))
    assert got["display"] == 322
    assert got["caliber"] in ("short", "full")
    assert "firstPage" in got["evidence"]


# ---------------- 附加：整期号输入也要正确归一（aocai 的 qs=2026322）----------------
def test_full_period_normalized(tmp_path):
    got = detect_period_safe("aocai", tmp_path / "current_period_aocai.json", log=lambda m: None,
                             first_page_rows=_rows(2026322, 2026322))
    assert got["period"] == 2026322
    assert got["display"] == 322, "展示口径必须是短期号"
    assert got["caliber"] == "full"


# ---------------- 附加：20:50 锁定档位，强制用台账期号（PERIOD_LOCK_POLICY）----------------
def test_lock_ledger_forces_ledger_period(tmp_path):
    """锁定档位时，即使第 1 页已是新期，也不换期（防止当晚数据被拆成两期）。"""
    got = detect_period_safe("dahkg", _ledger(tmp_path, 321), log=lambda m: None,
                             first_page_rows=_rows(322, 322), lock_ledger=True)
    assert got["period"] == 321, "锁定档位必须用台账期号"
    assert "锁定档位" in got["evidence"]["reason"]


def test_lock_ledger_without_ledger_falls_back(tmp_path):
    """锁定档位但无台账 → 退回正常判定（不能因为锁定就崩）。"""
    got = detect_period_safe("dahkg", _ledger(tmp_path, None), log=lambda m: None,
                             first_page_rows=_rows(322), lock_ledger=True)
    assert got["period"] == 322
