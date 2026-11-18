"""阶段 0 单测：`_find_period_dir` 支持指定期号（用户 v5 任务书第六部分·阶段 0）。

## 为什么需要这个参数（失败模式 6）
`--period 321` 已经能指定期号，但产物目录的选取函数还是「取文件最多的那期」。
实测后果（run 44139439359 smoke test）：
    merge 采完 321 期，却选了 322 期的目录去校验
    → [防线4] 检查 1080 帖、错误 1080 个 → exit 30 → 整轮失败

## 四个用例（任务书指定）
  1. period=0 + 多期目录          → 走原逻辑（不改变现有行为）
  2. period=321 + 有 321 目录     → 直接选中 321
  3. period=321 + 无 321 目录     → 退回原逻辑 + 打警告
  4. period=321 + 只有 321        → 选中 321
"""
from __future__ import annotations

import sys
from pathlib import Path

import pytest

sys.path.insert(0, r"D:\qode工作区")

from cloud.runner import _find_period_dir  # noqa: E402


def _make_period(base: Path, name: str, files: int = 3, with_posts: bool = True) -> Path:
    """造一个期目录：files 个文件，可选是否含 posts.json。"""
    d = base / name
    d.mkdir(parents=True, exist_ok=True)
    if with_posts:
        (d / "posts.json").write_text("[]", encoding="utf-8")
        files = max(files, 1)
    for i in range(max(0, files - (1 if with_posts else 0))):
        (d / f"f{i}.json").write_text("{}", encoding="utf-8")
    return d


def test_period_zero_keeps_legacy_behaviour(tmp_path: Path, capsys):
    """用例 1：period=0 时行为不变 —— 选「有 posts.json 且文件最多」的那期。"""
    base = tmp_path / "大港澳"
    _make_period(base, "澳门_2026_320期", files=2)
    rich = _make_period(base, "澳门_2026_321期", files=9)   # 文件最多
    _make_period(base, "澳门_2026_322期", files=1)

    got = _find_period_dir(tmp_path, tmp_path, "dahkg", period=0)
    assert got == rich, f"period=0 应选文件最多的期，实际 {got.name}"
    assert "候选期目录" in capsys.readouterr().out


def test_period_specified_picks_that_period(tmp_path: Path, capsys):
    """用例 2：period=321 时必须选 321，即使别的期文件更多。"""
    base = tmp_path / "大港澳"
    _make_period(base, "澳门_2026_320期", files=2)
    want = _make_period(base, "澳门_2026_321期", files=1)    # 文件最少
    _make_period(base, "澳门_2026_322期", files=9)           # 文件最多（诱饵）

    got = _find_period_dir(tmp_path, tmp_path, "dahkg", period=321)
    assert got == want, f"period=321 应选中 321，实际 {got.name}"
    out = capsys.readouterr().out
    assert "指定期号 321" in out and "直接选中" in out


def test_period_specified_missing_falls_back_with_warning(tmp_path: Path, capsys):
    """用例 3：指定期号但目录不存在 → 退回原逻辑 + 明确警告（不静默）。"""
    base = tmp_path / "大港澳"
    _make_period(base, "澳门_2026_320期", files=2)
    rich = _make_period(base, "澳门_2026_321期", files=8)

    got = _find_period_dir(tmp_path, tmp_path, "dahkg", period=999)
    assert got == rich, "缺少目标期时应退回原逻辑"
    out = capsys.readouterr().out
    assert "指定期号 999 的目录不存在" in out, "必须打警告，不能静默退回"


def test_period_specified_single_dir(tmp_path: Path, capsys):
    """用例 4：只有一期且指定它 → 选中。"""
    base = tmp_path / "大港澳"
    only = _make_period(base, "澳门_2026_321期", files=4)

    got = _find_period_dir(tmp_path, tmp_path, "dahkg", period=321)
    assert got == only


def test_full_period_caliber_also_matches(tmp_path: Path):
    """附加：整期号（2026321）也要能匹配到 `_321期` 目录（display_period 归一）。"""
    base = tmp_path / "大港澳"
    want = _make_period(base, "澳门_2026_321期", files=2)
    _make_period(base, "澳门_2026_322期", files=9)

    got = _find_period_dir(tmp_path, tmp_path, "dahkg", period=2026321)
    assert got == want, f"整期号 2026321 应匹配 321 期目录，实际 {got.name}"
