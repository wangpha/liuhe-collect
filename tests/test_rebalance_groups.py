"""`_rebalance_groups` 单测（修 400 倍失衡的核心算法）。

实测背景（run 44470578706）：
    [shard] 本片   4 图 → 0.1 分钟
    [shard] 本片 1605 图 → 52.7 分钟（上限 15）→ 转移 72% → merge 25.8 分钟
LPT 按用户分组，单个超大用户不可拆 → 必须在这里按**帖**再平衡。
"""
from __future__ import annotations

import sys

sys.path.insert(0, r"D:\qode工作区")

from cloud.runner import _rebalance_groups  # noqa: E402


def _loads(groups, w):
    return [sum(max(1, int(w.get(a, 1))) for a in g) for g in groups]


def test_400x_imbalance_is_fixed():
    """模拟「一片 1605 图、其余极少」的极端失衡 → 再平衡后极差应大幅下降。"""
    # 20 片：第 0 片有 1605 图的帖（拆成 53 帖 × 30 图），其余片各 4 图
    groups = [[*range(0, 53)]] + [[*range(100 + i, 100 + i + 1)] for i in range(19)]
    w = {a: 30 for a in range(0, 53)}
    for i in range(19):
        w[100 + i] = 4

    before = _loads(groups, w)
    assert max(before) / max(1, min(before)) > 10, "构造的初始状态应是极端失衡"

    out = _rebalance_groups(groups, w)
    after = _loads(out, w)

    assert max(after) / max(1, min(after)) < max(before) / max(1, min(before)), "极差必须下降"
    assert sum(len(g) for g in out) == sum(len(g) for g in groups), "帖子总数不能变（不重不漏）"
    flat = [a for g in out for a in g]
    assert len(flat) == len(set(flat)), "同一帖不能出现在两个片"


def test_already_balanced_is_untouched():
    """本来就均衡 → 不应搬动。"""
    groups = [[1, 2], [3, 4], [5, 6]]
    w = {a: 10 for a in range(1, 7)}
    out = _rebalance_groups(groups, w)
    assert out == groups


def test_single_post_shard_stops():
    """某片只有 1 帖（超大帖）→ 搬不动，函数应安全退出（不丢帖、不空片）。"""
    groups = [[999], [1, 2, 3]]
    w = {999: 5000, 1: 1, 2: 1, 3: 1}
    out = _rebalance_groups(groups, w)
    flat = [a for g in out for a in g]
    assert sorted(flat) == sorted([999, 1, 2, 3])


def test_len_below_two_is_noop():
    assert _rebalance_groups([], {}) == []
    assert _rebalance_groups([[1]], {1: 5}) == [[1]]
