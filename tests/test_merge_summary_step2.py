"""C1 第 2 步测试：merge-summary 的三个核心行为（先写测试，RED → 实现 → GREEN）。

## 要锁住的三条（都是本轮实测出来的规则，不许回退）
① 帖子排序 = list.json 原始顺序（实测 legacy 文字版 679/679 完全一致）
② fullText 还原 = 缓存 rows 按 ocr.rowCount 截断（实测 807/816 张一致，截断后差 0.247%→0.159%）
③ 模式切换 --merge-mode=legacy|summary，默认 legacy（安全）
"""
from __future__ import annotations

import sys

import pytest

sys.path.insert(0, r"D:\qode工作区")

from cloud.merge_summary import (  # noqa: E402
    build_fulltext_from_rows,
    order_posts_by_list,
    resolve_merge_mode,
)


# ============================================================
# ① 排序：list.json 原始顺序
# ============================================================
def test_order_by_list_index():
    posts = [{"articleId": 30}, {"articleId": 10}, {"articleId": 20}]
    list_order = {10: 2, 20: 0, 30: 1}
    got = order_posts_by_list(posts, list_order)
    assert [p["articleId"] for p in got] == [20, 30, 10], "应按 list.json 位置升序"


def test_order_unknown_aid_goes_last():
    posts = [{"articleId": 99}, {"articleId": 10}]
    got = order_posts_by_list(posts, {10: 5})
    assert [p["articleId"] for p in got] == [10, 99], "不在 list 里的帖应排最后"


def test_order_is_stable_for_equal_index():
    posts = [{"articleId": 1}, {"articleId": 2}]
    got = order_posts_by_list(posts, {})       # 都没有位置
    assert [p["articleId"] for p in got] == [1, 2], "位置相同应保持原序（稳定排序）"


# ============================================================
# ② fullText：按 rowCount 截断（与 core 的 "\\n".join 一致）
# ============================================================
def _rows(n):
    return [{"y0": float(i), "text": f"L{i}"} for i in range(n)]


def test_fulltext_no_truncate_when_rowcount_matches():
    rows = _rows(3)
    assert build_fulltext_from_rows(rows, 3) == "L0\nL1\nL2"


def test_fulltext_truncates_to_rowcount():
    """缓存 15 行、rowCount 6 → 只取前 6 行（实测图467：402 字 → 与 legacy 192 字对齐）"""
    rows = _rows(15)
    got = build_fulltext_from_rows(rows, 6)
    assert got.count("\n") == 5, "应只剩 6 行"
    assert got.startswith("L0") and got.endswith("L5")


def test_fulltext_rowcount_zero_or_none_uses_all():
    rows = _rows(4)
    assert build_fulltext_from_rows(rows, 0) == "L0\nL1\nL2\nL3"
    assert build_fulltext_from_rows(rows, None) == "L0\nL1\nL2\nL3"


def test_fulltext_rowcount_larger_than_rows_uses_all():
    rows = _rows(2)
    assert build_fulltext_from_rows(rows, 99) == "L0\nL1"


def test_fulltext_prefers_row_indexes_when_present():
    rows = _rows(5)
    got = build_fulltext_from_rows(rows, 5, row_indexes=[0, 2])
    assert got == "L0\nL2", "有 rowIndexes 时按索引取行"


def test_fulltext_keeps_empty_lines_like_core():
    """core 是 "\\n".join(row.text)，空行也保留 → 不许 strip/过滤"""
    rows = [{"text": "A"}, {"text": ""}, {"text": "B"}]
    assert build_fulltext_from_rows(rows, 3) == "A\n\nB"


# ============================================================
# ③ 模式切换：默认 legacy
# ============================================================
@pytest.mark.parametrize("argv,expected", [
    ([], "legacy"),
    (["--merge"], "legacy"),
    (["--merge", "--merge-mode", "legacy"], "legacy"),
    (["--merge", "--merge-mode", "summary"], "summary"),
    (["--merge", "--merge-mode=summary"], "summary"),
    (["--merge", "--merge-mode", "bogus"], "legacy"),   # 非法值回退到 legacy（安全）
])
def test_resolve_merge_mode(argv, expected):
    assert resolve_merge_mode(argv) == expected


def test_resolve_merge_mode_rejects_unknown_but_warns(capsys):
    got = resolve_merge_mode(["--merge", "--merge-mode", "typo"])
    assert got == "legacy"
    out = capsys.readouterr().out
    assert "typo" in out, "非法值要打警告，不能静默"
