"""merge-summary 的纯逻辑（C1）：分片产物合并 + run_meta 三类重建。

设计依据（用户 2026-11-18 13:2x 裁决 + 实测 schema）：
  · 产物形态即最终形态 → 合并只需按 articleId 去重拼接，**不重建 post**
  · claims 必须**重跑 collect_claims**（内嵌 claims 是原料：claimId=None、无 rank）
  · run_meta 按三类重建：A 覆盖 / B 求和 / C 实测

三类之所以不能混用（三个实测反例）：
  ① durationSec 各片求和 = 813 秒，但三片并行墙钟只有 300 秒
  ② stageSeconds 若求和，summary 模式会出现 details/ocr > 0，但 summary 根本不抓详情/不做 OCR
  ③ images.unique 若求和，同一张图被多片引用会重复计数
  ④ scan.* 若求和，各片都扫了全量列表 → 重复 N 倍；plan 才是权威
"""
from __future__ import annotations

import time

#: B 类：各片累加的子字段（父字段名 -> 需要求和的键）
SUM_FIELDS: dict[str, tuple[str, ...]] = {
    "detail": ("cached", "fetched", "failed"),
    "images": ("downloaded", "skipped", "failed"),
    "replies": ("total", "cached", "fetched", "failed", "authorReplies", "authorReplyImages"),
}

#: C 类：summary 模式下这些阶段必然为 0（不做这些事）
ZERO_STAGES = ("resolve", "list", "details", "media", "replies_images", "ocr")


def merge_posts(shard_post_lists: list[list[dict]]) -> tuple[list[dict], dict]:
    """把各片 posts 按 articleId 去重拼接（保持首次出现顺序）。

    返回 (merged, stats)。stats: {shards, inputTotal, duplicates, outputTotal}
    """
    seen: set[int] = set()
    merged: list[dict] = []
    total = 0
    for posts in shard_post_lists:
        for post in posts or []:
            total += 1
            try:
                aid = int(post.get("articleId") or 0)
            except (TypeError, ValueError):
                aid = 0
            if aid and aid in seen:
                continue
            if aid:
                seen.add(aid)
            merged.append(post)
    return merged, {
        "shards": len(shard_post_lists),
        "inputTotal": total,
        "duplicates": total - len(merged),
        "outputTotal": len(merged),
    }


def _sum_field(shard_metas: list[dict], parent: str, key: str) -> int:
    total = 0
    for m in shard_metas:
        block = (m or {}).get(parent) or {}
        v = block.get(key)
        if isinstance(v, (int, float)):
            total += int(v)
    return total


def _sum_ocr(shard_metas: list[dict]) -> dict:
    """ocr.* 全求和（键取各片并集，缺项按 0）。"""
    keys: set[str] = set()
    for m in shard_metas:
        keys |= set(((m or {}).get("ocr") or {}).keys())
    out = {}
    for k in sorted(keys):
        if any(isinstance(((m or {}).get("ocr") or {}).get(k), (int, float)) for m in shard_metas):
            out[k] = _sum_field(shard_metas, "ocr", k)
        else:
            # 非数值键（如 status 字符串）取第一个非空
            for m in shard_metas:
                v = ((m or {}).get("ocr") or {}).get(k)
                if v not in (None, ""):
                    out[k] = v
                    break
    return out


def build_merged_meta(
    *,
    template: dict,
    shard_metas: list[dict],
    merged_posts: list[dict],
    users: list[dict],
    claims: list[dict],
    claim_stats: dict,
    plan: dict,
    images_unique: int,
    timings: dict,
) -> dict:
    """按三类规则重建全量 run_meta。

    · A 覆盖：计数类 / 时间戳 / 期号类
    · B 求和：detail / images(除 unique) / ocr / replies
    · C 实测：durationSec / rebuildDurationSec / stageSeconds
    · scan 用 plan 的值（不求和）

    若 plan 给了 expectedPosts 且与合并后帖数不符 → SystemExit（不许静默出产物）。
    """
    tpl = dict(template or {})
    period = int((plan or {}).get("period") or tpl.get("period") or 0)

    expected = int((((plan or {}).get("counts") or {}).get("expectedPosts")) or 0)
    if expected and len(merged_posts) != expected:
        raise SystemExit(
            f"[merge-summary] ❌ 汇总后 {len(merged_posts)} 帖 != plan 预期 {expected} 帖"
            f"（各片 posts 可能有缺漏，拒绝出产物）"
        )

    now = time.time()
    meta = dict(tpl)                       # 以模板为底（source/lottery/latestDraw/options 等源级字段）

    # ---------- A 类：覆盖 ----------
    meta["postCount"] = len(merged_posts)
    meta["userCount"] = len(users)
    meta["imageCount"] = int(images_unique)
    meta["claims"] = len(claims)
    meta["claimStats"] = dict(claim_stats or {})
    meta["claimStats"]["claims"] = meta["claimStats"].get("claims", len(claims))
    meta["generatedAt"] = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(now))
    meta["generatedAtUnix"] = int(now)
    meta["period"] = period
    meta["periodsIncluded"] = [period]
    meta["onlyLatestPeriod"] = True

    # ---------- B 类：求和 ----------
    for parent, keys in SUM_FIELDS.items():
        base = dict((tpl.get(parent) or {}))
        for k in keys:
            base[k] = _sum_field(shard_metas, parent, k)
        if parent == "detail":
            base["total"] = len(merged_posts)      # total 用全量帖数，不是求和
        if parent == "images":
            base["unique"] = int(images_unique)    # ⚠️ 反例③：unique 用实测，不求和
        meta[parent] = base

    meta["ocr"] = _sum_ocr(shard_metas)

    # scan 不求和 → 用 plan 的值（plan 扫的就是全量），没有就退回模板
    plan_scan = (plan or {}).get("scan")
    meta["scan"] = dict(plan_scan) if isinstance(plan_scan, dict) and plan_scan else dict(tpl.get("scan") or {})

    # ---------- C 类：实测 ----------
    meta["durationSec"] = float((timings or {}).get("durationSec") or 0.0)
    meta["rebuildDurationSec"] = float((timings or {}).get("rebuildDurationSec") or 0.0)
    stage = {k: 0 for k in ZERO_STAGES}
    stage["claims"] = float((timings or {}).get("claimsSec") or 0.0)
    meta["stageSeconds"] = stage

    return meta


# ---------------------------------------------------------------------------
# C1 第 2 步：排序规则 / fullText 还原 / 模式解析（三条都是实测出来的，不许回退）
# ---------------------------------------------------------------------------

MERGE_MODES = ("legacy", "summary")


def order_posts_by_list(posts: list[dict], list_order: dict[int, int]) -> list[dict]:
    """按 `.cache/list.json` 的原始位置排序。

    ## 为什么是这样（2026-11-18 实测）
    legacy 文字版的帖子顺序 = **源站列表的原始顺序**（列表本身已按时间排好）。
    实测该排序使 legacy 与 summary 的帖序列 **679/679 = 100% 一致**；
    而「updateTime 降序」只有 8%、「updateTime + 次级键」19.6% —— 都错。

    不在 list 里的帖（理论上不该有）排最后，保持原序（稳定）。
    """
    def key(item: dict) -> int:
        try:
            aid = int(item.get("articleId") or 0)
        except (TypeError, ValueError):
            aid = 0
        return list_order.get(aid, 10 ** 9)

    return sorted(posts, key=key)


def build_fulltext_from_rows(
    rows: list[dict],
    row_count: int | None = None,
    row_indexes: list[int] | None = None,
) -> str:
    """把 OCR rows 拼成 fullText —— 与 core 的 `"\n".join(row.text)` 完全一致。

    ## 两条实测规则
    ① **优先用 rowIndexes**（若产物里有）：它是 legacy 当次命中的行索引
    ② 否则用 `row_count`（产物 `ocr.rowCount`）**截断**：
       缓存里的 rows 可能是「更晚/更全的版本」（实测 807/816 张一致，9 张缓存行数 > rowCount），
       rowCount 才是 legacy 当次用的行数。截断后字数差 0.247% → 0.159%。
    ③ 空行必须保留（core 不过滤），否则字数会偏少。
    """
    rs = list(rows or [])
    idx = [
        i for i in (row_indexes or [])
        if isinstance(i, int) and 0 <= i < len(rs)
    ]
    if idx:
        return "\n".join(str((rs[i] or {}).get("text") or "") for i in idx)
    if isinstance(row_count, int) and 0 < row_count < len(rs):
        rs = rs[:row_count]
    return "\n".join(str((r or {}).get("text") or "") for r in rs)


def resolve_merge_mode(argv: list[str] | None = None) -> str:
    """从命令行解析 merge 模式，**默认 legacy**（安全：出问题就是老路径）。

    支持 `--merge-mode summary` 与 `--merge-mode=summary` 两种写法。
    非法值打警告后回退 legacy（不静默、也不抛错阻断）。
    """
    args = list(argv if argv is not None else __import__("sys").argv)
    value = ""
    for i, token in enumerate(args):
        if token == "--merge-mode" and i + 1 < len(args):
            value = args[i + 1]
            break
        if token.startswith("--merge-mode="):
            value = token.split("=", 1)[1]
            break
    if not value:
        return "legacy"
    value = value.strip().lower()
    if value in MERGE_MODES:
        return value
    print(f"[merge] ⚠ 未知 --merge-mode={value!r}，回退 legacy（可选值：{'/'.join(MERGE_MODES)}）")
    return "legacy"
