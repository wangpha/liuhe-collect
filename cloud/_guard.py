"""防线 3（防漏新增内容）+ 防线 5 第 5 道（乱码扫描）。

## 防线 3：多字段快照
只比 replyPoint 会漏掉「楼主编辑正文但没发新回复」的情况（文档已知坑），
所以快照存 6 个字段：replyPoint / imgTotal / nicePoint / titleHash /
contentHash / imgHash。任一字段变化 → 该帖有更新。

同时提供**单调性断言**（本轮的帖子集合必须是上轮的超集、回复总数只增不减），
违反即中止 —— 不许静默通过。
"""
from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any, Callable

#: 防线 5 第 5 道：**硬**乱码特征 —— 出现即判定为编码损坏，进 abnormal_text 并失败
HARD_MOJIBAKE = ("\ufffd", "锟斤拷", "烫烫烫", "屯屯屯")
#: **软**特征：连续问号。2026-11-16 实测证明它会被正常用户输入命中
#: （原文："别要求那么多 OK????" —— 4 个问号，中文正常），所以只记录供人工复核，
#: 不直接判失败，避免把正常内容当故障、逼着后续把真问题也一起放宽。
SOFT_MOJIBAKE = ("???",)


def _sha(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8", errors="replace")).hexdigest()[:32]


def build_snapshot(posts: list[dict]) -> dict[str, dict]:
    """按 articleId 建多字段快照。"""
    snap: dict[str, dict] = {}
    for post in posts:
        aid = post.get("articleId")
        if not aid:
            continue
        images = post.get("images") or []
        reply_imgs = [img for r in (post.get("replies") or []) for img in (r.get("images") or [])]
        snap[str(aid)] = {
            "replyPoint": len(post.get("replies") or []),
            "imgTotal": len(images),
            "nicePoint": int(post.get("nicePoint") or post.get("likes") or 0),
            "titleHash": _sha(str(post.get("title") or "")),
            "contentHash": _sha(str(post.get("text") or "")),
            "imgHash": _sha(",".join(str(img.get("url") or "") for img in [*images, *reply_imgs])),
            "replyImageCount": len(reply_imgs),
        }
    return snap


def diff_snapshots(prev: dict, curr: dict) -> dict:
    """对比两轮快照，给出新增帖 / 变化帖 / 丢失帖。"""
    prev_ids, curr_ids = set(prev), set(curr)
    new_posts = sorted(curr_ids - prev_ids)
    lost_posts = sorted(prev_ids - curr_ids)
    changed: dict[str, list[str]] = {}
    for aid in prev_ids & curr_ids:
        fields = [
            key
            for key in ("replyPoint", "imgTotal", "nicePoint", "titleHash", "contentHash", "imgHash")
            if prev[aid].get(key) != curr[aid].get(key)
        ]
        if fields:
            changed[aid] = fields
    return {
        "prevCount": len(prev_ids),
        "currCount": len(curr_ids),
        "newCount": len(new_posts),
        "lostCount": len(lost_posts),
        "changedCount": len(changed),
        "newPosts": new_posts[:50],
        "lostPosts": lost_posts[:50],
        "changed": dict(list(changed.items())[:50]),
    }


def check_monotonic(prev: dict, curr: dict, prev_reply_total: int | None = None, curr_reply_total: int | None = None) -> dict:
    """防线 3 第 4 道：帖子集合只增不减、回复总数只增不减。"""
    prev_ids, curr_ids = set(prev), set(curr)
    lost = sorted(prev_ids - curr_ids)
    problems: list[str] = []
    if lost:
        problems.append(f"帖子集合减少 {len(lost)} 个：{lost[:10]}")
    if prev_reply_total is not None and curr_reply_total is not None:
        if curr_reply_total < prev_reply_total:
            problems.append(f"回复总数下降：{prev_reply_total} -> {curr_reply_total}")
    return {
        "prevPosts": len(prev_ids),
        "currPosts": len(curr_ids),
        "lostPosts": lost[:50],
        "prevReplyTotal": prev_reply_total,
        "currReplyTotal": curr_reply_total,
        "problems": problems,
        "passed": not problems,
    }


# ---------------------------------------------------------------------------
# 防线 5 第 5 道：乱码扫描
# ---------------------------------------------------------------------------
def _iter_strings(node: Any, path: str = ""):
    if isinstance(node, str):
        yield path, node
    elif isinstance(node, dict):
        for key, value in node.items():
            yield from _iter_strings(value, f"{path}.{key}")
    elif isinstance(node, list):
        for index, value in enumerate(node):
            yield from _iter_strings(value, f"{path}[{index}]")


def scan_mojibake(payload: Any, limit: int = 50, min_len: int = 2) -> dict:
    """扫描识别文本里的编码错误特征。

    分两级（实测依据见 SOFT/HARD_MOJIBAKE 注释）：
      * 硬命中（U+FFFD / 锟斤拷 / 烫烫烫 / 屯屯屯）→ `passed=False`，进 abnormal_text
      * 软命中（连续问号）→ 只记录，供人工复核，不算失败
    """
    hard: list[dict] = []
    soft: list[dict] = []
    scanned = 0
    for path, text in _iter_strings(payload):
        if not text:
            continue
        scanned += 1
        for marker in HARD_MOJIBAKE:
            if marker in text:
                hard.append({"path": path[:160], "marker": repr(marker), "level": "hard", "sample": text[:80]})
                break
        else:
            for marker in SOFT_MOJIBAKE:
                if marker in text:
                    soft.append({"path": path[:160], "marker": repr(marker), "level": "soft", "sample": text[:120]})
                    break
        if len(hard) + len(soft) >= limit:
            break
    return {
        "stringsScanned": scanned,
        "hardCount": len(hard),
        "softCount": len(soft),
        "hitCount": len(hard) + len(soft),
        "hits": hard + soft,
        "hard": hard,
        "soft": soft,
        "passed": not hard,
        "note": "软命中=连续问号，实测可由正常用户输入产生，需人工复核（文档 3-C 第 6 项）",
    }


def save_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(path)


def load_json(path: Path, default: Any = None) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return default if default is not None else {}
