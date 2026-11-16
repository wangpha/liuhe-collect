"""防线 1 / 2 / 4-第5道：独立对账（R11 漏帖 = 0，R12 漏图 = 0，R14 期号混 = 0）。

**独立**的含义：不复用采集链路的翻页/停止条件，用另一套逻辑重新扫一遍源站列表，
再把两边的 articleId 集合与每帖 `imgTotal` 对起来。差值必须 = 0。
"""
from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any, Callable

from ._period import normalize_period


# ---------------------------------------------------------------------------
# 防线 1：独立扫描源站列表
# ---------------------------------------------------------------------------
def independent_scan_dahkg(
    period: int,
    max_pages: int = 200,
    empty_streak_stop: int = 3,
    log: Callable[[str], None] | None = None,
) -> dict:
    """独立逻辑重扫列表，返回本期应有帖子集合 + 每帖 imgTotal。

    停止条件与 `core/dahkg.py::scan_period` **故意不同**：
      - 那边：出现「比目标更旧的期号」才停（抓往期时正确）；
      - 这里：连续 `empty_streak_stop` 页没有本期帖子就停（抓当期时更保守，
        宁可多翻几页，也不和采集链路共用同一个可能有 bug 的判定）。
    两种排序（按时间 / 按赞）各扫一遍，集合取并集，并记录各自结果供交叉核对。
    """
    from core import dahkg

    say = log or (lambda message: None)
    want = normalize_period(period)
    per_sort: dict[str, dict[str, Any]] = {}

    for sort_type in (1, 3):
        ids: dict[int, dict] = {}
        streak = 0
        pages = 0
        t0 = time.time()
        for page in range(1, max_pages + 1):
            try:
                data = dahkg._api_post(
                    {
                        "uri": dahkg.URI_LIST,
                        "currentPage": page,
                        "pageSize": dahkg.PAGE_SIZE,
                        "sortType": sort_type,
                        "lastRefreshTime": 0,
                    }
                )
            except Exception as exc:  # noqa: BLE001 - 单页失败如实记录，不静默
                say(f"[独立扫描] sortType={sort_type} 第{page}页失败：{exc}")
                streak += 1
                if streak >= empty_streak_stop:
                    break
                continue
            rows = (data.get("data") or {}).get("list") or []
            pages = page
            if not rows:
                break
            hit = 0
            for row in rows:
                if normalize_period(row.get("periods")) != want:
                    continue
                hit += 1
                aid = int(row.get("singaporeId") or 0)
                if aid:
                    ids[aid] = {
                        "imgTotal": int(row.get("imgTotal") or 0),
                        "replyPoint": int(row.get("replyPoint") or 0),
                        "nicePoint": int(row.get("nicePoint") or 0),
                        "isCurrPeriods": row.get("isCurrPeriods"),
                    }
            streak = 0 if hit else streak + 1
            if streak >= empty_streak_stop:
                say(f"[独立扫描] sortType={sort_type} 连续 {streak} 页无本期帖子，停止于第 {page} 页")
                break
        per_sort[f"sortType{sort_type}"] = {
            "pages": pages,
            "count": len(ids),
            "elapsed": round(time.time() - t0, 2),
            "ids": ids,
        }
        say(f"[独立扫描] sortType={sort_type}: {pages} 页 / 本期 {len(ids)} 帖（{per_sort[f'sortType{sort_type}']['elapsed']}s）")

    union: dict[int, dict] = {}
    for key in per_sort:
        for aid, meta in per_sort[key]["ids"].items():
            union.setdefault(aid, meta)

    a = set(per_sort.get("sortType1", {}).get("ids", {}))
    b = set(per_sort.get("sortType3", {}).get("ids", {}))
    return {
        "period": want,
        "unionIds": sorted(union),
        "unionMeta": {str(k): v for k, v in union.items()},
        "countUnion": len(union),
        "countByTime": len(a),
        "countByLike": len(b),
        "onlyByTime": sorted(a - b)[:50],
        "onlyByLike": sorted(b - a)[:50],
        "agree": a == b,
        "perSort": {k: {kk: vv for kk, vv in v.items() if kk != "ids"} for k, v in per_sort.items()},
    }


# ---------------------------------------------------------------------------
# 防线 1 第 2 道：帖子数对账
# ---------------------------------------------------------------------------
def reconcile_posts(posts: list[dict], scan: dict) -> dict:
    got = {int(p.get("articleId") or 0) for p in posts if p.get("articleId")}
    want = set(scan.get("unionIds") or [])
    missing = sorted(want - got)
    extra = sorted(got - want)
    return {
        "expected": len(want),
        "actual": len(got),
        "missingCount": len(missing),
        "extraCount": len(extra),
        "missing": missing[:50],
        "extra": extra[:50],
        "passed": not missing and not extra,
    }


# ---------------------------------------------------------------------------
# 防线 2：逐帖图片数对账
# ---------------------------------------------------------------------------
def reconcile_images(posts: list[dict], scan: dict, require_files: Path | None = None) -> dict:
    """每帖：正文图片数 vs 列表 `imgTotal`；并核对下载状态。

    - `imgTotal` 是源站列表行里的**正文图**总数（实测字段名确定，2026-11-16）；
    - 回复图不参与 `imgTotal`，单独按 `commentImageCount`/回复内 images 统计并报告；
    - `downloadStatus` 非 exists/downloaded 的计入失败清单（物理不可救要列明）。
    """
    union_meta = scan.get("unionMeta") or {}
    rows: list[dict] = []
    mismatch_total = 0
    not_downloaded_total = 0
    missing_file_total = 0

    for post in posts:
        aid = int(post.get("articleId") or 0)
        declared = int((union_meta.get(str(aid)) or {}).get("imgTotal") or 0)
        body_imgs = post.get("images") or []
        got = len(body_imgs)
        bad_status = [
            img.get("downloadStatus")
            for img in body_imgs
            if str(img.get("downloadStatus") or "") not in {"exists", "downloaded", "cached"}
        ]
        missing_files: list[str] = []
        if require_files is not None:
            for img in body_imgs:
                lp = img.get("localPath")
                if lp and not (require_files / str(lp)).exists():
                    missing_files.append(str(lp))
        delta = got - declared
        if delta != 0:
            mismatch_total += 1
        not_downloaded_total += len(bad_status)
        missing_file_total += len(missing_files)
        rows.append(
            {
                "articleId": aid,
                "declared": declared,
                "actual": got,
                "delta": delta,
                "badStatus": bad_status[:5],
                "missingFiles": missing_files[:5],
                "replyImages": sum(len(r.get("images") or []) for r in (post.get("replies") or [])),
            }
        )

    problems = [r for r in rows if r["delta"] != 0 or r["badStatus"] or r["missingFiles"]]
    return {
        "postsChecked": len(rows),
        "declaredTotal": sum(r["declared"] for r in rows),
        "actualTotal": sum(r["actual"] for r in rows),
        "replyImagesTotal": sum(r["replyImages"] for r in rows),
        "mismatchPosts": mismatch_total,
        "notDownloadedImages": not_downloaded_total,
        "missingFiles": missing_file_total,
        "problems": problems[:30],
        "passed": mismatch_total == 0 and not_downloaded_total == 0 and missing_file_total == 0,
    }


# ---------------------------------------------------------------------------
# 产物读取
# ---------------------------------------------------------------------------
def load_products(out_dir: Path) -> tuple[dict, list[dict]]:
    path = out_dir / "posts.json"
    if not path.exists():
        raise FileNotFoundError(f"产物缺失：{path}")
    data = json.loads(path.read_text(encoding="utf-8"))
    if isinstance(data, dict):
        return data.get("meta") or {}, data.get("posts") or []
    return {}, data


#: 3.5 节要求的 21 个产物
PRODUCTS_21 = [
    "users.json", "posts.json", "replies.json", "claims.json", "ocr.json",
    "全站文字版.json", "全站文字版.html", "全站文字版.txt", "六合_看板.html",
    "六合_结构化.xlsx", "claims.csv", "ocr.csv", "replies.csv", "posts.csv",
    "users.csv", "论坛原帖.html", "report.html", "report.md", "validation.json",
    "run_meta.json", "manifest.json",
]


def check_products(out_dir: Path) -> dict:
    present, missing, sizes = [], [], {}
    for name in PRODUCTS_21:
        p = out_dir / name
        if p.exists() and p.stat().st_size > 0:
            present.append(name)
            sizes[name] = p.stat().st_size
        else:
            missing.append(name)
    return {
        "expected": len(PRODUCTS_21),
        "present": len(present),
        "missing": missing,
        "sizes": sizes,
        "passed": not missing,
    }
