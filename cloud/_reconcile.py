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
    created_before: float = 0.0,
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
    late_ids: dict[int, float] = {}

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
                # 时间对齐：采集开始后新发的帖子不计入"应有集合"（当期还在长）
                ts = _ts_dahkg(row)
                if created_before and ts and ts > created_before:
                    if aid:
                        late_ids[aid] = ts
                    continue
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
    if late_ids:
        say(f"[独立扫描] 采集开始后新发 {len(late_ids)} 帖，不计入漏帖判定（时间对齐）")
    return {
        "period": want,
        "source": "dahkg",
        "unionIds": sorted(union),
        "unionMeta": {str(k): v for k, v in union.items()},
        "countUnion": len(union),
        "countByTime": len(a),
        "countByLike": len(b),
        "onlyByTime": sorted(a - b)[:50],
        "onlyByLike": sorted(b - a)[:50],
        "lateIds": sorted(late_ids),
        "lateCount": len(late_ids),
        "timeAligned": bool(created_before),
        "agree": a == b,
        "perSort": {k: {kk: vv for kk, vv in v.items() if kk != "ids"} for k, v in per_sort.items()},
    }


def _ts_tuku(raw: dict) -> float:
    """图库列表行的"**可见/变更**时间"（unix 秒）。

    实测（2026-11-16 10:17，期号 320）：
      某批帖子 `pubdate` = 09:27~09:50，但 `createTime` / `updateTime` 全是 **10:05:30**。
      也就是**发布时间与实际进列表时间不是同一个**（审核/审核通过后才可见）。
    只比 pubdate 会把"09:52 采集时还看不见、10:05 才出现"的帖子误判成漏帖。

    所以取三个时间信号里的**最大值**：只要任一字段晚于采集开始，就说明它可能是
    采集之后才变的，对账时排除（数量会写进日志，不静默）。
    """
    cands: list[float] = []

    pub = raw.get("pubdate")
    try:
        ts = float(pub)
        if ts > 1e12:
            ts /= 1000.0
        if ts > 0:
            cands.append(ts)
    except (TypeError, ValueError):
        pass

    upd = raw.get("updateTime")
    try:
        ts = float(upd)
        if ts > 1e12:          # 毫秒
            ts /= 1000.0
        if ts > 0:
            cands.append(ts)
    except (TypeError, ValueError):
        pass

    for key in ("createTimeStr", "createTime"):
        text = str(raw.get(key) or "")
        for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M"):
            try:
                cands.append(time.mktime(time.strptime(text, fmt)))
                break
            except ValueError:
                continue

    return max(cands) if cands else 0.0


def _ts_dahkg(raw: dict) -> float:
    """大港澳列表行时间戳（unix 秒）。实测字段：`createTime`(毫秒)。"""
    try:
        ms = float(raw.get("createTime") or 0)
    except (TypeError, ValueError):
        return 0.0
    if ms <= 0:
        return 0.0
    return ms / 1000.0 if ms > 1e11 else ms


# ---------------------------------------------------------------------------
# 防线 1：49图库（tuku）独立扫描
# ---------------------------------------------------------------------------
def independent_scan_tuku(
    period: int,
    max_pages: int = 60,
    empty_streak_stop: int = 2,
    include_pinned: bool = False,
    created_before: float = 0.0,
    log: Callable[[str], None] | None = None,
) -> dict:
    """49图库独立扫描。

    `created_before`：只统计**采集开始之前**就存在的帖子（unix 秒）。
    当期仍在发布，采集结束后再扫必然多出新帖（实测 2026-11-16：云端产物 984 帖，
    2 小时后重扫 996 帖，差 12 帖全部是采集之后新发的）—— 这不是漏帖。
    不传 0 以外的值时退化为"全部统计"，但会在返回里标 `timeAligned=False`。
    """
    from core import scraper

    say = log or (lambda message: None)
    want = normalize_period(period)
    sorts = scraper.period_sorts("full")
    say(f"[独立扫描/tuku] 期号 {want}，排序窗口 {sorts}")

    ids: dict[int, dict] = {}
    per_sort: dict[str, dict] = {}
    pinned_skipped = 0
    late_ids: dict[int, float] = {}
    for sort_value in sorts:
        t0 = time.time()
        streak = 0
        pages = 0
        for page in range(1, max_pages + 1):
            try:
                rows = scraper.search_page(
                    2,                       # lotteryType：2 = 澳门
                    page,
                    sort_value,
                    0.05,
                    number=want,
                    user_id=scraper.PERIOD_QUERY_USER_ID,
                )
            except Exception as exc:  # noqa: BLE001 - 单页失败如实记录
                say(f"[独立扫描/tuku] sort={sort_value} 第{page}页失败：{exc}")
                streak += 1
                if streak >= empty_streak_stop:
                    break
                continue
            if not rows:
                break
            pages = page
            hit = 0
            for raw in rows:
                if not isinstance(raw, dict):
                    continue
                if normalize_period(raw.get("period")) != want:
                    continue
                hit += 1
                aid = int(raw.get("articleId") or 0)
                # 时间对齐：采集开始之后新发的帖子不计入"应有集合"（当期还在长）
                ts = _ts_tuku(raw)
                if created_before and ts and ts > created_before:
                    if aid:
                        late_ids[aid] = ts
                    continue
                # 采集链路默认排除置顶帖（scraper.fetch_period_posts: top and not include_pinned）
                # 对账必须用同一口径，否则会把置顶帖误判成"漏帖"
                if raw.get("top") and not include_pinned:
                    pinned_skipped += 1
                    continue
                if not aid:
                    continue
                pics = raw.get("pictureList") or []
                ids.setdefault(
                    aid,
                    {
                        "imgTotal": len(pics) if isinstance(pics, list) else 0,
                        "commentCount": int(raw.get("commentCount") or 0),
                        "thumbUpCount": int(raw.get("thumbUpCount") or 0),
                    },
                )
            streak = 0 if hit else streak + 1
            if streak >= empty_streak_stop:
                break
        per_sort[f"sort{sort_value}"] = {
            "pages": pages,
            "elapsed": round(time.time() - t0, 2),
        }
        say(f"[独立扫描/tuku] sort={sort_value}: {pages} 页，累计本期 {len(ids)} 帖")

    if pinned_skipped:
        say(f"[独立扫描/tuku] 按采集口径排除置顶帖 {pinned_skipped} 次命中")
    if late_ids:
        say(f"[独立扫描/tuku] 采集开始后新发 {len(late_ids)} 帖，不计入漏帖判定（时间对齐）")
    return {
        "period": want,
        "source": "tuku",
        "unionIds": sorted(ids),
        "unionMeta": {str(k): v for k, v in ids.items()},
        "countUnion": len(ids),
        "pinnedSkipped": pinned_skipped,
        "lateIds": sorted(late_ids),
        "lateCount": len(late_ids),
        "timeAligned": bool(created_before),
        "perSort": per_sort,
        "agree": True,          # 图库只有一套列表口径，无第二条判据
    }


# ---------------------------------------------------------------------------
# 防线 1：澳彩交流（aocai）独立扫描
# ---------------------------------------------------------------------------
def independent_scan_aocai(
    period: int,
    max_pages: int = 200,
    empty_streak_stop: int = 3,
    created_before: float = 0.0,
    log: Callable[[str], None] | None = None,
) -> dict:
    """澳彩交流独立扫描。

    实测（2026-11-16）：列表接口 `/getLuntan`，
    参数 `{fid: 20, topLv: 0, began: <偏移>, paixu: <排序>}`，
    偏移步长 = `core.forum.PAGE_SIZE`；行内期号用 `core.forum.infer_item_period(raw, target)`
    （`qs` 徽标优先，缺失时按标题期号标记推断）。
    """
    from core import forum as forum_mod

    say = log or (lambda message: None)
    want = normalize_period(period)
    ids: dict[int, dict] = {}
    per_sort: dict[str, dict] = {}

    for paixu in (1, 2):
        t0 = time.time()
        streak = 0
        pages = 0
        step = int(forum_mod.PAGE_SIZE)
        for index in range(max_pages):
            began = index * step
            try:
                payload = forum_mod.fetch_luntan_page(began, delay=0.05, paixu=paixu)
                listing = forum_mod.parse_list_payload(payload)
            except Exception as exc:  # noqa: BLE001
                say(f"[独立扫描/aocai] paixu={paixu} began={began} 失败：{exc}")
                streak += 1
                if streak >= empty_streak_stop:
                    break
                continue
            if not listing:
                break
            pages += 1
            hit = 0
            for raw in listing:
                if not isinstance(raw, dict):
                    continue
                got = normalize_period(forum_mod.infer_item_period(raw, want))
                if got != want:
                    continue
                tid = int(forum_mod.as_int(raw.get("tid")) or 0)
                if not tid:
                    continue
                hit += 1
                ids.setdefault(
                    tid,
                    {
                        "imgTotal": 0,     # 澳彩列表行没有图片总数，图片在详情里
                        "huifushu": int(forum_mod.as_int(raw.get("huifushu")) or 0),
                    },
                )
            streak = 0 if hit else streak + 1
            if streak >= empty_streak_stop:
                break
        per_sort[f"paixu{paixu}"] = {"pages": pages, "elapsed": round(time.time() - t0, 2)}
        say(f"[独立扫描/aocai] paixu={paixu}: {pages} 页，累计本期 {len(ids)} 帖")

    return {
        "period": want,
        "source": "aocai",
        "unionIds": sorted(ids),
        "unionMeta": {str(k): v for k, v in ids.items()},
        "countUnion": len(ids),
        "perSort": per_sort,
        "agree": True,
    }


def independent_scan(
    source: str,
    period: int,
    max_pages: int = 200,
    created_before: float = 0.0,
    log=None,
) -> dict:
    """按源分派独立扫描。

    `created_before`（unix 秒）：只统计采集开始之前就存在的帖子。
    当期仍在发布时，采集后再扫必然多出新帖，不时间对齐就会把"涨期"误判成"漏帖"
    （2026-11-16 实测：云端 984 帖 vs 2 小时后重扫 996 帖，差 12 帖全是新发的）。
    """
    if source == "dahkg":
        return independent_scan_dahkg(
            period, max_pages=max_pages, created_before=created_before, log=log
        )
    if source == "tuku":
        return independent_scan_tuku(
            period, max_pages=min(max_pages, 60), created_before=created_before, log=log
        )
    if source == "aocai":
        return independent_scan_aocai(
            period, max_pages=max_pages, created_before=created_before, log=log
        )
    raise ValueError(f"未知源：{source}")


# ---------------------------------------------------------------------------
# 「这帖在源站还在不在」——逐帖求证（判定多出/幽灵的唯一可靠依据）
# ---------------------------------------------------------------------------
def probe_details(
    source: str,
    ids: list[int],
    log: Callable[[str], None] | None = None,
    target_period: int = 0,
) -> dict:
    """逐帖调源站**详情接口**确认存在性。

    为什么必须有这一步（2026-11-16 实测，期号 320）：
      列表接口每个排序窗口只给 1000 行且顺序不稳定（同一秒内多次请求返回的行集合会变），
      所以「独立扫描看不到某帖」**不等于**「产物里那帖是错的」：
      实测 42969692 / 42969732 / 42969831 三帖在列表 10 个窗口前 55 页都找不到，
      但详情接口都能正常返回 period=320 —— 那是**扫描盲区**，不是漏抓也不是多抓。

    返回 {"exists": {id: bool}, "periods": {id: period}}
    """
    import time as _time

    say = log or (lambda message: None)
    exists: dict[int, bool] = {}
    periods: dict[int, int] = {}

    if source == "tuku":
        from core import scraper

        for aid in ids:
            try:
                raw = scraper.api_get("/unite49/h5/article/detail", {"articleId": aid}, delay=0)
                ok = isinstance(raw, dict) and int(raw.get("articleId") or 0) == int(aid)
                exists[aid] = ok
                if ok:
                    periods[aid] = normalize_period(raw.get("period"))
            except Exception as exc:  # noqa: BLE001 - 详情失败=源站已无该帖
                exists[aid] = False
                say(f"[求证] tuku {aid}: 源站已无此帖（详情接口无有效返回，符合预期）")
            _time.sleep(0.05)

    elif source == "dahkg":
        from core import dahkg

        for aid in ids:
            try:
                raw = dahkg.fetch_detail(aid)
                ok = isinstance(raw, dict) and bool(raw)
                exists[aid] = ok
                if ok:
                    periods[aid] = normalize_period(raw.get("periods") or raw.get("period"))
            except Exception as exc:  # noqa: BLE001
                exists[aid] = False
                say(f"[求证] dahkg {aid}: 源站已无此帖（详情接口无有效返回，符合预期）")
            _time.sleep(0.05)

    elif source == "aocai":
        from core import forum as forum_mod

        for aid in ids:
            try:
                raw = forum_mod.fetch_thread_detail(aid, 0, include_replies=False, delay=0.0, max_pages=1)
                tiezi = (raw or {}).get("tiezi") or {}
                ok = bool(tiezi)
                exists[aid] = ok
                if ok:
                    # 实测（2026-11-16）：详情里 `qs` 常为 0，期号只能靠标题标记推断
                    periods[aid] = normalize_period(
                        forum_mod.infer_item_period(tiezi, target_period)
                    )
            except Exception as exc:  # noqa: BLE001 - 不存在时接口会直接报错，属预期
                exists[aid] = False
                say(f"[求证] aocai {aid}: 源站已无此帖（详情接口无有效返回，符合预期）")
            _time.sleep(0.05)

    else:
        say(f"[求证] {source} 暂无详情求证实现（只靠快照分类）")
        for aid in ids:
            exists[aid] = False

    alive = sum(1 for v in exists.values() if v)
    say(f"[求证] {source}: 求证 {len(ids)} 帖，源站仍在 {alive}，已消失 {len(ids) - alive}")
    return {"exists": exists, "periods": periods}


# ---------------------------------------------------------------------------
# 防线 1 第 2 道：帖子数对账
# ---------------------------------------------------------------------------
def reconcile_posts(
    posts: list[dict],
    scan: dict,
    previous_ids: set[int] | None = None,
    probe: dict | None = None,
    target_period: int = 0,
) -> dict:
    """帖子数对账。

    分类（每一类都明写，不静默）：
      * `missing`      源站列表有、产物没有 → **漏帖，硬失败**（R11 红线）
      * `scanGap`      产物有、列表没扫到、但**详情接口证明它存在且期号正确**
                       → 属列表窗口(1000 行上限 + 顺序不稳)的扫描盲区，不是漏抓，不判失败
      * `disappeared`  产物有、列表没扫到、**详情接口证明已不在源站**
                       → 源站删帖（文档防线3 第3道），如实记录，不判失败
      * `phantom`      产物有、列表没扫到、详情拿不到且上轮快照也没有 → **疑似垃圾数据，硬失败**
      * `late`         采集开始之后才出现/变更的帖子 → 两边都不计入（时间对齐）
    """
    got = {int(p.get("articleId") or 0) for p in posts if p.get("articleId")}
    late = {int(x) for x in (scan.get("lateIds") or [])}
    prev = {int(x) for x in (previous_ids or set())}
    want = set(scan.get("unionIds") or [])
    want_period = normalize_period(target_period) or int(scan.get("period") or 0)

    missing = sorted(want - got)
    extra_all = got - want - late
    probe_exists = (probe or {}).get("exists") or {}
    probe_periods = (probe or {}).get("periods") or {}

    scan_gap: list[int] = []
    disappeared: list[int] = []
    phantom: list[int] = []
    unverified: list[int] = []
    probe_available = bool(probe_exists)
    for aid in sorted(extra_all):
        if not probe_available:
            # 求证整体不可用（接口故障）→ 不冤枉产物，也不静默：单列待人工复核
            unverified.append(aid)
            continue
        if aid in probe_exists:
            if probe_exists[aid]:
                period_ok = (not want_period) or (normalize_period(probe_periods.get(aid)) == want_period)
                if period_ok:
                    scan_gap.append(aid)
                else:
                    # 源站有这帖，但期号不是本期 → 内容归属错了，必须失败
                    phantom.append(aid)
                continue
            # 源站已无此帖
            if aid in prev:
                disappeared.append(aid)
            else:
                phantom.append(aid)
        else:
            # 求证请求里没覆盖到它（理论上不会发生）
            unverified.append(aid)

    late_in_products = sorted(got & late)
    return {
        "expected": len(want),
        "actual": len(got),
        "lateExcluded": len(late),
        "lateButCollected": len(late_in_products),
        "missingCount": len(missing),
        "scanGapCount": len(scan_gap),
        "disappearedCount": len(disappeared),
        "phantomCount": len(phantom),
        "unverifiedCount": len(unverified),
        "extraCount": len(scan_gap) + len(disappeared) + len(phantom) + len(unverified),
        "missing": missing[:50],
        "scanGap": scan_gap[:50],
        "disappeared": disappeared[:50],
        "phantom": phantom[:50],
        "unverified": unverified[:50],
        "passed": not missing and not phantom,
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
    no_declared = 0
    # 图片文件是否存在**只能**在有图片的目录里检查。
    # 数据仓按设计不含 images（4.2：图片不上仓库），拿仓库目录来查会把 243 张全判缺失。
    files_checkable = bool(require_files is not None and (Path(require_files) / "images").is_dir())

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
        if files_checkable:
            for img in body_imgs:
                lp = img.get("localPath")
                if lp and not (Path(require_files) / str(lp)).exists():
                    missing_files.append(str(lp))
        # 源站行里没有图片数字段（如澳彩）→ 无法逐帖比声明，只统计不判定
        if declared == 0:
            no_declared += 1
            delta = 0 if not body_imgs else 0
        else:
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
        "noDeclaredPosts": no_declared,
        "filesChecked": files_checkable,
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


#: 文档 3.5 的 21 个产物（作为**参照清单**保留）
PRODUCTS_DOC_21 = [
    "users.json", "posts.json", "replies.json", "claims.json", "ocr.json",
    "全站文字版.json", "全站文字版.html", "全站文字版.txt", "六合_看板.html",
    "六合_结构化.xlsx", "claims.csv", "ocr.csv", "replies.csv", "posts.csv",
    "users.csv", "论坛原帖.html", "report.html", "report.md", "validation.json",
    "run_meta.json", "manifest.json",
]

#: **CLI 实际产物**（实测 2026-11-16，逐期目录比对出来的）
#:
#: 关键事实：文档列的 21 项里，`全站文字版.{json,html,txt}` 与 `report.html`
#: **不是 CLI 产出的** —— 实测 `core.dahkg` 的 CLI 跑完只有 17 个文件，
#: 而 314/316 期（用户用 GUI 跑的）有 21 个。所以：
#:   · 云端（纯 CLI）用 17 项判完整性；
#:   · 那 4 项属于 GUI 侧产物，CLI 跑不出来，不能因此判"产物不全"。
PRODUCTS_CLI = [
    "users.json", "users.csv", "posts.json", "posts.csv", "replies.json", "replies.csv",
    "claims.json", "claims.csv", "ocr.json", "ocr.csv", "六合_看板.html",
    "六合_结构化.xlsx", "论坛原帖.html", "report.md", "validation.json",
    "run_meta.json", "manifest.json",
]

#: GUI 侧才会生成的产物（云端不判定，只在报告里说明）
PRODUCTS_GUI_ONLY = ["全站文字版.json", "全站文字版.html", "全站文字版.txt", "report.html"]

#: 49图库实测产物（22 个，CLI 会生成全站文字版三件套）
PRODUCTS_TUKU = [
    "users.json", "users.csv", "posts.json", "posts.csv", "replies.json", "replies.csv",
    "claims.json", "claims.csv", "ocr.json", "ocr.csv", "全站文字版.json",
    "全站文字版.html", "全站文字版.txt", "文字版.html", "文字版.txt", "当期文字版.txt",
    "六合_看板.html", "六合_结构化.xlsx", "report.md", "validation.json",
    "run_meta.json", "manifest.json",
]

PRODUCTS_BY_SOURCE = {
    "dahkg": PRODUCTS_CLI,
    "aocai": PRODUCTS_CLI,
    "tuku": PRODUCTS_TUKU,
}

#: 兼容旧名字（自检里引用 21 项）
PRODUCTS_21 = PRODUCTS_DOC_21


def check_products(out_dir: Path, source: str = "dahkg") -> dict:
    expected_names = PRODUCTS_BY_SOURCE.get(source, PRODUCTS_CLI)
    present, missing, sizes = [], [], {}
    for name in expected_names:
        p = out_dir / name
        if p.exists() and p.stat().st_size > 0:
            present.append(name)
            sizes[name] = p.stat().st_size
        else:
            missing.append(name)
    gui_present = [n for n in PRODUCTS_GUI_ONLY if (out_dir / n).exists()]
    return {
        "source": source,
        "expected": len(expected_names),
        "present": len(present),
        "missing": missing,
        "sizes": sizes,
        "guiOnlyExpected": PRODUCTS_GUI_ONLY,
        "guiOnlyPresent": gui_present,
        "passed": not missing,
    }
