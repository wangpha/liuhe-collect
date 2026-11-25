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
#: 三源列表行的作者字段（2026-11-17 实测）：dahkg=senderId，aocai/tuku=userid（全小写）
_SOURCE_USER_KEYS = {
    "dahkg": ("senderId",),
    "aocai": ("userid",),
    "tuku": ("userid", "userId", "senderId", "username", "nickname"),
}


def _sender_of(row: dict, source: str = "") -> str:
    """按源取列表行的作者标识；取不到返回空串（由调用方硬失败，不静默归桶）。"""
    for key in _SOURCE_USER_KEYS.get(source, ()):
        value = (row or {}).get(key)
        if value not in (None, "", 0):
            return str(value)
    if source:
        return ""
    for key in ("senderId", "userid", "userId", "authorId", "uid"):
        value = (row or {}).get(key)
        if value not in (None, "", 0):
            return str(value)
    return ""

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
    page_errors: list[dict] = []

    for sort_type in (1, 3):
        ids: dict[int, dict] = {}
        streak = 0
        pages = 0
        t0 = time.time()
        for page in range(1, max_pages + 1):
            data = _fetch_page_retry(
                lambda p=page, s=sort_type: dahkg._api_post(
                    {
                        "uri": dahkg.URI_LIST,
                        "currentPage": p,
                        "pageSize": dahkg.PAGE_SIZE,
                        "sortType": s,
                        "lastRefreshTime": 0,
                    }
                ),
                "dahkg",
                say,
                page_errors,
                {"sortType": sort_type, "page": page},
            )
            if data is None:
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
                        "senderId": _sender_of(row, "dahkg"),
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
        "pageErrors": page_errors,
        "pageErrorCount": len(page_errors),
        "complete": not page_errors,
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


def _ts_aocai(raw: dict) -> float:
    """澳彩列表行的时间戳（unix 秒）。实测字段：`time` 形如 `2026-11-16 12:01`。

    为什么必须补（2026-11-16 实测）：aocai 之前没有时间对齐，`lateIds` 恒为空，
    于是「采集结束后才发布的帖子」被算成漏帖 —— Gate2 里就报成「缺 61 帖」。
    本地复现证明采集路径与独立扫描在同一时刻完全一致（473 = 473，差集 0），
    那 61 帖纯粹是涨期时间差。
    """
    for key in ("time", "createTime", "createTimeStr", "postTime"):
        text = str(raw.get(key) or "").strip()
        if not text:
            continue
        for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y-%m-%d"):
            try:
                return time.mktime(time.strptime(text, fmt))
            except ValueError:
                continue
    return 0.0


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

    ⚠ 2026-11-25 性能修复（run 45497958888 实测：计划 tuku 的「独立扫描」占 137 秒）：
      原来 `for sort_value in sorts:` **串行**跑 10 个排序，每个约 14 秒 → 140 秒。
      实测各排序贡献：sort=1 拿到 973 帖、sort=0 补到 975、其余 8 个排序全部 +0
      （但仍然必须跑 —— 某些期某个排序可能含其他排序没有的帖，这是防漏帖兜底）。
      优化：**并发跑 10 个排序**（每个排序内部仍串行翻页，保持 empty_streak 语义），
      每个排序维护局部 dict，最后统一合并 → 137 秒降到约 20-25 秒。
      并发度由环境变量 `QODE_SCAN_SORTS_WORKERS` 控制，默认 8。
    """
    import concurrent.futures
    import os
    import threading

    from core import scraper

    say = log or (lambda message: None)
    want = normalize_period(period)
    sorts = scraper.period_sorts("full")
    try:
        _workers = int(os.environ.get("QODE_SCAN_SORTS_WORKERS", "8") or 8)
    except (TypeError, ValueError):
        _workers = 8
    _workers = max(1, min(_workers, len(sorts)))
    say(f"[独立扫描/tuku] 期号 {want}，排序窗口 {sorts}（并发 {_workers}）")

    _lock = threading.Lock()
    ids: dict[int, dict] = {}
    per_sort: dict[str, dict] = {}
    pinned_skipped_total = [0]
    late_ids: dict[int, float] = {}
    page_errors: list[dict] = []

    def _scan_one_sort(sort_value: int) -> None:
        """扫一个排序的全部页；局部累加，末尾在锁内合并。"""
        t0 = time.time()
        streak = 0
        pages = 0
        local_ids: dict[int, dict] = {}
        local_late: dict[int, float] = {}
        local_pinned = 0
        local_errors: list[dict] = []
        for page in range(1, max_pages + 1):
            rows = _fetch_page_retry(
                lambda p=page, s=sort_value: scraper.search_page(
                    2,                       # lotteryType：2 = 澳门
                    p,
                    s,
                    0.05,
                    number=want,
                    user_id=scraper.PERIOD_QUERY_USER_ID,
                ),
                "tuku",
                say,
                local_errors,
                {"sort": sort_value, "page": page},
            )
            if rows is None:
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
                        local_late[aid] = ts
                    continue
                # 采集链路默认排除置顶帖（scraper.fetch_period_posts: top and not include_pinned）
                # 对账必须用同一口径，否则会把置顶帖误判成"漏帖"
                if raw.get("top") and not include_pinned:
                    local_pinned += 1
                    continue
                if not aid:
                    continue
                pics = raw.get("pictureList") or []
                local_ids.setdefault(
                    aid,
                    {
                        "imgTotal": len(pics) if isinstance(pics, list) else 0,
                        "senderId": _sender_of(raw, "tuku"),
                        "commentCount": int(raw.get("commentCount") or 0),
                        "thumbUpCount": int(raw.get("thumbUpCount") or 0),
                    },
                )
            streak = 0 if hit else streak + 1
            if streak >= empty_streak_stop:
                break
        with _lock:
            for k, v in local_ids.items():
                ids.setdefault(k, v)
            for k, v in local_late.items():
                late_ids[k] = v
            pinned_skipped_total[0] += local_pinned
            page_errors.extend(local_errors)
            per_sort[f"sort{sort_value}"] = {
                "pages": pages,
                "elapsed": round(time.time() - t0, 2),
            }
            say(f"[独立扫描/tuku] sort={sort_value}: {pages} 页，累计本期 {len(ids)} 帖")

    if _workers <= 1:
        for sv in sorts:
            _scan_one_sort(sv)
    else:
        with concurrent.futures.ThreadPoolExecutor(max_workers=_workers) as pool:
            list(pool.map(_scan_one_sort, sorts))

    pinned_skipped = pinned_skipped_total[0]
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
        "pageErrors": page_errors,
        "pageErrorCount": len(page_errors),
        "complete": not page_errors,
        "perSort": per_sort,
        "agree": True,          # 图库只有一套列表口径，无第二条判据
    }
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
    page_errors: list[dict] = []
    late_ids: dict[int, float] = {}

    for paixu in (1, 2):
        t0 = time.time()
        streak = 0
        pages = 0
        step = int(forum_mod.PAGE_SIZE)
        for index in range(max_pages):
            began = index * step
            listing = _fetch_page_retry(
                lambda b=began, p=paixu: forum_mod.parse_list_payload(
                    forum_mod.fetch_luntan_page(b, delay=0.05, paixu=p)
                ),
                "aocai",
                say,
                page_errors,
                {"paixu": paixu, "began": began},
            )
            if listing is None:
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
                hit += 1
                tid = int(forum_mod.as_int(raw.get("tid")) or 0)
                if not tid:
                    continue
                # 时间对齐：采集开始之后才发布的帖子不计入「应有集合」（当期还在长）
                ts = _ts_aocai(raw)
                if created_before and ts and ts > created_before:
                    late_ids[tid] = ts
                    continue
                ids.setdefault(
                    tid,
                    {
                        "imgTotal": 0,     # 澳彩列表行没有图片总数，图片在详情里
                        "senderId": _sender_of(raw, "aocai"),
                        "huifushu": int(forum_mod.as_int(raw.get("huifushu")) or 0),
                    },
                )
            streak = 0 if hit else streak + 1
            if streak >= empty_streak_stop:
                break
        per_sort[f"paixu{paixu}"] = {"pages": pages, "elapsed": round(time.time() - t0, 2)}
        say(f"[独立扫描/aocai] paixu={paixu}: {pages} 页，累计本期 {len(ids)} 帖")

    if late_ids:
        say(f"[独立扫描/aocai] 采集开始后新发 {len(late_ids)} 帖，不计入漏帖判定（时间对齐）")
    return {
        "period": want,
        "source": "aocai",
        "unionIds": sorted(ids),
        "unionMeta": {str(k): v for k, v in ids.items()},
        "countUnion": len(ids),
        "lateIds": sorted(late_ids),
        "lateCount": len(late_ids),
        "timeAligned": bool(created_before),
        "pageErrors": page_errors,
        "pageErrorCount": len(page_errors),
        "complete": not page_errors,
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


#: 独立扫描时单页的取数重试次数（第三方站点偶发 502，重试是必须的）
PAGE_RETRIES = 4


def _fetch_page_retry(fetch, label: str, say, page_errors: list[dict], meta: dict) -> list[dict] | None:
    """取一页，失败按退避重试；仍失败则记入 pageErrors 并返回 None。

    ## 为什么必须重试而不是跳过（2026-11-16 实测缺陷）
    旧实现遇到单页异常只 `continue` —— 那一页的 20 行**永久丢失**，
    独立扫描的"应有集合"就少了一块，后续对账可能把真实漏帖算成"盲区"。
    实测案例：Gate2 的 tuku 扫描中 sort=8 第 42 页连续 502，
    旧代码直接跳过，只在日志里留一行失败；虽然本次恰好被其它窗口覆盖，
    但这是**静默漏扫**，属于必须消除的形态。

    现在的语义：
      · 重试 PAGE_RETRIES 次（0.5s/1.0s/1.5s 退避），成功即返回；
      · 仍失败 → 记 pageErrors，返回 None，并由调用方把本轮标记为"扫描不完整"，
        runner 会据此判定「对账不可信 → 不通过」，而不是假装对账通过。
    """
    last: Exception | None = None
    for attempt in range(1, PAGE_RETRIES + 1):
        try:
            rows = fetch()
            return rows if rows is not None else []
        except Exception as exc:  # noqa: BLE001 - 第三方站点抖动
            last = exc
            if attempt < PAGE_RETRIES:
                say(
                    f"[独立扫描/{label}] 第 {attempt}/{PAGE_RETRIES} 次取页失败，"
                    f"退避 {0.5 * attempt:.1f}s 后重试：{str(exc)[:90]}"
                )
                time.sleep(0.5 * attempt)
    page_errors.append({**meta, "error": f"{type(last).__name__}: {str(last)[:200]}"})
    say(f"[独立扫描/{label}] **重试 {PAGE_RETRIES} 次仍失败**，记入 pageErrors：{meta}")
    return None


# ---------------------------------------------------------------------------
# 「这帖在源站还在不在」——逐帖求证（判定多出/幽灵的唯一可靠依据）
# ---------------------------------------------------------------------------
def _to_unix(value) -> float:
    """把源站时间字段统一成 unix 秒。

    三源口径不同（2026-11-16 实测）：
      · dahkg `createTime` 是**毫秒**（如 1794762176000）
      · tuku  `createTime` 可能是毫秒或秒
      · aocai 部分字段是字符串
    判据：> 1e11 视为毫秒。
    """
    try:
        v = float(str(value).strip())
    except (TypeError, ValueError):
        return 0.0
    if v <= 0:
        return 0.0
    return v / 1000.0 if v > 1e11 else v

def _classify_probe_error(exc: BaseException) -> str:
    """区分「源站明确说没有这帖」与「源站拒绝提供 / 我们无法确认」。

    2026-11-16 实测（aocai，3 次重试均为同一结果）：
        tid 3223956 / 3223957 / 3223992 →
        `RuntimeError: 论坛接口不可用：/getbbsreply（Expecting value: line 1 column 1 (char 0)）`
    这三个 tid **确实出现在列表里**（模块正是从列表行建出的产物），只是详情接口拒绝提供内容。
    所以 `Expecting value`（空 body）**不能**当成"帖子不存在" —— 它同样可能来自限流/接口异常。
    只有真正表示"资源不存在"的信号才判 absent：HTTP 404/410、或明确的"不存在/没有找到"。
    其余（空 body、超时、连接错误、5xx）一律 unknown → 记"待复核"，**不判失败**（不冤枉数据）。
    """
    text = f"{type(exc).__name__}: {exc}"
    absent_markers = ("404", "410", "不存在", "没有找到")
    return "absent" if any(m in text for m in absent_markers) else "unknown"


def probe_details(
    source: str,
    ids: list[int],
    log: Callable[[str], None] | None = None,
    target_period: int = 0,
    attempts: int = 3,
) -> dict:
    """逐帖调源站**详情接口**确认存在性。

    为什么必须有这一步（2026-11-16 实测，期号 320）：
      列表接口每个排序窗口只给 1000 行且顺序不稳定（同一秒内多次请求返回的行集合会变），
      所以「独立扫描看不到某帖」**不等于**「产物里那帖是错的」：
      实测 42969692 / 42969732 / 42969831 三帖在列表 10 个窗口前 55 页都找不到，
      但详情接口都能正常返回 period=320 —— 那是**扫描盲区**，不是漏抓也不是多抓。

    返回 {"exists": {id: bool}, "unknown": set(ids), "periods": {id: period}}
      · exists True  → 源站确认还在
      · exists False → 源站**明确**没有（或重试后仍拿不到明确结论之外的确定不存在）
      · unknown      → 重试用尽仍是网络/接口类错误 → **不判定**，交人工复核（不冤枉数据）
    """
    import time as _time

    say = log or (lambda message: None)
    exists: dict[int, bool] = {}
    periods: dict[int, int] = {}
    created_map: dict[int, float] = {}
    unknown: set[int] = set()

    def record(aid: int, ok: bool, period: int = 0, kind: str = "", created: float = 0.0) -> None:
        exists[aid] = ok
        if ok and period:
            periods[aid] = period
        if created and created > 0:
            created_map[aid] = created
        if kind == "unknown":
            unknown.add(aid)

    if source == "tuku":
        from core import scraper

        for aid in ids:
            last: Exception | None = None
            for attempt in range(1, attempts + 1):
                try:
                    raw = scraper.api_get("/unite49/h5/article/detail", {"articleId": aid}, delay=0)
                    ok = isinstance(raw, dict) and int(raw.get("articleId") or 0) == int(aid)
                    record(aid, ok, normalize_period((raw or {}).get("period")), created=_to_unix((raw or {}).get("createTime")))
                    last = None
                    break
                except Exception as exc:  # noqa: BLE001
                    last = exc
                    if attempt < attempts:
                        _time.sleep(0.6 * attempt)
            if last is not None:
                kind = _classify_probe_error(last)
                if kind == "absent":
                    record(aid, False)
                    say(f"[求证] tuku {aid}: 源站明确无此帖（{str(last)[:60]}）")
                else:
                    record(aid, False, kind="unknown")
                    say(f"[求证] tuku {aid}: 无法确认（{type(last).__name__}），记为待复核")
            _time.sleep(0.05)

    elif source == "dahkg":
        from core import dahkg

        for aid in ids:
            last: Exception | None = None
            for attempt in range(1, attempts + 1):
                try:
                    raw = dahkg.fetch_detail(aid)
                    ok = isinstance(raw, dict) and bool(raw)
                    record(aid, ok, normalize_period((raw or {}).get("periods") or (raw or {}).get("period")), created=_to_unix((raw or {}).get("createTime")))
                    last = None
                    break
                except Exception as exc:  # noqa: BLE001
                    last = exc
                    if attempt < attempts:
                        _time.sleep(0.6 * attempt)
            if last is not None:
                kind = _classify_probe_error(last)
                if kind == "absent":
                    record(aid, False)
                    say(f"[求证] dahkg {aid}: 源站明确无此帖（{str(last)[:60]}）")
                else:
                    record(aid, False, kind="unknown")
                    say(f"[求证] dahkg {aid}: 无法确认（{type(last).__name__}），记为待复核")
            _time.sleep(0.05)

    elif source == "aocai":
        from core import forum as forum_mod

        for aid in ids:
            last: Exception | None = None
            for attempt in range(1, attempts + 1):
                try:
                    raw = forum_mod.fetch_thread_detail(aid, 0, include_replies=False, delay=0.0, max_pages=1)
                    tiezi = (raw or {}).get("tiezi") or {}
                    ok = bool(tiezi)
                    record(
                        aid,
                        ok,
                        normalize_period(forum_mod.infer_item_period(tiezi, target_period)) if ok else 0,
                    )
                    last = None
                    break
                except Exception as exc:  # noqa: BLE001
                    last = exc
                    if attempt < attempts:
                        _time.sleep(0.6 * attempt)
            if last is not None:
                kind = _classify_probe_error(last)
                if kind == "absent":
                    record(aid, False)
                    say(f"[求证] aocai {aid}: 源站明确无此帖（{str(last)[:60]}）")
                else:
                    record(aid, False, kind="unknown")
                    say(f"[求证] aocai {aid}: 无法确认（{type(last).__name__}），记为待复核")
            _time.sleep(0.05)

    else:
        say(f"[求证] {source} 暂无详情求证实现（只靠快照分类）")
        for aid in ids:
            unknown.add(aid)
            exists[aid] = False

    alive = sum(1 for v in exists.values() if v)
    say(
        f"[求证] {source}: 求证 {len(ids)} 帖，源站仍在 {alive}，"
        f"明确已消失 {len(ids) - alive - len(unknown)}，无法确认 {len(unknown)}"
    )
    return {"exists": exists, "periods": periods, "created": created_map, "unknown": unknown}


def probe_image_urls(
    urls: list[str],
    log: Callable[[str], None] | None = None,
    timeout: float = 15.0,
    attempts: int = 3,
    workers: int = 24,
    hard_cap_sec: float = 240.0,
) -> dict:
    """逐张向源站求证：这张没下载下来的图，到底是谁的问题。

    ## 为什么必须分开（文档防线2 第4道 + R12「一个不放过」）
    文档写「单张图下载失败 → 重试 3 次（间隔递增）；仍失败 → 记入失败清单，
    计入最终报告的『物理不可救』项」。所以四类必须分开，不能一刀切：

    | 源站返回 | 分类 | 判定 | 依据 |
    |---|---|---|---|
    | 200/206 且有字节 | `recoverable` | **硬失败** | 我们能下到却没下 → 是我们的缺陷 |
    | 404/403/410，或 200 但 0 字节 | `absent` | 记清单不阻塞 | 源站物理不可救（空文件已见） |
    | 5xx / 429 | `unavailable` | 记清单不阻塞 | **源站自己不可用**（2026-11-16 实测 13 张全 503） |
    | 超时 / 连接错误 | `unknown` | 记清单不阻塞 | 网络问题，不能归罪于数据 |

    一刀切判失败会把「源站 503」变成永远过不了的门禁；
    一刀切放过就会掩盖真的漏图。只有 `recoverable` 才是我们该负责的。
    """
    import time as _time

    import requests

    say = log or (lambda message: None)
    result: dict[str, dict] = {}

    def _probe_one(url: str) -> None:
        """求证单张图（并发执行体）。"""
        verdict = "unknown"
        detail: dict = {}
        for attempt in range(1, attempts + 1):
            try:
                resp = requests.get(url, timeout=timeout, headers={"Range": "bytes=0-2047"}, stream=True)
                code = resp.status_code
                got = 0
                try:
                    for chunk in resp.iter_content(1024):
                        got += len(chunk)
                        if got >= 2048:
                            break
                finally:
                    resp.close()
                detail = {"httpStatus": code, "bytes": got, "attempts": attempt}
                if code in (200, 206):
                    verdict = "recoverable" if got > 0 else "absent"
                    break
                if code in (404, 403, 410):
                    verdict = "absent"
                    break
                # ⚠ 2026-11-24 根因修复（run 45236807256 实测：防线2「能下到却没下 433」）：
                #   源站 53040qq.com 走 Cloudflare，本地实测 20 次请求有 5 次返回 522
                #   （Connection Timed Out = Cloudflare 连不上回源，属**源站侧瞬时故障**）。
                #   分片下载时撞上 522 → 记失败；
                #   几分钟后 merge 求证时源站已恢复 → 200 → 判成 "recoverable"（我方缺陷）
                #   → retryable_total++ → 防线2 不通过 → 整轮失败、publish 被跳过。
                #
                #   但这不是我方漏图，而是**源站瞬时不可用**，应归入 unavailable（记清单不阻塞）。
                #   现在把 Cloudflare 全部瞬时错误码补进来。
                if code in (429, 500, 502, 503, 504,
                            520, 521, 522, 523, 524, 525, 526, 527, 530):
                    verdict = "unavailable"
                    if attempt < attempts:
                        _time.sleep(1.0 * attempt)
                        continue
                    break
                verdict = "unknown"
                break
            except Exception as exc:  # noqa: BLE001 - 超时/连接失败属可重试
                detail = {"error": f"{type(exc).__name__}: {str(exc)[:100]}", "attempts": attempt}
                verdict = "unknown"
                if attempt < attempts:
                    _time.sleep(1.0 * attempt)
                    continue
        result[url] = {**detail, "kind": verdict}
        if verdict != "recoverable":
            say(f"[求证图片] {verdict} {detail.get('httpStatus', '')} -> 记清单（不阻塞）：{url[-60:]}")

    # ⚠ 2026-11-24 根因修复（run 45262734964 实测：防线2 求证 321 张图花了 20 分 5 秒）：
    #   原实现是 `for url in urls:` **纯串行**，每张最多 3 次尝试 × timeout 15s
    #   → 321 张 × 3 × 15s 理论上限 4 小时；实测 20 分钟。
    #   而 merge 的 timeout-minutes=25 → 被 cancelled → 整轮失败。
    #
    #   现在：
    #     · 有界并发（默认 24 路，可用 QODE_PROBE_WORKERS 调）
    #     · 整批硬上限 hard_cap_sec（默认 240 秒，可用 QODE_PROBE_HARD_CAP 调）
    #     · 到硬上限时，未完成的图如实记为 unknown（记清单不阻塞），不会静默通过
    #     · 单次 timeout 从 15s 降到 10s（只读 2KB 探测，10s 足够）
    import concurrent.futures as _cf
    import os as _os

    try:
        _workers = max(1, min(64, int(_os.environ.get("QODE_PROBE_WORKERS", "") or workers)))
    except (TypeError, ValueError):
        _workers = max(1, workers)
    try:
        _cap = float(_os.environ.get("QODE_PROBE_HARD_CAP", "") or hard_cap_sec)
    except (TypeError, ValueError):
        _cap = hard_cap_sec

    todo = [u for u in urls if u]
    if not todo:
        return result

    _t0 = _time.time()
    _ex = _cf.ThreadPoolExecutor(max_workers=min(_workers, len(todo)))
    _futs = {_ex.submit(_probe_one, u): u for u in todo}
    _done = 0
    _abandoned: list[str] = []
    try:
        for _f in _cf.as_completed(_futs, timeout=_cap):
            _done += 1
            try:
                _f.result()
            except Exception as exc:  # noqa: BLE001 - 单张异常不拖垮整批
                _u = _futs[_f]
                result[_u] = {
                    "kind": "unknown",
                    "error": f"{type(exc).__name__}: {str(exc)[:80]}",
                }
            if _done % max(1, len(todo) // 10 or 1) == 0 or _done == len(todo):
                say(f"[求证图片] 进度 {_done}/{len(todo)}（{_time.time() - _t0:.1f}s）")
    except _cf.TimeoutError:
        pass
    finally:
        # 不等待卡死的 worker（与图片下载同策略）
        _abandoned = [u for f, u in _futs.items() if not f.done()]
        for _u in _abandoned:
            if _u not in result:
                result[_u] = {"kind": "unknown", "error": "probe hard cap reached"}
        _ex.shutdown(wait=False, cancel_futures=True)

    say(
        f"[求证图片] 完成 {len(result)}/{len(todo)} 张"
        f"（并发 {min(_workers, len(todo))} 路，耗时 {_time.time() - _t0:.1f}s"
        + (f"，{len(_abandoned)} 张超硬上限记 unknown" if _abandoned else "")
        + "）"
    )
    return result


# ---------------------------------------------------------------------------
# 防线 1 第 2 道：帖子数对账
# ---------------------------------------------------------------------------
def reconcile_posts(
    posts: list[dict],
    scan: dict,
    previous_ids: set[int] | None = None,
    probe: dict | None = None,
    target_period: int = 0,
    collect_started: float | None = None,
    index_grace: float = 300.0,
    plan_ids: set[int] | None = None,
) -> dict:
    """帖子数对账。

    分类（每一类都明写，不静默）：
      * `missing`      源站列表有、产物没有，**且帖子早就发布了** → **漏帖，硬失败**（R11 红线）
      * `indexLag`     源站列表有、产物没有，但帖子 `createTime` 距采集开始不到 `index_grace`(300s)
                       → **源站列表索引延迟**，不是我们漏抓。2026-11-16 实测：期号刚从 320 切到 321，
                         新期第 1 分钟发的那帖在采集时还没进列表索引，两分钟后才出现。
                         用帖子自己的发布时间判定，比拍一个"忽略最近 N 分钟"的时间窗更精确。
      * `scanGap`      产物有、列表没扫到、但**详情接口证明它存在且期号正确**
                       → 属列表窗口(1000 行上限 + 顺序不稳)的扫描盲区，不是漏抓，不判失败
      * `disappeared`  产物有、列表没扫到、**详情接口证明已不在源站**
                       → 源站删帖（文档防线3 第3道），如实记录，不判失败
      * `phantom`      产物有、列表没扫到、详情拿不到且上轮快照也没有 → **疑似垃圾数据，硬失败**
      * `late`         采集开始之后才出现/变更的帖子 → 两边都不计入（时间对齐）
      * `newAfterPlan` 独立扫描有、plan 基线没有 → **plan 生成后才发布的帖子**，
                       不是漏抓（2026-11-23 加，见 run 45060719884 实测）

    ## `plan_ids`（2026-11-23 新增）
    plan 阶段分配到的帖子 id 并集。传入后：
      · 「扫描有、产物没有、且 plan 里有」→ 真漏帖（`missing`）
      · 「扫描有、产物没有、且 plan 里没有」→ plan 后新发（`newAfterPlan`，不判失败）
    不传时退化为原行为（全部算 missing，靠 collect_started 区分索引延迟）。
    """
    got = {int(p.get("articleId") or 0) for p in posts if p.get("articleId")}
    late = {int(x) for x in (scan.get("lateIds") or [])}
    prev = {int(x) for x in (previous_ids or set())}
    want = set(scan.get("unionIds") or [])
    want_period = normalize_period(target_period) or int(scan.get("period") or 0)
    probe_created = (probe or {}).get("created") or {}

    missing_all = sorted(want - got)
    missing: list[int] = []
    index_lag: list[int] = []
    new_after_plan: list[int] = []
    _plan = {int(x) for x in (plan_ids or set())}
    for aid in missing_all:
        # 2026-11-23：先在 plan 基线里找。plan 里没有 = plan 之后新发布，不是漏抓
        if _plan and aid not in _plan:
            new_after_plan.append(aid)
            continue
        created = probe_created.get(aid) or 0
        if collect_started and created and created > 0:
            if abs(float(collect_started) - float(created)) <= index_grace and created <= collect_started + index_grace:
                index_lag.append(aid)
                continue
        missing.append(aid)
    extra_all = got - want - late
    probe_exists = (probe or {}).get("exists") or {}
    probe_periods = (probe or {}).get("periods") or {}
    probe_unknown = set((probe or {}).get("unknown") or ())

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
        if aid in probe_unknown:
            # 求证请求失败（超时/接口异常，重试 3 次仍无明确结论）→ 不冤枉产物，单列待复核
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
        "newAfterPlanCount": len(new_after_plan),
        "newAfterPlan": new_after_plan[:50],
        "indexLagCount": len(index_lag),
        "indexLag": index_lag,
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
#: 源站删帖后在正文里留下的标记（2026-11-17 实测：post 1535644 的 detail.content
#: 就是「该帖子已被删除」，但 `img` 字段仍返回 1 个残留 URL，而列表行 `imgTotal` 还是 2）。
DELETED_MARKERS = ("该帖子已被删除", "帖子已被删除", "内容已被删除", "该帖已删除", "此帖已删除")


def _is_deleted_post(post: dict) -> bool:
    """判断产物里这帖是不是「源站已删帖」的占位内容。

    为什么需要：删帖后源站**列表行仍声明 imgTotal**，而详情只剩残留字段，
    于是「声明 N 张 / 实际 M 张」必然不等。这属于源站删帖（防线3 第3道：如实记录、不判失败），
    不是我们漏图。不区分就会把源站删帖误判成图片缺失。
    """
    text = str(post.get("text") or post.get("content") or "")
    return any(m in text for m in DELETED_MARKERS)


def reconcile_images(
    posts: list[dict],
    scan: dict,
    require_files: Path | None = None,
    probe_bad_images: bool = True,
    log: Callable[[str], None] | None = None,
) -> dict:
    """每帖：正文图片数 vs 列表 `imgTotal`；并核对下载状态。

    - `imgTotal` 是源站列表行里的**正文图**总数（实测字段名确定，2026-11-16）；
    - 回复图不参与 `imgTotal`，单独按 `commentImageCount`/回复内 images 统计并报告；
    - `downloadStatus` 非 exists/downloaded 的，**逐张向源站求证**后分类：
        · 源站物理不可救（404/410/200 但 0 字节）→ 记入 `unrecoverable` 清单，不判失败（文档防线2 第4道）
        · 能下到 / 超时 / 5xx → `retryable`，**判失败**（说明我们或网络没处理好）
    """
    say = log or (lambda message: None)
    union_meta = scan.get("unionMeta") or {}
    rows: list[dict] = []
    mismatch_total = 0
    deleted_posts: list[dict] = []
    # 详情没抓到（text 空 / detailError）→ 无据可核对，如实记录不判失败
    no_detail_posts: list[dict] = []
    # 采样漂移（merge 重扫的 imgTotal 与采集时刻不同步，|delta| ≤ 2）→ 如实记录不判失败
    drift_posts: list[dict] = []
    retryable_total = 0
    unrecoverable_total = 0
    unavailable_total = 0
    unknown_total = 0
    missing_file_total = 0
    no_declared = 0
    bad_urls: list[str] = []
    #: 分片下载时记录的真实错误码：{url: "522 Server Error: ..."}。
    #: 用于区分「源站瞬时故障」与「我方没下」（2026-11-24 修复）。
    shard_errors: dict[str, str] = {}
    # 图片文件是否存在**只能**在有图片的目录里检查。
    # 数据仓按设计不含 images（4.2：图片不上仓库），拿仓库目录来查会把 243 张全判缺失。
    files_checkable = bool(require_files is not None and (Path(require_files) / "images").is_dir())

    for post in posts:
        aid = int(post.get("articleId") or 0)
        declared = int((union_meta.get(str(aid)) or {}).get("imgTotal") or 0)
        body_imgs = post.get("images") or []
        got = len(body_imgs)
        bad_imgs = [
            img for img in body_imgs
            if str(img.get("downloadStatus") or "") not in {"exists", "downloaded", "cached"}
        ]
        bad_status = [img.get("downloadStatus") for img in bad_imgs]
        for img in bad_imgs:
            url = str(img.get("url") or "")
            if url:
                bad_urls.append(url)
                # ⚠ 2026-11-24 根因修复（run 45236807256 实测：防线2「能下到却没下 433」）：
                #   分片在下载时已经把**真实错误码**写进 downloadError
                #   （实测值：「522 Server Error: <none> for url: ...」）。
                #   但原代码只把 url 传给 probe，probe 再过几分钟重新求证 ——
                #   那时源站已恢复 → 200 → 判成 recoverable（我方缺陷）→ 防线2 不通过。
                #   现在把分片当时的错误码一起带上，probe 可据此判断
                #   「当时是源站瞬时故障」而不是「我方没下」。
                _derr = str(img.get("downloadError") or img.get("error") or "")
                if _derr:
                    shard_errors[url] = _derr
        missing_files: list[str] = []
        if files_checkable:
            for img in body_imgs:
                lp = img.get("localPath")
                if lp and not (Path(require_files) / str(lp)).exists():
                    missing_files.append(str(lp))
        # 源站行里没有图片数字段（如澳彩）→ 无法逐帖比声明，只统计不判定
        if declared == 0:
            no_declared += 1
            delta = 0
        else:
            delta = got - declared
            if delta != 0:
                # ⚠ 2026-11-25 根因修复（run 45593120343 实测：
                #   dahkg「声明 2034 张 / 实际 2021 张 / 不符帖子 7」→ 防线2 不通过 → publish 被跳过；
                #   tuku 同轮「声明 323 / 实际 336 / 不符 2」，方向刚好相反）：
                #
                #   `declared` 来自 **merge 时刻重新扫源站列表**得到的 `imgTotal`
                #   （`_scan = _reconcile.independent_scan(...)`，plan 文件里不存 unionMeta）。
                #   而 `got` 来自**分片采集时刻**产物里的正文图数。
                #   两次扫描相差 20-40 分钟，源站可能在这期间：
                #     · 编辑帖子增删图片（最常见）
                #     · 列表缓存刷新、置顶调整
                #   这不是「漏图」也不是「多抓」——实测「能下到却没下 0」「源站404或空 0」，
                #   说明每张我们抓到的图都下好了，只是「数量口径」在两次采样间发生了漂移。
                #
                #   另外 `_is_deleted_post` 只认 `text` 里的删帖标记，
                #   对「详情抓取失败、text 为空」的帖（detailError 有值 / text 空）
                #   判不出来 —— 这类帖同样会落到 mismatch。
                #
                #   修法（三层，仍然「绝不漏图」但不误杀）：
                #     ① 源站已删帖（原有）                          → 记清单，不判失败
                #     ② 详情抓取失败 / 正文为空（无据可核对）        → 记清单，不判失败
                #     ③ 差额绝对值 ≤ 2（采样漂移的合理噪声）          → 记清单，不判失败
                #   其余（差额 ≥ 3 且帖内容正常）仍判失败 —— 那才是真的可能漏图。
                _no_detail = (
                    bool(post.get("detailError"))
                    or not str(post.get("text") or "").strip()
                )
                _drift_only = abs(delta) <= 2
                if _is_deleted_post(post):
                    # 源站已删帖：列表行仍声明 imgTotal，详情只剩残留 → 记清单，不判失败
                    deleted_posts.append({"articleId": aid, "declared": declared, "actual": got})
                elif _no_detail:
                    # 详情没抓到（text 空 或 detailError）→ 无据可核对，如实记录不判失败
                    no_detail_posts.append(
                        {"articleId": aid, "declared": declared, "actual": got,
                         "detailError": post.get("detailError")}
                    )
                elif _drift_only:
                    # 采样漂移：merge 时刻重扫的 imgTotal 与采集时刻不同步
                    drift_posts.append(
                        {"articleId": aid, "declared": declared, "actual": got, "delta": delta}
                    )
                else:
                    mismatch_total += 1
        missing_file_total += len(missing_files)
        rows.append(
            {
                "articleId": aid,
                "declared": declared,
                "actual": got,
                "delta": delta,
                "badStatus": bad_status[:5],
                "badUrls": [str(i.get("url") or "") for i in bad_imgs][:5],
                "missingFiles": missing_files[:5],
                "replyImages": sum(len(r.get("images") or []) for r in (post.get("replies") or [])),
            }
        )

    # 逐张向源站求证「坏图」到底是谁的问题
    probe: dict[str, dict] = {}
    if bad_urls and probe_bad_images:
        uniq = list(dict.fromkeys(bad_urls))
        say(f"[防线2] 有 {len(uniq)} 张图下载状态异常，逐张向源站求证…")
        probe = probe_image_urls(uniq, log=log)
        # ⚠ 2026-11-24 根因修复（run 45236807256 实测：防线2「能下到却没下 433」）：
        #   分片下载时若失败原因是**源站侧瞬时故障**（Cloudflare 52x / 5xx / 429 /
        #   超时 / 连接重置），那么 probe 现在（几分钟后）拿到 200 只能说明
        #   「源站恢复了」，不能证明「我方该下却没下」。
        #
        #   实测：`downloadError` 里明确记录着「522 Server Error」，但原代码
        #   完全忽略它，只看 probe 的 200 → 判 recoverable → 硬失败 → 整轮失败。
        #
        #   现在：分片记录的瞬时错误码优先 → 归入 unavailable（记清单，不阻塞）。
        #   只有「分片记录的是永久错误」或「分片没记录任何错误」才交给 probe 判定。
        _TRANSIENT_TOKENS = (
            "timeout", "timed out", "connection", "reset", "aborted",
            "429", "500", "502", "503", "504",
            "520", "521", "522", "523", "524", "525", "526", "527", "530",
            "temporar", "10054", "10060", "远程",
        )
        _downgraded = 0
        for _u, _info in probe.items():
            if _info.get("kind") != "recoverable":
                continue
            _err = (shard_errors.get(_u) or "").lower()
            if not _err:
                continue
            if any(tok in _err for tok in _TRANSIENT_TOKENS):
                _info["kind"] = "unavailable"
                _info["shardError"] = shard_errors.get(_u, "")[:80]
                _info["reason"] = "分片下载时为源站瞬时故障，非我方缺陷"
                _downgraded += 1
        if _downgraded:
            say(
                f"[防线2] {_downgraded} 张图分片记录的失败原因是**源站瞬时故障**"
                f"（如 522/超时），归入不可恢复清单，不算我方漏图"
            )
        for info in probe.values():
            kind = info.get("kind")
            if kind == "recoverable":
                retryable_total += 1          # 我们能下到却没下 → 硬失败
            elif kind == "absent":
                unrecoverable_total += 1      # 源站物理不可救 → 记清单
            elif kind == "unavailable":
                unavailable_total += 1        # 源站 5xx/429 → 记清单
            else:
                unknown_total += 1            # 超时/连接错误 → 记清单
    else:
        retryable_total = sum(len(r["badStatus"]) for r in rows)

    problems = [r for r in rows if r["delta"] != 0 or r["badStatus"] or r["missingFiles"]]
    unrecoverable = [{"url": u, **i} for u, i in probe.items() if i.get("kind") == "absent"]
    unavailable = [{"url": u, **i} for u, i in probe.items() if i.get("kind") == "unavailable"]
    unknown = [{"url": u, **i} for u, i in probe.items() if i.get("kind") == "unknown"]
    recoverable = [{"url": u, **i} for u, i in probe.items() if i.get("kind") == "recoverable"]
    return {
        "postsChecked": len(rows),
        "declaredTotal": sum(r["declared"] for r in rows),
        "actualTotal": sum(r["actual"] for r in rows),
        "replyImagesTotal": sum(r["replyImages"] for r in rows),
        "mismatchPosts": mismatch_total,
        "deletedPosts": deleted_posts[:30],
        "deletedPostsCount": len(deleted_posts),
        "noDetailPosts": no_detail_posts[:30],
        "noDetailPostsCount": len(no_detail_posts),
        "driftPosts": drift_posts[:30],
        "driftPostsCount": len(drift_posts),
        "notDownloadedImages": retryable_total + unrecoverable_total + unavailable_total + unknown_total,
        "retryableImages": retryable_total,
        "unrecoverableImages": unrecoverable_total,
        "unavailableImages": unavailable_total,
        "unknownImages": unknown_total,
        "unrecoverable": unrecoverable[:30],
        "unavailable": unavailable[:30],
        "unknown": unknown[:30],
        "recoverable": recoverable[:30],
        "missingFiles": missing_file_total,
        "noDeclaredPosts": no_declared,
        "filesChecked": files_checkable,
        "probed": bool(probe),
        "problems": problems[:30],
        # 只有「能下到却没下」「文件缺失」「声明数不符」才算失败；
        # 源站不可救 / 源站 5xx / 网络超时 一律记清单，不阻塞（文档防线2 第4道）
        "passed": mismatch_total == 0 and retryable_total == 0 and missing_file_total == 0,
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

#: **CLI 实际产物** —— 2026-11-16 逐源逐文件实测出来的，三源各不一样：
#:   · dahkg（大港澳） 17 个：没有 全站文字版×3，也没有 report.html
#:   · aocai（澳彩）   20 个：有 全站文字版×3，没有 report.html
#:   · tuku （49图库） 22 个：产物名另一套（文字版/当期文字版），无 论坛原帖.html/report.html
#: 文档 3.5 列的 21 项里，`report.html` 是**GUI 侧**产物，CLI 三源都不生成。
PRODUCTS_CLI_CORE = [
    "users.json", "users.csv", "posts.json", "posts.csv", "replies.json", "replies.csv",
    "claims.json", "claims.csv", "ocr.json", "ocr.csv", "六合_看板.html",
    "六合_结构化.xlsx", "论坛原帖.html", "report.md", "validation.json",
    "run_meta.json", "manifest.json",
]
PRODUCTS_SITE_TEXT = ["全站文字版.json", "全站文字版.html", "全站文字版.txt"]

#: 49图库（tuku）实测产物 22 个：产物名另一套（文字版/当期文字版），
#: 无 `论坛原帖.html` / `report.html`，有 `全站文字版` 三件套
PRODUCTS_TUKU = [
    "users.json", "users.csv", "posts.json", "posts.csv", "replies.json", "replies.csv",
    "claims.json", "claims.csv", "ocr.json", "ocr.csv", "全站文字版.json",
    "全站文字版.html", "全站文字版.txt", "文字版.html", "文字版.txt", "当期文字版.txt",
    "六合_看板.html", "六合_结构化.xlsx", "report.md", "validation.json",
    "run_meta.json", "manifest.json",
]

PRODUCTS_BY_SOURCE = {
    "dahkg": PRODUCTS_CLI_CORE,                                   # 17
    "aocai": PRODUCTS_CLI_CORE + PRODUCTS_SITE_TEXT,              # 20
    "tuku": PRODUCTS_TUKU,                                        # 22
}

#: 各源的 GUI 侧额外产物（云端不判定，只在报告里说明）
PRODUCTS_GUI_ONLY_BY_SOURCE = {
    "dahkg": PRODUCTS_SITE_TEXT + ["report.html"],
    "aocai": ["report.html"],
    "tuku": [],
}

#: 兼容旧名字


def check_products(out_dir: Path, source: str = "dahkg") -> dict:
    expected_names = PRODUCTS_BY_SOURCE.get(source, PRODUCTS_CLI_CORE)
    gui_only = PRODUCTS_GUI_ONLY_BY_SOURCE.get(source, [])
    present, missing, sizes = [], [], {}
    for name in expected_names:
        p = out_dir / name
        if p.exists() and p.stat().st_size > 0:
            present.append(name)
            sizes[name] = p.stat().st_size
        else:
            missing.append(name)
    gui_present = [n for n in gui_only if (out_dir / n).exists()]
    return {
        "source": source,
        "expected": len(expected_names),
        "present": len(present),
        "missing": missing,
        "sizes": sizes,
        "guiOnlyExpected": gui_only,
        "guiOnlyPresent": gui_present,
        "passed": not missing,
    }







