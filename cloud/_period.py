"""防线 4：期号四重校验（R6 / R14）。

**为什么必须自己做**（2026-11-16 实测）：
  `core/dahkg.py::detect_current_period()` 走的是 `fetch_list_page()`，
  而 `fetch_list_page` 里把 `sortType` 写死成 `SORT_MOST_LIKED=3`（按赞）。
  文档第 4 道防线「六条硬性约束」第 1 条明确要求**必须用 sortType=1（按时间）**，
  第 2 条要求 isCurrPeriods 交叉验证 —— 现有实现两条都没有。

  本文档的实测（2026-11-16 09:20，期号 320）：
    sortType=1 第1页 -> {320: 20} ；sortType=3 第1页 -> {320: 20}（此刻恰好一致）
    开奖接口 periods = 121，列表期号 320，偏移 = 199（11-14 实测 197/198，逐期 +1）
  说明「两判据恰好一致」是运气，不是保证 —— 新期刚开、高赞帖还在旧期时，
  sortType=3 会把旧期顶到第一页。所以校验必须每次都跑，不能靠上次结论。

本模块只做「探测 + 校验 + 证据」，不改 core 源码。
"""
from __future__ import annotations

import time
from typing import Any, Callable


class PeriodAbort(RuntimeError):
    """期号校验不通过 —— 必须中止整轮，不许带病出产物（文档 3-C）。"""


def normalize_period(value: Any) -> int:
    """短期号归一化：2026320 -> 320（沿用 core/source.py::display_period 口径）。"""
    try:
        period = int(value)
    except (TypeError, ValueError):
        return 0
    if period <= 0:
        return 0
    return period % 1000 if period >= 1000000 else period


def _dahkg():
    from core import dahkg

    return dahkg


def _list_page(page: int, sort_type: int, page_size: int = 20) -> list[dict]:
    """直接打列表接口（不经 fetch_list_page，后者把 sortType 写死成 3）。"""
    d = _dahkg()
    data = d._api_post(
        {
            "uri": d.URI_LIST,
            "currentPage": int(page),
            "pageSize": int(page_size),
            "sortType": int(sort_type),
            "lastRefreshTime": 0,
        }
    )
    if not data.get("success"):
        return []
    return (data.get("data") or {}).get("list") or []


def _draw_periods() -> int:
    """开奖接口的 periods —— 与列表期号是**两套编号**（禁止当列表期号用）。"""
    try:
        d = _dahkg()
        payload = d._api_get("sys/openPrize/index")
        return int((payload.get("data") or {}).get("periods") or 0)
    except Exception:  # noqa: BLE001 - 开奖接口只用于交叉提示，不可成为阻塞点
        return 0


def detect_verified_dahkg(
    last_period: int = 0,
    learned_offset: int = 0,
    flag_pages: int = 5,
    log: Callable[[str], None] | None = None,
) -> tuple[int, dict]:
    """大港澳当期期号 —— 四重校验，任一不一致即 `PeriodAbort`。

    返回 (期号, 证据字典)。证据会写进 run_meta，供验收逐条核对。
    """
    say = log or (lambda message: None)
    ev: dict[str, Any] = {"source": "dahkg", "checkedAt": time.strftime("%Y-%m-%d %H:%M:%S")}
    t0 = time.time()

    # ---- 校验 1（主）：按时间排序的第 1 页 ----
    r1 = _list_page(1, sort_type=1)
    periods_time = [normalize_period(row.get("periods")) for row in r1]
    periods_time = [p for p in periods_time if p > 0]
    if not periods_time:
        raise PeriodAbort("校验1失败：sortType=1 第1页没有任何有效期号（源站异常）")
    period_by_time = max(periods_time)
    ev["check1_sortType1_page1"] = {
        "period": period_by_time,
        "rows": len(r1),
        "distinct": sorted(set(periods_time), reverse=True),
    }
    say(f"[期号校验1] sortType=1 第1页 -> {period_by_time}（{len(r1)} 行，期号集合 {sorted(set(periods_time), reverse=True)}）")

    # ---- 校验 2：isCurrPeriods 标记（翻前 N 页，两种排序一起收） ----
    flagged: dict[int, int] = {}
    pages_seen = 0
    for sort_type in (1, 3):
        for page in range(1, flag_pages + 1):
            try:
                rows = _list_page(page, sort_type=sort_type)
            except Exception as exc:  # noqa: BLE001 - 单页失败不否定标记校验
                say(f"[期号校验2] sortType={sort_type} 第{page}页取失败（跳过该页）：{exc}")
                continue
            if not rows:
                break
            pages_seen += 1
            for row in rows:
                if row.get("isCurrPeriods") is True:
                    p = normalize_period(row.get("periods"))
                    if p > 0:
                        flagged[p] = flagged.get(p, 0) + 1
    flag_set = sorted(flagged, reverse=True)
    ev["check2_isCurrPeriods"] = {"pagesScanned": pages_seen, "periods": flag_set, "counts": flagged}
    if len(flag_set) > 1:
        raise PeriodAbort(f"校验2失败：isCurrPeriods 同时标了多个期号 {flag_set}（源站数据矛盾）")
    period_by_flag = flag_set[0] if flag_set else 0
    say(f"[期号校验2] isCurrPeriods 标记 -> {period_by_flag or '（前 N 页无标记）'}（扫 {pages_seen} 页）")

    # ---- 校验 3：单调性（与上次期号比较） ----
    ev["check3_monotonic"] = {"lastPeriod": last_period, "current": period_by_time}
    if last_period:
        if period_by_time < last_period:
            raise PeriodAbort(f"校验3失败：期号倒退 {period_by_time} < 上次 {last_period}")
        if period_by_time - last_period > 2:
            raise PeriodAbort(f"校验3失败：期号跳变过大 {last_period} -> {period_by_time}（>2）")
        say(f"[期号校验3] 单调性通过：上次 {last_period} -> 本次 {period_by_time}")
    else:
        say("[期号校验3] 无历史期号记录，跳过单调性检查（首轮）")

    # ---- 校验 4：开奖接口交叉验证（只 warn，不 abort —— 两套编号差 197+） ----
    draw = _draw_periods()
    ev["check4_drawCrossCheck"] = {
        "drawPeriods": draw,
        "learnedOffset": learned_offset,
        "impliedOffset": (period_by_time - draw) if draw else 0,
    }
    if draw and learned_offset:
        if draw + learned_offset != period_by_time:
            say(
                f"[期号校验4] 提示：开奖序号+偏移({draw}+{learned_offset}) != 列表期号"
                f"({period_by_time}) —— 两套编号，仅作提示"
            )
    elif draw:
        say(f"[期号校验4] 开奖序号 {draw}，列表期号 {period_by_time}，偏移 {period_by_time - draw}（首轮学习）")

    # ---- 一致性判定 ----
    if period_by_flag and period_by_flag != period_by_time:
        raise PeriodAbort(
            f"两判据不一致：按时间={period_by_time} isCurrPeriods={period_by_flag}"
        )
    if not period_by_flag:
        say("[期号校验2] 提示：前 N 页没有 isCurrPeriods=True，本判据缺席（仅靠校验1/3/4）")

    ev["elapsedSeconds"] = round(time.time() - t0, 2)
    ev["period"] = period_by_time
    ev["passed"] = True
    say(
        f"[大港澳] 期号四重校验通过: 按时间={period_by_time} isCurrPeriods={period_by_flag or 'N/A'} "
        f"上次={last_period or 'N/A'} 开奖序号={draw}（耗时 {ev['elapsedSeconds']}s）"
    )
    return period_by_time, ev


def check_posts_period(posts: list[dict], target: int) -> dict:
    """防线 4 第 5 道：内容期号逐帖校验（错误数必须 = 0）。"""
    want = normalize_period(target)
    wrong: list[dict] = []
    for post in posts:
        raw = post.get("period")
        got = normalize_period(raw)
        if got != want:
            wrong.append(
                {
                    "articleId": post.get("articleId"),
                    "period": raw,
                    "normalized": got,
                    "title": str(post.get("title") or "")[:40],
                }
            )
    return {
        "checked": len(posts),
        "target": want,
        "wrongCount": len(wrong),
        "wrong": wrong[:20],
        "passed": len(wrong) == 0,
    }
