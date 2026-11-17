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
        say("[期号校验3] 无历史期号记录，本轮建立基线（首轮，无需比较）")

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


def detect_period_tuku(delay: float = 0.05, log: Callable[[str], None] | None = None) -> dict:
    """49图库当期期号。

    口径（读了 `core/scraper.py::resolve_target` 与 `main()` 确认）：
      `resolve_target(2, delay, 0)` 返回 `(latest, year, last, page1, manual)`，
      `latest` 是**整期号**（如 2026320）；`main()` 里随后会 `short_period(latest)` 得到 320。
      `--period` 帮助文本写明「可写 316 或 2026316」，两种都收。
    → 这里统一把**整期号**作为 pass 值，展示用短期号。
    """
    say = log or (lambda message: None)
    from core import scraper, source

    latest, year, last, _page1, manual = scraper.resolve_target(2, delay, 0)
    if not latest:
        raise PeriodAbort("49图库 resolve_target 返回空期号")
    short = int(source.display_period(latest)) or int(latest)
    ev = {
        "source": "tuku",
        "caliber": "full",
        "periodFull": int(latest),
        "periodShort": short,
        "year": int(year or 0),
        "manual": bool(manual),
        "lastPeriod": last.get("period") if isinstance(last, dict) else None,
        "checkedAt": time.strftime("%Y-%m-%d %H:%M:%S"),
    }
    say(f"[期号/tuku] resolve_target -> 整期号 {latest}（展示 {short} 期，year={year}）")
    return {"period": int(latest), "display": short, "evidence": ev}


def detect_period_aocai(delay: float = 0.05, log: Callable[[str], None] | None = None) -> dict:
    """澳彩交流当期期号。

    口径（读了 `core/forum.py` 主流程 1763-1769 行确认）：
      `first_items = parse_list_payload(fetch_luntan_page(0))`
      `latest = max(as_int(item["qs"]) for item in first_items)` —— **整期号**（2026320）
      目录名用 `display_period(target)` 得到 320。
    ⚠️ aocai 的 `scan_period`/`infer_item_period` 拿 target 与行内 `qs` 直接比，
       所以传**整期号**才是对的；传 320 会让所有行判成"非本期"。
    """
    say = log or (lambda message: None)
    from core import forum as forum_mod, source

    payload = forum_mod.fetch_luntan_page(0, delay=delay)
    items = forum_mod.parse_list_payload(payload)
    if not items:
        raise PeriodAbort("澳彩交流版块首页无帖子，无法确定当期")
    periods = [forum_mod.as_int(it.get("qs")) for it in items if forum_mod.as_int(it.get("qs")) > 0]
    if not periods:
        raise PeriodAbort("澳彩交流首页行内 qs 全为 0，无法确定当期")
    latest = max(periods)
    short = int(source.display_period(latest)) or int(latest)
    ev = {
        "source": "aocai",
        "caliber": "full",
        "periodFull": int(latest),
        "periodShort": short,
        "rows": len(items),
        "distinct": sorted({forum_mod.as_int(it.get("qs")) for it in items if forum_mod.as_int(it.get("qs")) > 0}, reverse=True)[:5],
        "checkedAt": time.strftime("%Y-%m-%d %H:%M:%S"),
    }
    say(f"[期号/aocai] 首页 max(qs) -> 整期号 {latest}（展示 {short} 期）")
    return {"period": int(latest), "display": short, "evidence": ev}


def detect_period(
    source_id: str,
    last_period: int = 0,
    learned_offset: int = 0,
    log: Callable[[str], None] | None = None,
) -> dict:
    """统一入口：返回 {period(传给模块的值), display(短期号), evidence, caliber}。

    三源口径**不同**，不能照抄：
      · dahkg（大港澳）：列表口径是**短期号**（320）；四重校验；传短期号。
      · aocai（澳彩）  ：`qs` 是**整期号**（2026320）；传整期号。
      · tuku （49图库）：`--period` 两种都收；为一致性传整期号，展示用短期号。
    """
    if source_id == "dahkg":
        period, ev = detect_verified_dahkg(
            last_period=last_period, learned_offset=learned_offset, log=log
        )
        return {"period": period, "display": normalize_period(period), "evidence": ev,
                "caliber": "short" if period < 1000000 else "full"}
    if source_id == "aocai":
        return detect_period_aocai(log=log)
    if source_id == "tuku":
        return detect_period_tuku(log=log)
    raise ValueError(f"未知源：{source_id}")


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



# ---------------------------------------------------------------------------
# 阶段 1（v5 任务书）：安全期号判定 =「第 1 页唯一性 + 台账锁定」
# ---------------------------------------------------------------------------

#: 各档位的换期策略（20:50 那轮锁台账，避免当晚数据被拆成两期）
PERIOD_LOCK_POLICY: dict[str, str] = {
    "12:00": "allow_switch",
    "15:00": "allow_switch",
    "18:00": "allow_switch",
    "20:15": "allow_switch",
    "20:50": "keep_ledger",
}


def _display(period: int) -> int:
    """展示口径：整期号 2026322 -> 322。"""
    return int(normalize_period(period)) or int(period)


def _caliber(period: int) -> str:
    return "full" if int(period) >= 1_000_000 else "short"


def _fetch_first_page(source: str) -> list[dict]:
    """抓各源**第 1 页**列表行，统一成 [{"periods": <int>}, ...]。

    · dahkg：直接打 URI_LIST，sortType=1（按时间；`fetch_list_page` 写死 3，不能用）
    · aocai：论坛首页，行内 `qs` 就是整期号
    · tuku ：其列表接口必须带期号 → 先用 resolve_target 拿当期，再抓该期第 1 页；
             拿不到就返回空列表（交给「空页」分支处理）
    """
    if source == "dahkg":
        rows = _list_page(1, 1)
        return [{"periods": int(r.get("periods") or 0)} for r in (rows or [])]
    if source == "aocai":
        from core import forum as forum_mod

        payload = forum_mod.fetch_luntan_page(0, delay=0.05)
        items = forum_mod.parse_list_payload(payload)
        return [{"periods": int(forum_mod.as_int(it.get("qs")) or 0)} for it in (items or [])]
    if source == "tuku":
        from core import scraper

        latest, _year, _last, _page1, _manual = scraper.resolve_target(2, 0.05, 0)
        if not latest:
            return []
        posts, _meta = scraper.fetch_period_posts(49, int(latest), 1, 0.0)
        out = []
        for row in posts or []:
            try:
                out.append({"periods": int(str(row.get("periods") or row.get("period") or latest))})
            except (TypeError, ValueError):
                out.append({"periods": int(latest)})
        return out
    raise ValueError(f"未知源：{source}")


def detect_period_safe(
    source: str,
    ledger_path,
    log: Callable[[str], None] | None = None,
    first_page_rows: list[dict] | None = None,
) -> dict:
    """安全期号判定：第 1 页唯一性 + 台账锁定（8 种情况，见 v5 任务书阶段 1）。

    与现有 `detect_period` 的关系：**并存**，不改旧函数。
    `first_page_rows` 只为单测注入；生产传 None 时走 `_fetch_first_page(source)`。
    """
    import json as _json
    from pathlib import Path as _Path

    say = log or (lambda message: None)
    path = _Path(ledger_path)
    ledger = {}
    if path.exists():
        try:
            ledger = _json.loads(path.read_text(encoding="utf-8")) or {}
        except Exception:  # noqa: BLE001 - 台账坏了按「无台账」处理，但要明说
            say(f"[期号] ⚠ 台账 {path.name} 解析失败，按无台账处理")
            ledger = {}
    ledger_period = int(ledger.get("period") or 0)

    rows = first_page_rows if first_page_rows is not None else _fetch_first_page(source)
    periods = {int(r["periods"]) for r in (rows or []) if int(r.get("periods") or 0) > 0}

    def _result(period: int, reason: str, **extra) -> dict:
        ev = {"reason": reason, "firstPage": sorted(periods), "ledger": ledger_period,
              "source": source, "code": extra.pop("code", "")}
        ev.update(extra)
        say(f"[期号/{source}] {reason} → 采用 {period}（展示 {_display(period)} 期）")
        return {"period": int(period), "display": _display(period),
                "caliber": _caliber(period), "evidence": ev}

    if len(periods) == 1:                                  # 情况 1/2/3/4
        p = periods.pop()
        if not ledger_period:
            return _result(p, "首轮建立基线", code="single_no_ledger")
        if p == ledger_period:
            return _result(p, "与台账一致", code="single_consistent")
        if p > ledger_period:
            return _result(p, f"换期 {ledger_period}→{p}", code="single_greater")
        raise PeriodAbort(
            f"[期号/{source}] 源站期号回退：台账 {ledger_period} > 当前 {p}（第1页 {sorted(periods | {p})}）"
        )
    if len(periods) > 1:                                   # 情况 5/6
        if ledger_period:
            return _result(ledger_period, "过渡期混期，用台账", code="mixed_ledger")
        pmax = max(periods)
        return _result(pmax, "过渡期无台账取最大", code="mixed_max")
    if ledger_period:                                      # 情况 7
        return _result(ledger_period, "第1页无期号，用台账", code="empty_ledger")
    raise PeriodAbort(f"[期号/{source}] 第1页无期号且无台账，拒绝猜期号")   # 情况 8
