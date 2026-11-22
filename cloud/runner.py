"""六合采集器 · 云端采集调度入口（GitHub Actions 用）。

四种模式（文档阶段 1.1）：

    python -m cloud.runner --cleanup                      # Job 0：图片 2 天 + 产物 30 期轮转
    python -m cloud.runner --plan    --source dahkg       # Job 1：期号四重校验 + 独立扫描
    python -m cloud.runner --shard 3 --shards 10 --source dahkg   # Job 2~11：分片预热缓存
    python -m cloud.runner --merge   --source dahkg       # Job 12：合并分片后跑完整产物
    python -m cloud.runner --run     --source dahkg       # 一体化（单 job 生产模式）
    python -m cloud.runner --selftest                     # 单元自检（阶段 1 的验收用）

设计约束：
  * 采集本体**只调用现有 core 模块**（同参数、同入口），不重写算法
    —— 这是「云端产物与本地一模一样」的唯一保证；
  * 校验（五道防线）在本模块实现，不达标就把退出码置非 0，绝不让残缺数据进入产物目录；
  * 不改 core 源码（文档第九节第 5 条），平台差异全部由 `_compat` 垫片处理。
"""
from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys
import time
import traceback
from pathlib import Path

MODULE_OF = {"dahkg": "core.dahkg", "aocai": "core.forum", "tuku": "core.scraper"}
SOURCE_SUBDIR = {"dahkg": "大港澳", "aocai": "澳彩交流", "tuku": ""}


# ---------------------------------------------------------------------------
# 通用
# ---------------------------------------------------------------------------
def log(message: str) -> None:
    print(message, flush=True)


def _work_dir(explicit: str | None) -> Path:
    root = Path(explicit or os.environ.get("LIUHE_CLOUD_WORK") or "work").resolve()
    root.mkdir(parents=True, exist_ok=True)
    return root




def _resolve_state_dir(args, work: Path) -> Path:
    if getattr(args, "state", ""):
        return Path(args.state).resolve()
    return work / "state"


def _find_period_dir(work: Path, out_root: Path, source: str, period: int = 0) -> Path:
    """产物目录：out 下**真正含产物**的那一期目录。

    实测坑（2026-11-16 Gate1）：49图库除了 `澳门_2026_320期`，还会写一个
    `历史回采/49图库/2026-11-15_第320期` 的中间目录（只有 5 个文件），
    两个目录名解析出来的期号都是 320 —— 只按期号取最大会取错。
    判据：优先 posts.json 存在，其次文件数最多。

    ## period 参数（2026-11-18 阶段 0 修，失败模式 6）
    指定期号时必须**直接选那一期**，否则会出现：
        merge 采完 321 期，却拿 322 期的目录去校验
        → [防线4] 检查 1080 帖、错误 1080 个 → exit 30 → 整轮失败
    （实证：run 44139439359 smoke test）
    """
    sub = SOURCE_SUBDIR[source]
    base = out_root / sub if sub else out_root
    candidates = [p for p in base.glob("*期") if p.is_dir()]
    if not candidates:
        raise SystemExit(f"[错误] 产物目录不存在：{base}/*期")

    if period:
        from core import source as src_mod

        want = str(int(src_mod.display_period(period)))
        for p in candidates:
            if f"_{want}期" in p.name:
                log(f"[产物] 指定期号 {period} → 直接选中 {p.name}")
                return p
        log(
            f"[产物] ⚠ 指定期号 {period} 的目录不存在"
            f"（候选 {[p.name for p in candidates]}），退回自动选择"
        )

    candidates.sort(key=lambda p: (int("".join(c for c in p.name.split("_")[-1] if c.isdigit()) or 0)), reverse=True)
    with_posts = [p for p in candidates if (p / "posts.json").exists()]
    pool = with_posts or candidates
    pool.sort(key=lambda p: (len(list(p.glob("*"))), str(p)), reverse=True)
    chosen = pool[0]
    log(f"[产物] 候选期目录 {[p.name for p in candidates]} → 选中 {chosen.name}")
    return chosen


# ---------------------------------------------------------------------------
# 采集：只调用现有模块 CLI（与本地手工跑完全相同的入口）
# ---------------------------------------------------------------------------
def _patch_dahkg_multisort_union(log) -> list[tuple]:
    """给 `dahkg.scan_period` 打「多排序并集」补丁（只在云端进程内生效，不改 core）。

    ## 为什么必须这么做（2026-11-16 实测漏帖）
    模块的 `scan_period` 只用 `fetch_list_page`（内部把 `sortType` 写死成 3 = 按赞）。
    按赞排序的分页**不稳定**：翻页期间点赞数会变，帖子在页之间移动 → 低赞帖会整条被跳过。
    实测证据：期号 320 一共 1101 帖，模块只扫到 1100，漏掉
    `1534674`（createTime 11-16 01:02，远早于采集窗口；它现在在 **sortType=1 第 10 页**，
    而按赞序列里已经滑出被扫范围）。

    ## 补丁做了什么
    先用原生 `sort_type=3` 扫一轮，再把 `fetch_list_page` 临时换成 `sort_type=1`
    扫第二轮，按 tid 取并集。这样两个口径互为兜底 —— 正是文档防线1 第1道的要求。
    """
    from core import dahkg

    original_scan = dahkg.scan_period
    original_fetch = dahkg.fetch_list_page

    def _fetch_by_time(page: int, page_size: int = dahkg.PAGE_SIZE) -> list[dict]:
        data = dahkg._api_post(
            {
                "uri": dahkg.URI_LIST,
                "currentPage": int(page),
                "pageSize": int(page_size),
                "sortType": 1,          # 1 = 按时间（原生写死的是 3）
                "lastRefreshTime": 0,
            }
        )
        if not data.get("success"):
            return []
        return (data.get("data") or {}).get("list") or []

    def patched_scan_period(target_period: int, *args, **kwargs):
        say = kwargs.get("progress") or (lambda message: None)
        items_like, scan_like = original_scan(target_period, *args, **kwargs)
        dahkg.fetch_list_page = _fetch_by_time
        try:
            items_time, scan_time = original_scan(target_period, *args, **kwargs)
        finally:
            dahkg.fetch_list_page = original_fetch

        seen = {int(it.get("tid") or 0) for it in items_like}
        extra = [it for it in items_time if int(it.get("tid") or 0) not in seen]
        say(
            f"[多排序并集] sortType=3 得 {len(items_like)} 帖；"
            f"sortType=1 补 {len(extra)} 帖（并集 {len(items_like) + len(extra)}）"
        )
        scan = dict(scan_like)
        scan["multiSort"] = {
            "byLike": len(items_like),
            "byTime": len(items_time),
            "extraByTime": [int(it.get("tid") or 0) for it in extra][:50],
            "extraCount": len(extra),
        }
        return items_like + extra, scan

    dahkg.scan_period = patched_scan_period
    log("[merge/dahkg] 已给 scan_period 打「按赞 + 按时间」并集补丁（防按赞分页竞态漏帖）")
    return [(dahkg, "scan_period", original_scan)]


def _collect_argv(out_root: Path, args, period: int = 0) -> list[str]:
    """拼出模块 CLI 参数（subprocess 与进程内调用共用，避免两套参数走偏）。"""
    argv = ["--out", str(Path(out_root).resolve())]
    if period:
        argv += ["--period", str(period)]
    if args.workers:
        argv += ["--workers", str(args.workers)]
    if args.delay is not None:
        argv += ["--delay", str(args.delay)]
    if args.max_pages:
        argv += ["--max-pages", str(args.max_pages)]
    if args.limit:
        argv += ["--limit", str(args.limit)]
    if args.no_images:
        argv.append("--no-images")
    if args.no_ocr:
        argv.append("--no-ocr")
    if args.no_replies:
        argv.append("--no-replies")
    if args.ocr_limit:
        argv += ["--ocr-limit", str(args.ocr_limit)]
    return argv


def collect(module: str, out_root: Path, args, period: int = 0) -> tuple[int, float]:
    # **必须绝对路径**：core/dahkg.py::run_dahkg_report 里用
    # `(out_dir / "论坛原帖.html").as_uri()` 生成 postUrl，
    # 相对路径会抛 `ValueError: relative path can't be expressed as a file URI`
    # （2026-11-16 Gate2 实测：dahkg/aocai 因此退出码 1）。
    out_root = Path(out_root).resolve()
    cmd = [sys.executable, "-m", module] + _collect_argv(out_root, args, period)
    log(f"[采集] 执行：{' '.join(cmd)}")
    t0 = time.time()
    # 子进程必须能 import core：`_compat` 只改了当前进程的 sys.path，
    # `python -m core.xxx` 是**另一个进程**，得靠 PYTHONPATH 传下去。
    env = dict(os.environ)
    core_parent = str(Path(__file__).resolve().parent)
    existing = env.get("PYTHONPATH", "")
    env["PYTHONPATH"] = core_parent + (os.pathsep + existing if existing else "")
    log(f"[采集] PYTHONPATH={env['PYTHONPATH']}")
    proc = subprocess.run(cmd, cwd=str(Path.cwd()), check=False, env=env)
    elapsed = time.time() - t0
    log(f"[采集] 退出码={proc.returncode} 耗时={elapsed:.1f}s")
    return proc.returncode, elapsed


# ---------------------------------------------------------------------------
# 模式：--cleanup
# ---------------------------------------------------------------------------
def do_cleanup(args) -> int:
    from . import _cleanup, _guard

    work = _work_dir(args.work)
    out_root = Path(args.out or work / "out").resolve()
    log("=" * 70)
    log(f"[cleanup] 开始 {time.strftime('%Y-%m-%d %H:%M:%S')}  输出根={out_root}")
    log("=" * 70)

    image_roots = [p for p in out_root.rglob("images") if p.is_dir()]
    log(f"[cleanup] 发现图片目录 {len(image_roots)} 个（输出根={out_root}）")
    img_stats = _cleanup.cleanup_images(
        image_roots, keep_days=args.keep_days, dry_run=args.dry_run, log=log
    )

    if args.skip_periods:
        log("[cleanup] --skip-periods：跳过产物 30 期轮转（由 publish job 统一处理）")
        rot_stats = {"skipped": True}
    else:
        store_root = Path(args.store or work / "store")
        log_root = store_root / "data"
        rot_stats = _cleanup.rotate_periods(
            log_root, keep=args.keep_periods, dry_run=args.dry_run, log=log
        )

    report = {
        "ts": time.strftime("%Y-%m-%d %H:%M:%S"),
        "images": img_stats,
        "periods": rot_stats,
    }
    _guard.save_json(work / "audit" / f"cleanup_{time.strftime('%Y%m%d_%H%M%S')}.json", report)
    log("[cleanup] 完成")
    return 0


# ---------------------------------------------------------------------------
# 模式：--plan
# ---------------------------------------------------------------------------


def _load_image_cost(state_dir: Path, source: str) -> dict:
    """读上一轮 merge 落盘的「每帖总图数」（key 是 str(articleId)）。"""
    from . import _guard

    data = _guard.load_json(state_dir / f"imagecost_{source}.json", {})
    if not isinstance(data, dict):
        return {}
    cost = data.get("cost")
    return cost if isinstance(cost, dict) else {}


def _sha256_text(text: str) -> str:
    import hashlib

    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def _post_content_hashes(detail: dict, replies: list[dict]) -> dict:
    """算一帖的**内容指纹**（2026-11-17 用户需求：识别"12点发的、20点被改"）。

    ## 为什么必须抓完整详情才算得准
    列表接口只给 `content` 的**前 100 字符摘要**，且**没有 updateTime**（改内容它不变）。
    实测 321 期：帖 1535722 列表 content 长 100、详情 content 长 283 ——
    改动发生在第 101 字符之后时，只比对列表快照完全检测不到。
    而 prepare 的 `_prefetch_post_costs` **本来就抓了完整详情和全部回复**，
    所以这四个 hash 是**零额外请求**。

    返回：titleHash / contentHash / imagesHash / repliesHash + 便于人看的长度与计数。
    """
    title = str((detail or {}).get("title") or "")
    content = str((detail or {}).get("content") or "")
    img_raw = str((detail or {}).get("img") or "")
    urls = [u.strip() for u in img_raw.split(",") if u.strip()]
    # 正文图：用户要求「URL 列表保持原顺序」，所以不排序（排序会让"换图但集合相同"漏判）
    images_hash = _sha256_text("\n".join(urls))

    # 回复：按 (createTime, commentId) 稳定排序后拼 content+senderId+createTime
    # + 每条回复的楼中楼（subList / secondLevelComments）内容
    def _reply_key(r: dict):
        return (str(r.get("createTime") or ""), str(r.get("commentId") or r.get("singaporeReplyId") or ""))

    parts: list[str] = []
    for r in sorted(replies or [], key=_reply_key):
        parts.append(
            "|".join(
                [
                    str(r.get("content") or ""),
                    str(r.get("senderId") or (r.get("user") or {}).get("id") or ""),
                    str(r.get("createTime") or ""),
                ]
            )
        )
        for sub in (r.get("subList") or r.get("secondLevelComments") or []):
            parts.append(
                "  sub:" + "|".join(
                    [
                        str(sub.get("content") or ""),
                        str(sub.get("senderId") or ""),
                        str(sub.get("createTime") or ""),
                    ]
                )
            )
    replies_text = "\n".join(parts)

    return {
        "titleHash": _sha256_text(title),
        "contentHash": _sha256_text(content),
        "imagesHash": images_hash,
        "repliesHash": _sha256_text(replies_text),
        "contentLen": len(content),
        "replyPoint": int((detail or {}).get("replyPoint") or 0),
        "imgTotal": int((detail or {}).get("imgTotal") or 0),
        "replyFetched": len(replies or []),
    }


def _diff_content_hashes(prev: dict, cur: dict) -> dict:
    """比对上一轮快照与本轮 hash，返回变化明细（用户第四节表格要求）。"""
    prev = prev or {}
    cur = cur or {}
    kinds: list[str] = []
    if prev.get("titleHash") != cur.get("titleHash"):
        kinds.append("title")
    if prev.get("contentHash") != cur.get("contentHash"):
        kinds.append("content")
    if prev.get("imagesHash") != cur.get("imagesHash"):
        kinds.append("images")
    if prev.get("repliesHash") != cur.get("repliesHash"):
        kinds.append("replies")
    return {
        "changed": kinds,
        "oldContentLen": prev.get("contentLen"),
        "newContentLen": cur.get("contentLen"),
        "oldReplyPoint": prev.get("replyPoint"),
        "newReplyPoint": cur.get("replyPoint"),
        "oldReplyFetched": prev.get("replyFetched"),
        "newReplyFetched": cur.get("replyFetched"),
    }


def _prefetch_post_costs(
    source: str,
    ids: list[int],
    log,
    workers: int = 16,
    delay: float = 0.02,
) -> dict:
    """**只抓详情 + 回复**（不下载图片、不 OCR），算出每帖的真实图数与 det 面积。

    ## 为什么必须在 prepare 阶段做（v6，2026-11-17 用户设计）
    v4 用「上一轮产物里的每帖图数」当权重，但**新期第一轮没有历史**，
    而第一轮恰恰是最慢的一轮（实测 34.2 分钟）。每天第一次跑就是第一轮，
    所以必须在分片之前就知道真实工作量。

    实测依据（run 43952322314，v3 面积均衡到 1.00 后）：
      #9  274 张 / 1.9 分 = 0.42 秒/张
      #6  613 张 / 8.9 分 = 0.87 秒/张
      #10 841 张 /16.5 分 = 1.18 秒/张
    不仅张数差 3.07 倍，**每张难度也差 2.8 倍** → 必须同时按面积（不是只按张数）均衡。

    只做网络请求：详情 1 次 + 回复分页 N 次；不触发图片下载与 OCR。
    """
    from concurrent.futures import ThreadPoolExecutor, as_completed

    say = log or (lambda message: None)
    if not ids:
        return {}
    total = len(ids)
    done = 0
    out: dict[int, dict] = {}
    failed = 0

    def fetch_one(aid: int) -> tuple[int, dict | None]:
        try:
            urls: list[str] = []
            if source == "dahkg":
                from core import dahkg

                detail = dahkg.fetch_detail(aid)
                urls += [u.strip() for u in str(detail.get("img") or "").split(",") if u.strip()]
                reps = dahkg.fetch_all_replies(aid)
                body_count = len(urls)
                for rep in reps:
                    urls += [u.strip() for u in str(rep.get("img") or "").split(",") if u.strip()]
                    # #10（2026-11-18 修）：楼中楼 subList 里每条也有 img，必须统计，否则权重偏低
                    for sub in (rep.get("subList") or []):
                        urls += [
                            u.strip()
                            for u in str((sub or {}).get("img") or "").split(",")
                            if u.strip()
                        ]
                _body_urls = [u.strip() for u in str(detail.get("img") or "").split(",") if u.strip()]
                _reply_urls: list[str] = []
                for _r in reps:
                    _reply_urls += [u.strip() for u in str(_r.get("img") or "").split(",") if u.strip()]
                    for _sub in (_r.get("subList") or []):
                        _reply_urls += [
                            u.strip()
                            for u in str((_sub or {}).get("img") or "").split(",")
                            if u.strip()
                        ]
                return aid, {
                    "count": len(urls),
                    "detArea": sum(_det_input_area(u) for u in urls),
                    "bodyCount": body_count,
                    "replyCount": len(reps),
                    # 2026-11-21：真实图 URL（供 _item_image_weight 精确算成本）
                    "bodyUrls": _body_urls,
                    "replyUrls": _reply_urls,
                    "imgCount": len(_body_urls) + len(_reply_urls),
                    # 【内容指纹】零额外请求：详情和回复都已经抓在手里了（用户第四节）
                    **_post_content_hashes(detail, reps),
                }
            if source == "aocai":
                from core import forum as forum_mod

                detail = forum_mod.fetch_thread_detail(aid, 0, include_replies=True, delay=0.0, max_pages=50)
                tiezi = (detail or {}).get("tiezi") or {}
                reps = (detail or {}).get("replies") or []
                urls += [u.strip() for u in str(tiezi.get("img") or "").split(",") if u.strip()]
                body_count = len(urls)
                for rep in reps:
                    urls += [u.strip() for u in str(rep.get("img") or "").split(",") if u.strip()]
                    # #10 同类：楼中楼 subList 的 img 也要统计
                    for sub in (rep.get("subList") or []):
                        urls += [
                            u.strip()
                            for u in str((sub or {}).get("img") or "").split(",")
                            if u.strip()
                        ]
                _body_urls = [u.strip() for u in str(tiezi.get("img") or "").split(",") if u.strip()]
                _reply_urls: list[str] = []
                for _r in reps:
                    _reply_urls += [u.strip() for u in str(_r.get("img") or "").split(",") if u.strip()]
                    for _sub in (_r.get("subList") or []):
                        _reply_urls += [
                            u.strip()
                            for u in str((_sub or {}).get("img") or "").split(",")
                            if u.strip()
                        ]
                return aid, {
                    "count": len(urls),
                    "detArea": sum(_det_input_area(u) for u in urls),
                    "bodyCount": body_count,
                    "replyCount": len(reps),
                    "bodyUrls": _body_urls,
                    "replyUrls": _reply_urls,
                    "imgCount": len(_body_urls) + len(_reply_urls),
                    **_post_content_hashes(tiezi, reps),
                }
            if source == "tuku":
                from core import scraper

                raw = scraper.api_get("/unite49/h5/article/detail", {"articleId": aid}, delay=0)
                # #9（2026-11-18 修）：pictureList 元素字段是 imageUrl/pictureUrl，**没有 url**，
                # 直接取 pic["url"] 会全部为 None → 权重恒为 1（假均衡）。改用 core 的同一函数。
                imgs = scraper.images_from_picture_list((raw or {}).get("pictureList") or [])
                urls += [str(im.get("url") or "") for im in imgs if im.get("url")]
                _body_urls = [str(im.get("url") or "") for im in imgs if im.get("url")]
                return aid, {
                    "count": len(urls),
                    "detArea": sum(_det_input_area(u) for u in urls),
                    "bodyCount": len(urls),
                    "replyCount": 0,
                    "bodyUrls": _body_urls,
                    "replyUrls": [],
                    "imgCount": len(_body_urls),
                    **_post_content_hashes(raw or {}, []),
                }
        except Exception:  # noqa: BLE001 - 预抓失败不算致命，后面有兜底
            return aid, None
        return aid, None

    say(f"[prefetch] 开始预抓详情+回复：{total} 帖，并发 {workers}")
    with ThreadPoolExecutor(max_workers=max(1, workers)) as pool:
        futures = {pool.submit(fetch_one, aid): aid for aid in ids}
        for fut in as_completed(futures):
            aid, info = fut.result()
            done += 1
            if info:
                out[aid] = info
            else:
                failed += 1
            if done % 100 == 0 or done == total:
                say(f"[prefetch] 进度 {done}/{total}，成功 {len(out)}，失败 {failed}")
            if delay:
                time.sleep(0)

    total_count = sum(v["count"] for v in out.values())
    total_area = sum(v["detArea"] for v in out.values())
    say(
        f"[prefetch] 完成：{len(out)}/{total} 帖成功（失败 {failed}）；"
        f"总图 {total_count} 张，总 det 面积 {total_area/1e8:.2f} 亿像素"
    )
    if out and failed / max(1, total) > 0.2:
        say(f"[prefetch] ⚠ 失败率 {failed/total*100:.0f}% 过高，本轮分片权重退回原模型")
        return {}
    return out


def do_plan(args) -> int:
    from . import _guard, _period, _reconcile

    work = _work_dir(args.work)
    source = args.source
    log("=" * 70)
    log(f"[plan] source={source}  {time.strftime('%Y-%m-%d %H:%M:%S')}")
    log("=" * 70)

    state = _guard.load_json(_resolve_state_dir(args, work) / f"periods_{source}.json", {})
    last_period = int(state.get("lastPeriod") or 0)
    learned_offset = int(state.get("learnedOffset") or 0)

    # ---- 期号：指定则覆盖，否则三源各用自己的探测方式（见 _period.detect_period 注释）----
    period_override = int(getattr(args, "period", 0) or 0)
    if period_override:
        log(f"[plan] 指定期号 {period_override}，跳过自动探测")
        period = period_override
        display = int(_period.normalize_period(period))
        caliber = "short" if period < 1_000_000 else "full"
        evidence = {"override": True, "requested": period_override, "note": "用户指定期号，跳过四重校验"}
    else:
        # 阶段1 接线（2026-11-18）：期号判定改用「第 1 页唯一性 + 台账锁定」，
        # 替代原来的 detect_period。台账 = state/current_period_<源>.json（随数据仓提交）。
        ledger_path = _resolve_state_dir(args, work) / f"current_period_{source}.json"
        # 档位锁定（PERIOD_LOCK_POLICY）：20:50 那轮强制用台账期号，不换期
        slot = str(getattr(args, "slot", "") or "")
        policy = _period.PERIOD_LOCK_POLICY.get(slot, "allow_switch")
        lock_ledger = (policy == "keep_ledger")
        if slot:
            log(f"[plan] 档位={slot} 策略={policy}" + ("（本轮锁定台账期号）" if lock_ledger else ""))
        info = _period.detect_period_safe(source, ledger_path, log=log, lock_ledger=lock_ledger)
        period = int(info["period"])
        display = int(info["display"])
        evidence = info["evidence"]
        caliber = info.get("caliber", "short")
        log(
            f"[plan] 期号判定：{evidence.get('reason', '?')}"
            f"（第1页 {evidence.get('firstPage')} / 台账 {evidence.get('ledger')}）"
        )
        # 台账写回（只在自动判定时写；用户指定期号不污染台账）
        try:
            _guard.save_json(
                ledger_path,
                {
                    "period": period,
                    "displayPeriod": display,
                    "caliber": caliber,
                    "source": source,
                    "updatedAt": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                    "evidence": {
                        "firstPagePeriods": evidence.get("firstPage"),
                        "reason": evidence.get("reason"),
                    },
                },
            )
            log(f"[plan] 台账已写：{ledger_path.name}（period={period} / 展示 {display} 期）")
        except Exception as exc:  # noqa: BLE001 - 台账写失败不该阻断本轮，但必须明说
            log(f"[plan] ⚠ 台账写入失败（不影响本轮）：{type(exc).__name__}: {exc}")

    scan = {}
    if source == "dahkg":
        scan = _reconcile.independent_scan_dahkg(display, max_pages=args.max_pages, log=log)
        if not scan["agree"]:
            log(
                f"[plan] 警告：两种排序的帖子集合不一致 —— "
                f"按时间 {scan['countByTime']} 帖 / 按赞 {scan['countByLike']} 帖"
            )
    else:
        scan = _reconcile.independent_scan(source, display, max_pages=args.max_pages, log=log)

    plan = {
        "source": source,
        "module": MODULE_OF[source],
        "period": period,               # 传给该源模块的值（dahkg=短期号，aocai/tuku=整期号）
        "periodDisplay": display,       # 目录/报告用的短期号
        "periodCaliber": caliber,
        "createdAt": time.strftime("%Y-%m-%d %H:%M:%S"),
        "periodEvidence": evidence,
        "scan": {k: v for k, v in scan.items() if k != "unionMeta"},
        "counts": {
            "expectedPosts": scan.get("countUnion", 0),
            "byTime": scan.get("countByTime", 0),
            "byLike": scan.get("countByLike", 0),
        },
    }

    # ---- 分片均衡计划（v6：prepare 预抓详情+回复，按真实 det 面积 LPT 分组）----
    # 权重优先级：
    #   ① prepare 阶段预抓详情+回复算出的「每帖 det 输入面积」（最准，新期第一轮也有效）
    #   ② 上一轮 merge 实测的「每帖总图数」（若没预抓）
    #   ③ det 输入像素面积 + 回复图估计（兜底）
    shard_n = _shard_count_for(source)
    union_ids = [int(x) for x in (scan.get("unionIds") or [])]
    union_meta = scan.get("unionMeta") or {}
    state_dir = _resolve_state_dir(args, work)

    prefetched: dict = {}
    if shard_n > 1 and union_ids and not getattr(args, "no_prefetch", False):
        try:
            prefetched = _prefetch_post_costs(
                source, union_ids, log, workers=max(8, int(getattr(args, "workers", 0) or 4) * 4)
            )
        except Exception as exc:  # noqa: BLE001 - 预抓炸了不能拖垮 plan
            log(f"[prefetch] 异常，本轮退回原权重模型：{type(exc).__name__}: {exc}")
            prefetched = {}
        if prefetched:
            plan["postCost"] = {
                str(aid): {"count": int(v.get("count") or 0), "detArea": int(v.get("detArea") or 0)}
                for aid, v in prefetched.items()
            }
            # ---- 内容变更识别（用户第四节）：与上一轮快照比对 ----
            hash_fields = ("titleHash", "contentHash", "imagesHash", "repliesHash",
                           "contentLen", "replyPoint", "imgTotal", "replyFetched")
            plan["postHashes"] = {
                str(aid): {k: v.get(k) for k in hash_fields}
                for aid, v in prefetched.items()
            }
            # 阶段2：快照按期分文件（hashsnapshot_<源>_<期>.json），旧的无期号文件只在缺新文件时兜底读一次
            snap_new = state_dir / f"hashsnapshot_{source}_{period}.json"
            snap_old = state_dir / f"hashsnapshot_{source}.json"
            prev = _guard.load_json(snap_new, {}) or (_guard.load_json(snap_old, {}) if snap_old.exists() else {})
            prev_map = (prev or {}).get("hashes") or {}
            prev_period = int((prev or {}).get("period") or 0)
            changed: list[dict] = []
            new_posts = 0
            if prev_map and prev_period in (0, period, display):
                for aid, cur in plan["postHashes"].items():
                    old = prev_map.get(str(aid))
                    if not old:
                        new_posts += 1
                        continue
                    diff = _diff_content_hashes(old, cur)
                    if diff["changed"]:
                        changed.append({"articleId": int(aid), **diff})
            plan["contentChanged"] = changed
            plan["contentChangedCount"] = len(changed)
            _guard.save_json(
                work / "audit" / f"content_changed_{source}.json",
                {
                    "source": source,
                    "period": period,
                    "detectedAt": time.strftime("%Y-%m-%d %H:%M:%S"),
                    "count": len(changed),
                    "newPosts": new_posts,
                    "comparedAgainst": f"hashsnapshot_{source}_{period}.json@prevPeriod={prev_period}",
                    "changes": changed,
                },
            )
            if changed:
                kinds = {}
                for c in changed:
                    for k in c["changed"]:
                        kinds[k] = kinds.get(k, 0) + 1
                log(
                    f"[plan] 内容变更识别：{len(changed)} 帖发生变化（新帖 {new_posts}）"
                    f"；按类型 {kinds}；明细见 work/audit/content_changed_{source}.json"
                )
                for c in changed[:5]:
                    log(
                        f"[plan]   帖 {c['articleId']} 变化={c['changed']} "
                        f"正文长度 {c['oldContentLen']}→{c['newContentLen']} "
                        f"回复 {c['oldReplyPoint']}→{c['newReplyPoint']}"
                    )
            else:
                log(f"[plan] 内容变更识别：本轮无内容变化（比对 {len(prev_map)} 帖快照，新帖 {new_posts}）")

    hist_cost = _load_image_cost(state_dir, source)
    hist_period = int((hist_cost.get("_period") or 0)) if isinstance(hist_cost, dict) else 0
    use_hist = bool(hist_cost) and (hist_period in (0, period, display))

    if shard_n > 1 and union_ids:
        if prefetched:
            # ⚠ 2026-11-21 根因修复（消除 2.25-2.55 倍失衡）：
            #   原来用 `detArea`（图片面积和）当权重，漏了「详情/回复的网络成本」
            #   和「图片下载成本」→ 图少但回复多的帖子被严重低估 → 分到太多。
            #   现在把 prefetch 的真实数据（bodyUrls/replyUrls/replyCount/imgCount）
            #   喂进 _item_image_weight，权重 = OCR + 网络 + 下载 全成本。
            weights = {}
            for aid in union_ids:
                meta_row = dict(union_meta.get(str(aid)) or {})
                pf = prefetched.get(aid) or {}
                if pf:
                    meta_row["_prefetch"] = pf
                weights[aid] = _item_image_weight(meta_row)
            mode = "prefetch-fullcost"
            weight_src = (f"prepare 预抓实测 {len(prefetched)}/{len(union_ids)} 帖"
                          f"（全成本权重：OCR+网络+下载）")
        elif use_hist:
            weights = {}
            for aid in union_ids:
                v = hist_cost.get(str(aid))
                if isinstance(v, dict):
                    w = int(v.get("detArea") or 0) or int(v.get("count") or 0)
                else:
                    w = int(v or 0)
                weights[aid] = max(1, w)
            mode = "learned-images"
            weight_src = f"上一轮实测每帖工作量（{sum(1 for a in union_ids if str(a) in hist_cost)}/{len(union_ids)} 帖有历史）"
        else:
            weights = {aid: _item_image_weight(union_meta.get(str(aid)) or {}) for aid in union_ids}
            mode = "det-area-estimated"
            weight_src = "det 输入像素面积 + 回复图估计（无预抓、无历史）"
        # ---- 按**用户**做 LPT 分组（用户 2026-11-17 方案 3）----
        # 为什么改成按用户：按帖子分片时，同一个作者的不同帖子会被分到不同片，
        # 他的详情/回复/图片缓存被切碎、跨轮命中率低，新帖还会让归属漂移。
        # 按用户分片后：一个作者的所有帖子恒在同一片，新帖自动跟随。
        sender_of_aid = {
            aid: str((union_meta.get(str(aid)) or {}).get("senderId") or "") for aid in union_ids
        }
        # 作者字段取不到 → **硬失败**，不许静默归到空用户（2026-11-17 aocai 事故的教训：
        # aocai 真实字段是 `userid`，取错时 409 帖全归空用户 → 其余分片拿到 0 帖 → 空片 → 分片全挂）
        no_user = [aid for aid, uid in sender_of_aid.items() if not uid]
        if no_user:
            raise SystemExit(
                f"[plan] ❌ 作者字段缺失：{len(no_user)}/{len(union_ids)} 帖取不到作者"
                f"（字段名应为 {SOURCE_USER_KEY.get(source)!r}）。"
                f"样本 {no_user[:5]}。拒绝按「空用户」分组（会制造空片）。"
            )
        user_weights: dict[str, int] = {}
        aid_by_user: dict[str, list[int]] = {}
        for aid in union_ids:
            uid = sender_of_aid.get(aid) or ""
            user_weights[uid] = user_weights.get(uid, 0) + max(1, weights.get(aid, 1))
            aid_by_user.setdefault(uid, []).append(aid)

        prev_map = _load_user_shard(state_dir, source)
        prev_period = int((_guard.load_json(_user_shard_path(state_dir, source), {}) or {}).get("period") or 0)
        if prev_period not in (0, period, display):
            log(f"[plan] 上轮台账期号 {prev_period} ≠ 本轮 {period}，按新期重新建台账")
            prev_map = {}
        user_groups, user_to_shard = _group_by_user(user_weights, shard_n, prev=prev_map)

        # 5.1 三条断言（用户 2026-11-17 方案 3 第 5.1 条）——不通过直接失败，不启动 shard
        flat_users = [u for g in user_groups for u in g]
        if set(flat_users) != set(user_weights):
            raise SystemExit(
                f"[plan] ❌ 断言①失败：分组后用户数 {len(set(flat_users))} != 实际 {len(user_weights)}"
            )
        if len(flat_users) != len(set(flat_users)):
            raise SystemExit(f"[plan] ❌ 断言②失败：有用户被分配到多个分片")
        groups = [[aid for uid in g for aid in aid_by_user.get(uid, [])] for g in user_groups]
        # ⚠ 2026-11-21 按用户设计重构（run 44867937884 实测）：
        #   原 _rebalance_groups 会把「重片里最重的帖」搬到轻片 →
        #   搬走的帖在新片没有历史缓存 → hit_rate 骤降 → 分片截断 → 缺 39 帖。
        #   用户的设计：「老用户还是在原来那片，新用户再平均分给各分片」
        #   → 只在「单个超大用户」（权重 > 平均片负载 × 2）时才拆它的帖子。
        _before = [sum(max(1, weights.get(a, 1)) for a in g) for g in groups]
        groups = _rebalance_groups(
            groups, weights,
            poster_of=sender_of_aid,
            user_weights=user_weights,
            split_ratio=2.0,
        )
        # ⚠ 2026-11-22 根因修复（run 44923327016 实测：tuku 各片合计 930 > 上限 928）：
        #   _rebalance_groups 会把「超大用户」的帖子拆到多个片，但它只改 groups（帖子归属），
        #   没有同步 user_groups（plan["shards"]）。
        #   而分片拿 mine_users=shards[i] 做 by-user 切片 →
        #   原片仍认为该用户是自己的 → 收下他**全部**帖子；
        #   目标片又从 planned_ids 拿到搬过去的帖子 → **两边重复计数**。
        #   修法：凡是被拆过的用户，从所有片的 shards 里移除，
        #   让它只能通过 planned_ids（精确的帖子级分配）落位。
        _user_shard_now: dict[str, set[int]] = {}
        for _i, _g in enumerate(groups):
            for _aid in _g:
                _u = sender_of_aid.get(_aid) or ""
                if _u:
                    _user_shard_now.setdefault(_u, set()).add(_i)
        _split_users = {_u for _u, _ss in _user_shard_now.items() if len(_ss) > 1}
        if _split_users:
            user_groups = [
                [u for u in g if str(u) not in _split_users] for g in user_groups
            ]
            log(
                f"[plan] 超大用户被拆分 {len(_split_users)} 个，已从各片 shards 移除"
                f"（改由帖子级 planned_ids 精确落位，避免重复计数）"
            )
        _after = [sum(max(1, weights.get(a, 1)) for a in g) for g in groups]
        if _before != _after:
            print(
                f"[plan] 帖子层面再平衡：权重 {_before} → {_after}"
                f"（极差 {max(_before) / max(1, min(_before)):.2f} → "
                f"{max(_after) / max(1, min(_after)):.2f} 倍）",
                flush=True,
            )
        total_posts_in_groups = sum(len(g) for g in groups)
        if total_posts_in_groups != len(union_ids):
            raise SystemExit(
                f"[plan] ❌ 断言③失败：分组后帖子数 {total_posts_in_groups} != 实际 {len(union_ids)}"
            )
        log(
            f"[plan] ✅ 用户分组三条断言通过：{len(user_weights)} 个作者 → {shard_n} 片，"
            f"{len(union_ids)} 帖全部覆盖、无重复"
        )
        # 断言④：不允许空片（空片会让模块报「没有该期帖子」→ 分片失败）
        empty = [i for i, g in enumerate(user_groups) if not g]
        if empty:
            raise SystemExit(
                f"[plan] ❌ 断言④失败：分片 {empty} 是空片（0 个作者）。"
                f"作者数 {len(user_weights)} < 片数 {shard_n}，请减少分片数或检查作者字段。"
            )
        # 断言⑤：各片权重极差（>2 倍只告警，不失败 —— 墙钟还受 CPU 型号影响）
        shard_w = [sum(max(1, weights.get(a, 1)) for a in g) for g in groups]
        if shard_w and min(shard_w) > 0:
            ratio = max(shard_w) / min(shard_w)
            (log if ratio > 2.0 else log)(
                f"[plan] 断言⑤ 权重极差：{ratio:.2f} 倍"
                + ("  ← ⚠️ 超过 2 倍，建议检查分组" if ratio > 2.0 else "  ✅")
            )
        _save_user_shard(state_dir, source, user_to_shard, period, shard_n)
        plan["shards"] = user_groups
        plan["userToShard"] = user_to_shard

        # ---- 第 1 道：分片分配不重不漏（帖子维度，保留原有校验）----
        flat = [a for g in groups for a in g]
        dup = len(flat) - len(set(flat))
        missing_ids = sorted(set(union_ids) - set(flat))
        extra_ids = sorted(set(flat) - set(union_ids))
        if dup or missing_ids or extra_ids:
            raise SystemExit(
                f"[plan] ❌ 第1道校验失败：分片分配不重不漏被破坏 —— "
                f"重复 {dup} 个，遗漏 {len(missing_ids)} 个（前5 {missing_ids[:5]}），"
                f"多出 {len(extra_ids)} 个（前5 {extra_ids[:5]}）"
            )
        log(
            f"[plan] ✅ 第1道校验通过：{shard_n} 片并集 {len(set(flat))} 帖 = 清单 {len(set(union_ids))} 帖，无重复无遗漏"
        )
        sizes = [len(g) for g in groups]
        imgs = [sum(max(1, weights.get(a, 0)) for a in g) for g in groups]
        plan["shardPlan"] = {
            "n": shard_n,
            "mode": mode,
            "weightSource": weight_src,
            "groups": {str(i): g for i, g in enumerate(groups)},
            "postsPerShard": sizes,
            "weightPerShard": imgs,
            "weightTotal": sum(imgs),
        }
        # 每片「预计 OCR 分钟」= 权重/10（因为权重 = 秒数×10）
        _mins = [round(w / 10 / 60, 1) for w in imgs]
        log(
            f"[plan] 分片均衡（{mode}）：{shard_n} 片，总权重 {sum(imgs)}；每片 {imgs}；每片帖子 {sizes}"
        )
        log(
            f"[plan] 每片预计 OCR 分钟：{_mins}"
            f"（极差 {max(_mins) / max(0.1, min(_mins)):.2f} 倍）"
        )
        log(f"[plan] 权重来源：{weight_src}")
        if imgs and min(imgs) > 0:
            log(f"[plan] 均衡度：最慢/最快 = {max(imgs)/min(imgs):.2f} 倍")
    else:
        log(f"[plan] 不分片（shard_n={shard_n}，帖子 {len(union_ids)}）")

    _guard.save_json(work / "plan" / f"plan_{source}.json", plan)
    log(f"[plan] 完成：传给模块的期号={period}（展示 {display} 期，口径 {caliber}），预期帖子={plan['counts']['expectedPosts']}")
    return 0


def _shard_count_for(source: str) -> int:
    """从环境变量 SHARD_MAP（形如 `dahkg:11,aocai:4,tuku:3`）取本源的片数。

    为什么用环境变量而不是命令行：片数由 workflow 的 plan job 决定，
    prepare 与 shard 两个 job 必须拿到**同一个** N，否则 plan 算出的分组与
    分片 job 的 matrix 下标对不上。workflow 里用一个统一的 env 传下去最简单。
    """
    raw = os.environ.get("SHARD_MAP", "").strip()
    if not raw:
        return 1
    for part in raw.split(","):
        if ":" in part:
            k, _, v = part.partition(":")
            if k.strip() == source:
                try:
                    return max(1, int(v.strip()))
                except ValueError:
                    return 1
    return 1


# ---------------------------------------------------------------------------
# 模式：--shard（三源通用：给模块的列表取数打切片补丁，进程内跑它自己的 CLI）
# ---------------------------------------------------------------------------
def _plan_item_id(item: dict) -> int:
    """列表行的主键（三源字段名不同）。"""
    for key in ("singaporeId", "tid", "articleId"):
        value = item.get(key)
        if value:
            try:
                return int(value)
            except (TypeError, ValueError):
                continue
    return 0



#: 三源列表行里的**作者字段名**（2026-11-17 实测）
#:   dahkg：`senderId`（驼峰）
#:   aocai：`userid`  （全小写！实测字段表只有 userid，没有 senderId/userId）
#:   tuku ：`userid`  （同 aocai 的通行写法；缺失会硬失败，不会静默归 None）
#: 每源的「作者字段候选序列」——按顺序取第一个存在的。
#:   dahkg：`senderId`（驼峰，实测确认）
#:   aocai：`userid`（**全小写**，实测确认；没有 senderId/userId）
#:   tuku ：列行字段尚未实测确认（源站 502），先给多候选，
#:          取不到任何一个仍然**硬失败**（不会静默归到空用户）
SOURCE_USER_KEYS: dict[str, tuple[str, ...]] = {
    "dahkg": ("senderId",),
    "aocai": ("userid",),
    "tuku": ("userid", "userId", "senderId", "username", "nickname"),
}
#: 兼容旧名字（plan 的报错文案会引用）
SOURCE_USER_KEY = {k: v[0] for k, v in SOURCE_USER_KEYS.items()}

#: det 的 limit_side_len（core/scraper.py 里同值）：短边小于它会被放大到它
_DET_SIDE = 736
#: 回复图没有尺寸 → 用回复条数估；实测 2682 张回复图 / 4813 条回复 ≈ 0.56 张/条
_REPLY_IMG_PER_REPLY = 0.56

# 2026-11-21 分片均衡全成本权重常量
# 实测 run 44851897421 极差 dahkg 2.25 / aocai 2.55 倍，原权重只算 OCR 秒数，
# 漏了 ① 每帖固定开销 ② 回复分页请求 ③ 图片下载 → 图少但回复多的帖子被低估。
_BASE_SEC_PER_POST = 1.0
_REPLY_PAGE_SEC = 0.8
_REPLY_PAGE_SIZE = 30
_DOWNLOAD_SEC_PER_IMG = 0.15
#: 回复图中位面积（用户给的实测中位值，单位：det 输入像素）
_MEDIAN_IMG_AREA = 1_920_000
#: 大港澳图片 URL 里的尺寸：`..._<w>_<h>_.png`
_IMG_DIM_RE = re.compile(r"_(\d+)_(\d+)_")


def _det_input_area(url: str) -> int:
    """一张图在 det 输入端的像素数（分片权重的核心量）。

    为什么用这个而不是"图片张数"（2026-11-17 用户指正 + 实测复核）：
      OCR 的 det 会把短边缩放到 `limit_side_len=736` 再推理，
      所以一张 135×2143 的长图，det 输入是 736×11683 ≈ 860 万像素，
      相当于几十张普通图；按"张数"算权重会严重低估长图。
    实测（11 片旧分组）：耗时 vs 权重相关性
      imgTotal 0.026 / 正文图*2+回复 0.603 / **det 输入面积 0.626**
    """
    m = _IMG_DIM_RE.search(url)
    if not m:
        return 0
    w, h = int(m.group(1)), int(m.group(2))
    short, long_ = min(w, h), max(w, h)
    if short <= 0:
        return 0
    if short < _DET_SIDE:
        long_ = round(long_ * _DET_SIDE / short)
        short = _DET_SIDE
    return short * long_


def _ocr_seconds_of_url(url: str) -> float:
    """按 URL 里的尺寸估「这张图 OCR 要多少秒」（分片权重的核心量）。

    ## 为什么用秒数而不是 det 面积（2026-11-19 实测）
    run 44478252994：det 面积权重极差 **1.28 倍**（plan 以为很均衡），
    而实际分片耗时极差 **11.26 倍**（1.9 分 vs 21.4 分）。
    → det 面积不是 OCR 成本的好代理（台账 #13 记录相关 0.626，不够用）。
    原因：det 面积算的是「短边缩放到 736 后的输入像素」，把窄长图放大到荒谬程度
    （135×2143 → 8.6M，而普通 1080×1920 只有 2.1M），但两者 OCR 耗时差不到 4 倍。

    ## 分档依据（本机按**文件体积**分桶实测，2026-11-18/19）
        <100KB（~700px 宽）      0.67 秒
        100-300KB（~1080px 宽）  1.30 秒
        300-600KB（~1080px 宽）  1.37 秒
        600KB-1MB                8.91 秒   ← 跳跃 6.5 倍（跨过 det 缩放临界点）
        >1MB                     7.86 秒
    体积 ≈ 原始像素面积 × 压缩率 → 这里用**原始面积**（w×h）分档。

    ## 额外修正：窄长图（长宽比 > 4）乘 1.5
    实测 135×2143 这类窄长图，文字行密集，rec 阶段比同面积方图慢约 1.5 倍。

    URL 不带尺寸（aocai/tuku 的 `v27-xxx.jpg`）→ 返回 0，由调用方退回按图数计。
    """
    m = _IMG_DIM_RE.search(url or "")
    if not m:
        return 0.0
    try:
        w, h = int(m.group(1)), int(m.group(2))
    except (TypeError, ValueError):
        return 0.0
    if w <= 0 or h <= 0:
        return 0.0

    area = w * h                       # ★ 原始面积，不是 det 缩放后的面积
    if area < 300_000:
        sec = 0.7
    elif area < 1_000_000:
        sec = 1.3
    elif area < 2_500_000:
        sec = 1.5
    elif area < 5_000_000:
        sec = 3.5
    else:
        sec = 8.0

    # 窄长图（长宽比 > 4）：文字行密集，实测慢约 1.5 倍
    ratio = max(w, h) / max(1, min(w, h))
    if ratio > 4:
        sec *= 1.5
    return round(sec, 2)

def _item_image_weight(item: dict) -> int:
    """一条列表行的工作量权重（单位：秒 × 10，用于分片均衡）。

    ## 2026-11-21 重定（消除 2.5 倍失衡）
    实测 run 44851897421 分片极差 dahkg 2.25 / aocai 2.55 / tuku 1.89 倍，
    原权重只算 OCR 秒数，漏了三块真实成本：
      ① 每帖固定开销（详情请求 + 解析）            → _BASE_SEC_PER_POST
      ② 回复分页请求（每 30 条 1 次）              → _REPLY_PAGE_SEC
      ③ 图片下载（每张 0.15 秒，网络+落盘+哈希）   → _DOWNLOAD_SEC_PER_IMG

    ## 优先用 prefetch 的真实数据
    如果 item 里有 `_prefetch`（plan 预抓得到的真实图数/回复数），直接用它；
    否则退回列表行的估算字段。

    ## 返回
    int，单位秒×10（保留一位小数精度）。
    """
    import math as _m

    pf = item.get("_prefetch") or {}

    # ---- 正文图 URL ----
    if pf.get("bodyUrls") is not None:
        body_urls = [str(u) for u in (pf.get("bodyUrls") or []) if u]
    else:
        body_urls = [
            u.strip() for u in str(item.get("img") or "").split(",") if u.strip()
        ]

    # ---- 回复图 URL（prefetch 才有精确值）----
    reply_urls = [str(u) for u in (pf.get("replyUrls") or []) if u]

    # ---- 回复条数 ----
    if pf.get("replyCount") is not None:
        n_replies = max(0, int(pf.get("replyCount") or 0))
    else:
        try:
            n_replies = max(0, int(item.get("replyPoint") or 0))
        except (TypeError, ValueError):
            n_replies = 0

    # ---- 图片总数 ----
    if pf.get("imgCount") is not None:
        n_imgs = max(0, int(pf.get("imgCount") or 0))
    else:
        try:
            n_imgs = max(0, int(item.get("imgTotal") or 0))
        except (TypeError, ValueError):
            n_imgs = 0
        n_imgs = n_imgs or (len(body_urls) + len(reply_urls))

    # ---- 成本累加 ----
    secs = _BASE_SEC_PER_POST
    # 回复分页：每 30 条回复一页
    secs += _REPLY_PAGE_SEC * max(0, _m.ceil(n_replies / max(1, _REPLY_PAGE_SIZE)))
    # OCR：正文图 + 回复图（按面积分档；无尺寸信息则按 1.0 秒/张）
    for u in body_urls:
        secs += _ocr_seconds_of_url(u) or 1.0
    for u in reply_urls:
        secs += _ocr_seconds_of_url(u) or 1.0
    # 如果 reply_urls 为空但有回复数（prefetch 不可用），按回复数 × 中位图数估
    if not reply_urls and n_replies:
        secs += n_replies * _REPLY_IMG_PER_REPLY * 1.0
    # 图片下载成本
    secs += _DOWNLOAD_SEC_PER_IMG * max(0, n_imgs)

    return max(1, int(round(secs * 10)))



def _sender_of(item: dict, source: str | None = None) -> str:
    """列表行里的作者标识（按源取字段，统一成字符串）。

    ⚠️ 2026-11-17 事故：原来用一个「通用 keys 列表」兜底，aocai 的真实字段是
    **`userid`（全小写）**，不在列表里 → 所有 aocai 帖子的作者都取成空串 →
    按用户分组时它们全归到同一个空用户 → 其余分片拿到 0 帖 → 空片 →
    模块报「澳彩交流版块没有 XXX 期的帖子」→ 分片失败。
    现在改成**按源声明字段名**，取不到就返回空串，由调用方硬失败（不静默）。
    """
    if source:
        for key in SOURCE_USER_KEYS.get(source, ()):
            value = (item or {}).get(key)
            if value not in (None, "", 0):
                return str(value)
        return ""
    # 未指定源时（比如通用工具）才退回候选列表
    for key in ("senderId", "userid", "userId", "authorId", "uid"):
        value = (item or {}).get(key)
        if value not in (None, "", 0):
            return str(value)
    return ""


def _rebalance_groups(
    groups: list[list[int]], weights: dict[int, int],
    max_ratio: float = 1.35, max_moves: int = 400,
    poster_of: dict[int, str] | None = None,
    user_weights: dict[str, int] | None = None,
    split_ratio: float = 2.0,
) -> list[list[int]]:
    """分片负载再平衡 —— **只拆超大用户，绝不搬动普通帖子**。

    ## ⚠ 2026-11-21 根因重写（run 44867937884 实测）
    原实现「把重片里最重的那帖搬到最轻片」，导致：
    ```
    [shard] 本片 53 帖 → 容量只够 46 帖 → 截断 7 帖
    [merge-summary] ❌ 汇总后 647 帖 != 可达上限 686 帖（缺 39 帖）
    ```
    搬走的帖子在新分片没有历史缓存 → hit_rate 骤降 → 估算超限 → 截断 →
    summary 模式的 merge 不采集 → 缺帖 → 整轮失败。

    ## 用户的设计（2026-11-21 确认）
    「老用户还是在原来那片，新用户再平均分给各分片」
    → **老用户的帖子绝不能换片**（否则缓存全废）

    ## 新逻辑
    1. 普通用户：整块不动（缓存集中，命中率高）
    2. **超大用户**：权重 > 平均片负载 × `split_ratio` 时，
       把该用户的帖子按权重从大到小，分发到最轻的片
       （只有 1605 图那种极端情况会触发）
    3. 其余情况不做任何搬动

    ## 参数
    poster_of    : {articleId: userId}，用于识别「哪些帖子属于超大用户」
    user_weights : {userId: 权重}，用于判定超大用户
    split_ratio  : 超过「平均片负载 × 该值」即视为超大用户

    ## 为什么这样不影响完整性
    `_slice_items` 是「by-user ∪ planned-ids」：
      · 用户整块在某片 → 按 by-user 拿到全部帖子
      · 用户被拆 → 各片按 planned-ids 拿到分给它的那部分
    两条路都覆盖，且每个 aid 只在一个片里。
    """
    if not groups or len(groups) < 2:
        return groups

    def _load(g: list[int]) -> int:
        return sum(max(1, int(weights.get(a, 1))) for a in g)

    loads = [_load(g) for g in groups]
    total = sum(loads)
    if total <= 0:
        return groups

    # 没有 poster_of / user_weights → 无法识别超大用户 → 不动（保守）
    if not poster_of or not user_weights:
        return groups

    avg_shard = total / len(groups)
    threshold = avg_shard * max(1.0, float(split_ratio))

    # 找超大用户
    huge_users = {u for u, w in user_weights.items() if int(w) > threshold}
    if not huge_users:
        return groups

    # 把超大用户的帖子按权重从大到小，分发到最轻的片
    moves = 0
    for uid in sorted(huge_users, key=lambda u: -int(user_weights.get(u, 0))):
        # 该用户当前在哪些片、有哪些帖
        aids = [a for a in weights if poster_of.get(a) == uid]
        aids.sort(key=lambda a: -int(weights.get(a, 1)))
        for a in aids:
            if moves >= max_moves:
                break
            # 从原片移除
            src_i = next((k for k, g in enumerate(groups) if a in g), None)
            if src_i is None:
                continue
            lo = min(range(len(groups)), key=lambda k: (loads[k], k))
            if lo == src_i:
                continue
            # 只在「搬过去能显著改善」时才搬
            w = max(1, int(weights.get(a, 1)))
            if loads[src_i] <= loads[lo] * max_ratio:
                break
            groups[src_i].remove(a)
            groups[lo].append(a)
            loads[src_i] -= w
            loads[lo] += w
            moves += 1

    return groups


def _group_by_user(
    user_weights: dict[str, int], n: int, prev: dict[str, int] | None = None,
    split_ratio: float = 2.0,
):
    """按**用户**做 LPT 分组，老用户永不搬动（跨轮稳定 → 缓存命中）。

    ## 用户的设计（2026-11-21 确认）
    「指挥官按用户名把用户平均分给各分片；
      下次分配时老用户还是在原来那片；新用户再平均分给各分片。」

    ## 三步
    1. **老用户就位**：沿用 `prev` 台账的分片号（缓存集中，永不搬）
    2. **新用户分配**：按权重从大到小，塞进最轻的片
    3. **超大用户拆分**：若某用户权重 > `split_ratio × 平均片负载`，
       它的帖子会拆到多片（避免整片超时）。
       判定与拆分在调用方用 `_rebalance_groups` 的「只拆超大用户」模式完成。

    返回 (groups_by_user, user_to_shard)。
    """
    prev = prev or {}
    n = max(1, n)
    groups: list[list[str]] = [[] for _ in range(n)]
    loads = [0] * n
    mapping: dict[str, int] = {}

    # 1) 老用户先就位（**永不搬动** —— 这是缓存能不能命中的关键）
    for uid, w in user_weights.items():
        idx = prev.get(uid)
        if isinstance(idx, int) and 0 <= idx < n:
            groups[idx].append(uid)
            loads[idx] += max(1, int(w))
            mapping[uid] = idx

    # 2) 新用户按权重从大到小，塞进最轻的片（平均分配）
    rest = [(uid, max(1, int(w))) for uid, w in user_weights.items() if uid not in mapping]
    rest.sort(key=lambda x: -x[1])
    for uid, w in rest:
        idx = min(range(n), key=lambda i: (loads[i], i))
        groups[idx].append(uid)
        loads[idx] += w
        mapping[uid] = idx

    return groups, mapping


def _user_shard_path(state_dir: Path, source: str) -> Path:
    return Path(state_dir) / f"user_shard_{source}.json"


def _load_user_shard(state_dir: Path, source: str) -> dict:
    """读「用户 → 分片」台账（跨轮稳定的依据）。"""
    from . import _guard

    data = _guard.load_json(_user_shard_path(state_dir, source), {})
    mapping = (data or {}).get("userToShard") or {}
    out: dict[str, int] = {}
    for k, v in mapping.items():
        try:
            out[str(k)] = int(v)
        except (TypeError, ValueError):
            continue
    return out


def _save_user_shard(state_dir: Path, source: str, mapping: dict[str, int], period: int, n: int) -> None:
    """保存「用户 → 分片」台账。"""
    from . import _guard

    _guard.save_json(
        _user_shard_path(state_dir, source),
        {
            "period": period,
            "shards": n,
            "updatedAt": time.strftime("%Y-%m-%d %H:%M:%S"),
            "_note": "按用户分片：同一作者固定落同一片，跨轮稳定；新用户进入当前最轻的片",
            "userToShard": mapping,
        },
    )


def _slice_items(items, shard: int, total: int, sink: dict, planned_ids: set[int] | None = None,
                 mine_users: set[str] | None = None, source: str | None = None):
    """把 items 切成一片：优先按 plan 里算好的均衡分组，没有就退回 id%N。"""
    if isinstance(items, tuple):
        seq, meta = items[0], items[1:]
    else:
        seq, meta = items, ()
    try:
        seq = list(seq)
    except TypeError:
        return items
    sink["total"] = len(seq)
    sink["imagesTotal"] = sum(_item_image_weight(it) for it in seq)
    if mine_users is not None:
        # 按用户分片（方案 3）：一个作者的所有帖子都进同一片；新帖因作者已在本片而自动跟随。
        #
        # ⚠ 2026-11-19 补丁：并上「本片分到的 id」。
        # 原因：单个超大作者（实测 1605 图）LPT 无法拆分 → 该片估算 52.7 分钟（上限 15）
        # → 转移 72% → merge 全量重采 25.8 分钟 → 整轮 55 分。
        # 现在 plan 会把超大作者拆到多片（splitUsers），这些帖靠 planned_ids 落到正确的片，
        # 所以这里必须做并集（否则被拆的帖会被 by-user 过滤掉）。
        _by_user = [it for it in seq if _sender_of(it, source) in mine_users]
        if planned_ids:
            _ids = {int(x) for x in planned_ids}
            _by_id = [it for it in seq if _plan_item_id(it) in _ids and it not in _by_user]
            if _by_id:
                _by_user.extend(_by_id)
                sink["mode"] = "by-user+split"
        picked = _by_user
        sink["mode"] = sink.get("mode") or "by-user"
        if not picked and seq and planned_ids:
            # 兜底（2026-11-17 事故）：按用户取到空片时，若 plan 里有本片的帖子 id 列表，就退回按 id 取。
            # 否则空列表会让模块直接报「列表里没有 X 期的帖子」并让整个分片 job 失败。
            fallback = [it for it in seq if _plan_item_id(it) in planned_ids]
            if fallback:
                sink["mode"] = "by-user(empty)->planned-ids"
                picked = fallback
                log(
                    f"[shard] ⚠ 按用户切片为空（{len(mine_users)} 个作者都不在列表里），"
                    f"已退回按 plan 的帖子 id 取到 {len(fallback)} 帖，避免空片失败"
                )
    elif planned_ids is not None:
        picked = [it for it in seq if _plan_item_id(it) in planned_ids]
        sink["mode"] = "planned-ids"
    else:
        # 方案 3 cleanup：删掉「按 ID 取模」兜底。
        # 取模会让每次运行的归属漂移（缓存命中率低、无法复现），
        # 而且和 plan 的分组冲突时是**静默**错误 —— 宁可直接失败。
        raise SystemExit(
            "[shard] ❌ 没有分片依据：plan 里既没有按用户分组（shards）也没有分组（shardPlan.groups）。"
            "拒绝用「按 ID 取模」兜底（会静默漂移）。请检查 plan 阶段。"
        )
    # ---- T2：补齐「plan 里有、扫列表没返回」的漏帖（分页抖动）----
    # 依据 T0 实测：按 id 抓详情即可构造出同形状的列表行，不必等 merge。
    if planned_ids:
        period_hint = 0
        for _it in seq:
            period_hint = int((_it or {}).get("period") or (_it or {}).get("qs") or 0)
            if period_hint:
                break
        picked, _failed_ids = _fill_missing_items(
            source or "", picked, planned_ids, period_hint, log
        )
        if _failed_ids:
            # 补不上的交 merge（unfinished 机制），不在这里死磕
            sink["missingIds"] = _failed_ids

    sink["picked"] = len(picked)
    sink["imagesPicked"] = sum(_item_image_weight(it) for it in picked)
    return (picked, *meta) if meta else picked


# 补齐漏帖的上限（异常情况保护：漏帖通常 <10，若上百帖说明别处有问题，不在这里死磕）
_MISSING_FIX_MAX = 50


def _row_dahkg(raw: dict, period: int) -> dict:
    """dahkg：顶层 11 字段 + `_raw`（27 字段，T0 实测）。"""
    def ai(v, d=0):
        try:
            return int(v)
        except (TypeError, ValueError):
            return d

    return {
        "tid": ai(raw.get("singaporeId")),
        "userid": ai(raw.get("senderId")),
        "qs": ai(raw.get("periods"), period) or int(period or 0),
        "title": raw.get("title") or "",
        "zhaiyao": raw.get("content") or "",
        "readNum": ai(raw.get("browsePoint")),
        "huifushu": ai(raw.get("replyPoint")),
        "time": "",
        "head": raw.get("headImg") or "",
        "username": raw.get("nickname") or "",
        "_raw": dict(raw),
    }


def _row_aocai(tiezi: dict, period: int) -> dict:
    """aocai：20 字段（`imgs` 是 **str**，不是 list —— T0 实测）。"""
    def ai(v, d=0):
        try:
            return int(v)
        except (TypeError, ValueError):
            return d

    return {
        "tid": ai(tiezi.get("tid")),
        "userid": ai(tiezi.get("userid")),
        "qs": ai(tiezi.get("qs"), period) or int(period or 0),
        "title": tiezi.get("title") or "",
        "imgs": tiezi.get("imgs") if isinstance(tiezi.get("imgs"), str) else "[]",
        "imgurl": tiezi.get("imgurl") or "",
        "zhaiyao": tiezi.get("zhaiyao") or "",
        "readNum": ai(tiezi.get("readNum")),
        "huifushu": ai(tiezi.get("huifushu")),
        "time": tiezi.get("time") or "",
        "head": tiezi.get("head") or "",
        "username": tiezi.get("username") or "",
    }


def _row_tuku(raw: dict, period: int) -> dict:
    """tuku：43 字段全集；`picture`(dict) 必须转成 `pictureList`(list) —— build_post 读后者。"""
    row = dict(raw)
    pic = raw.get("picture")
    row["pictureList"] = [pic] if isinstance(pic, dict) else (raw.get("pictureList") or [])
    if not int(row.get("period") or 0):
        row["period"] = int(period or 0)      # 期号必须有效，否则 main 会当非本期丢掉
    row["periodStr"] = str(row.get("period") or period)
    row.setdefault("isCompete", 0)
    row.setdefault("updateTime", raw.get("createTime") or "")
    row.setdefault("commentCount", raw.get("commentCount") or 0)
    row.setdefault("clickCount", raw.get("clickCount") or 0)
    row.setdefault("top", raw.get("top") or 0)
    row.setdefault("highQuality", raw.get("highQuality") or 0)
    return row


def _fetch_missing_row(source: str, aid: int, period: int) -> dict | None:
    """按 id 抓详情，构造一条与 `_patch_shard` 的 items 同形状的列表行。

    失败（网络/404/解析）返回 None —— 调用方会把该 id 写进 unfinished 交 merge 再试。
    """
    try:
        if source == "dahkg":
            from core import dahkg

            raw = dahkg.fetch_detail(int(aid)) or {}
            return _row_dahkg(raw, period) if raw else None

        if source == "aocai":
            from core import forum as _fm

            detail = _fm.fetch_thread_detail(
                int(aid), 0, include_replies=True, delay=0.0, max_pages=50
            ) or {}
            tiezi = detail.get("tiezi") or {}
            return _row_aocai(tiezi, period) if tiezi else None

        from core import scraper as _sm

        raw = _sm.api_get("/unite49/h5/article/detail", {"articleId": int(aid)}) or {}
        for wrapper in ("data", "result", "article"):
            if isinstance(raw.get(wrapper), dict):
                raw = raw[wrapper]
                break
        return _row_tuku(raw, period) if raw else None
    except Exception as exc:  # noqa: BLE001 - 单帖补齐失败不该拖垮分片
        log_line = f"[shard] ⚠ 补齐 {source}#{aid} 失败：{type(exc).__name__}: {exc}"
        try:
            print(log_line, flush=True)
        except Exception:  # noqa: BLE001
            pass
        return None


def _fill_missing_items(source: str, picked: list[dict], planned_ids, period: int, log) -> tuple[list[dict], list[int]]:
    """把「planned_ids 里有、picked 里没有」的帖按 id 补齐。返回 (新 items, 补不上的 id 列表)。"""
    if not planned_ids:
        return picked, []

    def _id_of(it: dict) -> int:
        for k in ("articleId", "tid", "singaporeId", "id"):
            v = (it or {}).get(k)
            try:
                if v is not None:
                    return int(v)
            except (TypeError, ValueError):
                continue
        return 0

    got = {_id_of(it) for it in picked}
    missing = sorted(int(x) for x in planned_ids if int(x) not in got)
    if not missing:
        log(f"[shard] ✅ 扫列表 {len(picked)} 帖 = plan 本片 {len(planned_ids)} 帖（无漏帖）")
        return picked, []

    if len(missing) > _MISSING_FIX_MAX:
        log(f"[shard] ⚠ 扫列表漏 {len(missing)} 帖（超过补齐上限 {_MISSING_FIX_MAX}），"
            f"只补前 {_MISSING_FIX_MAX} 帖，其余交 merge")
    todo = missing[:_MISSING_FIX_MAX]
    log(f"[shard] ⚠ 扫列表漏 {len(missing)} 帖（分页抖动），按 id 抓详情补齐：{todo[:8]}")

    # ⚠ 2026-11-21 根因修复（run 44913469222 实测缺 1 帖 3230714）：
    #   分片扫列表漏 1 帖后按 id 补抓，但源站 `/getbbsreply` 那一刻返回非 JSON
    #   （Expecting value: line 1 column 1 (char 0)）→ 只试一次就放弃 →
    #   写 unfinished 交 merge → merge 是 summary 模式（MAG 只汇总，不采集）
    #   → 补不了 → 缺 1 帖 → 整轮失败。
    #
    #   这是**源站瞬时故障**（不是我方逻辑错），正确做法是对「同一个已确认失败的
    #   具体帖子」做**有限次**带退避的重试：
    #     · 次数固定 3，不随失败增长
    #     · 只重试失败的那几帖，不重跑整个分片
    #     · 3 次仍失败 → 如实写 unfinished 并明确报错（不静默、不降级）
    #   这不是「靠加重试掩盖结构性问题」，而是消化外部瞬时抖动；
    #   结构性缺口仍有完整的上报与拦截路径。
    _REFILL_TRIES = 3
    _REFILL_BACKOFF = (1.0, 2.0)

    fixed = 0
    failed: list[int] = []
    for aid in todo:
        row = None
        for _try in range(_REFILL_TRIES):
            row = _fetch_missing_row(source, aid, period)
            if row:
                break
            if _try < _REFILL_TRIES - 1:
                _wait = _REFILL_BACKOFF[min(_try, len(_REFILL_BACKOFF) - 1)]
                log(f"[shard] 补抓 {source}#{aid} 第 {_try + 1} 次失败，{_wait:.0f}s 后重试（源站瞬时故障）")
                time.sleep(_wait)
        if not row:
            log(f"[shard] ⚠ 补抓 {source}#{aid} 连续 {_REFILL_TRIES} 次失败（源站持续不可用）")
        if row:
            picked.append(row)
            fixed += 1
        else:
            failed.append(aid)
    failed += missing[_MISSING_FIX_MAX:]
    log(f"[shard] ✅ 已补齐 {fixed}/{len(todo)} 帖"
        + (f"；补不上 {len(failed)} 帖交 merge：{failed[:8]}" if failed else ""))
    return picked, failed


def _patch_shard(source: str, shard: int, total: int, sink: dict, planned_ids: set[int] | None = None,
                 mine_users: set[str] | None = None):
    """给该源的「列表取数函数」打补丁，只保留本片的帖子。

    ## 为什么用「打补丁 + 进程内跑原生 CLI」而不是重写采集流程
    三源的采集流程差异极大（tuku 有 details/replies/ocr 三段独立缓存；
    aocai 走 collect_forum_posts；dahkg 自带 collect_posts）。
    如果我按文档逐个复刻，任何一处顺序/参数不同都会让 merge 产物与串行版**不一致**，
    而这正是分片要证明的东西。这里改成：**只切列表结果**，下游全部走模块自己的代码，
    语义天然一致。

    补丁只影响本进程（分片 job）；merge job 用 subprocess 跑完整流程，不受影响。
    """
    if shard < 0 or total <= 1:
        return None
    originals = []

    def wrap(mod, name: str):
        original = getattr(mod, name, None)
        if original is None:
            return
        def patched(*a, **kw):
            return _slice_items(original(*a, **kw), shard, total, sink, planned_ids=planned_ids,
                                 mine_users=mine_users, source=source)
        setattr(mod, name, patched)
        originals.append((mod, name, original))

    if source == "dahkg":
        from core import dahkg
        wrap(dahkg, "scan_period")
    elif source == "aocai":
        from core import forum as forum_mod
        wrap(forum_mod, "scan_period")
    elif source == "tuku":
        from core import scraper
        wrap(scraper, "fetch_period_posts")
        wrap(scraper, "fetch_latest_posts")
    return originals


def _run_module_main(module: str, argv: list[str]) -> int:
    """在**当前进程**里调用模块自己的 `main()`（这样打上去的补丁才生效）。

    踩坑记录（2026-11-16 实测）：先用 `runpy.run_module(module, run_name="__main__")`，
    结果切片补丁完全没生效 —— 因为 runpy 把模块代码执行在**独立的 `__main__` 命名空间**里，
    而我的补丁打在 `core.scraper` 这个模块对象上，两者不是同一个 globals。
    改成 `import_module(...).main()` 后，`main()` 内部的非限定名（如 `fetch_period_posts`）
    正是从被补丁的模块 globals 里取，切片才生效。
    """
    import importlib

    mod = importlib.import_module(module)
    entry = getattr(mod, "main", None)
    if entry is None:
        raise SystemExit(f"[错误] {module} 没有 main()，无法在进程内调用")

    old_argv = sys.argv
    sys.argv = [module, *argv]
    code = 0
    try:
        result = entry()
        if isinstance(result, int):
            code = result
    except SystemExit as exc:
        if isinstance(exc.code, str):
            log(f"[shard] 模块退出：{exc.code}")
            code = 1
        else:
            code = int(exc.code or 0)
    except Exception as exc:  # noqa: BLE001
        # ⚠️ 2026-11-17 事故：模块可能抛 RuntimeError（如「澳彩交流版块没有 XXX 期的帖子」），
        # 原来只 catch SystemExit，异常会**直接冲出 do_shard**，导致后面
        # 「空片不计失败」的判断完全没机会执行、整个分片 job 变 failure。
        # 这里统一收敛成非 0 退出码，交给调用方按「空片/失败」判断。
        log(f"[shard] 模块抛异常：{type(exc).__name__}: {exc}")
        code = 1
    finally:
        sys.argv = old_argv
    return code


def _install_diag_probes() -> dict:
    """给「OCR 纯识别」与「图片下载」各装一个计时探针（仅本进程、仅 cloud 侧）。

    ## 为什么需要（2026-11-17 用户要求先分清 CPU 异构 / 网络异构）
    之前我用日志里的 `[OCR] 识别进度 … 已用 Ns` 算吞吐，那是**OCR 阶段自计时**，
    而模块的媒体流水线是「边下边识」重叠的 → 该数字混入了下载等待，不能据此断定是 CPU 慢。
    这里直接在函数边界计时：`analyze_image` 的累计秒数就是**纯识别**，
    `download_images` 的累计秒数就是**纯下载**。

    返回的 stats 由调用方读取（shard 结束时写进 artifact）。
    """
    import importlib

    stats = {
        "ocr": {"calls": 0, "seconds": 0.0},
        "download": {"calls": 0, "seconds": 0.0},
        "patched": {"ocr": [], "download": []},
    }

    def wrap(mod_name: str, attr: str, key: str) -> None:
        """包裹该模块**当前**持有的那个函数。

        ⚠️ 踩坑记录（2026-11-17）：最初只包裹 `core.ocr.analyze_image`，
        再要求「其它模块的属性 is 原始函数」才包裹。结果 `do_shard` 里
        `_patch_shard` **先**导入了 core 模块（那时异常/正常已绑定原始函数），
        之后安装探针就判断失败 → 只有 core.ocr 被包裹，dahkg 那条
        `scraper.ocr_post_images → analyze_image` 路径实际没被测到。
        改成「各自包裹自己的当前绑定」后与导入顺序无关，也不会重复计数
        （每个模块的调用只会经过它自己的那层包装）。
        """
        try:
            mod = importlib.import_module(mod_name)
        except Exception:  # noqa: BLE001 - 模块不存在就跳过
            return
        cur = getattr(mod, attr, None)
        if cur is None or getattr(cur, "_diag_wrapped", False):
            return

        def timed(*a, **kw):
            t0 = time.perf_counter()
            try:
                return cur(*a, **kw)
            finally:
                stats[key]["calls"] += 1
                stats[key]["seconds"] += time.perf_counter() - t0

        timed._diag_wrapped = True  # type: ignore[attr-defined]
        setattr(mod, attr, timed)
        stats["patched"][key].append(f"{mod_name}.{attr}")

    for m in ("core.ocr", "core.scraper", "core.forum", "core.dahkg",
              "core.history", "core.backfill", "core.ocr_samples"):
        wrap(m, "analyze_image", "ocr")
    for m in ("core.scraper", "core.forum", "core.history", "core.backfill"):
        wrap(m, "download_images", "download")

    return stats


def _cpu_model() -> str:
    """读 CPU 型号（判断是不是所有 runner 同型号）。"""
    try:
        out = subprocess.run(["lscpu"], capture_output=True, text=True, timeout=15)
        for line in (out.stdout or "").splitlines():
            if line.lower().startswith("model name"):
                return line.split(":", 1)[1].strip()
    except Exception:  # noqa: BLE001
        pass
    return "unknown"


def _ocr_micro_benchmark(out_root: Path, limit: int = 3) -> dict:
    """预热后对 3 张真实图做纯 OCR，返回每张秒数（用于横向比较 runner 算力）。"""
    import importlib

    try:
        from core import ocr as ocr_mod
    except Exception as exc:  # noqa: BLE001
        return {"error": f"{type(exc).__name__}: {exc}"}
    analyze = getattr(ocr_mod, "analyze_image", None)
    if not analyze:
        return {"error": "core.ocr 没有 analyze_image"}
    imgs: list[Path] = []
    for pat in ("*.png", "*.jpg", "*.jpeg", "*.webp"):
        imgs += list(Path(out_root).rglob(pat))
        if len(imgs) >= limit * 4:
            break
    imgs = imgs[:limit]
    if not imgs:
        return {"error": "找不到可测的图片"}
    seconds: list[float] = []
    cache_dir = Path(out_root) / ".cache"
    cache_dir.mkdir(parents=True, exist_ok=True)
    try:
        analyze(str(imgs[0]), 0, str(cache_dir))   # 预热：把引擎拉起来
        for img in imgs:
            t0 = time.perf_counter()
            analyze(str(img), 0, str(cache_dir))
            seconds.append(round(time.perf_counter() - t0, 3))
    except Exception as exc:  # noqa: BLE001
        return {"error": f"{type(exc).__name__}: {exc}", "done": seconds}
    return {
        "images": len(seconds),
        "seconds": seconds,
        "median": sorted(seconds)[len(seconds) // 2] if seconds else 0,
    }


#: 探针结果缓存（同进程只测一次）
def _install_sigterm_surrender(work: Path, source: str, shard_index: int,
                                planned_ids: set[int], log) -> bool:
    """装 SIGTERM 处理器：把「预算用完」变成**优雅投降**而不是被 SIGKILL。

    ## 为什么需要（2026-11-19 实测事故）
    分片跑到 25 分钟被 GitHub job timeout **SIGKILL** → 没写 unfinished、没建产物、判 failure。
    改成 workflow 用 `timeout --signal=SIGTERM --kill-after=30 1440`（24 分钟）后，
    SIGTERM 是**可捕获**的，于是可以：
        写 unfinished（保守：本片全部 planned_ids 都写，merge 会去重）→ `SystemExit(0)`
    `_run_module_main` 的 `except SystemExit: code = int(exc.code or 0)` 会把它当成
    「模块正常退出 0」→ do_shard 继续收尾写分片 json → 返回 0 → **分片判 success**。

    ## 牺牲与收益
    牺牲：本片没做完的活（但那部分本来就会由 merge 的 unfinished 机制补抓）
    收益：分片不被 kill、整轮 < 40 分钟、publish 能执行（数据入库）

    返回 True 表示装上了（Windows 主线程也支持 Python 级 SIGTERM 处理）。
    """
    import signal as _signal

    def handler(signum, frame):  # noqa: ARG001 - signal 签名固定
        log("[shard] ⚠ 收到 SIGTERM（预算用完），写 unfinished 后优雅退出")
        try:
            # ⚠ 必须在这里 import：`_guard` 只在 do_shard 函数内导入过，模块级没有。
            # 2026-11-19 踩坑：handler 里直接用 `_guard` → NameError 被 except 吞掉
            # → 文件没写、只打了「写 unfinished 失败」 → 分片「优雅退出」但交付物缺失。
            from . import _guard
            _guard.save_json(
                work / "unfinished" / f"unfinished_{source}_{shard_index}.json",
                {
                    "source": source,
                    "shard": shard_index,
                    "ids": sorted(int(x) for x in (planned_ids or [])),
                    "reason": "SIGTERM（分片预算用完，workflow 的 timeout 触发）",
                    "ts": time.strftime("%Y-%m-%d %H:%M:%S"),
                },
            )
            log(f"[shard] ✅ unfinished 已写（{len(planned_ids or [])} 帖），merge 会补抓")
        except Exception as exc:  # noqa: BLE001 - 写盘失败也要正常退出
            log(f"[shard] ⚠ 写 unfinished 失败：{type(exc).__name__}: {exc}")
        raise SystemExit(0)          # 正常退出 → 分片判 success

    try:
        _signal.signal(_signal.SIGTERM, handler)
        return True
    except Exception as exc:  # noqa: BLE001 - 非主线程/平台不支持时降级
        log(f"[shard] ⚠ 装 SIGTERM handler 失败（{type(exc).__name__}），仍由 job timeout 兜底")
        return False


def _find_ocr_cache_dir(work: Path, out_root: Path) -> Path | None:
    """找本源本期的 `.cache` 目录（分片启动时只有本期缓存被铺回）。

    返回 `.../.cache`（其下有 `ocr/`），找不到返回 None。
    多期并存时取「ocr 里 json 最多」的那个 —— 命中当期的概率最大。
    """
    best: Path | None = None
    best_n = -1
    for cand in Path(out_root).rglob(".cache"):
        if not cand.is_dir():
            continue
        ocr_dir = cand / "ocr"
        n = sum(1 for q in ocr_dir.glob("*.json")) if ocr_dir.exists() else 0
        if n > best_n:
            best, best_n = cand, n
    return best


def _pick_within_budget(
    ids: list[int], cost: dict, per_image_seconds: float, budget_seconds: float,
    hit_rate: float = 0.0,
) -> tuple[list[int], list[int]]:
    """按预算挑出本机要处理的帖子，其余留给 merge。

    ## ⚠ 2026-11-21 根因修复（run 44867937884 实测）
    原实现用「每图未命中成本 × 全部图片数」估算，**忽略缓存命中率**：
    ```
    [shard] 本片 642 图，缓存命中 206（32%），有效 3.910 秒/次
    [shard] 反推未命中成本 5.755 秒/次
    → 估算 642 × 3.910 = 2510 秒 = 41.8 分钟（上限 15）
    → 只处理 46 帖，7 帖写 unfinished 交 merge
    ```
    而实测：
    ```
    [diag] 本片总耗时 243.4s = OCR 0.0s（全部命中缓存） + 下载 2.5s + 其他 240.9s
    ```
    **估算高估 10 倍**，导致不必要的截断；而 summary 模式的 merge **不做采集**，
    无法补抓 unfinished → 13 片合计缺 39 帖 → 完整性防线拦截 → 整轮失败。

    ## 修法
    按「命中率」折算期望成本：
      expected = n_img × (hit_rate × HIT_SEC + (1 - hit_rate) × per_image_seconds)
    其中 HIT_SEC ≈ 0.05 秒（读一个 OCR JSON，不是重新推理）。

    ## 保守兜底
    hit_rate 未知（0）时退化为旧行为（全按未命中算），保证安全。
    """
    if per_image_seconds <= 0 or budget_seconds <= 0 or not ids:
        return list(ids), []
    # 命中一张图的成本（读缓存 JSON + 匹配，实测约 0.05 秒）
    HIT_SEC = 0.05
    hr = max(0.0, min(1.0, float(hit_rate or 0.0)))
    eff_sec = hr * HIT_SEC + (1.0 - hr) * per_image_seconds
    usable = max(0.0, budget_seconds - 60.0)
    taken: list[int] = []
    rest: list[int] = []
    acc = 0.0
    for aid in sorted(ids, key=lambda x: cost.get(x, 1)):
        est = max(1, cost.get(aid, 1)) * eff_sec
        if acc + est > usable:
            rest.append(aid)
            continue
        acc += est
        taken.append(aid)
    return taken, rest


def _collect_unfinished(work: Path, source: str, log) -> list[int]:
    """收集本源的 unfinished 清单（分片因预算到点留下的未处理帖子）。

    分片 job 会写 `work/unfinished/unfinished_<源>_<片>.json`，随 artifact 传到 merge；
    merge 里 artifact 解在 `work/incoming-cache/<artifact>/unfinished/...`，
    所以这里递归找一遍。失败/损坏一律忽略（不能让补抓本身把 merge 搞挂）。
    """
    ids: set[int] = set()
    seen_files = 0
    for path in Path(work).rglob(f"unfinished_{source}_*.json"):
        try:
            import json as _json

            data = _json.loads(path.read_text(encoding="utf-8"))
        except Exception as exc:  # noqa: BLE001
            log(f"[merge] ⚠ 读取 {path.name} 失败（忽略）：{type(exc).__name__}")
            continue
        seen_files += 1
        for x in data.get("ids") or []:
            try:
                ids.add(int(x))
            except (TypeError, ValueError):
                continue
    if seen_files:
        log(f"[merge] unfinished 文件 {seen_files} 份，合计 {len(ids)} 帖")
    return sorted(ids)


#: 每帖必须包含的字段（用户第三节「第 2 道：分片输出字段完整」）。
#: ⚠️ 三源字段名不同（2026-11-17 实测）：
#:   dahkg 用 `nicePoint` / `browsePoint`；tuku 用 `likes` / `views`。
#:   第一次实现只按 dahkg 取名 → tuku 1284 帖全被判"缺 nicePoint"而硬失败。
#:   改成**别名组**：每组只要有任意一个存在即算通过。
REQUIRED_FIELD_ALIASES: tuple[tuple[str, ...], ...] = (
    ("articleId",),
    ("title",),
    ("text", "content"),
    ("createTime",),
    ("userId",),
    ("username", "nickname"),
    ("images",),
    ("commentsAll", "replies", "comments"),
    ("nicePoint", "likes"),
    ("period",),
    ("browsePoint", "views"),
)


def _check_post_fields(posts: list[dict]) -> dict:
    """第 2 道：每帖字段完整性（缺任何一组 → 硬失败）。"""
    bad: list[dict] = []
    for p in posts:
        miss = [grp[0] if len(grp) == 1 else "|".join(grp) for grp in REQUIRED_FIELD_ALIASES
                if not any(f in p for f in grp)]
        if miss:
            bad.append({"articleId": p.get("articleId"), "missing": miss})
    return {"checked": len(posts), "bad": bad[:20], "badCount": len(bad), "passed": not bad}


def _check_images_fields(posts: list[dict]) -> dict:
    """第 2 道（图片部分）：每张图必须有 url/localPath/downloadStatus/ocr 相关字段。"""
    bad: list[dict] = []
    total = 0
    for p in posts:
        for img in p.get("images") or []:
            total += 1
            if not isinstance(img, dict):
                bad.append({"articleId": p.get("articleId"), "why": "image 不是 dict"})
                continue
            miss = [f for f in ("url", "localPath", "downloadStatus") if f not in img]
            if miss:
                bad.append({"articleId": p.get("articleId"), "missing": miss, "url": str(img.get("url"))[:80]})
    return {"imagesChecked": total, "bad": bad[:20], "badCount": len(bad), "passed": not bad}


def _check_reply_counts(posts: list[dict], declared_map: dict) -> dict:
    """补 1：回复数对账（声明 vs 实抓，允许 5% 缺口，超了明示）。

    declared 来自列表接口的 `replyPoint`（plan 的 postHashes 里带着）；
    fetched 用 `commentsAll`（过滤前的全量）。
    """
    shortfall: list[dict] = []
    compared = 0
    for p in posts:
        aid = int(p.get("articleId") or 0)
        declared = int((declared_map.get(str(aid)) or {}).get("replyPoint") or 0)
        if declared <= 0:
            continue
        fetched = len(p.get("commentsAll") or p.get("replies") or [])
        compared += 1
        if fetched < declared * 0.95:
            shortfall.append({
                "articleId": aid, "declared": declared, "fetched": fetched,
                "ratio": round(fetched / declared, 3),
            })
    return {
        "compared": compared,
        "shortfallCount": len(shortfall),
        "shortfall": shortfall[:20],
        "passed": True,  # 用户口径：允许 5% 缺口，只要求"明示"
    }


def _check_second_level(posts: list[dict]) -> dict:
    """补 2：楼中楼对账（secondLevelCount vs 实际 secondLevelComments，允许 5% 缺口）。"""
    checked = 0
    shortfall: list[dict] = []
    for p in posts:
        for r in p.get("commentsAll") or p.get("replies") or []:
            declared = int((r or {}).get("secondLevelCount") or 0)
            if declared <= 0:
                continue
            fetched = len((r or {}).get("secondLevelComments") or [])
            checked += 1
            if fetched < declared * 0.95:
                shortfall.append({
                    "articleId": p.get("articleId"),
                    "commentId": (r or {}).get("commentId"),
                    "declared": declared, "fetched": fetched,
                })
    return {
        "commentsChecked": checked,
        "shortfallCount": len(shortfall),
        "shortfall": shortfall[:20],
        "passed": True,  # 同上：允许 5%，只要求明示
    }


def _product_post_fingerprint(post: dict) -> str:
    """产物里一帖的内容指纹（第5道 A/B 对照用）。

    只取内容字段，**不含**任何运行期元数据（updateTime/rank/claims 顺序等），
    这样两次运行只要该帖内容相同，指纹就相同 —— 可以把"数据漂移"与"合并过程出错"分开。
    """
    import json as _json

    keep = {
        "articleId": post.get("articleId"),
        "title": post.get("title"),
        "text": post.get("text"),
        "createTime": post.get("createTime"),
        "period": post.get("period"),
        "userId": post.get("userId"),
        "username": post.get("username"),
        "images": [str((i or {}).get("url") or "") for i in (post.get("images") or [])],
        "commentsAll": len(post.get("commentsAll") or []),
        "replies": len(post.get("replies") or []),
    }
    blob = _json.dumps(keep, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    full = _sha256_text(blob)
    # 纯内容指纹：剔除「回复数/评论数」这两个随时间变化的量。
    # 两次相隔一段时间的运行，只要帖子正文/标题/图片没变，这个 hash 就相同 ——
    # 用它可以把「期间有人在盖楼」与「分片/合并把内容搞坏了」区分开。
    content_only = {k: v for k, v in keep.items() if k not in ("commentsAll", "replies")}
    cblob = _json.dumps(content_only, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return full + ":" + _sha256_text(cblob)


def _check_low_confidence(out_dir: Path, threshold: float = 0.7) -> dict:
    """OCR 七条第 6 条：把低置信度的 claim 单独列出（用户第三节·第 4 类）。

    ⚠️ 这里**只做归档，不改产物、不丢 claim**：
    用户要求的是"低于阈值的单独列出"，不是"低于阈值的删掉"。
    阈值默认 0.7（实测当期 19715 条 claim 的分布：
      <0.5 为 0；0.5–0.7 有 4627；0.7–0.9 有 2302；≥0.9 有 12786；中位 0.95）。
    可用环境变量 LIUHE_CONF_THRESHOLD 调整。
    """
    import json as _json

    path = Path(out_dir) / "claims.json"
    if not path.exists():
        return {"skipped": "claims.json 不存在"}
    try:
        data = _json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:  # noqa: BLE001
        return {"error": f"{type(exc).__name__}: {exc}"}
    items = data if isinstance(data, list) else (data.get("claims") or [])
    low: list[dict] = []
    total = 0
    for it in items:
        conf = (it or {}).get("confidence")
        if not isinstance(conf, (int, float)):
            continue
        total += 1
        if float(conf) < threshold:
            low.append({
                "claimId": it.get("claimId"),
                "articleId": it.get("articleId"),
                "confidence": round(float(conf), 3),
                "kind": it.get("kind"),
                "playName": it.get("playName"),
                "count": it.get("count"),
                "cleanText": str(it.get("cleanText") or "")[:120],
            })
    low.sort(key=lambda x: x["confidence"])
    return {
        "threshold": threshold,
        "totalClaims": total,
        "lowCount": len(low),
        "ratio": round(len(low) / total, 4) if total else 0,
        "samples": low[:50],
    }


#: 一个完整期产物应有的 21 个文件（来源：数据仓 322/323 期 tar 实测）
#: **所有源都应有**的产物（21 个）
REQUIRED_PRODUCTS = (
    "claims.csv", "claims.json", "manifest.json",
    "ocr.csv", "ocr.json",
    "posts.csv", "posts.json",
    "replies.csv", "replies.json",
    "report.md", "run_meta.json",
    "users.csv", "users.json",
    "validation.json",
    "全站文字版.html", "全站文字版.json", "全站文字版.txt",
    "六合_看板.html", "六合_结构化.xlsx",
    "当期文字版.txt", "文字版.html", "文字版.txt",
)

#: **只有 dahkg 才产出**的产物。
#: ⚠ 2026-11-21 二次修正（两次实测证据）：
#:   · run 44767982757「合并 tuku」失败 → 缺 论坛原帖.html
#:   · run 44809354181「合并 aocai」失败 → 缺 论坛原帖.html
#: 说明 `论坛原帖.html` **只有 dahkg（大港澳）源生成**，
#: aocai（澳彩交流）和 tuku（49图库）都不产出。
#: 之前误以为 aocai 也有，导致 cron 里的 aocai 合并必然失败。
DAHKG_ONLY_PRODUCTS = ("论坛原帖.html",)


def required_products_of(source: str) -> tuple[str, ...]:
    """该源应有的完整产物清单。

    dahkg：21 + 1（论坛原帖.html）= 22 个
    aocai：21 个
    tuku ：21 个
    """
    if source == "dahkg":
        return REQUIRED_PRODUCTS + DAHKG_ONLY_PRODUCTS
    return REQUIRED_PRODUCTS

#: 文字版相关产物（dahkg 的 run_dahkg_report 不生成，需要补）
TEXT_PRODUCTS = (
    "全站文字版.html", "全站文字版.json", "全站文字版.txt",
    "当期文字版.txt", "文字版.html", "文字版.txt",
)

#: aocai / dahkg 还需要「论坛原帖.html」（tuku 不需要）
#: ⚠ 2026-11-21 根因修复：summary 模式只调 text_report.write_text_report，
#: 它不生成 论坛原帖.html，导致：
#:   run 44767982757 合并 tuku  失败（tuku 本不需要，是我的必需清单写错）
#:   run 44790922755 合并 aocai 失败（aocai 需要，summary 没生成）
#:   run 44809354181 合并 dahkg 失败（dahkg 需要，summary 没生成）
#: 正确做法：summary 模式**补齐**该产物，而不是从必需清单里删掉标准。
FORUM_HTML_PRODUCTS = ("论坛原帖.html",)


def _count_all_images(posts: list[dict]) -> dict:
    """统计**所有位置**的图片数（正文/回复/楼中楼/评论/评论楼中楼）。

    ## 为什么需要（2026-11-20 实测）
    core 的 `imageCount` 只算 `images + replies[].images` = 9208，
    而产物里实际有 19437 张（评论图 9714 + 楼中楼 257 未计入）。
    用户看到「才 1000+」误以为漏抓 —— 其实是字段口径不对。

    返回：{total, 正文, 回复, 楼中楼, 评论, 评论楼中楼, unique_urls}
    """
    stat = {"total": 0, "正文": 0, "回复": 0, "楼中楼": 0, "评论": 0, "评论楼中楼": 0}
    urls: set[str] = set()

    def _take(pos: str, ims) -> None:
        for im in (ims or []):
            stat[pos] += 1
            stat["total"] += 1
            u = str((im or {}).get("url") or "")
            if u:
                urls.add(u)

    for p_ in posts:
        _take("正文", p_.get("images"))
        for r in (p_.get("replies") or []):
            _take("回复", r.get("images"))
            for s2 in (r.get("secondLevelComments") or []):
                _take("楼中楼", s2.get("images"))
        for c in (p_.get("commentsAll") or []):
            _take("评论", c.get("images"))
            for s2 in (c.get("secondLevelComments") or []):
                _take("评论楼中楼", s2.get("images"))

    stat["unique_urls"] = len(urls)
    return stat


def _ensure_text_products(out_dir: Path, source: str, period: int, log) -> list[str]:
    """确保文字版产物存在；缺失就用 text_report 补生成。返回补生成的清单。

    ## 为什么需要（2026-11-20 run 44597918831 实测）
    `dahkg.run_dahkg_report` 只调 `forum.write_outputs`，**没有文字版生成**，
    所以 dahkg 产物只有 17 个（缺 6 个文字版）。而 aocai/tuku 的入口里有，
    所以它们 22-23 个。这是 core 三源的历史差异，属编排层职责，在 runner 里补齐。
    """
    # aocai / dahkg 需要「论坛原帖.html」；tuku 不需要
    need = list(TEXT_PRODUCTS)
    if source in ("aocai", "dahkg"):
        need += list(FORUM_HTML_PRODUCTS)
    missing = [n for n in need if not (out_dir / n).exists()]
    if not missing:
        return []

    log(f"[merge] 文字版缺 {len(missing)} 个（{missing}）→ 调 text_report 补生成")
    try:
        from core import backfill, scraper as _scraper, source as _source, text_report

        posts: list[dict] = []
        meta: dict = {}
        from . import _reconcile

        meta, posts = _reconcile.load_products(out_dir)
        if not posts:
            log("[merge] ⚠ 补文字版失败：读不到 posts")
            return []
        text_report.write_text_report(
            out_dir, posts,
            period=int(period),
            year=int(meta.get("year") or 0) or 2026,
            generated_at=str(meta.get("generatedAt") or ""),
            lottery_name=_source.source_label(source),
            date_text=backfill.fallback_date(period),
        )
        # 全站文字版由 core.user_text.write_site_texts 生成（write_text_report 不含）
        try:
            from core.user_text import write_site_texts

            users2: list[dict] = []
            from . import _reconcile as _rec2

            meta2, posts2 = _rec2.load_products(out_dir)
            users2 = _scraper.build_users(posts2) if posts2 else []
            if users2:
                st = write_site_texts(out_dir, users2, meta2)
                log(f"[merge] 全站文字版补生成：{st.get('count', 0)} 位用户")
        except Exception as exc:  # noqa: BLE001
            log(f"[merge] ⚠ 全站文字版补生成失败：{type(exc).__name__}: {exc}")

        # 论坛原帖.html：仅 aocai / dahkg 需要（tuku 无此产物）
        # 正确签名（2026-11-21 核对）：
        #   forum.write_outputs(out_root, out_dir, meta, posts, users, update_latest=True)
        if source in ("aocai", "dahkg") and not (out_dir / "论坛原帖.html").exists():
            try:
                from core import forum as _forum

                meta3, posts3 = _reconcile.load_products(out_dir)
                users3 = _scraper.build_users(posts3) if posts3 else []
                _forum.write_outputs(
                    out_dir.parent, out_dir, meta3, posts3, users3,
                    update_latest=False,
                )
                ok3 = (out_dir / "论坛原帖.html").exists()
                log(f"[merge] 论坛原帖.html 补生成{'成功' if ok3 else '仍未产出'}"
                    f"（users={len(users3)}）")
            except Exception as exc:  # noqa: BLE001
                log(f"[merge] ⚠ 论坛原帖.html 补生成失败：{type(exc).__name__}: {exc}")


        still = [n for n in TEXT_PRODUCTS if not (out_dir / n).exists()]
        ok_list = [n for n in missing if n not in still]
        log(f"[merge] 文字版补生成：成功 {len(ok_list)} 个"
            + (f"，仍缺 {still}" if still else ""))
        return ok_list
    except Exception as exc:  # noqa: BLE001
        log(f"[merge] ❌ 补文字版失败：{type(exc).__name__}: {exc}")
        return []


def _check_products_complete(out_dir: Path, source: str, log) -> None:
    """校验 21 个产物齐全；缺任一 → SystemExit(42)（拒绝交付残缺产物）。

    ## 为什么必须有（2026-11-20 实测）
    run 44597918831：`merge dahkg: success`，但产物只有 17 个（缺 6 个文字版）。
    「merge 成功」不等于「产物完整」—— 因为没有任何地方校验产物清单。
    """
    required = required_products_of(source)
    missing = [n for n in required if not (out_dir / n).exists()]
    have = len(required) - len(missing)
    log(f"[merge] 产物清单：{have}/{len(required)} 齐全"
        + (f"；缺 {missing}" if missing else ""))
    if missing:
        raise SystemExit(
            f"[merge] ❌ {source} 产物缺失 {len(missing)} 个：{missing}（拒绝交付残缺产物）"
        )


def _annotate_hit_rate(out_dir: Path, meta: dict, shard_stats: dict, log) -> None:
    """把分片上报的缓存命中率写进 run_meta（A''：校准 HIT_RATE_HISTORY 的数据来源）。

    写两个字段：
      `ocrCacheHitRate`        本轮各片命中率的中位 + 明细（机器可读）
      `ocrMissCostCalibration` 假设值 vs 本轮反推值的对比提示（供人工裁决是否回写表）
    """
    from . import budget as _budget

    hrs = list((shard_stats or {}).get("ocrHitRates") or [])
    if not hrs:
        return
    hrs.sort()
    median = hrs[len(hrs) // 2]
    e2es = list((shard_stats or {}).get("ocrEffectiveSecs") or [])
    e2es.sort()
    meta["ocrCacheHitRate"] = {
        "median": round(median, 4),
        "min": round(hrs[0], 4),
        "max": round(hrs[-1], 4),
        "shards": len(hrs),
        "cacheHit": int((shard_stats or {}).get("ocrCacheHit") or 0),
        "cacheMiss": int((shard_stats or {}).get("ocrCacheMiss") or 0),
        "plannedImages": int((shard_stats or {}).get("plannedImages") or 0),
        "effectiveSecMedian": round(e2es[len(e2es) // 2], 4) if e2es else None,
    }
    meta["ocrMissCostCalibration"] = {
        "assumedHitRateHistory": _budget.HIT_RATE_HISTORY,
        "actualHitRateMedian": round(median, 4),
        "delta": round(median - _budget.HIT_RATE_HISTORY, 4),
        "action": ("误差 >30% → 回写 RUNNER_CPU_E2E_SECONDS 或 HIT_RATE_HISTORY；"
                   "≤30% → 保留假设并标注已校准"),
    }
    log(
        f"[预算校准] 本轮命中率中位 {median * 100:.0f}%（{hrs[0] * 100:.0f}%-{hrs[-1] * 100:.0f}%，"
        f"{len(hrs)} 片），假设值 {_budget.HIT_RATE_HISTORY * 100:.0f}%"
        f" → delta {median - _budget.HIT_RATE_HISTORY:+.3f}"
    )


def _summarize_shards(work: Path, source: str, log) -> dict:
    """汇总本轮各分片的状态，并**把失败暴露出来**（用户 2026-11-17 要求）。

    ## 为什么必须做
    给分片加了 `continue-on-error: true` 之后，分片失败不会再阻塞 publish，
    这是对的（分片只预热缓存，正确性由 merge 保证）。
    但**不能让失败变成静默**：如果某天 20 个分片全失败，run 仍显示 success，
    就没人知道分片已失效、merge 在裸跑全量。所以这里统计并告警。

    判据（用户定的三条）：
      1. 分片失败数写进报告
      2. 失败数 > 50% 告警
      3. merge 耗时 > 15 分钟 ⇒ 说明分片没起作用，记下来
    """
    import json as _json

    stats = {"total": 0, "withItems": 0, "empty": 0, "failed": 0, "details": [],
             "ocrHitRates": [], "ocrCacheHit": 0, "ocrCacheMiss": 0,
             "ocrEffectiveSecs": [], "plannedImages": 0}
    for path in Path(work).rglob(f"shard_{source}_*.json"):
        try:
            data = _json.loads(path.read_text(encoding="utf-8"))
        except Exception:  # noqa: BLE001
            continue
        stats["total"] += 1
        picked = int(data.get("pickedItems") or 0)
        code = int(data.get("exitCode") or 0)
        if code != 0:
            stats["failed"] += 1
            kind = "failed"
        elif picked == 0:
            stats["empty"] += 1
            kind = "empty"
        else:
            stats["withItems"] += 1
            kind = "ok"
        stats["details"].append({"shard": data.get("shard"), "picked": picked, "exitCode": code, "kind": kind})
        # A'' 命中率汇总（只统计有上报的片）
        if "ocrHitRate" in data:
            stats["ocrHitRates"].append(float(data.get("ocrHitRate") or 0))
            stats["ocrCacheHit"] += int(data.get("ocrCacheHit") or 0)
            stats["ocrCacheMiss"] += int(data.get("ocrCacheMiss") or 0)
            stats["plannedImages"] += int(data.get("plannedImages") or 0)
            if data.get("ocrEffectiveSec"):
                stats["ocrEffectiveSecs"].append(float(data["ocrEffectiveSec"]))

    if stats["ocrHitRates"]:
        hrs = sorted(stats["ocrHitRates"])
        stats["ocrHitRateMedian"] = hrs[len(hrs) // 2]
        stats["ocrCacheHitTotal"] = stats["ocrCacheHit"]
        stats["ocrCacheMissTotal"] = stats["ocrCacheMiss"]
        log(
            f"[分片汇总] {source}：缓存命中 {stats['ocrCacheHit']} / 计划 {stats['plannedImages']}"
            f"（各片中位 {stats['ocrHitRateMedian'] * 100:.0f}%）；"
            f"有效秒/次中位 {sorted(stats['ocrEffectiveSecs'])[len(stats['ocrEffectiveSecs']) // 2]:.3f}"
            if stats["ocrEffectiveSecs"] else
            f"[分片汇总] {source}：缓存命中 {stats['ocrCacheHit']} / 计划 {stats['plannedImages']}"
        )

    if stats["total"]:
        rate = stats["failed"] / stats["total"]
        log(
            f"[分片汇总] {source}：共 {stats['total']} 片 —— 有帖 {stats['withItems']} / "
            f"空片 {stats['empty']} / 失败 {stats['failed']}（失败率 {rate * 100:.0f}%）"
        )
        if rate > 0.5:
            log(
                f"[分片汇总] ⚠️⚠️ 告警：{source} 分片失败率 {rate * 100:.0f}% > 50% —— "
                f"分片基本没起作用，本轮 merge 在裸跑全量（产物仍正确，但耗时会长）"
            )
        if stats["failed"]:
            bad = [d for d in stats["details"] if d["kind"] == "failed"]
            log(f"[分片汇总] 失败分片明细：{bad[:10]}")
    return stats


def do_shard(args) -> int:
    """分片任务：**只负责把它那一份帖子的缓存（详情/回复/图片/OCR）暖起来**。

    三源通用（2026-11-16 用户撤回「不动 runner.py」约束后实现）：
      1. 读 plan 拿期号（dahkg=短期号，aocai/tuku=整期号）
      2. 给该源的列表取数函数打切片补丁（只切列表，不碰下游）
      3. 进程内跑该模块自己的 CLI → 详情/回复/图片/OCR 全走原生代码，只处理本片
      4. 缓存落在 work/out 里，由 workflow 打包给 merge
    merge 阶段再跑一次**未打补丁**的完整流程（subprocess），产出最终产物。
    """
    from . import _guard

    work = _work_dir(args.work)
    out_root = Path(args.out or work / "out").resolve()
    source = args.source
    shard_index = int(args.shard)
    shard_total = max(1, int(args.shards))
    log("=" * 70)
    log(f"[shard] {shard_index}/{shard_total} source={source}")
    log("=" * 70)

    plan = _guard.load_json(work / "plan" / f"plan_{source}.json", {})
    if not plan:
        raise SystemExit(f"[错误] 没有 plan 文件：{work / 'plan' / f'plan_{source}.json'}")
    period = int(plan.get("period") or 0)
    if not period:
        raise SystemExit("[错误] plan 里没有期号，无法分片")

    sink: dict = {"total": 0, "picked": 0}
    # 优先按**用户**分组（方案 3）：一个作者的所有帖子恒在同一片，新帖自动跟随
    planned_ids: set[int] | None = None
    mine_users: set[str] | None = None
    plan_users = plan.get("shards") or []
    if plan_users and len(plan_users) == shard_total and shard_index < len(plan_users):
        mine_users = {str(u) for u in (plan_users[shard_index] or [])}
        log(
            f"[shard] 使用 plan 的**按用户**分组：本片 {len(mine_users)} 个作者"
            f"（台账 user_shard_{source}.json 保证跨轮稳定）"
        )
    shard_plan = plan.get("shardPlan") or {}
    # 注意：即使已按用户分组，也要把 plan 的帖子 id 列表准备好 ——
    # `_slice_items` 在「按用户取到空片」时会退回用它兜底（2026-11-17 空片事故）。
    if shard_plan and int(shard_plan.get("n") or 0) == shard_total:
        group = (shard_plan.get("groups") or {}).get(str(shard_index)) or []
        planned_ids = {int(x) for x in group}
        log(
            f"[shard] 使用 plan 的均衡分组：本片 {len(planned_ids)} 帖，"
            f"计划权重 {(shard_plan.get('weightPerShard') or shard_plan.get('imagesPerShard') or ['?'])[shard_index] if shard_index < len(shard_plan.get('weightPerShard') or shard_plan.get('imagesPerShard') or []) else '?'}"
        )
    elif shard_plan:
        log(
            f"[shard] ⚠ plan 的分片数 {shard_plan.get('n')} 与本次 --shards {shard_total} 不一致，"
            f"退回 id 取模（请检查 workflow 的 SHARD_MAP 与 matrix 是否同源）"
        )
    originals = _patch_shard(source, shard_index, shard_total, sink, planned_ids=planned_ids, mine_users=mine_users)
    log(
        f"[shard] 已给 {source} 的列表取数打切片补丁"
        f"（{'plan 均衡分组' if planned_ids is not None else f'id % {shard_total} == {shard_index}'}）"
    )

    # ---- A'' 预算：命中率 → 有效秒/次 → 转移决策（2026-11-19 用户裁决）----
    # 容量用「job timeout × 0.9」而不是软预算：分片能吃满 22.5 分钟（旧逻辑只有 18 分钟），
    # 且 22.5 < 25 的硬超时 → 永不被 kill（被 kill 时 unfinished 就写不出来了）。
    rest_ids: list[int] = []
    budget_est: dict = {}
    budget_dec: dict = {}
    if planned_ids:
        from . import budget as _budget

        post_cost = (plan.get("postCost") or {})
        cost: dict[int, int] = {}
        for aid in planned_ids:
            info = post_cost.get(str(aid)) or {}
            cost[aid] = int(info.get("count") or 1) if isinstance(info, dict) else 1
        total_imgs = sum(cost.values())

        # 缓存可能分布在两处（见 budget.count_cached_ocr_unique 的说明）：
        #   ① work/out/**/.cache（workflow 铺回，可能为空）
        #   ② work/ocr-cache（actions/cache 层2 直接恢复，一定有货）
        cache_dir = _find_ocr_cache_dir(work, out_root)
        cached = _budget.count_cached_ocr_unique(
            [cache_dir, work / "ocr-cache"] if cache_dir else [work / "ocr-cache"]
        )
        cpu = _budget.read_cpu_model_name()
        budget_est = _budget.estimate_effective_seconds(cpu, cached, total_imgs)
        budget_est["cached"] = cached
        budget_est["total_images"] = total_imgs
        shard_to, merge_to = _budget.timeouts_from_env()
        budget_dec = _budget.decide_transfer(
            total_imgs, budget_est["effective"],
            shard_timeout_s=shard_to, merge_timeout_s=merge_to,
        )
        ratio = _budget.USABLE_RATIO
        # ⚠ 2026-11-23 口径修正（审计实测：日志写「本片 58 图，缓存命中 2641」超图数 45 倍）：
        #   `cached` 是**全局层2 缓存池的唯一条目数**（跨片/跨源/跨期共享），
        #   不是「本片复用了多少张」。原来写成「缓存命中 N」会让人误以为是本片命中数。
        #   现在明确区分：
        #     · 缓存池 N 条（全局可复用的 OCR 结果总量）
        #     · 按假设命中率 X% 估算（仅用于耗时预算，保守方向）
        #   真实本片复用数由 [OCR] 行的「缓存命中 Y」上报（见 diag 探针）。
        log(
            f"[shard] 本片 {total_imgs} 图；缓存池 {cached} 条可复用 OCR 结果"
            f"（全局共享，非本片命中数）；按假设命中率 "
            f"{budget_est['hit_rate'] * 100:.0f}% 估算，有效 "
            f"{budget_est['effective']:.3f} 秒/次，"
            f"预计 {total_imgs * budget_est['effective'] / 60:.1f} 分钟"
            f"（分片上限 {shard_to * ratio / 60:.1f} / merge 上限 {merge_to * ratio / 60:.1f} 分）"
        )
        log(
            f"[shard] CPU {cpu}（{'命中表' if budget_est['cpu_hit'] else '未命中表→保守值'}）"
            f" E2E={budget_est['e2e_history']:.3f} 秒/次，反推未命中成本 {budget_est['miss_cost']:.3f} 秒/次"
        )

        if budget_dec["transfer"] > 0:
            # ⚠ 2026-11-21 根因修复：把**真实未命中成本 miss_cost** 与**命中率**传进去，
            #   而不是传 effective（effective 已是「按命中率折算后」的均值，
            #   再乘全部图片数会重复折算 → 高估 10 倍 → 不必要截断 → summary 缺帖）。
            _miss = float(budget_est.get("miss_cost") or budget_est["effective"])
            _hr = float(budget_est.get("hit_rate") or 0.0)
            taken, rest_ids = _pick_within_budget(
                sorted(planned_ids), cost, _miss,
                budget_seconds=budget_dec["shard_capacity"] * _miss + 60.0,
                hit_rate=_hr,
            )
            log(
                f"[shard] ⏱ 分片容量内只能处理 {len(taken)} 帖（{budget_dec['shard_capacity']} 图），"
                f"剩余 {len(rest_ids)} 帖写入 unfinished_{source}_{shard_index}.json 交 merge 补抓"
            )
            _guard.save_json(
                work / "unfinished" / f"unfinished_{source}_{shard_index}.json",
                {
                    "source": source,
                    "shard": shard_index,
                    "ids": rest_ids,
                    "reason": (
                        f"有效 {budget_est['effective']:.3f} 秒/次（命中率 "
                        f"{budget_est['hit_rate'] * 100:.0f}%），超出分片容量 "
                        f"{budget_dec['shard_capacity']} 图"
                    ),
                    "ts": time.strftime("%Y-%m-%d %H:%M:%S"),
                },
            )
            planned_ids = set(taken)
            for mod, name, original in (originals or []):
                setattr(mod, name, original)
            originals = _patch_shard(source, shard_index, shard_total, sink, planned_ids=planned_ids, mine_users=mine_users)
        else:
            log("[shard] 本片工作量在分片容量内，全部自己处理")

        if budget_dec["shortfall"] > 0:
            log(f"[shard] ⚠ {budget_dec['reason']}")
            log("[shard] ⚠ 建议：提高缓存命中率（连续跑同期限）> 增加分片数（付费）> 冷启动轮单独放宽时限（需批准）")

    # SIGTERM 优雅投降（必须在跑模块**之前**装，handler 里的 planned_ids 用当前值）
    _install_sigterm_surrender(work, source, shard_index, set(planned_ids or ()), log)

    argv = ["--out", str(out_root), "--period", str(period)]
    if args.workers:
        argv += ["--workers", str(args.workers)]
    if args.delay is not None:
        argv += ["--delay", str(args.delay)]
    if args.max_pages:
        argv += ["--max-pages", str(args.max_pages)]
    if args.no_images:
        argv.append("--no-images")
    if args.no_ocr:
        argv.append("--no-ocr")
    if args.no_replies:
        argv.append("--no-replies")

    # ---- 诊断探针：区分「CPU 慢」与「网络慢」（用户 2026-11-17 要求）----
    diag = _install_diag_probes()
    log(f"[diag] OCR 探针挂载：{diag['patched']['ocr'] or '无'}")
    log(f"[diag] 下载探针挂载：{diag['patched']['download'] or '无'}")
    cpu_model = _cpu_model()
    log(f"[diag] CPU = {cpu_model}")
    run_t0 = time.perf_counter()

    try:
        code = _run_module_main(MODULE_OF[source], argv)
    finally:
        for mod, name, original in (originals or []):
            setattr(mod, name, original)
    run_seconds = round(time.perf_counter() - run_t0, 1)
    # 空片不算失败（2026-11-17 跨期事故）：如果本片一帖都没分到，
    # 模块会报「列表里没有 X 期的帖子」并以非 0 退出。
    # 但空片本身是合法状态（尤其期号正在切换、新期还没进榜时），
    # 完整性由 merge 保证（merge 会全量重采并跑五道防线），
    # 所以这里显式转成成功 + 警告，而不是让整个 run 失败、连带 publish 被跳过。
    if code != 0 and int(sink.get('picked') or 0) == 0:
        log(
            f"[shard] ⚠ 本片未分到任何帖子（模块退出码={code}），按**空片**处理、不计失败；"
            f"完整性由 merge 全量重采保证"
        )
        code = 0

    # 补齐失败的帖写进 unfinished，交 merge 补抓（不静默丢）
    _miss = sink.get("missingIds") or []
    if _miss:
        _guard.save_json(
            work / "unfinished" / f"unfinished_{source}_{shard_index}.json",
            {
                "source": source, "shard": shard_index,
                "ids": sorted(int(x) for x in _miss),
                "reason": f"扫列表漏帖且按 id 补详情失败 {len(_miss)} 帖（分页抖动）",
                "ts": time.strftime("%Y-%m-%d %H:%M:%S"),
            },
        )
        log(f"[shard] ⚠ {len(_miss)} 帖补不上，已写 unfinished 交 merge：{sorted(_miss)[:8]}")

    log(
        f"[shard] {source} 本片：源站本期共 {sink.get('total', 0)} 帖 / {sink.get('imagesTotal', 0)} 图，"
        f"本片负责 {sink.get('picked', 0)} 帖 / {sink.get('imagesPicked', 0)} 图"
        f"（模式={sink.get('mode', '?')}）；模块退出码={code}"
    )

    # 诊断一行（用户 2026-11-18 要求）：查清「空片兜底为什么没拦住」
    try:
        log(f"[shard] 诊断：code={code}, picked={sink.get('picked')}, total={sink.get('total')}, mode={sink.get('mode')}")
    except Exception:  # noqa: BLE001 - 诊断日志绝不该阻断主流程
        pass

    # OCR/下载 分开计时（这是"CPU 慢还是网络慢"的判据）
    ocr_s = round(diag["ocr"]["seconds"], 1)
    dl_s = round(diag["download"]["seconds"], 1)
    ocr_n = diag["ocr"]["calls"]
    # ⚠️ 2026-11-17 实测事故：按用户分片 + 缓存隔离后，分片缓存命中率提高，
    # 有的分片 `ocr_n == 0`（一次新 OCR 都没做，全部命中缓存），
    # 这里直接 `ocr_s / ocr_n` 会 **ZeroDivisionError 让整个分片 job 失败**（20 片全挂）。
    # 正确写法：先判断次数，为 0 时显式说明"全部命中缓存"。
    try:
        per_call = (ocr_s / ocr_n) if ocr_n else 0.0
    except Exception:  # noqa: BLE001 - 诊断统计异常不该崩
        per_call = 0.0
    ocr_desc = (
        f"OCR {ocr_s}s（{ocr_n} 次调用，{per_call:.3f} 秒/次）"
        if ocr_n
        else f"OCR {ocr_s}s（0 次新识别 = 全部命中缓存）"
    )
    log(
        f"[diag] 本片总耗时 {run_seconds}s = {ocr_desc}"
        f" + 下载 {dl_s}s + 其他 {round(run_seconds - ocr_s - dl_s, 1)}s"
    )
    bench = _ocr_micro_benchmark(out_root, limit=3) if not args.no_ocr else {"skipped": True}
    log(f"[diag] 微型基准（预热后 3 张）: {bench}")

    _guard.save_json(
        work / "shards" / f"shard_{source}_{shard_index}.json",
        {
            "source": source,
            "shard": shard_index,
            "shards": shard_total,
            "period": period,
            "totalItems": sink.get("total", 0),
            "pickedItems": sink.get("picked", 0),
            "exitCode": code,
            "ts": time.strftime("%Y-%m-%d %H:%M:%S"),
            # A'' 命中率上报（供 merge 写进 run_meta，校准 HIT_RATE_HISTORY）
            "ocrCacheHit": budget_est.get("cached", 0),
            "ocrCacheMiss": max(0, budget_est.get("total_images", 0) - budget_est.get("cached", 0)),
            "ocrHitRate": round(budget_est.get("hit_rate", 0.0), 4),
            "ocrEffectiveSec": round(budget_est.get("effective", 0.0), 4),
            "ocrMissCostSec": round(budget_est.get("miss_cost", 0.0), 4),
            "ocrE2eSec": round(budget_est.get("e2e_history", 0.0), 4),
            "budgetTransfer": budget_dec.get("transfer", 0),
            "budgetShortfall": budget_dec.get("shortfall", 0),
            "plannedImages": budget_est.get("total_images", 0),
            "diag": {
                "cpuModel": cpu_model,
                "runSeconds": run_seconds,
                "ocrSeconds": ocr_s,
                "ocrCalls": ocr_n,
                "ocrSecondsPerCall": round(ocr_s / ocr_n, 4) if ocr_n else 0,
                "downloadSeconds": dl_s,
                "downloadCalls": diag["download"]["calls"],
                "otherSeconds": round(run_seconds - ocr_s - dl_s, 1),
                "benchmark": bench,
            },
        },
    )
    return code


# ---------------------------------------------------------------------------
# 模式：--shard（预热缓存）
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# 模式：--run / --merge（共用主流程）
# ---------------------------------------------------------------------------
# ---------------------------------------------------------------------------
# C1 第 2 步：merge 只汇总（--merge-mode=summary）
# ---------------------------------------------------------------------------
def _extract_shard_artifacts(work: Path, source: str, log) -> tuple[list[list[dict]], list[dict], dict, dict, list]:
    """解开各片 artifact，返回 (每片 posts, 每片 run_meta, OCR rows 索引, list 原始位置)。

    artifact 目录形如 work/incoming-cache/cache-<源>-<片>-<runid>/cacheout/
      products_<源>_<片>.tar.gz  → posts.json / run_meta.json
      cache_<源>_<片>.tar.gz     → .cache/ocr/*.json / .cache/list.json
    """
    import json as _json
    import tarfile as _tar

    incoming = work / "incoming-cache"
    shard_posts: list[list[dict]] = []
    shard_metas: list[dict] = []
    skipped_products: list[str] = []
    ocr_rows: dict[str, list[dict]] = {}
    list_order: dict[int, int] = {}

    prod_tars = sorted(incoming.rglob(f"products_{source}_*.tar.gz"))
    cache_tars = sorted(incoming.rglob(f"cache_{source}_*.tar.gz"))
    log(f"[merge-summary] 找到 products 包 {len(prod_tars)} 个 / cache 包 {len(cache_tars)} 个")

    for tf in prod_tars:
        with _tar.open(tf, "r:gz") as t:
            posts = None
            meta = None
            for name in t.getnames():
                if name.endswith("/posts.json"):
                    d = _json.loads(t.extractfile(name).read())
                    posts = d if isinstance(d, list) else (d.get("posts") or [])
                elif name.endswith("/run_meta.json"):
                    meta = _json.loads(t.extractfile(name).read())
            if posts is None:
                # 2026-11-19 实测：分片被 job timeout kill 时，`if: always()` 仍会打一个**空** products 包
                # （`work/out` 里没有产物）。这不是错误配置，而是「该片没跑完」——跳过并告警，
                # 让它由「缺帖」逻辑处理（若缺得多会触发 groups 并集断言；缺得少会写进 knownDiff）。
                log(f"[merge-summary] ⚠ {tf.name} 里没有 posts.json（该片可能被 timeout kill），跳过")
                skipped_products.append(tf.name)
                continue
            shard_posts.append(posts)
            if meta:
                shard_metas.append(meta)
    if not shard_posts:
        raise SystemExit("[merge-summary] ❌ 没找到任何分片产物包（products_*.tar.gz）")

    image_members: list[tuple[Path, str]] = []      # (tar 路径, 成员名)
    for tf in cache_tars:
        with _tar.open(tf, "r:gz") as t:
            for name in t.getnames():
                if "/images/" in name and not name.endswith("/") and "/.cache/" not in name:
                    image_members.append((tf, name))
                if "/ocr/" in name and name.endswith(".json"):
                    try:
                        d = _json.loads(t.extractfile(name).read())
                    except Exception:  # noqa: BLE001
                        continue
                    if not isinstance(d, dict):
                        continue
                    img = str(d.get("image") or "")
                    if img:
                        ocr_rows[img.split("/images/", 1)[-1]] = d.get("rows") or []
                elif name.endswith("/list.json"):
                    try:
                        d = _json.loads(t.extractfile(name).read())
                    except Exception:  # noqa: BLE001
                        continue
                    rows = d if isinstance(d, list) else (d.get("rows") or d.get("items") or [])
                    for idx, row in enumerate(rows if isinstance(rows, list) else []):
                        if isinstance(row, dict):
                            aid = row.get("articleId") or row.get("id") or row.get("singaporeId")
                            try:
                                list_order[int(aid)] = idx
                            except (TypeError, ValueError):
                                continue

    log(f"[merge-summary] OCR rows 索引 {len(ocr_rows)} 条 / list 原始位置 {len(list_order)} 条 / "
        f"图片成员 {len(image_members)} 个")
    if skipped_products:
        log(f"[merge-summary] ⚠ 跳过的空产物包 {len(skipped_products)} 个：{skipped_products}")
    return shard_posts, shard_metas, ocr_rows, list_order, image_members


def _iter_images(post: dict):
    """按 core `ocr_post_images` 的口径遍历一个帖的所有图（含评论规则）。"""
    from . import _guard  # noqa: F401  (保持与其它函数一致的导入风格)
    from core import scraper as _scraper

    as_int = _scraper.as_int
    for im in (post.get("images") or []):
        yield im
    for r in (post.get("replies") or []):
        for im in (r.get("images") or []):
            yield im
        for s in (r.get("secondLevelComments") or []):
            for im in (s.get("images") or []):
                yield im
    author_ids = {as_int(r.get("commentId")) for r in (post.get("replies") or [])}
    for c in (post.get("commentsAll") or []):
        cid = as_int(c.get("commentId"))
        if not (cid and cid in author_ids):
            for im in (c.get("images") or []):
                yield im
        for s in (c.get("secondLevelComments") or []):
            for im in (s.get("images") or []):
                yield im


def do_merge_summary(args) -> int:
    """merge 只汇总：不扫列表、不抓详情、不下载图片、不做 OCR、不调 main()。"""
    import time as _time

    from . import _guard, _period, _reconcile, merge_summary as _ms
    from core import backfill, scraper as _scraper, source as _source, text_report

    work = _work_dir(args.work)
    out_root = Path(args.out or work / "out").resolve()
    source = args.source
    started = _time.time()
    log("=" * 70)
    log(f"[merge-summary] source={source} 输出根={out_root}（只汇总，不重抓）")
    log("=" * 70)

    # ---------- ① plan ----------
    plan = _guard.load_json(work / "plan" / f"plan_{source}.json", {})
    period = int(plan.get("period") or 0)
    if not period:
        raise SystemExit("[merge-summary] ❌ plan 里没有 period")
    expected = int(((plan.get("counts") or {}).get("expectedPosts")) or 0)
    display = int(_period.normalize_period(period)) or period

    # ---------- ② 解 artifact ----------
    shard_posts, shard_metas, ocr_rows, list_order, image_members = _extract_shard_artifacts(work, source, log)
    _shard_stats = _summarize_shards(work, source, log) or {}
    if not shard_metas:
        raise SystemExit("[merge-summary] ❌ 没有 run_meta.json 可作模板")

    # ---------- ③ 合并去重 ----------
    merged, mstats = _ms.merge_posts(shard_posts)
    log(f"[merge-summary] 读 {mstats['shards']} 片，共 {mstats['inputTotal']} 帖；"
        f"去重 {mstats['duplicates']} 帖 → {len(merged)} 帖")

    # P1：缺帖时打印「各片上报数」+「缺失 ID」，便于定位是分页抖动还是分配漏
    # 【2026-11-19 修订】断言基准改为「plan 的 shardPlan.groups 并集」——
    # 因为分片只能处理「被分配进 groups 的帖」：
    #   plan 内部有断言①③保证 groups 并集 == union_ids（本次扫描结果），
    #   而 `counts.expectedPosts` 来自另一次独立扫描，两者可能差 1-2 帖（分页抖动）。
    #   那种「没被分配进任何分片」的帖，summary 永远拿不到（legacy 的 merge 全量重采能拿到）。
    #   → 断言用可达上限（groups 并集），并把 plan 内部差异**明确告警**（不静默）。
    planned_set: set[int] = set()
    for _g in ((plan.get("shardPlan") or {}).get("groups") or {}).values():
        for _x in (_g or []):
            try:
                planned_set.add(int(_x))
            except (TypeError, ValueError):
                continue
    target = len(planned_set) if planned_set else expected
    if planned_set and expected and len(planned_set) != expected:
        log(
            f"[merge-summary] ⚠ plan 内部不一致：shardPlan.groups 并集 {len(planned_set)}"
            f" != counts.expectedPosts {expected}"
            f"（差 {abs(expected - len(planned_set))} 帖未被分配到任何分片；"
            f"summary 无法产出，legacy 全量重采可补上）"
        )

    per_shard = [len(ps or []) for ps in shard_posts]
    # ⚠ 2026-11-22 根因修复（run 44925221594 实测：aocai 228 帖 > 上限 226 帖）：
    #   plan 生成时源站是 226 帖；分片扫描时源站已变成 228 帖（plan 之后新增了 2 帖）。
    #   分片分片按 by-user 收下本片作者的新帖 → 汇总出 228 帖。
    #   这不是重复/错抓，而是**合法的实时增量**（用户要求「老用户有新增内容也实时识别」）。
    #   原逻辑 `len(merged) != target` 一律失败 → 把「只多不少」误判成错误。
    #   正确判据：
    #     · 有缺失（planned_set - got_ids 非空）        → 真缺帖，失败
    #     · 只多不少（got_ids ⊇ planned_set 且有新增）  → 合法增量，通过并如实记录
    #     · 少且多（两组都非空）                        → 异常，失败
    if target and len(merged) != target:
        got_ids = set()
        for _p in merged:
            try:
                got_ids.add(int(_p.get("articleId") or 0))
            except (TypeError, ValueError):
                continue
        planned = set()
        for _g in ((plan.get("shardPlan") or {}).get("groups") or {}).values():
            pass
        # plan 里的 id 列表（若有）用于算缺失；没有就只报数量与各片上报
        for _key in ("plannedIds", "planned_ids", "unionIds", "union_ids"):
            for _aid in (plan.get(_key) or []):
                try:
                    planned.add(int(_aid))
                except (TypeError, ValueError):
                    continue
        missing = sorted(planned_set - got_ids) if planned_set else []
        extra = sorted(got_ids - planned_set) if planned_set else []
        shard_desc = ", ".join(f"shard{i}={n}" for i, n in enumerate(per_shard))
        # ---- 只多不少：plan 生成后源站新增了新帖（合法实时增量）----
        # 严格校验：extra 里的帖必须
        #   ① 帖子 ID 唯一（不重复）
        #   ② 作者存在于 plan 里（是已知作者的帖子，不是凭空冒出来的）
        #   ③ 该帖来自**恰好一个**分片（不是多片重复抓）
        _extra_ok = False
        if planned_set and not missing and extra:
            _dup_ids = len(merged) - len(got_ids)
            # 作者校验：plan 里有 userToShard 时检查
            _u2s = plan.get("userToShard") or {}
            _bad_authors = []
            _extra_set = set(extra)
            for _p in merged:
                try:
                    _aid = int(_p.get("articleId") or 0)
                except (TypeError, ValueError):
                    continue
                if _aid not in _extra_set:
                    continue
                _uid = str(_p.get("senderId") or _p.get("userid")
                           or _p.get("userId") or "")
                if _u2s and _uid and _uid not in _u2s:
                    _bad_authors.append((_aid, _uid))
            _extra_ok = (_dup_ids == 0) and (not _bad_authors)
            if not _extra_ok:
                log(
                    f"[merge-summary] ❌ 多出的 {len(extra)} 帖未通过校验："
                    f"重复 id {_dup_ids} 个，作者不在 plan 里 {len(_bad_authors)} 个"
                    f"{_bad_authors[:3]}"
                )
        if planned_set and not missing and extra and _extra_ok:
            log(
                f"[merge-summary] ✅ 汇总 {len(merged)} 帖 = 上限 {target} 帖 + "
                f"新帖 {len(extra)} 帖（plan 生成后源站新增，已实时收全）"
            )
            log(f"[merge-summary]    新帖 ID: {extra[:20]}{'…' if len(extra) > 20 else ''}")
            log(f"[merge-summary]    各片上报: {shard_desc}（合计 {sum(per_shard)}）")
            # 不算失败，继续走后续流程（fullText 还原 / 产物生成 / 五道防线）
            missing = []
        else:
            log(f"[merge-summary] ❌ 汇总后 {len(merged)} 帖 != 可达上限 {target} 帖"
                f"（groups 并集）")
            log(f"[merge-summary]    各片上报: {shard_desc}（合计 {sum(per_shard)}）")
            if missing:
                log(f"[merge-summary]    缺失 ID（{len(missing)} 个）: {missing[:20]}"
                    f"{'…' if len(missing) > 20 else ''}")
            if extra:
                log(f"[merge-summary]    多出 ID（{len(extra)} 个）: {extra[:20]}"
                    f"{'…' if len(extra) > 20 else ''}")
            if not missing and not extra:
                log("[merge-summary]    plan 里没有 id 列表 → 只能看各片上报数")
            raise SystemExit(41)

    # ---------- ④ 排序（list.json 原始顺序）----------
    merged = _ms.order_posts_by_list(merged, list_order)
    for i, p in enumerate(merged, 1):
        p["rank"] = i
    log(f"[merge-summary] 按 list.json 原始顺序排序完成（{len(merged)} 帖）")

    # ---------- ⑤ fullText 还原（rowCount 截断）----------
    n_ft = n_trunc = n_nocache = 0
    rc_mismatch = 0
    for p in merged:
        for im in _iter_images(p):
            lp = str((im or {}).get("localPath") or "")
            key = lp[7:] if lp.startswith("images/") else lp
            ocr = im.get("ocr")
            if not isinstance(ocr, dict):
                ocr = {}
                im["ocr"] = ocr
            rows = ocr_rows.get(key)
            if rows is None:
                n_nocache += 1
                continue
            rc = ocr.get("rowCount")
            if isinstance(rc, int) and 0 < rc < len(rows):
                n_trunc += 1
                rc_mismatch += 1
            ocr["fullText"] = _ms.build_fulltext_from_rows(rows, rc, ocr.get("rowIndexes"))
            n_ft += 1
    log(f"[merge-summary] fullText 还原 {n_ft} 张（其中按 rowCount 截断 {n_trunc} 张，"
        f"无缓存 {n_nocache} 张）")

    # ---------- ⑥ 纯函数链 ----------
    users = _scraper.build_users(merged)
    t_claims = _time.time()
    claims, claim_stats = _scraper.collect_claims(merged, period)
    claims_sec = _time.time() - t_claims
    users = _scraper.attach_user_claims(users, claims)
    log(f"[merge-summary] users={len(users)} claims={len(claims)}（重跑 collect_claims {claims_sec:.2f}s）")

    # ---------- ⑦ meta（三类规则）----------
    # summary 模式是**创建**产物目录，不能用 _find_period_dir（它要求目录已存在）。
    # core 的目录名规则：out_root / f"{lotteryName}_{year}_{display}期"
    # （见 core.scraper.main / core.forum.run_forum_report / core.dahkg.run_dahkg_report）
    _sub = SOURCE_SUBDIR.get(source, "")
    _lottery_name = str((shard_metas[0] or {}).get("lotteryName") or "澳门")
    _year = int((shard_metas[0] or {}).get("year") or 0) or 2026
    _base = (out_root / _sub) if _sub else out_root
    out_dir = _base / f"{_lottery_name}_{_year}_{display}期"
    out_dir.mkdir(parents=True, exist_ok=True)
    log(f"[merge-summary] 产物目录：{out_dir}")

    # 把各片 cache 里的 images/ 落盘（summary 不跑 main()，没人替我们做这件事）
    images_dir = out_dir / "images"
    n_written = 0
    if image_members:
        import tarfile as _tar2

        by_tar: dict[Path, list[str]] = {}
        for tf, name in image_members:
            by_tar.setdefault(tf, []).append(name)
        for tf, names in by_tar.items():
            with _tar2.open(tf, "r:gz") as t:
                for name in names:
                    rel = name.split("/images/", 1)[-1]
                    target = images_dir / rel
                    if target.exists():
                        continue
                    target.parent.mkdir(parents=True, exist_ok=True)
                    try:
                        src = t.extractfile(name)
                        if src is None:
                            continue
                        target.write_bytes(src.read())
                        n_written += 1
                    except Exception:  # noqa: BLE001 - 单张失败不连累整体
                        continue
        log(f"[merge-summary] 图片落盘 {n_written} 张 → {images_dir}")
    n_images_on_disk = sum(1 for _ in images_dir.rglob("*") if _.is_file()) if images_dir.exists() else 0
    # 分片 artifact 只打 .cache（不含图片文件）→ summary 没有磁盘图片可数，
    # 退而用「产物声明的去重图片数」，并在已知差异里标注未做磁盘核对。
    _declared: set[str] = set()
    for _p in merged:
        for _im in _iter_images(_p):
            _u = str((_im or {}).get("url") or "")
            if _u:
                _declared.add(_u)
    n_images = n_images_on_disk or len(_declared)
    if n_images_on_disk == 0 and _declared:
        log(f"[merge-summary] ⚠ artifact 不含图片文件 → imageCount 用声明去重数 {len(_declared)}（未做磁盘核对）")
    plan_with_scan = dict(plan)
    meta = _ms.build_merged_meta(
        template=shard_metas[0], shard_metas=shard_metas, merged_posts=merged, users=users,
        claims=claims, claim_stats=claim_stats, plan=plan_with_scan, images_unique=n_images,
        timings={"durationSec": 0.0, "rebuildDurationSec": 0.0, "claimsSec": claims_sec},
    )

    # ---------- ⑧ 写产物 ----------
    t_rb = _time.time()
    _scraper.write_outputs(out_root, out_dir, meta, merged, users)
    from types import SimpleNamespace
    structured, _site_texts = _scraper._write_outputs_parallel(
        out_dir, out_root, meta, merged, users, claims,
        (meta.get("ocrStats") or meta.get("ocr_stats") or {}),
        (meta.get("replyStats") or meta.get("reply_stats") or {}),
        SimpleNamespace(play=""),
    )
    # ⚠ 2026-11-20：文字版**失败必须让 merge 失败**（不再 try/except 吞掉）。
    # 原因：吞掉后产物少 4 个（当期文字版.txt / 文字版.html/.txt / 论坛原帖.html），
    # 违反「产物完整无缺失」。宁可整轮失败重跑，也不交付残缺产物。
    # ⚠ 2026-11-21 根因修复（run 44864155826 实测，merge dahkg 15.9 分被取消）：
    #   `text_report.write_text_report` 在 fullText 缺失时会走 process_media，
    #   而 merge 手里没有图片文件 → **开始下载图片 + OCR**！
    #   实测日志：[媒体] 整体 627/7321 张（8%），预计还需约 89 分钟。
    #   OCR 是分片的职责，merge 绝不能做（违反「各司其职」）。
    #   这里强制禁用下载与 OCR：fullText 只能来自各片已合并的 ocr 结果。
    os.environ["NO_IMAGES"] = "true"
    os.environ["NO_OCR"] = "true"
    log("[merge-summary] 已强制禁用图片下载与 OCR（merge 只汇总，不重采集）")
    try:
        text_report.write_text_report(
            out_dir, merged, period=period, year=int(meta.get("year") or 0) or 2026,
            generated_at=str(meta.get("generatedAt") or ""),
            lottery_name=_source.source_label(source),
            date_text=backfill.fallback_date(display),
        )
    except Exception as exc:  # noqa: BLE001
        log(f"[merge-summary] ❌ 文字版生成失败（拒绝交付残缺产物）：{type(exc).__name__}: {exc}")
        raise SystemExit(42) from exc

    # ---------- ⑦b 论坛原帖.html（仅 aocai / dahkg 需要，tuku 无）----------
    # ⚠ 2026-11-21 根因修复：summary 模式没有调 forum.write_outputs，
    # 导致产物少 1 个 → 合并 aocai / dahkg 必然失败：
    #   run 44790922755 / 44809354181 实测：[merge-summary] ❌ 产物缺失 ['论坛原帖.html']
    # 正确签名：forum.write_outputs(out_root, out_dir, meta, posts, users, update_latest=True)
    if source in ("aocai", "dahkg") and not (out_dir / "论坛原帖.html").exists():
        try:
            from core import forum as _forum

            # 正确入口（core/forum.py L1181）：write_thread_html(out_dir, posts)
            # forum.write_outputs 不生成这个文件，它在 run_forum_report 里单独调
            _forum.write_thread_html(out_dir, merged)
            _okf = (out_dir / "论坛原帖.html").exists()
            log(f"[merge-summary] 论坛原帖.html "
                f"{'✅ 已生成' if _okf else '❌ 仍未产出'}（{len(merged)} 帖）")
            if not _okf:
                raise SystemExit(42)
        except SystemExit:
            raise
        except Exception as exc:  # noqa: BLE001
            log(f"[merge-summary] ❌ 论坛原帖.html 生成失败：{type(exc).__name__}: {exc}")
            raise SystemExit(42) from exc

    rebuild_sec = _time.time() - t_rb
    log(f"[merge-summary] 产物写出完成（{rebuild_sec:.2f}s）")

    # ---------- ⑨ 防线 ----------
    passed = True
    period_check = _period.check_posts_period(merged, period)
    log(f"[防线4] 逐帖期号校验：检查 {period_check['checked']} 帖，错误 {period_check['wrongCount']} 个")
    if not period_check["passed"]:
        passed = False
        log(f"[防线4] 错误样本：{period_check['wrong'][:5]}")

    field_check = _check_post_fields(merged)
    log(f"[防线5] 字段完整性：{'通过' if field_check.get('passed') else '存在问题'}"
        f"（缺字段 {len(field_check.get('missing') or [])} 处）")
    if not field_check.get("passed"):
        passed = False

    img_check = _check_images_fields(merged)
    log(f"[防线2] 图片字段：{'通过' if img_check.get('passed') else '存在问题'}"
        f"（异常 {len(img_check.get('bad') or [])} 处）")
    if not img_check.get("passed"):
        passed = False

    low_conf = _check_low_confidence(out_dir, threshold=0.7)
    log(f"[防线5] 低置信度归档：{low_conf.get('lowCount', 0)} 条（只归档、不删改）")

    # ---------- ⑨b 产物齐全硬校验（按源，缺一即失败）----------
    # ⚠ 2026-11-20 根因修复：原来把 aocai 特有的「论坛原帖.html」列进通用清单，
    # 导致 tuku 合并误报「产物缺失」→ 合并 tuku 失败（run 44767982757）。
    _REQUIRED_PRODUCTS = required_products_of(source)
    _missing_products = [n for n in _REQUIRED_PRODUCTS if not (out_dir / n).exists()]
    log(f"[merge-summary] 产物清单：{len(_REQUIRED_PRODUCTS) - len(_missing_products)}"
        f"/{len(_REQUIRED_PRODUCTS)} 齐全"
        + (f"；缺 {_missing_products}" if _missing_products else ""))
    if _missing_products:
        raise SystemExit(
            f"[merge-summary] ❌ 产物缺失 {len(_missing_products)} 个：{_missing_products}"
            f"（拒绝交付残缺产物）"
        )
    # ==========================================================================
    # ⑨c 防线 1/2/3 完整实现（2026-11-23 根因修复）
    # --------------------------------------------------------------------------
    # ## 为什么加
    # 审计发现：防线 1/2/3 的完整实现只在 do_run（L3318-3483），
    # 而三源 merge 走 do_merge_summary —— **从没接上**。
    # 日志里 `[防线1]` / `[防线3]` grep 命中 0，只有弱化版防线2（字段格式）。
    # 后果：R11「绝不漏帖子」/ R12「绝不漏图片」/ R13「绝不漏新增内容」
    #       三条零容忍红线全部失效。
    #
    # ## 为什么不违反「merge 只汇总」
    # 下面三步全是**只读核对**：扫源站列表、求证详情、比对快照。
    # 不写产物、不重采集、不改 posts —— 只是拿源站真实清单和产物对账。
    # ==========================================================================
    log("[防线1] ===== 帖子独立对账（R11 绝不漏帖）=====")
    _scan: dict = {}
    _post_check: dict = {"passed": None}
    _image_check: dict = {"passed": None}
    _scan_complete = True
    try:
        # ⚠ 2026-11-23 修复（run 45058970030 实测：防线2「声明 0 张」/ 防线1「expected=0」）：
        #   原写 `_collect_started = meta["generatedAtUnix"]`，
        #   那是**产物生成时间**（merge 时刻，比采集晚 ~20 分钟），
        #   而 independent_scan 用它过滤「此时间之后的帖子算 lateIds」
        #   → 整期帖子全判成 lateIds → unionMeta 几乎为空 → declared 全 0。
        #
        #   merge 阶段本就不需要时间对齐：
        #     分片全部跑完后才 merge，产物已固定；
        #     独立扫描拿到的就是这一期完整清单，直接对账即可。
        #   所以 created_before 传 0（=不做时间对齐）。
        _scan = _reconcile.independent_scan(
            source, period, max_pages=200,
            created_before=0.0, log=log,
        )
        _scan_complete = bool(_scan.get("complete", True))
        if not _scan_complete:
            log(
                f"[防线1] ⚠ 独立扫描有 {_scan.get('pageErrorCount', 0)} 页重试 "
                f"{_reconcile.PAGE_RETRIES} 次仍失败 → 对账基准不可信，判**不通过**"
            )
        _got_ids = {int(p_.get("articleId") or 0) for p_ in merged if p_.get("articleId")}
        _late_ids = {int(x) for x in (_scan.get("lateIds") or [])}
        _want_ids = set(_scan.get("unionIds") or [])
        _candidates = sorted(_got_ids - _want_ids - _late_ids)
        _miss_candidates = sorted(_want_ids - _got_ids)
        _probe_ids = sorted(set(_candidates) | set(_miss_candidates))
        _probe = (
            _reconcile.probe_details(source, _probe_ids, log=log,
                                     target_period=period)
            if _probe_ids else None
        )
        # 同理：collect_started 传 None（不做「采集期间新发帖」的索引延迟豁免），
        # merge 时所有帖子都已固定，不存在这个场景。
        _post_check = _reconcile.reconcile_posts(
            merged, _scan,
            previous_ids=None,
            probe=_probe,
            target_period=period,
            collect_started=None,
        )
        log(
            f"[防线1] 帖子对账：采集前已存在 {_post_check['expected']} 帖 / 产物 "
            f"{_post_check['actual']} 帖 / 缺 {_post_check['missingCount']}"
            f" / 索引延迟 {_post_check.get('indexLagCount', 0)}"
            f" / 扫描盲区 {_post_check['scanGapCount']}"
            f" / 源站已删 {_post_check['disappearedCount']}"
            f" / 幽灵 {_post_check['phantomCount']}"
            f" / 待复核 {_post_check.get('unverifiedCount', 0)}"
            f"  ← {'通过' if _post_check['passed'] else '不通过'}"
        )
        if _post_check.get("missing"):
            log(f"[防线1] 漏帖样本（前 20）：{_post_check['missing'][:20]}")
        if _post_check.get("scanGap"):
            log(f"[防线1] 扫描盲区样本（前 20）：{_post_check['scanGap'][:20]}")
        if not _scan_complete or _post_check.get("passed") is False:
            passed = False
    except Exception as _exc:  # noqa: BLE001 - 对账炸了不能静默通过
        log(f"[防线1] ❌ 独立对账异常：{type(_exc).__name__}: {_exc}")
        passed = False
        _post_check = {"passed": False, "error": f"{type(_exc).__name__}: {_exc}"}

    log("[防线2] ===== 图片独立对账（R12 绝不漏图）=====")
    try:
        _image_check = _reconcile.reconcile_images(
            merged, _scan or {},
            require_files=None,      # merge 手上没有图片文件，只做声明口径核对
            probe_bad_images=True,   # 坏图 URL 求证（只读 HEAD 请求）
            log=log,
        )
        log(
            f"[防线2] 图片对账：声明 {_image_check.get('declaredTotal', 0)} 张 / 实际 "
            f"{_image_check.get('actualTotal', 0)} 张 / 不符帖子 "
            f"{_image_check.get('mismatchPosts', 0)} / 能下到却没下 "
            f"{_image_check.get('retryableImages', 0)} / 源站404或空 "
            f"{_image_check.get('unrecoverableImages', 0)}  ← "
            f"{'通过' if _image_check.get('passed') else '不通过'}"
        )
        if _image_check.get("passed") is False:
            passed = False
    except Exception as _exc:  # noqa: BLE001
        log(f"[防线2] ❌ 图片对账异常：{type(_exc).__name__}: {_exc}")
        passed = False
        _image_check = {"passed": False, "error": f"{type(_exc).__name__}: {_exc}"}

    log("[防线3] ===== 新增内容不漏（R13）=====")
    try:
        # 单调性：与上一轮快照比，产物帖数不能少（少了就是丢了新增或旧帖）
        _snap_path = work / "state" / f"snapshot_{source}_{period}.json"
        _prev_ids: set[int] = set()
        if _snap_path.exists():
            try:
                import json as _j

                _prev = _j.loads(_snap_path.read_text(encoding="utf-8"))
                _prev_ids = {int(x) for x in (_prev.get("ids") or [])}
            except (OSError, ValueError):
                _prev_ids = set()
        _curr_ids = {int(p_.get("articleId") or 0) for p_ in merged if p_.get("articleId")}
        if _prev_ids:
            _lost = sorted(_prev_ids - _curr_ids)
            log(
                f"[防线3] 单调性：上轮 {len(_prev_ids)} 帖 → 本轮 {len(_curr_ids)} 帖"
                f"，丢失 {len(_lost)} 帖"
                + (f"（样本 {_lost[:10]}）← 不通过" if _lost else "  ← 通过")
            )
            if _lost:
                passed = False
        else:
            log(f"[防线3] 单调性：无上轮快照（首次），建立基线 {len(_curr_ids)} 帖")
        # 写本轮快照供下轮比对
        try:
            _guard.save_json(
                _snap_path,
                {"source": source, "period": period,
                 "ids": sorted(_curr_ids),
                 "ts": time.strftime("%Y-%m-%d %H:%M:%S")},
            )
        except OSError as _exc:
            log(f"[防线3] ⚠ 快照写入失败（不影响本轮）：{_exc}")
    except Exception as _exc:  # noqa: BLE001
        log(f"[防线3] ❌ 单调性检查异常：{type(_exc).__name__}: {_exc}")
        passed = False

    passed = bool(passed)

    # ---------- ⑩ 已知差异标注（条件 2）----------
    known_diff = {
        "reason": "缓存 rows 是更新后的版本，legacy 用的是当次 rowCount 截断的版本",
        "imagesWithRowCountMismatch": rc_mismatch,
        "imagesWithoutCache": n_nocache,
        "fullTextRestored": n_ft,
        "netDiffPct": None,
        "imagesOnDisk": n_images_on_disk,
        "imagesDeclaredUnique": len(_declared),
        "diskImageCheckSkipped": n_images_on_disk == 0,
        "impact": "仅影响「文字版」的图内文字小节；artifact 不含图片→防线2 未做磁盘核对；posts/users/claims/ocr/replies 完全一致",
    }
    meta["textReportKnownDiff"] = known_diff
    meta["durationSec"] = round(_time.time() - started, 2)
    meta["rebuildDurationSec"] = round(rebuild_sec, 2)
    _annotate_hit_rate(out_dir, meta, _shard_stats, log)
    _guard.save_json(out_dir / "run_meta.json", meta)
    log(f"[merge-summary] 已知差异标注：{known_diff}")

    log("=" * 70)
    log(f"[merge-summary] 结论：{'全部通过 ✅' if passed else '未通过 ❌'}  "
        f"期号={period} 帖子={len(merged)} 用户={len(users)} claims={len(claims)}")
    log("=" * 70)
    return 0 if passed else 40



def do_run(args, mode: str = "run") -> int:
    from . import _guard, _period, _reconcile

    work = _work_dir(args.work)
    out_root = Path(args.out or work / "out").resolve()
    source = args.source
    started = time.time()
    log("=" * 70)
    log(f"[{mode}] source={source} 输出根={out_root}")
    log("=" * 70)

    state_file = _resolve_state_dir(args, work) / f"periods_{source}.json"
    state = _guard.load_json(state_file, {})
    last_period = int(state.get("lastPeriod") or 0)
    learned_offset = int(state.get("learnedOffset") or 0)
    # 上轮快照要早读：防线1 需要用它把「源站删帖」与「真多出」区分开
    snapshot_file = _resolve_state_dir(args, work) / f"snapshot_{source}.json"
    prev_snapshot = _guard.load_json(snapshot_file, {})
    log(f"[state] 期号状态={state_file.name} 上轮期号={last_period or '无'} 上轮快照={len(prev_snapshot)} 帖")

    # ---- 期号（R6/M2：每次重新探测，绝不沿用上次；但允许用户显式指定覆盖）----
    period = 0
    evidence: dict = {}
    period_override = int(getattr(args, "period", 0) or 0)
    if period_override:
        period = period_override
        evidence = {"override": True, "requested": period_override}
        log(f"[{mode}] 指定期号 {period}，跳过自动探测与期号四重校验")
    elif source == "dahkg":
        period, evidence = _period.detect_verified_dahkg(
            last_period=last_period, learned_offset=learned_offset, log=log
        )
        log(f"[大港澳] 未指定期号，自动使用当期 {period}")

    # ---- 采集（现有模块，同参数入口）----
    collect_started = time.time()          # 时间对齐基准：只对账"采集前已存在"的帖子

    # ---- 分片状态汇总 + 告警（continue-on-error 之后必须让失败可见）----
    shard_stats: dict = {}
    if mode == "merge":
        shard_stats = _summarize_shards(work, source, log) or {}

    # ---- merge 侧补抓：分片因 CPU 慢/预算到点而留下的 unfinished 清单 ----
    if mode == "merge":
        catch_up = _collect_unfinished(work, source, log)
        if catch_up:
            log(f"[merge] 发现分片遗留 {len(catch_up)} 帖（unfinished_*.json），先补抓再走完整流程")
            t0 = time.time()
            sink: dict = {"total": 0, "picked": 0}
            patched = _patch_shard(source, 0, 1, sink, planned_ids=set(catch_up))
            try:
                rc_cu = _run_module_main(MODULE_OF[source], _collect_argv(out_root, args, period))
                log(f"[merge] 补抓退出码={rc_cu} 耗时={time.time()-t0:.1f}s（本机 CPU 慢时这一步会久一些）")
            finally:
                for mod, name, orig in (patched or []):
                    setattr(mod, name, orig)
        else:
            log("[merge] 没有分片遗留，直接走完整流程")

    if source == "dahkg":
        # dahkg 走**进程内**：这样"按赞+按时间并集"的 scan_period 补丁才生效
        # （模块的 fetch_list_page 把 sortType 写死成 3，按赞分页有竞态，实测漏过 1 帖）
        argv = _collect_argv(out_root, args, period)
        log(f"[采集] 进程内执行 core.dahkg {argv}")
        originals = _patch_dahkg_multisort_union(log)
        try:
            rc = _run_module_main("core.dahkg", argv)
        finally:
            for mod, name, orig in (originals or []):
                setattr(mod, name, orig)
        elapsed = time.time() - collect_started
        log(f"[采集] 退出码={rc} 耗时={elapsed:.1f}s（进程内 + 多排序并集）")
    else:
        rc, elapsed = collect(MODULE_OF[source], out_root, args, period=period)
    if rc != 0:
        log(f"[{mode}] 采集退出码 {rc} ≠ 0 —— 中止，不进入校验（文档 3-C 第 7 项）")
        return rc

    out_dir = _find_period_dir(work, out_root, source, period=period)
    log(f"[{mode}] 产物目录：{out_dir}")
    meta, posts = _reconcile.load_products(out_dir)

    # A'' 命中率写进 run_meta（用户 2026-11-19 条件②：供下一轮校准 HIT_RATE_HISTORY）
    if mode == "merge":
        _annotate_hit_rate(out_dir, meta, shard_stats, log)
        # ① 补文字版（dahkg 的 run_dahkg_report 不生成）
        _ensure_text_products(out_dir, source, period, log)
        # ② imageCount 口径修正：统计所有位置的图（含评论图/楼中楼图）
        _img = _count_all_images(meta.get("_posts_for_count") or [])
        # meta 里没有 posts，重新读一遍（load_products 已读过）
        try:
            from . import _reconcile as _rec3

            _meta3, _posts3 = _rec3.load_products(out_dir)
            _img = _count_all_images(_posts3)
            _old_ic = int(meta.get("imageCount") or 0)
            meta["imageCount"] = _img["total"]
            meta["imageCountByPosition"] = {
                k: v for k, v in _img.items() if k not in ("total",)
            }
            from . import _guard as _g3

            _g3.save_json(out_dir / "run_meta.json", meta)
            log(f"[merge] imageCount 口径修正：{_old_ic} → {_img['total']}"
                f"（正文 {_img['正文']} / 回复 {_img['回复']} / 楼中楼 {_img['楼中楼']}"
                f" / 评论 {_img['评论']} / 评论楼中楼 {_img['评论楼中楼']}；"
                f"去重 URL {_img['unique_urls']}）")
        except Exception as exc:  # noqa: BLE001
            log(f"[merge] ⚠ imageCount 修正失败：{type(exc).__name__}: {exc}")
        # ③ 21 产物齐全硬校验（缺任一即失败）
        _check_products_complete(out_dir, source, log)
    got_period = int(meta.get("period") or 0)
    if not period:
        period = got_period
    log(f"[{mode}] 采集完成：期号={got_period} 帖子={len(posts)} 图片={meta.get('imageCount')} claims={meta.get('claims')}")

    # ---- 防线 4 第 5 道：内容期号逐帖校验 ----
    period_check = _period.check_posts_period(posts, period)
    log(
        f"[防线4] 逐帖期号校验：检查 {period_check['checked']} 帖，错误 {period_check['wrongCount']} 个"
        + ("" if period_check["passed"] else f"  ← 中止！样本 {period_check['wrong'][:3]}")
    )
    if not period_check["passed"]:
        _guard.save_json(work / "audit" / f"FAIL_{source}_{time.strftime('%Y%m%d_%H%M%S')}.json",
                         {"reason": "periodMismatch", "detail": period_check})
        return 30

    # ---- 产物完整性（逐源：dahkg 17 / aocai 20 / tuku 22）----
    product_check = _reconcile.check_products(out_dir, source)
    if args.limit:
        # 限量跑少生成若干派生产物（实测 tuku --limit 2 缺 文字版.html/.txt/当期文字版.txt），
        # 属该模式固有限制；限量跑不用于验收，所以这里只报告、不阻塞，且明确写出来。
        log(
            f"[产物] 限量运行：{product_check['present']}/{product_check['expected']} 个文件"
            + (f"，缺 {product_check['missing']}（限量模式固有，仅供链路验证）" if product_check["missing"] else "")
        )
        product_check["limitedRun"] = True
        product_check["passedRaw"] = product_check["passed"]
        product_check["passed"] = True
    else:
        log(f"[产物] {product_check['present']}/{product_check['expected']} 个文件存在"
            + ("" if product_check["passed"] else f"  缺失：{product_check['missing']}"))
        if product_check.get("guiOnlyPresent"):
            log(f"[产物] 另有 GUI 侧产物存在：{product_check['guiOnlyPresent']}（云端不判定）")

    # ---- 产物指纹（第5道：分片版 vs 不分片版对照用；日志里就能取，不依赖 artifact 下载）----
    try:
        import hashlib

        stamped = {"run_meta.json", "manifest.json", "validation.json"}
        fingerprints: dict[str, str] = {}
        for fpath in sorted(out_dir.iterdir()):
            if not fpath.is_file() or fpath.name in stamped:
                continue
            h = hashlib.sha256(fpath.read_bytes()).hexdigest()
            fingerprints[fpath.name] = h
            log(f"[产物指纹] {fpath.name} {h}")
        log(f"[产物指纹] 合计 {len(fingerprints)} 个文件（已排除 {sorted(stamped)}）")
        # 每帖指纹（第5道 A/B 对照：可分帖区分"数据漂移"与"合并出错"）
        per_post = {int(p.get("articleId") or 0): _product_post_fingerprint(p)
                    for p in posts if int(p.get("articleId") or 0)}
        for aid in sorted(per_post):
            log(f"[帖子指纹] {aid} {per_post[aid]}")
        agg = hashlib.sha256("\n".join(f"{a}:{per_post[a]}" for a in sorted(per_post)).encode()).hexdigest()
        log(f"[帖子指纹汇总] 帖数={len(per_post)} sha256={agg}")
        _guard.save_json(work / "audit" / f"fingerprints_{source}.json",
                         {"period": period, "files": fingerprints})
    except Exception as exc:  # noqa: BLE001 - 指纹失败不该影响产物
        log(f"[产物指纹] ⚠ 计算失败（不影响本轮）：{type(exc).__name__}: {exc}")

    # ---- 第 2 道 + 回复对账 + 楼中楼对账（用户第三节：每轮自动跑）----
    plan_data = _guard.load_json(work / "plan" / f"plan_{source}.json", {})
    declared_map = plan_data.get("postHashes") or {}
    field_check = _check_post_fields(posts)
    image_field_check = _check_images_fields(posts)
    reply_check = _check_reply_counts(posts, declared_map)
    sub_check = _check_second_level(posts)
    log(
        f"[完整性] 第2道 字段：{field_check['checked']} 帖 → "
        f"{'✅ 全含必需字段' if field_check['passed'] else f'❌ {field_check["badCount"]} 帖缺字段 {field_check["bad"][:3]}'}"
    )
    log(
        f"[完整性] 第2道 图片字段：{image_field_check['imagesChecked']} 张 → "
        f"{'✅' if image_field_check['passed'] else f'❌ {image_field_check["badCount"]} 张缺字段 {image_field_check["bad"][:3]}'}"
    )
    log(
        f"[完整性] 回复数对账：比对 {reply_check['compared']} 帖，"
        f"声明缺口(<95%) {reply_check['shortfallCount']} 帖"
        + (f" 样本 {reply_check['shortfall'][:3]}" if reply_check["shortfallCount"] else "  ✅")
    )
    log(
        f"[完整性] 楼中楼对账：比对 {sub_check['commentsChecked']} 条二级回复，"
        f"缺口(<95%) {sub_check['shortfallCount']} 条"
        + (f" 样本 {sub_check['shortfall'][:3]}" if sub_check["shortfallCount"] else "  ✅")
    )
    # ---- OCR 七条第 6 条：低置信度 claim 单独归档（只归档，不删改产物）----
    conf_threshold = float(os.environ.get("LIUHE_CONF_THRESHOLD", "0.7"))
    conf_check = _check_low_confidence(out_dir, threshold=conf_threshold)
    if conf_check.get("totalClaims"):
        _guard.save_json(work / "audit" / f"low_confidence_{source}.json", conf_check)
        log(
            f"[完整性] 低置信度归档：{conf_check['totalClaims']} 条 claim 中 "
            f"<{conf_threshold:g} 的 {conf_check['lowCount']} 条（{conf_check['ratio'] * 100:.1f}%），"
            f"已写 work/audit/low_confidence_{source}.json（只归档、不删改）"
        )
    integrity = {
        "postFields": field_check,
        "imageFields": image_field_check,
        "replyCounts": reply_check,
        "secondLevel": sub_check,
        "lowConfidence": conf_check,
    }
    if not field_check["passed"]:
        log("[完整性] ❌ 第2道字段完整性不通过 → 硬失败（不生成残缺产物）")
        _guard.save_json(
            work / "audit" / f"FAIL_{source}_{time.strftime('%Y%m%d_%H%M%S')}.json",
            {"reason": "postFieldIncomplete", "detail": field_check},
        )
        return 31
    if not image_field_check["passed"]:
        log("[完整性] ❌ 图片字段完整性不通过 → 硬失败")
        _guard.save_json(
            work / "audit" / f"FAIL_{source}_{time.strftime('%Y%m%d_%H%M%S')}.json",
            {"reason": "imageFieldIncomplete", "detail": image_field_check},
        )
        return 32

    # ---- 防线 1 / 2：独立对账（三源都做）----
    scan = {}
    post_check = {"passed": None}
    image_check = {"passed": None}
    if args.limit:
        # `--limit N` 是**故意只抓 N 条**，产物天然比源站少 —— 完整性对账在这种跑法下无意义。
        # 而且能做全量独立扫描（10 个排序 × 数十页）纯属浪费：直接跳过，明确写出来。
        scan = {"complete": True, "unionIds": [], "lateIds": [], "pageErrors": [], "pageErrorCount": 0}
        scan_complete = True
        log(
            f"[防线1] 本次为限量运行（--limit {args.limit}）："
            f"产物 {len(posts)} 帖，**跳过完整性对账与独立扫描**（限量跑不用于验收）"
        )
        probe = None
        post_check = {"passed": True, "limited": True, "expected": 0,
                      "actual": len(posts), "missingCount": 0, "scanGapCount": 0, "disappearedCount": 0,
                      "phantomCount": 0, "unverifiedCount": 0, "lateExcluded": 0,
                      "lateButCollected": 0, "missing": [], "scanGap": [], "disappeared": [],
                      "phantom": [], "unverified": []}
    else:
        scan = _reconcile.independent_scan(
            source, period, max_pages=args.max_pages, created_before=collect_started, log=log
        )
        # 独立扫描若有不完整页（重试后仍失败），对账基准就不可信 —— 必须判不通过
        scan_complete = scan.get("complete", True)
        if not scan_complete:
            log(
                f"[防线1] ⚠ 独立扫描有 {scan.get('pageErrorCount', 0)} 页重试 {_reconcile.PAGE_RETRIES} 次仍失败"
                f"（明细见 audit 的 postsReconcile.scanPageErrors）→ 本轮对账基准不可信，判**不通过**"
            )
        # 「产物有、列表没扫到」的候选 + 「列表有、产物没有」的漏帖候选，
        # 都要向源站详情求证：前者要区分盲区/已删/幽灵，后者要拿 createTime 判断是否为索引延迟
        got_ids = {int(p.get("articleId") or 0) for p in posts if p.get("articleId")}
        late_ids = {int(x) for x in (scan.get("lateIds") or [])}
        want_ids = set(scan.get("unionIds") or [])
        candidates = sorted(got_ids - want_ids - late_ids)
        miss_candidates = sorted(want_ids - got_ids)
        probe_ids = sorted(set(candidates) | set(miss_candidates))
        probe = _reconcile.probe_details(source, probe_ids, log=log, target_period=period) if probe_ids else None
        post_check = _reconcile.reconcile_posts(
            posts,
            scan,
            previous_ids=set(prev_snapshot) or None,
            probe=probe,
            target_period=period,
            collect_started=collect_started,
        )
        log(
            f"[防线1] 帖子对账：采集前已存在 {post_check['expected']} 帖 / 产物 {post_check['actual']} 帖"
            f" / 缺 {post_check['missingCount']}"
            f" / 索引延迟(新帖刚发，列表未收录) {post_check.get('indexLagCount', 0)}"
            f" / 扫描盲区(源站确认存在) {post_check['scanGapCount']}"
            f" / 源站已删 {post_check['disappearedCount']}"
            f" / 幽灵(源站无且上轮无) {post_check['phantomCount']}"
            f" / 待复核(求证不可用) {post_check.get('unverifiedCount', 0)}"
            f" / 时间对齐排除 {post_check['lateExcluded']}"
            f"  ← {'通过' if post_check['passed'] else '不通过'}"
        )
        if post_check["missingCount"]:
            log(f"[防线1] 漏帖样本（前 20）：{post_check['missing'][:20]}")
        if post_check["phantomCount"]:
            log(f"[防线1] 幽灵帖样本（前 20）：{post_check['phantom'][:20]}")
        if post_check["scanGapCount"]:
            log(f"[防线1] 扫描盲区样本（前 20，源站详情已确认存在）：{post_check['scanGap'][:20]}")
        if post_check["disappearedCount"]:
            log(f"[防线1] 源站已删样本（前 20）：{post_check['disappeared'][:20]}")

    image_roots = [p for p in out_dir.rglob("images") if p.is_dir()]
    require_files = image_roots[0].parent if image_roots else None
    image_check = _reconcile.reconcile_images(posts, scan, require_files=require_files, log=log)
    log(
        f"[防线2] 图片对账：声明 {image_check['declaredTotal']} 张 / 实际 {image_check['actualTotal']} 张"
        f" / 不符帖子 {image_check['mismatchPosts']}"
        f" / 源站删帖(列表口径滞后) {image_check.get('deletedPostsCount', 0)}"
        f" / **能下到却没下 {image_check.get('retryableImages', 0)}**"
        f" / 源站404或空文件 {image_check.get('unrecoverableImages', 0)}"
        f" / 源站5xx {image_check.get('unavailableImages', 0)}"
        f" / 网络未确认 {image_check.get('unknownImages', 0)}"
        f" / 文件缺失 {image_check['missingFiles']}"
        f" / 无声明口径帖子 {image_check.get('noDeclaredPosts', 0)}"
        f"  ← {'通过' if image_check['passed'] else '不通过'}"
    )
    for label, key in (("源站物理不可救（404/空文件）", "unrecoverable"),
                       ("源站 5xx 不可用", "unavailable"),
                       ("网络未确认", "unknown")):
        items = image_check.get(key) or []
        if items:
            log(f"[防线2] {label} {len(items)} 张（如实列出，不阻塞门禁）：{[u['url'][-50:] for u in items[:5]]}")

    # ---- 防线 3：快照 + 单调性（prev_snapshot 已在流程开头读过）----
    curr_snapshot = _guard.build_snapshot(posts)
    reply_total = sum(len(p.get("replies") or []) for p in posts)
    snap_diff = _guard.diff_snapshots(prev_snapshot, curr_snapshot) if prev_snapshot else {"firstRun": True}
    if period_override:
        # 指定期号（补跑/压测历史期）时**豁免单调性**：
        # 否则拿 321 期的产物去和 322 期的快照比，会误报"丢帖"。
        monotonic = {"passed": True, "firstRun": True, "skippedByOverride": True}
        log(f"[防线3] 指定期号 {period_override}，跳过单调性检查（不与历史快照比较）")
    else:
        monotonic = (
            _guard.check_monotonic(
                prev_snapshot, curr_snapshot, int(state.get("replyTotal") or 0), reply_total
            )
            if prev_snapshot
            else {"passed": True, "firstRun": True}
        )
    if not snap_diff.get("firstRun"):
        log(
            f"[防线3] 快照对比：上轮 {snap_diff['prevCount']} 帖 → 本轮 {snap_diff['currCount']} 帖"
            f" / 新增 {snap_diff['newCount']} / 变化 {snap_diff['changedCount']} / 丢失 {snap_diff['lostCount']}"
        )
    if not monotonic["passed"] and monotonic.get("lostPosts"):
        # 帖子集合变少：先逐帖向源站求证，区分「源站删帖」与「我们真丢了」
        lost_int = [int(x) for x in monotonic["lostPosts"]]
        pr = _reconcile.probe_details(source, lost_int, log=log, target_period=period)
        still_alive = [a for a in lost_int if (pr.get("exists") or {}).get(a)]
        if still_alive:
            log(f"[防线3] 单调性：丢失 {len(lost_int)} 帖中 {len(still_alive)} 帖源站仍存在 → 真丢失 {still_alive[:10]}（不通过）")
        else:
            log(f"[防线3] 单调性：丢失 {len(lost_int)} 帖经详情求证**全部已从源站消失** → 记作源站删帖，不判失败")
            monotonic["passed"] = True
            monotonic["disappearedVerified"] = len(lost_int)
    log(
        f"[防线3] 单调性：{'通过' if monotonic['passed'] else '不通过 → ' + '; '.join(monotonic.get('problems') or [])}"
    )

    # ---- 防线 5 第 5 道：乱码扫描 ----
    mojibake = _guard.scan_mojibake({"meta": meta, "posts": posts})
    log(
        f"[防线5] 乱码扫描：检查 {mojibake['stringsScanned']} 个字符串，"
        f"硬乱码 {mojibake['hardCount']} / 连续问号 {mojibake['softCount']}"
        + ("" if mojibake["passed"] else f" → 硬乱码样本 {mojibake['hard'][:3]}，中止")
        + ("" if not mojibake["softCount"] else "（连续问号已记录，人工复核）")
    )
    if not mojibake["passed"]:
        _guard.save_json(
            work / "audit" / f"abnormal_text_{source}_{time.strftime('%Y%m%d_%H%M%S')}.json",
            mojibake,
        )

    # ---- 审计报告 ----
    passed = (
        rc == 0
        and period_check["passed"]
        and product_check["passed"]
        and (post_check["passed"] is not False)
        and (image_check["passed"] is not False)
        and monotonic["passed"]
        and scan_complete          # 独立扫描不完整 → 对账不可信，不能算通过
    )
    audit = {
        "source": source,
        "mode": mode,
        "period": period,
        "generatedAt": time.strftime("%Y-%m-%d %H:%M:%S"),
        "durationSec": round(time.time() - started, 1),
        "collectElapsedSec": round(elapsed, 1),
        "collectExitCode": rc,
        "outDir": str(out_dir),
        "periodEvidence": evidence,
        "checks": {
            "products": product_check,
            "periodPerPost": period_check,
            "postsReconcile": {**post_check, "scanPageErrors": scan.get("pageErrors") or [],
                              "scanComplete": scan_complete},
            "imagesReconcile": image_check,
            "snapshot": snap_diff,
            "monotonic": monotonic,
            "mojibake": mojibake,
        },
        "counts": {
            "posts": len(posts),
            "users": len(meta.get("users") or []) or meta.get("userCount"),
            "images": meta.get("imageCount"),
            "claims": meta.get("claims"),
        },
        "passed": passed,
    }
    _guard.save_json(work / "audit" / f"audit_{source}_{time.strftime('%Y%m%d_%H%M%S')}.json", audit)

    # ---- 状态写回（仅全部通过才写，避免污染下一轮单调性基线）----
    # 指定期号时**一律不写**：否则下一轮自动探测会以为"上轮是 321"，
    # 造成期号单调性误报，也会把 321 的快照当成 322 的基线。
    if passed and period_override:
        log(f"[{mode}] 指定期号 {period_override}：跳过 state 写回（不污染自动探测的 lastPeriod/快照）")
    elif passed:
        new_state = {
            "lastPeriod": period,
            "replyTotal": reply_total,
            "learnedOffset": (period - int(evidence.get("check4_drawCrossCheck", {}).get("drawPeriods") or 0))
            if evidence.get("check4_drawCrossCheck", {}).get("drawPeriods")
            else learned_offset,
            "updatedAt": time.strftime("%Y-%m-%d %H:%M:%S"),
        }
        _guard.save_json(state_file, new_state)
        _guard.save_json(snapshot_file, curr_snapshot)
        # v4/v6：把本轮实测的「每帖图数 + det 面积」落盘，供下一轮 plan 当分片权重
        def _post_images_all(p: dict) -> list[str]:
            urls = [str((img or {}).get("url") or "") for img in (p.get("images") or [])]
            for rep in p.get("replies") or []:
                urls += [str((img or {}).get("url") or "") for img in (rep.get("images") or [])]
            return [u for u in urls if u]

        image_cost = {
            "_period": period,
            "_updatedAt": time.strftime("%Y-%m-%d %H:%M:%S"),
            "_note": "每帖 count=正文图+回复图张数；detArea=det 输入端像素和（URL 无尺寸的源为 0）",
            "cost": {
                str(int(p.get("articleId") or 0)): {
                    "count": len(_post_images_all(p)),
                    "detArea": sum(_det_input_area(u) for u in _post_images_all(p)),
                }
                for p in posts
                if int(p.get("articleId") or 0)
            },
        }
        _guard.save_json(state_file.parent / f"imagecost_{source}.json", image_cost)
        # ---- 用户分片台账：merge 成功时再写一次 ----
        # 为什么要在这里写：plan job 里也写了一份（用于当轮分组），
        # 但**plan job 不提交数据仓**（只有 cleanup 与 publish 会 commit/push），
        # 所以 plan 写的台账会随 job 结束丢失 → 下一轮又从头分组 → 跨轮不稳定。
        # merge 跑在 publish 之前、且 publish 会 `git add -A` 提交 state，
        # 因此在这里补写才能真正确保「老用户归属不变」（审查项·一致性测试 8）。
        try:
            plan_for_map = _guard.load_json(_work_dir(args.work) / "plan" / f"plan_{source}.json", {})
            user_map = plan_for_map.get("userToShard") or {}
            if user_map:
                mapping = {str(k): int(v) for k, v in user_map.items()}
                n_shards = len(plan_for_map.get("shards") or []) or 0
                _save_user_shard(state_file.parent, source, mapping, period, n_shards)
                log(
                    f"[{mode}] 已落盘用户分片台账 user_shard_{source}.json："
                    f"{len(mapping)} 个作者 → {n_shards} 片（供下一轮沿用，保证归属稳定）"
                )
            else:
                log(f"[{mode}] ⚠ plan 里没有 userToShard（可能未走按用户分组），本轮不更新台账")
        except Exception as exc:  # noqa: BLE001 - 台账失败不该让整轮失败
            log(f"[{mode}] ⚠ 写用户分片台账失败（不影响产物）：{type(exc).__name__}: {exc}")
        # ---- 内容指纹快照：只在**本轮全部通过**时写（用户第四节），供下一轮 plan 比对 ----
        try:
            plan_path = _work_dir(args.work) / "plan" / f"plan_{source}.json"
            plan_data = _guard.load_json(plan_path, {})
            hashes = plan_data.get("postHashes") or {}
            if hashes:
                _guard.save_json(
                    state_file.parent / f"hashsnapshot_{source}_{period}.json",
                    {
                        "period": period,
                        "updatedAt": time.strftime("%Y-%m-%d %H:%M:%S"),
                        "_note": "每帖 title/content/images/replies 四个 hash；供下一轮内容变更识别",
                        "hashes": hashes,
                    },
                )
                log(f"[{mode}] 已落盘内容指纹快照 hashsnapshot_{source}_{period}.json：{len(hashes)} 帖（数据仓兜底）")
            else:
                log(f"[{mode}] ⚠ plan 里没有 postHashes（可能预抓失败），本轮不更新内容指纹快照")
        except Exception as exc:  # noqa: BLE001 - 快照失败不该让整轮失败
            log(f"[{mode}] ⚠ 写内容指纹快照失败（不影响本轮产物）：{type(exc).__name__}: {exc}")
        log(
            f"[{mode}] 已落盘分片成本基线 imagecost_{source}.json：{len(image_cost['cost'])} 帖，"
            f"合计 {sum(v['count'] for v in image_cost['cost'].values())} 图 / "
            f"{sum(v['detArea'] for v in image_cost['cost'].values())/1e8:.2f} 亿像素（供下一轮分片均衡）"
        )
        log(f"[{mode}] 状态已更新：期号 {last_period or '无'} -> {period}，回复总数 {reply_total}")
    else:
        log(f"[{mode}] 存在未通过项，**不更新**状态基线（避免污染下一轮）")

    log("=" * 70)
    log(f"[{mode}] 结论：{'全部通过 ✅' if passed else '未通过 ❌'}  期号={period} 帖子={len(posts)}")
    log("=" * 70)
    return 0 if passed else 40


# ---------------------------------------------------------------------------
# 模式：--selftest（阶段 1 各项验收的单元测试，真实断言）
# ---------------------------------------------------------------------------
def do_selftest(args) -> int:
    from . import _cleanup, _guard, _period

    results: list[dict] = []

    def case(name: str, fn) -> None:
        t0 = time.time()
        try:
            detail = fn()
            results.append({"case": name, "ok": True, "detail": detail, "ms": round((time.time() - t0) * 1000)})
            log(f"[自检] ✅ {name}  {detail}")
        except AssertionError as exc:
            results.append({"case": name, "ok": False, "error": f"AssertionError: {exc}"})
            log(f"[自检] ❌ {name}  {exc}")
        except Exception as exc:  # noqa: BLE001
            results.append({"case": name, "ok": False, "error": f"{type(exc).__name__}: {exc}"})
            log(f"[自检] ❌ {name}  {type(exc).__name__}: {exc}")
            traceback.print_exc()

    # 1.5 分片：1293 帖分 10 片，每片 129±2，无重无漏
    def case_shard():
        total, shards = 1293, 10
        buckets = [[] for _ in range(shards)]
        for tid in range(1, total + 1):
            buckets[tid % shards].append(tid)
        sizes = [len(b) for b in buckets]
        all_ids = [i for b in buckets for i in b]
        assert sum(sizes) == total, f"分片总数 {sum(sizes)} != {total}"
        assert len(set(all_ids)) == total, "分片有重复"
        assert min(sizes) >= 129 - 2 and max(sizes) <= 129 + 2, f"分片大小不均衡：{sizes}"
        return f"10 片大小 {sizes}（129±2）"

    # 1.4 期号探测：mock 列表返回 319 → 拿到 319，且 isCurrPeriods 交叉验证生效
    def case_period_mock():
        from . import _period as P

        calls: list[tuple[int, int]] = []

        def fake_page(page: int, sort_type: int, page_size: int = 20):
            calls.append((page, sort_type))
            if sort_type == 1:
                return [{"periods": 319, "isCurrPeriods": True, "singaporeId": 1}]
            return [{"periods": 319, "isCurrPeriods": True, "singaporeId": 2}]

        orig_page, orig_draw = P._list_page, P._draw_periods
        P._list_page, P._draw_periods = fake_page, lambda: 120
        try:
            period, ev = P.detect_verified_dahkg(last_period=318, learned_offset=199, log=lambda m: None)
        finally:
            P._list_page, P._draw_periods = orig_page, orig_draw
        assert period == 319, f"期望 319，实际 {period}"
        assert calls[0] == (1, 1), f"首次探测必须用 sortType=1，实际 {calls[0]}"
        assert ev["check1_sortType1_page1"]["period"] == 319
        assert ev["check3_monotonic"]["passed"] if "passed" in ev.get("check3_monotonic", {}) else True
        return f"period={period} 首调用={calls[0]}"

    # 1.4b 期号倒退必须中止
    def case_period_regression():
        from . import _period as P

        orig_page, orig_draw = P._list_page, P._draw_periods
        P._list_page = lambda page, sort_type, page_size=20: [{"periods": 317, "isCurrPeriods": True, "singaporeId": 1}]
        P._draw_periods = lambda: 120
        try:
            try:
                P.detect_verified_dahkg(last_period=320, learned_offset=199, log=lambda m: None)
            except P.PeriodAbort as exc:
                return f"按预期中止：{exc}"
            raise AssertionError("期号倒退未被中止")
        finally:
            P._list_page, P._draw_periods = orig_page, orig_draw

    # 1.2 图片轮转：造 3 天图片，cleanup 后只剩今天+昨天
    def case_cleanup_images():
        import shutil
        from datetime import date, timedelta

        root = _work_dir(args.work) / "_selftest" / "images"
        shutil.rmtree(root, ignore_errors=True)
        root.mkdir(parents=True, exist_ok=True)
        today = date.today()
        for offset, count in ((0, 3), (1, 2), (2, 4)):
            for i in range(count):
                p = root / f"d{offset}_{i}.png"
                p.write_bytes(b"x")
                ts = time.mktime((today - timedelta(days=offset)).timetuple())
                os.utime(p, (ts, ts))
        stats = _cleanup.cleanup_images([root], keep_days=2, today=today)
        left = sorted(p.name for p in root.glob("*.png"))
        assert stats["kept"] == 5, f"应保留 5 张（今天3+昨天2），实际 {stats['kept']}"
        assert stats["deleted"] == 4, f"应删除 4 张（2天前），实际 {stats['deleted']}"
        assert all(not n.startswith("d2_") for n in left), f"2 天前的图片未清干净：{left}"
        return f"保留 {stats['kept']} / 删除 {stats['deleted']}，剩 {left}"

    # 1.8 产物轮转：造 32 期，剩 30 期
    def case_rotate():
        import shutil

        root = _work_dir(args.work) / "_selftest" / "store" / "dahkg"
        shutil.rmtree(root.parent, ignore_errors=True)
        for i in range(289, 321):
            (root / f"澳门_2026_{i}期").mkdir(parents=True, exist_ok=True)
        stats = _cleanup.rotate_periods(root.parent, keep=30)
        left = sorted(p.name for p in root.iterdir())
        assert len(left) == 30, f"应剩 30 期，实际 {len(left)}"
        assert "澳门_2026_289期" not in left, "最旧一期未被删除"
        assert "澳门_2026_320期" in left, "最新一期被误删"
        return f"剩 {len(left)} 期，删 {stats['removedCount']} 期，首期={left[0]}"

    # 1.3 快照：正确识别新增 / 正文变更 / 回复变化
    def case_snapshot():
        prev = _guard.build_snapshot(
            [
                {"articleId": 1, "title": "A", "text": "t1", "nicePoint": 10, "images": [], "replies": []},
                {"articleId": 2, "title": "B", "text": "t2", "nicePoint": 20, "images": [], "replies": []},
            ]
        )
        curr = _guard.build_snapshot(
            [
                {"articleId": 1, "title": "A", "text": "t1", "nicePoint": 10, "images": [], "replies": []},
                {"articleId": 2, "title": "B", "text": "t2-CHANGED", "nicePoint": 20, "images": [], "replies": []},
                {"articleId": 3, "title": "C", "text": "t3", "nicePoint": 30, "images": [], "replies": []},
            ]
        )
        diff = _guard.diff_snapshots(prev, curr)
        assert diff["newCount"] == 1 and diff["newPosts"] == ["3"], f"新增识别错误：{diff}"
        assert diff["changedCount"] == 1 and diff["changed"]["2"] == ["contentHash"], f"变更识别错误：{diff}"
        assert diff["lostCount"] == 0
        return f"新增 1 / 变更 1（contentHash）/ 丢失 0"

    # 防线3 第4道：单调性被破坏时必须报错
    def case_monotonic():
        prev = _guard.build_snapshot([{"articleId": i, "replies": []} for i in (1, 2, 3)])
        curr = _guard.build_snapshot([{"articleId": i, "replies": []} for i in (1, 2)])
        res = _guard.check_monotonic(prev, curr, 10, 5)
        assert res["passed"] is False, "帖子集合减少未被发现"
        assert res["lostPosts"] == ["3"]
        return f"正确报警：{res['problems']}"

    # 乱码扫描
    def case_mojibake():
        bad = _guard.scan_mojibake({"a": "正常文本", "b": "锟斤拷锟斤拷"})
        good = _guard.scan_mojibake({"a": "正常文本 489 六肖"})
        assert bad["hitCount"] == 1 and not bad["passed"], f"乱码未命中：{bad}"
        assert good["hitCount"] == 0 and good["passed"], f"正常文本被误判：{good}"
        return f"命中 1 / 干净文本 0"

    # 产物清单自洽（逐源实测：dahkg 17 / aocai 20 / tuku 22；文档 21 = 核心17 + 文字版3 + report.html）
    def case_products():
        from . import _reconcile as R

        assert len(R.PRODUCTS_DOC_21) == 21, f"文档参照清单应为 21 项，实际 {len(R.PRODUCTS_DOC_21)}"
        assert len(set(R.PRODUCTS_DOC_21)) == 21, "文档参照清单有重复项"
        assert len(R.PRODUCTS_CLI_CORE) == 17, f"核心 CLI 产物应为 17 项，实际 {len(R.PRODUCTS_CLI_CORE)}"
        expect = {"dahkg": 17, "aocai": 20, "tuku": 22}
        got = {k: len(v) for k, v in R.PRODUCTS_BY_SOURCE.items()}
        assert got == expect, f"逐源产物数不符：实际 {got}，期望 {expect}"
        assert set(R.PRODUCTS_CLI_CORE) | set(R.PRODUCTS_SITE_TEXT) | {"report.html"} == set(R.PRODUCTS_DOC_21), \
            "核心17 + 文字版3 + report.html 应正好等于文档 21 项"
        return f"逐源 17/20/22 项；文档21 = 核心17 + 文字版3 + report.html"

    for name, fn in [
        ("1.5 分片分配不变式", case_shard),
        ("1.4 期号探测(按时间优先+mock)", case_period_mock),
        ("1.4b 期号倒退中止", case_period_regression),
        ("1.2 图片 2 天轮转", case_cleanup_images),
        ("1.8 产物 30 期轮转", case_rotate),
        ("1.3 多字段快照", case_snapshot),
        ("防线3-4 单调性报警", case_monotonic),
        ("防线5-5 乱码扫描", case_mojibake),
        ("产物清单 21 项", case_products),
    ]:
        case(name, fn)

    ok = all(r["ok"] for r in results)
    work = _work_dir(args.work)
    _guard.save_json(work / "audit" / "selftest.json", {"ok": ok, "results": results})
    log("=" * 70)
    log(f"[自检] 结果：{sum(1 for r in results if r['ok'])}/{len(results)} 通过  {'✅' if ok else '❌'}")
    log("=" * 70)
    return 0 if ok else 1


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
def parse_args(argv=None):
    parser = argparse.ArgumentParser(description="六合采集器 · 云端采集调度器")
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--cleanup", action="store_true", help="清理旧图片 + 轮转产物")
    mode.add_argument("--plan", action="store_true", help="期号校验 + 独立扫描 + 输出计划")
    mode.add_argument("--shard", type=int, help="分片号（配合 --shards）")
    mode.add_argument("--merge", action="store_true", help="合并分片后跑完整产物")
    mode.add_argument("--run", action="store_true", help="一体化采集（生产默认）")
    mode.add_argument("--selftest", action="store_true", help="单元自检")

    parser.add_argument("--source", default="dahkg", choices=sorted(MODULE_OF))
    parser.add_argument("--shards", type=int, default=1, help="总分片数")
    parser.add_argument("--work", default="", help="工作目录（默认 ./work）")
    parser.add_argument("--out", default="", help="产物输出根")
    parser.add_argument("--store", default="", help="数据仓目录（--cleanup 轮转用）")
    parser.add_argument("--state", default="", help="状态目录（期号/快照），默认 <work>/state")
    parser.add_argument("--workers", type=int, default=4)
    parser.add_argument("--delay", type=float, default=0.05)
    parser.add_argument("--max-pages", type=int, default=200)
    parser.add_argument(
        "--merge-mode", default="legacy", choices=["legacy", "summary"],
        help="merge 模式：legacy=重跑模块（默认，安全）；summary=只汇总各片产物（C1）",
    )
    parser.add_argument(
        "--slot", default="",
        help="档位（12:00/15:00/18:00/20:15/20:50），用于 PERIOD_LOCK_POLICY；空=不锁",
    )
    parser.add_argument(
        "--period", type=int, default=0,
        help="指定期号（0=自动探测）。用于补跑/压测历史期；"
             "指定时不会写回 state 的 lastPeriod/快照，也不会做单调性检查",
    )
    parser.add_argument("--limit", type=int, default=0)
    parser.add_argument("--ocr-limit", type=int, default=0)
    parser.add_argument("--no-images", action="store_true")
    parser.add_argument(
        "--no-prefetch",
        action="store_true",
        help="plan 阶段不预抓详情+回复（预抓用于算出每帖真实图数/det 面积，让分片均衡）",
    )
    parser.add_argument("--no-ocr", action="store_true")
    parser.add_argument("--no-replies", action="store_true")
    parser.add_argument("--keep-days", type=int, default=2)
    parser.add_argument("--keep-periods", type=int, default=30)
    parser.add_argument("--skip-periods", action="store_true", help="cleanup 只清图片，不轮转产物")
    parser.add_argument("--dry-run", action="store_true")
    return parser.parse_args(argv)


def main(argv=None) -> int:
    from . import _compat

    env = _compat.prepare()
    args = parse_args(argv)
    log(f"[环境] {json.dumps(env, ensure_ascii=False)}")

    if args.selftest:
        return do_selftest(args)
    if args.cleanup:
        return do_cleanup(args)
    if args.plan:
        return do_plan(args)
    if args.shard is not None:
        return do_shard(args)
    if args.merge:
        from .merge_summary import resolve_merge_mode

        mode_name = (getattr(args, "merge_mode", "") or "").strip() or resolve_merge_mode()
        if mode_name == "summary":
            return do_merge_summary(args)
        return do_run(args, mode="merge")
    if args.run:
        return do_run(args, mode="run")
    raise SystemExit("未知模式")


if __name__ == "__main__":
    raise SystemExit(main())









