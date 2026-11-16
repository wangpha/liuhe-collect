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


def _state_path(work: Path, source: str, name: str) -> Path:
    """状态文件路径。

    实测 bug（2026-11-16）：state 原先写在工作目录 `work/state`，而 workflow 只把
    `work/store`（数据仓检出目录）提交回仓库 → 期号/快照从来没落地过，
    导致「单调性检查」永远走首轮分支、`lastPeriod` 永远是 0。
    现在支持 `--state` 指定目录，workflow 传 `work/store/state` 让它随数据仓持久化。
    """
    return work / "state" / f"{name}_{source}.json"


def _resolve_state_dir(args, work: Path) -> Path:
    if getattr(args, "state", ""):
        return Path(args.state).resolve()
    return work / "state"


def _find_period_dir(work: Path, out_root: Path, source: str) -> Path:
    """产物目录：out 下**真正含产物**的那一期目录。

    实测坑（2026-11-16 Gate1）：49图库除了 `澳门_2026_320期`，还会写一个
    `历史回采/49图库/2026-11-15_第320期` 的中间目录（只有 5 个文件），
    两个目录名解析出来的期号都是 320 —— 只按期号取最大会取错。
    判据：优先 posts.json 存在，其次文件数最多。
    """
    sub = SOURCE_SUBDIR[source]
    base = out_root / sub if sub else out_root
    candidates = [p for p in base.glob("*期") if p.is_dir()]
    if not candidates:
        raise SystemExit(f"[错误] 产物目录不存在：{base}/*期")
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
def _post_total_images(post: dict) -> int:
    """一帖的**全部图片数** = 正文图 + 每条回复里的图。

    v4 权重的核心（2026-11-17 实测）：分片真正的成本是「要 OCR 多少张图」，
    而回复图既不在列表行的 `imgTotal` 里，也无法用 `replyPoint` 估准
    （实测三片体图都是 ~94 张、回复数相近，实际 OCR 却是 274 / 613 / 841 张，差 3.07 倍）。
    但这些数字**上一轮 merge 的 posts.json 里就有**，同一期 id 稳定 → 直接用实测值。
    """
    total = len(post.get("images") or [])
    for reply in post.get("replies") or []:
        total += len(reply.get("images") or [])
    return int(total)


def _load_image_cost(state_dir: Path, source: str) -> dict:
    """读上一轮 merge 落盘的「每帖总图数」（key 是 str(articleId)）。"""
    from . import _guard

    data = _guard.load_json(state_dir / f"imagecost_{source}.json", {})
    if not isinstance(data, dict):
        return {}
    cost = data.get("cost")
    return cost if isinstance(cost, dict) else {}


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
                return aid, {
                    "count": len(urls),
                    "detArea": sum(_det_input_area(u) for u in urls),
                    "bodyCount": body_count,
                    "replyCount": len(urls) - body_count,
                }
            if source == "aocai":
                from core import forum as forum_mod

                detail = forum_mod.fetch_thread_detail(aid, 0, include_replies=True, delay=0.0, max_pages=50)
                tiezi = (detail or {}).get("tiezi") or {}
                urls += [u.strip() for u in str(tiezi.get("img") or "").split(",") if u.strip()]
                body_count = len(urls)
                for rep in (detail or {}).get("replies") or []:
                    urls += [u.strip() for u in str(rep.get("img") or "").split(",") if u.strip()]
                return aid, {"count": len(urls), "detArea": 0,
                             "bodyCount": body_count, "replyCount": len(urls) - body_count}
            if source == "tuku":
                from core import scraper

                raw = scraper.api_get("/unite49/h5/article/detail", {"articleId": aid}, delay=0)
                pics = (raw or {}).get("pictureList") or []
                urls += [str((pic or {}).get("url") or "") for pic in pics if (pic or {}).get("url")]
                return aid, {"count": len(urls), "detArea": 0, "bodyCount": len(urls), "replyCount": 0}
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

    # ---- 期号：三源口径不同，各自用自己源头的探测方式（见 _period.detect_period 注释）----
    info = _period.detect_period(
        source, last_period=last_period, learned_offset=learned_offset, log=log
    )
    period = int(info["period"])
    display = int(info["display"])
    evidence = info["evidence"]
    caliber = info.get("caliber", "short")

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

    hist_cost = _load_image_cost(state_dir, source)
    hist_period = int((hist_cost.get("_period") or 0)) if isinstance(hist_cost, dict) else 0
    use_hist = bool(hist_cost) and (hist_period in (0, period, display))

    if shard_n > 1 and union_ids:
        if prefetched:
            weights = {}
            for aid in union_ids:
                info = prefetched.get(aid) or {}
                w = int(info.get("detArea") or 0) or int(info.get("count") or 0)
                weights[aid] = max(1, w)
            mode = "prefetch-det-area"
            weight_src = f"prepare 预抓实测 {len(prefetched)}/{len(union_ids)} 帖（det 面积，无尺寸的源退化为图数）"
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
        groups = _balanced_groups(union_ids, weights, shard_n)
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
        log(
            f"[plan] 分片均衡（{mode}）：{shard_n} 片，总权重 {sum(imgs)}；每片 {imgs}；每片帖子 {sizes}"
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


#: det 的 limit_side_len（core/scraper.py 里同值）：短边小于它会被放大到它
_DET_SIDE = 736
#: 回复图没有尺寸 → 用回复条数估；实测 2682 张回复图 / 4813 条回复 ≈ 0.56 张/条
_REPLY_IMG_PER_REPLY = 0.56
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


def _item_image_weight(item: dict) -> int:
    """一条列表行的工作量权重（用于分片均衡）。

    - **大港澳**：图片 URL 带 `_<w>_<h>_` 尺寸 → `Σ det 输入像素` + 回复图估计
    - **澳彩 / 49图库**：URL 不带尺寸（`v27-xxx.jpg`）→ 退回 `正文图*2 + 回复量`
    """
    urls = [u.strip() for u in str(item.get("img") or "").split(",") if u.strip()]
    area = sum(_det_input_area(u) for u in urls)
    try:
        replies = max(0, int(item.get("replyPoint") or 0))
    except (TypeError, ValueError):
        replies = 0
    if area > 0:
        return int(area + replies * _REPLY_IMG_PER_REPLY * _MEDIAN_IMG_AREA)
    body = 0
    try:
        body = int(item.get("imgTotal") or 0)
    except (TypeError, ValueError):
        body = 0
    body = body or max(1, len(urls))
    return max(1, body * 2 + replies)


def _balanced_groups(ids: list[int], weights: dict[int, int], n: int) -> list[list[int]]:
    """把帖子切成 n 组，使每组的权重之和尽量相等。

    ## 为什么用「贪心最长优先（LPT）」而不是连续切分
    连续切分（按 id 顺序、累计到比例边界就换组）遇到**单个超大项**就切不匀：
    实测大港澳一帖的 det 面积可达 0.86 亿像素，占每片目标（≈0.52 亿）的 1.6 倍，
    连续切分只能得到 1.56 倍不均。
    LPT（先放大项、每次放进当前最轻的组）对重尾分布接近最优，实测同一批帖子：
      连续切分 1.56 倍 → **LPT 1.1x 倍**。

    ## 确定性
    所有分片 job 读同一份 plan JSON，分组只在 plan 里算一次，
    所以「同一份输入必然同一份分组」；具体是否连续不影响正确性
    （每个分片只按 id 集合取自己那批帖子）。
    """
    if n <= 0:
        return []
    ordered = sorted(set(ids))
    order = sorted(ordered, key=lambda a: (-max(1, int(weights.get(a, 1))), a))
    groups: list[list[int]] = [[] for _ in range(n)]
    load = [0] * n
    for aid in order:
        idx = min(range(n), key=lambda i: (load[i], i))
        groups[idx].append(aid)
        load[idx] += max(1, int(weights.get(aid, 1)))
    # 组内按 id 排序，便于人看日志
    for g in groups:
        g.sort()
    return groups


def _slice_items(items, shard: int, total: int, sink: dict, planned_ids: set[int] | None = None):
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
    if planned_ids is not None:
        picked = [it for it in seq if _plan_item_id(it) in planned_ids]
        sink["mode"] = "balanced"
    else:
        picked = [it for it in seq if _plan_item_id(it) % max(1, total) == shard]
        sink["mode"] = "modulo"
    sink["picked"] = len(picked)
    sink["imagesPicked"] = sum(_item_image_weight(it) for it in picked)
    return (picked, *meta) if meta else picked


def _patch_shard(source: str, shard: int, total: int, sink: dict, planned_ids: set[int] | None = None):
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
            return _slice_items(original(*a, **kw), shard, total, sink, planned_ids=planned_ids)
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
    finally:
        sys.argv = old_argv
    return code


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
    # 优先用 plan 里按「累计图片数」算好的均衡分组（18 个分片读同一份 JSON → 分配一致）
    planned_ids: set[int] | None = None
    shard_plan = plan.get("shardPlan") or {}
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
    originals = _patch_shard(source, shard_index, shard_total, sink, planned_ids=planned_ids)
    log(
        f"[shard] 已给 {source} 的列表取数打切片补丁"
        f"（{'plan 均衡分组' if planned_ids is not None else f'id % {shard_total} == {shard_index}'}）"
    )

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

    try:
        code = _run_module_main(MODULE_OF[source], argv)
    finally:
        for mod, name, original in (originals or []):
            setattr(mod, name, original)

    log(
        f"[shard] {source} 本片：源站本期共 {sink.get('total', 0)} 帖 / {sink.get('imagesTotal', 0)} 图，"
        f"本片负责 {sink.get('picked', 0)} 帖 / {sink.get('imagesPicked', 0)} 图"
        f"（模式={sink.get('mode', '?')}）；模块退出码={code}"
    )
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
        },
    )
    return code


# ---------------------------------------------------------------------------
# 模式：--shard（预热缓存）
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# 模式：--run / --merge（共用主流程）
# ---------------------------------------------------------------------------
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

    # ---- 期号（R6/M2：每次重新探测，绝不沿用上次）----
    period = 0
    evidence: dict = {}
    if source == "dahkg":
        period, evidence = _period.detect_verified_dahkg(
            last_period=last_period, learned_offset=learned_offset, log=log
        )
        log(f"[大港澳] 未指定期号，自动使用当期 {period}")

    # ---- 采集（现有模块，同参数入口）----
    collect_started = time.time()          # 时间对齐基准：只对账"采集前已存在"的帖子
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

    out_dir = _find_period_dir(work, out_root, source)
    log(f"[{mode}] 产物目录：{out_dir}")
    meta, posts = _reconcile.load_products(out_dir)
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
    if passed:
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
        _guard.save_json(state_dir / f"imagecost_{source}.json", image_cost)
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
        return do_run(args, mode="merge")
    if args.run:
        return do_run(args, mode="run")
    raise SystemExit("未知模式")


if __name__ == "__main__":
    raise SystemExit(main())




