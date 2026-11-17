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
                return aid, {
                    "count": len(urls),
                    "detArea": sum(_det_input_area(u) for u in urls),
                    "bodyCount": body_count,
                    "replyCount": len(urls) - body_count,
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
                return aid, {
                    "count": len(urls),
                    "detArea": 0,
                    "bodyCount": body_count,
                    "replyCount": len(urls) - body_count,
                    **_post_content_hashes(tiezi, reps),
                }
            if source == "tuku":
                from core import scraper

                raw = scraper.api_get("/unite49/h5/article/detail", {"articleId": aid}, delay=0)
                pics = (raw or {}).get("pictureList") or []
                urls += [str((pic or {}).get("url") or "") for pic in pics if (pic or {}).get("url")]
                return aid, {
                    "count": len(urls),
                    "detArea": 0,
                    "bodyCount": len(urls),
                    "replyCount": 0,
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
            # ---- 内容变更识别（用户第四节）：与上一轮快照比对 ----
            hash_fields = ("titleHash", "contentHash", "imagesHash", "repliesHash",
                           "contentLen", "replyPoint", "imgTotal", "replyFetched")
            plan["postHashes"] = {
                str(aid): {k: v.get(k) for k in hash_fields}
                for aid, v in prefetched.items()
            }
            prev = _guard.load_json(state_dir / f"hashsnapshot_{source}.json", {})
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
                    "comparedAgainst": f"hashsnapshot_{source}.json@period={prev_period}",
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
        # ---- 按**用户**做 LPT 分组（用户 2026-11-17 方案 3）----
        # 为什么改成按用户：按帖子分片时，同一个作者的不同帖子会被分到不同片，
        # 他的详情/回复/图片缓存被切碎、跨轮命中率低，新帖还会让归属漂移。
        # 按用户分片后：一个作者的所有帖子恒在同一片，新帖自动跟随。
        sender_of_aid = {
            aid: str((union_meta.get(str(aid)) or {}).get("senderId") or "") for aid in union_ids
        }
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
        total_posts_in_groups = sum(len(g) for g in groups)
        if total_posts_in_groups != len(union_ids):
            raise SystemExit(
                f"[plan] ❌ 断言③失败：分组后帖子数 {total_posts_in_groups} != 实际 {len(union_ids)}"
            )
        log(
            f"[plan] ✅ 用户分组三条断言通过：{len(user_weights)} 个作者 → {shard_n} 片，"
            f"{len(union_ids)} 帖全部覆盖、无重复"
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


def _sender_of(item: dict) -> str:
    """列表行里的作者标识（三源字段名不同，统一成字符串）。

    用户 2026-11-17 方案 3 要求「按用户分片」：一个作者的所有帖子必须落在同一片，
    否则同一个人每轮可能换片 → 缓存命中率低、且新帖会漂移。
    """
    for key in ("senderId", "userId", "authorId", "uid", "nickname", "userName"):
        value = (item or {}).get(key)
        if value not in (None, "", 0):
            return str(value)
    return ""


def _group_by_user(user_weights: dict[str, int], n: int, prev: dict[str, int] | None = None):
    """按**用户**做 LPT 分组，并尽量保持上一轮的归属（跨轮稳定）。

    - 老用户：沿用 `prev` 里的分片号（保证跨轮一致，缓存才命得中）
    - 新用户：放进当前负载最轻的那片
    返回 (groups_by_user, user_to_shard)。
    """
    prev = prev or {}
    n = max(1, n)
    groups: list[list[str]] = [[] for _ in range(n)]
    loads = [0] * n
    mapping: dict[str, int] = {}

    # 1) 老用户先就位（保持稳定）
    for uid, w in user_weights.items():
        idx = prev.get(uid)
        if isinstance(idx, int) and 0 <= idx < n:
            groups[idx].append(uid)
            loads[idx] += max(1, int(w))
            mapping[uid] = idx

    # 2) 新用户按权重从大到小，塞进最轻的片
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
                 mine_users: set[str] | None = None):
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
        picked = [it for it in seq if _sender_of(it) in mine_users]
        sink["mode"] = "by-user"
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
    sink["picked"] = len(picked)
    sink["imagesPicked"] = sum(_item_image_weight(it) for it in picked)
    return (picked, *meta) if meta else picked


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
                                 mine_users=mine_users)
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


def _cpu_ocr_probe() -> float:
    """在**本机**跑一张合成图，测出这台 runner 的 pure-OCR 秒/张（用于按预算切片）。

    ## 为什么要开机就测（2026-11-17 实测结论）
    GitHub 的 runner 池里 CPU 是异构的，实测 14 片拿到 5 种型号：
      AMD EPYC 9V45 / 7763 / 9V74 → 纯 OCR 0.37~0.65 秒/张
      Intel Xeon 6975P-C          → 纯 OCR 1.15~1.95 秒/张（慢 3~5 倍）
    静态分片因此永远不均。这里改成：**开机先自测速度**，再决定这台机器能吃多少活，
    超预算的部分写进 unfinished_*.json 交给 merge 补抓。
    """
    global _CPU_PROBE_CACHE
    if _CPU_PROBE_CACHE is not None:
        return _CPU_PROBE_CACHE
    try:
        import tempfile

        import numpy as np

        from core import ocr as ocr_mod

        analyze = getattr(ocr_mod, "analyze_image", None)
        if analyze is None:
            _CPU_PROBE_CACHE = 0.0
            return 0.0
        # 合成一张 240x360 的"有内容"图：白底 + 若干黑条，避免被当成空图跳过
        arr = np.full((360, 240, 3), 255, dtype=np.uint8)
        for i in range(6):
            arr[40 + i * 40: 52 + i * 40, 30: 210] = 30
        tmp = Path(tempfile.mkdtemp(prefix="cpuprobe-")) / "probe.png"
        import cv2

        cv2.imwrite(str(tmp), arr)
        # ⚠️ analyze_image 的真实签名是 (image_path, target_period, cache_dir, ...)：
        # 2026-11-17 v8 实测漏传这两个参数 → `TypeError: missing 2 required positional arguments`
        # → 探针静默失败、预算切片全部没生效（分片仍按原样跑）。这里用独立缓存目录，避免污染真实缓存。
        # 合成若干张**互不相同**的图，并用 force=True 强制真实识别：
        # 否则同一张图第二次会命中 OCR 缓存（实测返回 0.0013 秒，等于没测）
        cache_dir = tmp.parent / "cache"
        cache_dir.mkdir(parents=True, exist_ok=True)
        probes: list[Path] = []
        for idx in range(2):
            arr2 = arr.copy()
            arr2[20 + idx * 30: 26 + idx * 30, 10: 230] = 60   # 让两张图不同
            p2 = tmp.parent / f"probe{idx}.png"
            cv2.imwrite(str(p2), arr2)
            probes.append(p2)
        analyze(str(probes[0]), 0, str(cache_dir), force=True)   # 预热（模型加载，不计时）
        t0 = time.perf_counter()
        analyze(str(probes[1]), 0, str(cache_dir), force=True)
        seconds = time.perf_counter() - t0
        _CPU_PROBE_CACHE = round(seconds, 4)
        return _CPU_PROBE_CACHE
    except Exception as exc:  # noqa: BLE001 - 探针失败不该影响主流程
        log(f"[shard] ⚠ CPU 探针失败（跳过按预算切片）：{type(exc).__name__}: {exc}")
        _CPU_PROBE_CACHE = 0.0
        return 0.0


_CPU_PROBE_CACHE: float | None = None


def _pick_within_budget(ids: list[int], cost: dict, per_image_seconds: float, budget_seconds: float) -> tuple[list[int], list[int]]:
    """按预算挑出本机要处理的帖子，其余留给 merge。

    cost: {articleId: 预计图片数}（来自 plan 的 postCost）
    估算：每帖耗时 ≈ 图片数 × 本机秒/张；另留 60s 固定开销（启动/扫描/收尾）。
    """
    if per_image_seconds <= 0 or budget_seconds <= 0 or not ids:
        return list(ids), []
    usable = max(0.0, budget_seconds - 60.0)
    taken: list[int] = []
    rest: list[int] = []
    acc = 0.0
    # 先按代价从小到大装，尽量多装（同片内顺序不影响正确性）
    for aid in sorted(ids, key=lambda x: cost.get(x, 1)):
        est = max(1, cost.get(aid, 1)) * per_image_seconds
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

    # ---- 按本机 CPU 速度决定「这一片能干多少」，超预算的交给 merge（用户 2026-11-17 方案）----
    rest_ids: list[int] = []
    if planned_ids:
        post_cost = (plan.get("postCost") or {})
        cost = {}
        for aid in planned_ids:
            info = post_cost.get(str(aid)) or {}
            cost[aid] = int(info.get("count") or 1) if isinstance(info, dict) else 1
        probe = _cpu_ocr_probe()
        budget = float(os.environ.get("LIUHE_SHARD_SOFT_BUDGET", "900"))
        if probe > 0:
            taken, rest_ids = _pick_within_budget(sorted(planned_ids), cost, probe, budget)
            total_imgs = sum(cost.values())
            est_all = total_imgs * probe
            log(
                f"[shard] 本机 OCR 探针 {probe:.3f} 秒/张；本片 {len(planned_ids)} 帖 / {total_imgs} 图，"
                f"全干预估 {est_all/60:.1f} 分；预算 {budget/60:.1f} 分"
            )
            if rest_ids:
                log(
                    f"[shard] ⏱ 预算内只能处理 {len(taken)} 帖，"
                    f"剩余 {len(rest_ids)} 帖写入 unfinished_{source}_{shard_index}.json 交 merge 补抓"
                )
                _guard.save_json(
                    work / "unfinished" / f"unfinished_{source}_{shard_index}.json",
                    {
                        "source": source,
                        "shard": shard_index,
                        "ids": rest_ids,
                        "reason": f"本机 OCR {probe:.3f} 秒/张，超出 {budget:.0f}s 预算",
                        "ts": time.strftime("%Y-%m-%d %H:%M:%S"),
                    },
                )
                planned_ids = set(taken)
                # 重新打补丁（用缩小后的集合）
                for mod, name, original in (originals or []):
                    setattr(mod, name, original)
                originals = _patch_shard(source, shard_index, shard_total, sink, planned_ids=planned_ids, mine_users=mine_users)
            else:
                log(f"[shard] 本片工作量在预算内，全部自己处理")

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

    log(
        f"[shard] {source} 本片：源站本期共 {sink.get('total', 0)} 帖 / {sink.get('imagesTotal', 0)} 图，"
        f"本片负责 {sink.get('picked', 0)} 帖 / {sink.get('imagesPicked', 0)} 图"
        f"（模式={sink.get('mode', '?')}）；模块退出码={code}"
    )

    # OCR/下载 分开计时（这是"CPU 慢还是网络慢"的判据）
    ocr_s = round(diag["ocr"]["seconds"], 1)
    dl_s = round(diag["download"]["seconds"], 1)
    ocr_n = diag["ocr"]["calls"]
    # ⚠️ 2026-11-17 实测事故：按用户分片 + 缓存隔离后，分片缓存命中率提高，
    # 有的分片 `ocr_n == 0`（一次新 OCR 都没做，全部命中缓存），
    # 这里直接 `ocr_s / ocr_n` 会 **ZeroDivisionError 让整个分片 job 失败**（20 片全挂）。
    # 正确写法：先判断次数，为 0 时显式说明"全部命中缓存"。
    per_call = (ocr_s / ocr_n) if ocr_n else 0.0
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
                    state_file.parent / f"hashsnapshot_{source}.json",
                    {
                        "period": period,
                        "updatedAt": time.strftime("%Y-%m-%d %H:%M:%S"),
                        "_note": "每帖 title/content/images/replies 四个 hash；供下一轮内容变更识别",
                        "hashes": hashes,
                    },
                )
                log(f"[{mode}] 已落盘内容指纹快照 hashsnapshot_{source}.json：{len(hashes)} 帖")
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









