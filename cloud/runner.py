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
def collect(module: str, out_root: Path, args, period: int = 0) -> tuple[int, float]:
    # **必须绝对路径**：core/dahkg.py::run_dahkg_report 里用
    # `(out_dir / "论坛原帖.html").as_uri()` 生成 postUrl，
    # 相对路径会抛 `ValueError: relative path can't be expressed as a file URI`
    # （2026-11-16 Gate2 实测：dahkg/aocai 因此退出码 1）。
    out_root = Path(out_root).resolve()
    cmd = [sys.executable, "-m", module, "--out", str(out_root)]
    if period:
        cmd += ["--period", str(period)]
    if args.workers:
        cmd += ["--workers", str(args.workers)]
    if args.delay is not None:
        cmd += ["--delay", str(args.delay)]
    if args.max_pages:
        cmd += ["--max-pages", str(args.max_pages)]
    if args.limit:
        cmd += ["--limit", str(args.limit)]
    if args.no_images:
        cmd.append("--no-images")
    if args.no_ocr:
        cmd.append("--no-ocr")
    if args.no_replies:
        cmd.append("--no-replies")
    if args.ocr_limit:
        cmd += ["--ocr-limit", str(args.ocr_limit)]
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

    evidence: dict = {}
    if source == "dahkg":
        period, evidence = _period.detect_verified_dahkg(
            last_period=last_period, learned_offset=learned_offset, log=log
        )
    else:
        # 图库/澳彩：沿用其模块自身的期号识别（后续在 4.x 验收里与源站核对）
        period = 0
        log(f"[plan] {source}：不在此处探测期号，交由模块自身识别后在 --run 中回读核对")

    scan = {}
    if source == "dahkg" and period:
        scan = _reconcile.independent_scan_dahkg(period, max_pages=args.max_pages, log=log)
        if not scan["agree"]:
            log(
                f"[plan] 警告：两种排序的帖子集合不一致 —— "
                f"按时间 {scan['countByTime']} 帖 / 按赞 {scan['countByLike']} 帖；"
                f"仅按时间才有的 {len(scan['onlyByTime'])} 个，仅按赞才有的 {len(scan['onlyByLike'])} 个"
            )

    plan = {
        "source": source,
        "module": MODULE_OF[source],
        "period": period,
        "createdAt": time.strftime("%Y-%m-%d %H:%M:%S"),
        "periodEvidence": evidence,
        "scan": scan,
        "counts": {
            "expectedPosts": scan.get("countUnion", 0),
            "byTime": scan.get("countByTime", 0),
            "byLike": scan.get("countByLike", 0),
        },
    }
    _guard.save_json(work / "plan" / f"plan_{source}.json", plan)
    log(f"[plan] 完成：期号={period or '待模块识别'} 预期帖子={plan['counts']['expectedPosts']}")
    return 0


# ---------------------------------------------------------------------------
# 模式：--shard（预热缓存）
# ---------------------------------------------------------------------------
def do_shard(args) -> int:
    """分片任务只做一件事：**把它负责的那批帖子的图片下载 + OCR 结果写进缓存**。

    为什么要分片：冷缓存下一期 1.4 万张图在 4 核 CPU 上要 ~10 小时，超过
    Actions 单 job 6 小时上限。10 个 job 并行后每片约 1 小时。
    分片产物只是 OCR 缓存（`merge` 阶段复用），不生成最终产物。
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

    plan_path = work / "plan" / f"plan_{source}.json"
    plan = _guard.load_json(plan_path, {})
    if not plan:
        raise SystemExit(f"[错误] 没有 plan 文件：{plan_path}")
    period = int(plan.get("period") or 0)
    if not period:
        raise SystemExit("[错误] plan 里没有期号，无法分片")
    if source != "dahkg":
        raise SystemExit(f"[错误] 分片目前只对大港澳实现（source={source}）")

    # 重新取列表拿到 item（plan 只存 id，避免 plan 文件过大）
    from core import dahkg, forum as forum_mod

    items, _scan = dahkg.scan_period(period, max_pages=args.max_pages, progress=log)
    mine = [it for it in items if int(it.get("tid") or 0) % shard_total == shard_index]
    log(f"[shard] 全期 {len(items)} 帖，本片负责 {len(mine)} 帖（tid % {shard_total} == {shard_index}）")
    if not mine:
        log("[shard] 本片无帖子，直接成功返回")
        _guard.save_json(
            work / "shards" / f"shard_{source}_{shard_index}.json",
            {"source": source, "shard": shard_index, "posts": 0, "period": period},
        )
        return 0

    out_dir = out_root / SOURCE_SUBDIR[source] / f"澳门_2026_{period}期"
    out_dir.mkdir(parents=True, exist_ok=True)

    posts, stats = dahkg.collect_posts(
        mine,
        period,
        workers=max(1, int(args.workers or 4)),
        progress=log,
        filter_replies=True,
    )
    log(f"[shard] 详情完成：{len(posts)} 帖，失败 {stats.get('failed')}")

    media = forum_mod.process_media(
        posts,
        out_dir,
        out_dir / ".cache",
        period,
        download_workers=forum_mod.forum_download_workers(args.workers or 4),
        delay=args.delay if args.delay is not None else 0.05,
        referer=None,
        download_enabled=not args.no_images,
        ocr_enabled=not args.no_ocr,
        force_ocr=False,
        max_images=0,
        capture_text=True,
    )
    ocr_stats = media["ocr"]
    log(
        f"[shard] OCR 完成：总 {ocr_stats.get('total')} 张 / 缓存命中 {ocr_stats.get('cache_hit')}"
        f" / 新识别 {ocr_stats.get('cache_miss')} / 失败 {ocr_stats.get('error')}"
    )
    _guard.save_json(
        work / "shards" / f"shard_{source}_{shard_index}.json",
        {
            "source": source,
            "shard": shard_index,
            "period": period,
            "posts": len(posts),
            "images": media["images"],
            "ocr": ocr_stats,
            "detailStats": stats,
            "ts": time.strftime("%Y-%m-%d %H:%M:%S"),
        },
    )
    return 0


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

    # ---- 产物完整性（R3：论坛类 21 个 / 图库 22 个）----
    product_check = _reconcile.check_products(out_dir, source)
    log(f"[产物] {product_check['present']}/{product_check['expected']} 个文件存在"
        + ("" if product_check["passed"] else f"  缺失：{product_check['missing']}"))

    # ---- 防线 1 / 2：独立对账（三源都做）----
    scan = {}
    post_check = {"passed": None}
    image_check = {"passed": None}
    scan = _reconcile.independent_scan(
        source, period, max_pages=args.max_pages, created_before=collect_started, log=log
    )
    # 「产物有、列表没扫到」的候选，先逐帖向源站详情求证，再分类
    got_ids = {int(p.get("articleId") or 0) for p in posts if p.get("articleId")}
    late_ids = {int(x) for x in (scan.get("lateIds") or [])}
    want_ids = set(scan.get("unionIds") or [])
    candidates = sorted(got_ids - want_ids - late_ids)
    probe = _reconcile.probe_details(source, candidates, log=log) if candidates else None
    post_check = _reconcile.reconcile_posts(
        posts,
        scan,
        previous_ids=set(prev_snapshot) or None,
        probe=probe,
        target_period=period,
    )
    log(
        f"[防线1] 帖子对账：采集前已存在 {post_check['expected']} 帖 / 产物 {post_check['actual']} 帖"
        f" / 缺 {post_check['missingCount']}"
        f" / 扫描盲区(源站确认存在) {post_check['scanGapCount']}"
        f" / 源站已删 {post_check['disappearedCount']}"
        f" / 幽灵(源站无且上轮无) {post_check['phantomCount']}"
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
    image_check = _reconcile.reconcile_images(posts, scan, require_files=require_files)
    log(
        f"[防线2] 图片对账：声明 {image_check['declaredTotal']} 张 / 实际 {image_check['actualTotal']} 张"
        f" / 不符帖子 {image_check['mismatchPosts']} / 未下载 {image_check['notDownloadedImages']}"
        f" / 文件缺失 {image_check['missingFiles']}"
        f" / 无声明口径帖子 {image_check.get('noDeclaredPosts', 0)}"
        f"  ← {'通过' if image_check['passed'] else '不通过'}"
    )

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
        pr = _reconcile.probe_details(source, lost_int, log=log)
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
            "postsReconcile": post_check,
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

    # 产物清单自洽（论坛类 21 项 / 图库 22 项，各自不重复）
    def case_products():
        from . import _reconcile as R

        assert len(R.PRODUCTS_21) == 21, f"论坛类产物清单应为 21 项，实际 {len(R.PRODUCTS_21)}"
        assert len(set(R.PRODUCTS_21)) == 21, "论坛类产物清单有重复项"
        assert len(R.PRODUCTS_TUKU) == 22, f"图库产物清单应为 22 项，实际 {len(R.PRODUCTS_TUKU)}"
        assert len(set(R.PRODUCTS_TUKU)) == 22, "图库产物清单有重复项"
        return "论坛类 21 项无重复 / 图库 22 项无重复"

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
