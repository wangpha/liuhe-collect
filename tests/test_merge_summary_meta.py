"""C1 merge-summary 的 meta 重建规则单测（用户 2026-11-18 13:2x 裁决的三类规则）。

## 三类规则（不许混用）
【A 覆盖】全量重算：postCount / userCount / imageCount / claims / claimStats /
          generatedAt / generatedAtUnix / periodsIncluded / onlyLatestPeriod
【B 求和】各片累加：detail.{cached,fetched,failed} / images.{downloaded,skipped,failed} /
          ocr.* / replies.*
          ⚠️ images.unique 不求和 → 用合并后 images/ 实际文件数
          ⚠️ scan.* 不求和 → 用 plan 的值（各片都扫了全量，求和会重复 N 倍）
【C 实测】merge 自己：durationSec / rebuildDurationSec /
          stageSeconds={resolve:0,list:0,details:0,media:0,replies_images:0,ocr:0,claims:<实测>}

## 三个反例（本测试专门锁死）
① durationSec 若求和 = 813 秒（3 片各 271 秒），但墙钟只有 300 秒 → 必须是自己耗时
② stageSeconds 若求和，summary 模式会出现 details/ocr > 0 → 但 summary 根本不抓详情/不做 OCR
③ images.unique 若求和，同一张图被多片引用会重复计数
"""
from __future__ import annotations

import sys

import pytest

sys.path.insert(0, r"D:\qode工作区")

from cloud.merge_summary import build_merged_meta, merge_posts  # noqa: E402


# ---------------- 夹具 ----------------
def _meta(post_count, image_count, user_count, duration, scan_scanned,
          detail=(10, 5, 1), images=(100, 2, 0, 999), claims=3):
    """造一个「分片级」run_meta（结构照真实字段）。"""
    cached, fetched, failed = detail
    dl, skipped, failed_img, unique = images
    return {
        "source": "六合论坛（高手论坛）· 最多赞",
        "sourceId": "tuku", "sourceName": "49图库",
        "sourceUrl": "https://49208.com/#/forum",
        "metricLabel": "点赞", "metricField": "likes",
        "generatedAt": "2026-11-18 04:57:51", "generatedAtUnix": 1794977871,
        "lotteryType": 2, "lotteryName": "澳门", "year": 2026, "period": 322,
        "postCount": post_count, "userCount": user_count, "imageCount": image_count,
        "periodsIncluded": [322], "onlyLatestPeriod": True,
        "latestDraw": {"period": 321, "numbers": ["01", "02"]},
        "scan": {"itemsScanned": scan_scanned, "mode": "period"},
        "detail": {"cached": cached, "fetched": fetched, "failed": failed},
        "images": {"downloaded": dl, "skipped": skipped, "failed": failed_img, "unique": unique},
        "ocr": {"total": 50, "explicit": 40, "inherited": 8, "no_target": 2},
        "replies": {"total": 20, "cached": 5, "fetched": 15, "failed": 0,
                    "authorReplies": 3, "authorReplyImages": 1},
        "durationSec": duration,
        "options": {"ocrEnabled": True, "imagesEnabled": True},
        "claimStats": {"claims": claims},
        "claims": claims,
        "stageSeconds": {"resolve": 5, "list": 30, "details": 100, "media": 40,
                         "replies_images": 10, "ocr": 80, "claims": 6},
    }


def _posts(n, start=1):
    return [{"articleId": start + i, "title": f"t{i}", "text": "x",
             "images": [], "replies": []} for i in range(n)]


# ============================================================
# ② merge_posts：按 articleId 去重
# ============================================================
def test_merge_posts_dedupes_by_article_id():
    a = _posts(3, 1)          # 1,2,3
    b = _posts(3, 3)          # 3,4,5  ← 3 重复
    merged, stats = merge_posts([a, b])
    assert [p["articleId"] for p in merged] == [1, 2, 3, 4, 5]
    assert stats["inputTotal"] == 6
    assert stats["duplicates"] == 1
    assert stats["shards"] == 2


# ============================================================
# A 类：覆盖
# ============================================================
def test_a_class_overrides_counts():
    template = _meta(679, 486, 608, 271.6, 5000)
    shards = [template, _meta(600, 400, 500, 280.0, 5000)]
    merged = _posts(1279)
    users = [{"userId": str(i)} for i in range(1100)]
    claims = [{"claimId": f"C{i:05d}"} for i in range(1230)]
    plan = {"period": 322, "counts": {"expectedPosts": 1279}}

    meta = build_merged_meta(
        template=template, shard_metas=shards, merged_posts=merged, users=users,
        claims=claims, claim_stats={"claims": 1230}, plan=plan,
        images_unique=1234, timings={"durationSec": 42.5, "rebuildDurationSec": 8.0,
                                     "claimsSec": 3.2},
    )
    assert meta["postCount"] == 1279, "postCount 必须是合并后帖数"
    assert meta["userCount"] == 1100, "userCount 必须是 build_users 长度"
    assert meta["imageCount"] == 1234, "imageCount 必须是 images/ 实际文件数"
    assert meta["claims"] == 1230
    assert meta["claimStats"]["claims"] == 1230
    assert meta["periodsIncluded"] == [322]
    assert meta["onlyLatestPeriod"] is True


def test_a_class_generated_at_is_current_not_template():
    template = _meta(10, 10, 10, 100, 1000)
    meta = build_merged_meta(
        template=template, shard_metas=[template], merged_posts=_posts(10),
        users=[], claims=[], claim_stats={"claims": 0}, plan={"period": 322},
        images_unique=5, timings={"durationSec": 1, "rebuildDurationSec": 1, "claimsSec": 0},
    )
    assert meta["generatedAt"] != template["generatedAt"], "generatedAt 必须用本轮时间"
    assert meta["generatedAtUnix"] > template["generatedAtUnix"]


# ============================================================
# B 类：求和
# ============================================================
def test_b_class_sums_detail_images_ocr_replies():
    m1 = _meta(100, 100, 50, 100, 5000, detail=(10, 5, 1), images=(100, 2, 0, 999))
    m2 = _meta(200, 200, 80, 200, 5000, detail=(20, 7, 2), images=(150, 3, 1, 999))
    merged = _posts(300)
    meta = build_merged_meta(
        template=m1, shard_metas=[m1, m2], merged_posts=merged, users=[], claims=[],
        claim_stats={"claims": 0}, plan={"period": 322}, images_unique=777,
        timings={"durationSec": 10, "rebuildDurationSec": 5, "claimsSec": 1},
    )
    assert meta["detail"]["cached"] == 30, "detail.cached 应求和 10+20"
    assert meta["detail"]["fetched"] == 12, "detail.fetched 应求和 5+7"
    assert meta["detail"]["failed"] == 3, "detail.failed 应求和 1+2"
    assert meta["images"]["downloaded"] == 250, "images.downloaded 应求和 100+150"
    assert meta["images"]["skipped"] == 5
    assert meta["images"]["failed"] == 1
    assert meta["ocr"]["total"] == 100, "ocr.total 应求和 50+50"
    assert meta["replies"]["total"] == 40, "replies.total 应求和 20+20"
    assert meta["replies"]["authorReplies"] == 6


def test_b_class_images_unique_is_measured_not_summed():
    """反例③：unique 若求和 = 999+999 = 1998，但实测去重后是 777。"""
    m1 = _meta(100, 100, 50, 100, 5000, images=(100, 0, 0, 999))
    m2 = _meta(100, 100, 50, 100, 5000, images=(100, 0, 0, 999))
    meta = build_merged_meta(
        template=m1, shard_metas=[m1, m2], merged_posts=_posts(200), users=[], claims=[],
        claim_stats={"claims": 0}, plan={"period": 322}, images_unique=777,
        timings={"durationSec": 1, "rebuildDurationSec": 1, "claimsSec": 0},
    )
    assert meta["images"]["unique"] == 777, "unique 必须是实测去重数，不能求和"
    assert meta["images"]["unique"] != 1998


def test_b_class_scan_comes_from_plan_not_sum():
    """反例④：各片 scan 都是全量（5000），求和会变 10000。必须用 plan 的值。"""
    m1 = _meta(100, 100, 50, 100, 5000)
    m2 = _meta(100, 100, 50, 100, 5000)
    plan = {"period": 322, "scan": {"itemsScanned": 5000, "mode": "period"}}
    meta = build_merged_meta(
        template=m1, shard_metas=[m1, m2], merged_posts=_posts(200), users=[], claims=[],
        claim_stats={"claims": 0}, plan=plan, images_unique=10,
        timings={"durationSec": 1, "rebuildDurationSec": 1, "claimsSec": 0},
    )
    assert meta["scan"]["itemsScanned"] == 5000, "scan 必须来自 plan，不是求和"
    assert meta["scan"]["itemsScanned"] != 10000


# ============================================================
# C 类：实测（三个反例中最关键的两个）
# ============================================================
def test_c_class_duration_is_measured_not_summed():
    """反例①：3 片各 271 秒求和 = 813，但 merge 实测 42.5 秒。"""
    m = _meta(100, 100, 50, 271.6, 5000)
    meta = build_merged_meta(
        template=m, shard_metas=[m, m, m], merged_posts=_posts(100), users=[], claims=[],
        claim_stats={"claims": 0}, plan={"period": 322}, images_unique=10,
        timings={"durationSec": 42.5, "rebuildDurationSec": 8.0, "claimsSec": 3.2},
    )
    assert meta["durationSec"] == 42.5, "durationSec 必须是 merge 自己耗时"
    assert meta["durationSec"] != pytest.approx(814.8, abs=1)
    assert meta["rebuildDurationSec"] == 8.0


def test_c_class_stage_seconds_zero_except_claims():
    """反例②：summary 模式不抓详情/不做 OCR，这些阶段必须是 0。"""
    m = _meta(100, 100, 50, 100, 5000)
    meta = build_merged_meta(
        template=m, shard_metas=[m, m], merged_posts=_posts(100), users=[], claims=[],
        claim_stats={"claims": 0}, plan={"period": 322}, images_unique=10,
        timings={"durationSec": 10, "rebuildDurationSec": 5, "claimsSec": 3.2},
    )
    st = meta["stageSeconds"]
    assert st["details"] == 0, "summary 模式不抓详情，details 必须 0"
    assert st["ocr"] == 0, "summary 模式不做 OCR，ocr 必须 0"
    assert st["list"] == 0, "summary 模式不扫列表，list 必须 0"
    assert st["media"] == 0
    assert st["replies_images"] == 0
    assert st["resolve"] == 0
    assert st["claims"] == 3.2, "claims 阶段必须是实测耗时 > 0"


# ============================================================
# 端到端：postCount 断言（plan 预期不符要报错）
# ============================================================
def test_post_count_mismatch_raises():
    m = _meta(100, 100, 50, 100, 5000)
    with pytest.raises(SystemExit) as ei:
        build_merged_meta(
            template=m, shard_metas=[m], merged_posts=_posts(50), users=[], claims=[],
            claim_stats={"claims": 0},
            plan={"period": 322, "counts": {"expectedPosts": 100}},
            images_unique=10,
            timings={"durationSec": 1, "rebuildDurationSec": 1, "claimsSec": 0},
        )
    assert "预期" in str(ei.value) or "expected" in str(ei.value).lower()
