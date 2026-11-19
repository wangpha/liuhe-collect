"""校验三源分片版 workflow（v3）的结构。

对应 2026-11-16 用户要求：
  · 三源都能分片（do_plan 三源期号探测 + do_shard 三分支）
  · 分片数按图片数分配（方案A：dahkg 11 / aocai 4 / tuku 3）
  · shard 与 merge 都拆成三个独立 job（因为 needs 是 job 级）
  · publish 依赖三个 merge
  · 生产 schedule 仍处注释
"""
from __future__ import annotations

import sys
import json
from pathlib import Path

import yaml

WF = Path(r"D:\qode工作区\work\cloud_build\crawl.sharded.yml")
SRCS = ("dahkg", "aocai", "tuku")

d = yaml.safe_load(WF.read_text(encoding="utf-8"))
jobs = d["jobs"]
ok = True


def check(cond, msg):
    global ok
    print(("  ✅ " if cond else "  ❌ ") + msg)
    if not cond:
        ok = False


def step_run(job: str, keyword: str) -> str:
    for s in jobs.get(job, {}).get("steps") or []:
        if keyword in (s.get("name") or ""):
            return s.get("run") or ""
    return ""


print(f"校验目标: {WF}")
print("jobs:", list(jobs))
print("\n逐个 job：")
for name, job in jobs.items():
    matrix = list(((job.get("strategy") or {}).get("matrix") or {}).keys())
    print(f"  {name:14s} steps={len(job.get('steps') or []):2d} needs={job.get('needs')} matrix={matrix}")

print("\n关键校验：")
# watchdog = 2026-11-18 新增的整轮 40 分钟硬上限 job
expected = ({"plan", "cleanup", "prepare"}
            | {f"shard_{s}" for s in SRCS}
            | {f"merge_{s}" for s in SRCS} | {"publish", "watchdog"})
check(set(jobs) == expected, f"job 集合应为 plan/cleanup/prepare + 3 shard + 3 merge + publish + watchdog")

# watchdog 必须真的能兜住：依赖 plan、always()、带 40 分钟上限、会取消本轮
wd = jobs.get("watchdog") or {}
check(bool(wd), "watchdog job 存在")
if wd:
    blob = json.dumps(wd, ensure_ascii=False)
    check(str(wd.get("if", "")).startswith("always()"), "watchdog 用 always() 执行")
    check("MAX_MIN" in blob and "40" in blob, "watchdog 带 MAX_MIN=40")
    check("gh run cancel" in blob, "watchdog 到点会取消本轮")

for src in SRCS:
    sj, mj = f"shard_{src}", f"merge_{src}"
    check(sj in jobs, f"{sj} 存在")
    check("shard" in ((jobs[sj].get("strategy") or {}).get("matrix") or {}), f"{sj} 的 matrix 含 shard 维度")
    run = step_run(sj, "分片预热")
    check("--shard" in run and "--shards" in run, f"{sj} 调用 --shard/--shards")
    check(f'--source "{src}"' in run or f"--source \"{src}\"" in run, f"{sj} 的 --source 是 {src}")
    check(mj in jobs, f"{mj} 存在")
    check(jobs[mj].get("needs") and sj in jobs[mj]["needs"], f"{mj} 只依赖 {sj}（不跨源）")
    check("--merge" in step_run(mj, "完整跑一次"), f"{mj} 调用 --merge")
    cond = jobs[mj].get("if") or ""
    check(f"contains(needs.plan.outputs.sources, '{src}')" in cond,
          f"{mj} 的 if 带源过滤（否则 tuku-only 运行会拉起别的源的 merge）")

check(set(jobs["publish"].get("needs") or []) == {f"merge_{s}" for s in SRCS}, "publish 依赖三个 merge")
check("count_dahkg" in (jobs["plan"].get("outputs") or {}), "plan 输出 count_dahkg")
check("count_aocai" in (jobs["plan"].get("outputs") or {}), "plan 输出 count_aocai")
check("count_tuku" in (jobs["plan"].get("outputs") or {}), "plan 输出 count_tuku")
check("13" in WF.read_text(encoding="utf-8") or "3.13" in WF.read_text(encoding="utf-8"), "Python 版本写的是 3.13")
check("cloud/requirements.txt" in WF.read_text(encoding="utf-8"), "依赖来自 cloud/requirements.txt（不自写版本号）")

pre = WF.read_text(encoding="utf-8").split("jobs:")[0]
active_schedule = "\n  schedule:" in pre
check(not active_schedule, "生产 schedule 仍处于注释状态")

print("\n结论：" + ("结构校验通过 ✅" if ok else "存在结构问题 ❌"))
sys.exit(0 if ok else 1)

