#!/usr/bin/env bash
# 超时/失败诊断（v5 任务书 §四）。用法：
#   source scripts/timeout_diag.sh
#   timeout_diag <exit_code> [log_glob]
# 行为：把「最后进度 / 最近 3 条 / 日志末尾 30 行」写进 $GITHUB_STEP_SUMMARY（本地则打印到 stdout）。
timeout_diag() {
  local exit_code="$1"
  local log_glob="${2:-work/run_*.log}"
  local sink="${GITHUB_STEP_SUMMARY:-/dev/stdout}"
  {
    echo "## ⏱ 超时诊断"
    echo ""
    echo "| 项 | 值 |"
    echo "|---|---|"
    if [ "$exit_code" = "124" ]; then
      echo "| 类型 | **软超时（timeout 触发）** |"
    else
      echo "| 退出码 | $exit_code |"
    fi
    echo "| 最后进度 | $(grep -E '\[详情\]|\[OCR\]|\[图片\]' $log_glob 2>/dev/null | tail -1) |"
    echo "| 最近3条 | $(grep -E '\[详情\] 进度|\[OCR\] 识别进度' $log_glob 2>/dev/null | tail -3 | tr '\n' ';') |"
    echo ""
    echo "<details><summary>日志末尾 30 行</summary>"
    echo ""
    echo '```'
    tail -30 $log_glob 2>/dev/null
    echo '```'
    echo "</details>"
  } >> "$sink"
}
