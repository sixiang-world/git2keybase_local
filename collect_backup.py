#!/usr/bin/env python3
"""
Git2Keybase 备份收集脚本 — 防删库版 (Local Hermes Cron)

核心防删策略：
  仓库数据大小 = 安全锚点。
  如果本轮 fetch 后本地缓存大小骤降（< 20% 旧大小），说明上游被删库/清空了。
  此时跳过推送，保留 Keybase 上已有的完整备份不变，并发送微信告警。

备份方式：
  - fetch 用安全 refspec（+refs/heads/*:refs/heads/* +refs/tags/*:refs/tags/*）
    即使上游删了分支/tag，本地缓存也不丢
  - push 用 --all --tags --force（不会删除远程已有的 ref）
  - 每次备份打 archive-YYYYMMDD 时间戳 tag 作为快照锚点

环境变量：
  KEYBASE_USERNAME  — Keybase 用户名
  GITHUB_TOKEN      — GitHub PAT
  REPOS_FILE        — 仓库列表文件路径（默认同目录 repos.txt）
  BACKUP_WORKDIR    — 备份工作目录（默认同目录 repos_cache）
  WXPUSH_API_TOKEN  — wxpush 通知 token（可选，检测到删库时发送微信告警）

输出：JSON 格式的备份摘要（最后一行打印 JSON），与 run_backup.sh 兼容。
"""

import os
import sys
import json
import shlex
import urllib.parse
from datetime import datetime, timezone
from pathlib import Path

import requests

# ── 共享库导入 ────────────────────────────────────────
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "git2keybase" / "scripts"))

from git2keybase_lib import (
    log,
    run_cmd,
    get_repo_size,
    check_size_and_update_db,
    _fmt_size,
    make_repo_key,
    make_safe_name,
    send_wxpush_notification,
    SIZE_DROP_RATIO,
)

# ── 配置 ──────────────────────────────────────────────
SCRIPT_DIR = Path(__file__).parent
SIZE_DB = SCRIPT_DIR / "repo_sizes.json"

REPOS_FILE = Path(os.environ.get("REPOS_FILE", SCRIPT_DIR / "repos.txt"))
WORKDIR = Path(os.environ.get("BACKUP_WORKDIR", SCRIPT_DIR / "repos_cache"))
USERNAME = os.environ.get("KEYBASE_USERNAME", "")
GH_TOKEN = os.environ.get("GITHUB_TOKEN", "")
WXPUSH_API_TOKEN = os.environ.get("WXPUSH_API_TOKEN", "")

WORKDIR.mkdir(parents=True, exist_ok=True)


# ── 认证 URL ──────────────────────────────────────────

def get_auth_git_url(repo_url: str) -> str:
    """
    返回带 token 认证的 git URL。
    对 GitHub 仓库使用 x-access-token 认证。
    """
    parsed = urllib.parse.urlparse(repo_url)
    is_github = parsed.netloc == "github.com"
    if is_github and GH_TOKEN:
        return (
            f"https://x-access-token:{GH_TOKEN}@{parsed.netloc}{parsed.path}"
        )
    return repo_url


# ── 核心备份逻辑 ──────────────────────────────────────

def backup_repo(repo_url: str) -> dict:
    """
    备份单个仓库到 Keybase。

    返回状态字典。
    """
    safe_name = make_safe_name(repo_url)
    repo_key = make_repo_key(repo_url)
    repo_dir = WORKDIR / f"{safe_name}.git"
    auth_url = get_auth_git_url(repo_url)

    result = {
        "url": repo_url,
        "safe_name": safe_name,
        "status": "skipped",
        "action": "none",
        "size": 0,
        "warnings": [],
        "errors": [],
    }

    log(f"\n{'=' * 50}")
    log(f"处理: {repo_url}")

    # ── 1. 确保 Keybase remote 存在 ──
    if USERNAME:
        run_cmd(
            f"keybase git create {shlex.quote(safe_name)} || true",
            timeout=10,
            silent=True,
        )

    is_new = not repo_dir.exists()

    try:
        # ── 2a. 测量旧大小（fetch 前）— 用于 old vs new 比较 ──
        if not is_new:
            old_size = get_repo_size(str(repo_dir))
        else:
            old_size = 0

        # ── 2b. 克隆或增量 fetch ──
        if is_new:
            log("首次克隆...")
            r = run_cmd(
                f"git clone --bare {shlex.quote(auth_url)} {shlex.quote(str(repo_dir))}",
                timeout=600,
                cwd=str(WORKDIR),
            )
            if r.returncode != 0:
                result["status"] = "failed"
                result["errors"].append(f"克隆失败: {r.stderr.strip()[:200]}")
                return result
            action = "cloned"
        else:
            log("增量 fetch（安全 refspec，不丢失本地 ref）...")
            r = run_cmd(
                "git fetch origin "
                "'+refs/heads/*:refs/heads/*' '+refs/tags/*:refs/tags/*' --force --tags",
                timeout=600,
                cwd=str(repo_dir),
            )
            if r.returncode != 0:
                log(
                    f"fetch 失败: {r.stderr.strip()[:200]}，推送当前缓存到 Keybase",
                    "WARN",
                )
                result["warnings"].append(
                    f"fetch 失败，推送当前缓存: {r.stderr.strip()[:100]}"
                )
            action = "fetched"

        # ── 3. 测量新大小（fetch 后） ──
        size = get_repo_size(str(repo_dir))
        result["size"] = size
        log(f"仓库大小: {_fmt_size(size)}")

        # ── 4. 大小为零 → 跳过（不更新 DB） ──
        if size == 0:
            log("仓库大小测量为 0，跳过推送", "WARN")
            result["status"] = "failed"
            result["errors"].append("仓库大小测量为 0，跳过推送")
            return result

        # ── 5. 防删库检查：old_size vs new_size ──
        if old_size > 0 and size < old_size * SIZE_DROP_RATIO:
            drop_pct = (1 - size / old_size) * 100
            msg = (
                f"仓库在本轮 fetch 后大小骤降 {drop_pct:.1f}% "
                f"({_fmt_size(old_size)} → {_fmt_size(size)})，疑似上游删库"
            )
            log(msg, "ERROR")
            send_wxpush_notification(repo_key, old_size, size, WXPUSH_API_TOKEN)
            result["status"] = "preserved"
            result["action"] = "deletion_detected_skipped"
            result["warnings"].append(msg)
            log("✅ Keybase 已有备份保持完好，未覆盖")
            return result

        # ── 6. DB 大小检查 + 20-50% 范围警告 ──
        safe, check_msg, warnings = check_size_and_update_db(
            repo_key, size, str(SIZE_DB), WXPUSH_API_TOKEN
        )
        result["warnings"].extend(warnings)
        if not safe:
            result["status"] = "preserved"
            result["action"] = "deletion_detected_skipped"
            log("✅ Keybase 已有备份保持完好，未覆盖")
            return result

        # ── 7. 打时间戳锚点 tag ──
        ts = datetime.now().strftime("%Y%m%d")
        run_cmd(
            f"git tag archive-{ts} --force",
            timeout=10,
            silent=True,
            cwd=str(repo_dir),
        )

        # ── 8. 推送到 Keybase（安全模式） ──
        if USERNAME:
            log("推送到 Keybase...")
            kb_remote = f"keybase://private/{USERNAME}/{safe_name}"
            run_cmd(
                "git remote remove keybase || true",
                timeout=5,
                silent=True,
                cwd=str(repo_dir),
            )
            run_cmd(
                f"git remote add keybase {shlex.quote(kb_remote)}",
                timeout=5,
                silent=True,
                cwd=str(repo_dir),
            )

            r_branches = run_cmd(
                "git push keybase --all --force",
                timeout=600,
                silent=True,
                cwd=str(repo_dir),
            )
            r_tags = run_cmd(
                "git push keybase --tags --force",
                timeout=600,
                silent=True,
                cwd=str(repo_dir),
            )

            if r_branches.returncode != 0 and r_tags.returncode != 0:
                log("普通推送失败，尝试保底推送 archive tag...", "WARN")
                r_fallback = run_cmd(
                    f"git push keybase refs/tags/archive-{ts}:refs/tags/archive-{ts} --force",
                    timeout=300,
                    silent=True,
                    cwd=str(repo_dir),
                )
                if r_fallback.returncode != 0:
                    result["status"] = "partial"
                    result["errors"].append(
                        f"Keybase 推送失败: {r_fallback.stderr.strip()[:100]}"
                    )
                else:
                    result["status"] = "partial"
                    result["warnings"].append(
                        "分支/Tag 推送失败，archive tag 已推送"
                    )
            else:
                log("✅ Keybase 推送完成")
        else:
            log("⚠️ KEYBASE_USERNAME 未配置，跳过 Keybase 推送")

        result["status"] = "success" if USERNAME else "skipped"
        result["action"] = action
        log(f"✅ 完成: {repo_url}")

    except Exception as e:
        result["status"] = "failed"
        result["errors"].append(str(e))
        log(f"❌ 异常: {e}", "ERROR")

    return result


# ── 主流程 ────────────────────────────────────────────

def main():
    if not REPOS_FILE.exists():
        log(f"❌ 仓库列表文件不存在: {REPOS_FILE}", "ERROR")
        summary = {
            "timestamp": datetime.now().isoformat(),
            "error": f"仓库列表文件不存在: {REPOS_FILE}",
            "total": 0,
            "success": 0,
            "preserved": 0,
            "failed": 0,
            "repos": [],
        }
        print(json.dumps(summary, ensure_ascii=False, indent=2))
        sys.exit(1)

    repos = [
        line.strip()
        for line in REPOS_FILE.read_text().splitlines()
        if line.strip() and not line.strip().startswith("#")
    ]

    if not repos:
        log("⚠️ 仓库列表为空", "WARN")
        summary = {
            "timestamp": datetime.now().isoformat(),
            "error": "仓库列表为空",
            "total": 0,
            "success": 0,
            "preserved": 0,
            "failed": 0,
            "repos": [],
        }
        print(json.dumps(summary, ensure_ascii=False, indent=2))
        sys.exit(0)

    log(f"✅ 加载 {len(repos)} 个仓库")

    summary = {
        "timestamp": datetime.now().isoformat(),
        "keybase_user": USERNAME,
        "total": len(repos),
        "success": 0,
        "preserved": 0,
        "failed": 0,
        "repos": [],
    }

    for r_url in repos:
        result = backup_repo(r_url)
        summary["repos"].append(result)
        if result["status"] == "success":
            summary["success"] += 1
        elif result["status"] == "preserved":
            summary["preserved"] += 1
        elif result["status"] == "failed":
            summary["failed"] += 1

    log(
        f"\n{'=' * 50}"
        f"\n汇总: 成功 {summary['success']}, "
        f"保留 {summary['preserved']}, "
        f"失败 {summary['failed']}, "
        f"共 {summary['total']}"
    )

    # 输出 JSON 到 stdout（与 run_backup.sh 兼容）
    print(json.dumps(summary, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
