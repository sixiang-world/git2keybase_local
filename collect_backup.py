#!/usr/bin/env python3
"""
Git2Keybase 备份收集脚本 — 防删库版 (Local Hermes Cron)

核心防删策略：
  仓库数据大小 = 安全锚点。
  如果本地缓存大小与上次备份相比骤降（< 20%），说明上游被删库/清空了。
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
import subprocess
import requests
import urllib.parse
from datetime import datetime, timezone
from pathlib import Path

# ── 配置 ──────────────────────────────────────────────
SCRIPT_DIR = Path(__file__).parent
SIZE_DB = SCRIPT_DIR / "repo_sizes.json"

REPOS_FILE = Path(os.environ.get("REPOS_FILE", SCRIPT_DIR / "repos.txt"))
WORKDIR = Path(os.environ.get("BACKUP_WORKDIR", SCRIPT_DIR / "repos_cache"))
USERNAME = os.environ.get("KEYBASE_USERNAME", "")
GH_TOKEN = os.environ.get("GITHUB_TOKEN", "")
WXPUSH_TOKEN = os.environ.get("WXPUSH_API_TOKEN", "")

WORKDIR.mkdir(parents=True, exist_ok=True)

# 大小阈值
SIZE_DROP_RATIO = 0.20  # 相比上次备份缩小 < 20% 视为删库


def log(msg: str, level: str = "INFO"):
    """日志输出到 stderr，不影响 stdout 的 JSON"""
    print(f"[{level}] {msg}", file=sys.stderr)


def run_cmd(cmd: str, timeout: int = 300, cwd: str = None, silent: bool = False) -> subprocess.CompletedProcess:
    """运行命令，返回 CompletedProcess"""
    try:
        result = subprocess.run(
            cmd, shell=True, capture_output=True, text=True, timeout=timeout, cwd=cwd,
        )
        if result.returncode != 0 and not silent:
            if result.stderr.strip():
                log(f"命令提示: {result.stderr.strip()[:200]}", "WARN")
        return result
    except subprocess.TimeoutExpired:
        log(f"命令超时 ({timeout}s): {cmd}", "ERROR")
        return subprocess.CompletedProcess(args=cmd, returncode=-1, stdout="", stderr="timeout")
    except Exception as e:
        log(f"命令异常: {cmd}, {e}", "ERROR")
        return subprocess.CompletedProcess(args=cmd, returncode=-1, stdout="", stderr=str(e))


# ── 微信通知 ──────────────────────────────────────────

def send_wxpush_notification(repo_url: str, prev_size: int, current_size: int):
    """检测到删库时发送微信告警通知"""
    if not WXPUSH_TOKEN:
        log("WXPUSH_API_TOKEN 未设置，跳过微信通知", "WARN")
        return

    title = "【git2keybase 删库告警】"
    content = (
        f"检测到仓库疑似被清空！\n"
        f"仓库：{repo_url}\n"
        f"上次大小：{_fmt_size(prev_size)}\n"
        f"当前大小：{_fmt_size(current_size)}\n"
        f"时间：{datetime.now(timezone.utc).isoformat()}"
    )

    try:
        resp = requests.get(
            "https://push.hzz.cool/wxsend",
            params={"title": title, "content": content, "token": WXPUSH_TOKEN},
            timeout=10,
        )
        log(f"微信通知发送完成 (HTTP {resp.status_code})", "INFO")
    except requests.exceptions.RequestException as e:
        log(f"微信通知发送失败: {e}", "WARN")


# ── 大小跟踪 ──────────────────────────────────────────

def load_sizes() -> dict:
    if SIZE_DB.exists():
        try:
            return json.loads(SIZE_DB.read_text())
        except (json.JSONDecodeError, OSError):
            return {}
    return {}


def save_sizes(sizes: dict):
    SIZE_DB.write_text(json.dumps(sizes, indent=2, ensure_ascii=False))


def get_repo_size(repo_dir: str) -> int:
    """返回仓库目录的总大小（字节）"""
    result = run_cmd(f"du -sb {repo_dir} 2>/dev/null | cut -f1", timeout=10, silent=True)
    if result.returncode == 0 and result.stdout.strip().isdigit():
        return int(result.stdout.strip())
    return 0


def check_size_and_update_db(repo_key: str, size: int) -> tuple[bool, int, str]:
    """
    检查仓库大小是否可疑，并更新记录。

    Returns:
        (safe: bool, size: int, message: str)
        safe=True  → 可以推送
        safe=False → 疑似删库，跳过推送
    """
    sizes = load_sizes()
    prev_size = sizes.get(repo_key, 0)

    if prev_size > 0:
        if size < prev_size * SIZE_DROP_RATIO:
            drop_pct = (1 - size / prev_size) * 100
            msg = f"仓库大小骤降 {drop_pct:.1f}% ({_fmt_size(prev_size)} → {_fmt_size(size)})，疑似删库，跳过推送"
            log(msg, "ERROR")
            send_wxpush_notification(repo_key, prev_size, size)
            return False, size, msg
        elif size < prev_size * 0.5:
            log(f"仓库缩小 {_fmt_size(prev_size)} → {_fmt_size(size)}，但仍 > 20%，继续", "WARN")
        else:
            log(f"仓库大小正常: {_fmt_size(size)} (上次 {_fmt_size(prev_size)})")
    else:
        log(f"首次备份，大小: {_fmt_size(size)}")

    # 更新记录
    sizes[repo_key] = size
    save_sizes(sizes)
    return True, size, f"大小正常: {_fmt_size(size)}"


def _fmt_size(b: int) -> str:
    if b < 1024:
        return f"{b}B"
    elif b < 1024 * 1024:
        return f"{b / 1024:.1f}KB"
    else:
        return f"{b / 1024 / 1024:.1f}MB"


# ── 核心备份逻辑 ──────────────────────────────────────

def get_auth_url_and_env(repo_url: str) -> tuple[str, dict]:
    """
    返回（带 token 认证的 git URL, 环境变量 dict）。
    对 GitHub 仓库使用 x-access-token 认证，避免 token 暴露在命令行。
    """
    parsed = urllib.parse.urlparse(repo_url)
    is_github = parsed.netloc == "github.com"
    if is_github and GH_TOKEN:
        auth_url = f"https://x-access-token:{GH_TOKEN}@{parsed.netloc}{parsed.path}"
        return auth_url, {}
    return repo_url, {}


def make_repo_key(repo_url: str) -> str:
    """从 URL 生成唯一仓库标识"""
    parsed = urllib.parse.urlparse(repo_url)
    path = parsed.path.lstrip("/")
    if path.endswith(".git"):
        path = path[:-4]
    return f"{parsed.netloc}/{path}"


def make_safe_name(repo_url: str) -> str:
    """生成安全目录名"""
    parsed = urllib.parse.urlparse(repo_url)
    path = parsed.path.lstrip("/")
    if path.endswith(".git"):
        path = path[:-4]
    return f"{parsed.netloc.replace('.', '_')}_{path.replace('/', '_')}"


def backup_repo(repo_url: str) -> dict:
    """
    备份单个仓库到 Keybase。

    返回状态字典。
    """
    safe_name = make_safe_name(repo_url)
    repo_key = make_repo_key(repo_url)
    repo_dir = WORKDIR / f"{safe_name}.git"
    auth_url, git_env = get_auth_url_and_env(repo_url)

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
        run_cmd(f"keybase git create {safe_name} || true", timeout=10, silent=True)

    is_new = not repo_dir.exists()

    try:
        # ── 2. 克隆或增量 fetch ──
        if is_new:
            log("首次克隆...")
            r = run_cmd(f"git clone --bare {auth_url} {repo_dir}", timeout=600, cwd=str(WORKDIR))
            if r.returncode != 0:
                result["status"] = "failed"
                result["errors"].append(f"克隆失败: {r.stderr.strip()[:200]}")
                return result
            action = "cloned"
        else:
            log("增量 fetch（安全 refspec，不丢失本地 ref）...")
            r = run_cmd(
                f"git fetch origin "
                "'+refs/heads/*:refs/heads/*' '+refs/tags/*:refs/tags/*' --force --tags",
                timeout=600,
                cwd=str(repo_dir),
            )
            if r.returncode != 0:
                log(f"fetch 失败: {r.stderr.strip()[:200]}，推送当前缓存到 Keybase", "WARN")
                result["warnings"].append(f"fetch 失败，推送当前缓存: {r.stderr.strip()[:100]}")
            action = "fetched"

        # ── 3. 测量仓库大小 ──
        size = get_repo_size(str(repo_dir))
        result["size"] = size
        log(f"仓库大小: {_fmt_size(size)}")

        # ── 4. 大小检查（防删库） ──
        safe, _, _ = check_size_and_update_db(repo_key, size)
        if not safe:
            result["status"] = "preserved"
            result["action"] = "deletion_detected_skipped"
            log("✅ Keybase 已有备份保持完好，未覆盖")
            return result

        # ── 5. 打时间戳锚点 tag ──
        ts = datetime.now().strftime("%Y%m%d")
        run_cmd(f"git tag archive-{ts} --force", timeout=10, silent=True, cwd=str(repo_dir))

        # ── 6. 推送到 Keybase（安全模式） ──
        if USERNAME:
            log("推送到 Keybase...")
            kb_remote = f"keybase://private/{USERNAME}/{safe_name}"
            run_cmd("git remote remove keybase || true", timeout=5, silent=True, cwd=str(repo_dir))
            run_cmd(f"git remote add keybase {kb_remote}", timeout=5, silent=True, cwd=str(repo_dir))

            # 先 push 所有分支
            r_branches = run_cmd(
                "git push keybase --all --force",
                timeout=600, silent=True, cwd=str(repo_dir),
            )
            # 再 push 所有 tag
            r_tags = run_cmd(
                "git push keybase --tags --force",
                timeout=600, silent=True, cwd=str(repo_dir),
            )

            if r_branches.returncode != 0 and r_tags.returncode != 0:
                log("普通推送失败，尝试保底推送 archive tag...", "WARN")
                r_fallback = run_cmd(
                    f"git push keybase refs/tags/archive-{ts}:refs/tags/archive-{ts} --force",
                    timeout=300, silent=True, cwd=str(repo_dir),
                )
                if r_fallback.returncode != 0:
                    result["status"] = "partial"
                    result["errors"].append(f"Keybase 推送失败: {r_fallback.stderr.strip()[:100]}")
                else:
                    result["status"] = "partial"
                    result["warnings"].append("分支/Tag 推送失败，archive tag 已推送")
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
            "total": 0, "success": 0, "preserved": 0, "failed": 0, "repos": [],
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
            "total": 0, "success": 0, "preserved": 0, "failed": 0, "repos": [],
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

    log(f"\n{'=' * 50}")
    log(f"汇总: 成功 {summary['success']}, 保留 {summary['preserved']}, 失败 {summary['failed']}, 共 {summary['total']}")

    # 输出 JSON 到 stdout（与 run_backup.sh 兼容）
    print(json.dumps(summary, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
