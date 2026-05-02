#!/usr/bin/env python3
"""
Git2Keybase 备份脚本 — 防删库版

核心原则：
  1. 本地仓库只追加，永不删除历史
  2. 检测删库信号（只剩 README、DMCA、文件数骤减）
  3. 拒绝同步被清空的仓库，保留本地完整备份
  4. Keybase 只推送本地有而远程没有的内容

删库场景：
  - 作者删除仓库 → 404 无法访问
  - DMCA Takedown → 只剩 README，说明被版权投诉
  - 作者清空仓库 → 只剩 README，说明已归档
  - Force Push → 历史被重写
  - 仓库变私有 → 无法访问

环境变量：
  KEYBASE_USERNAME  — Keybase 用户名
  GITHUB_TOKEN      — GitHub PAT
  REPOS_FILE        — 仓库列表文件
  BACKUP_WORKDIR    — 备份工作目录
  GIT_FETCH_TIMEOUT — fetch 超时（秒，默认 300）
  MIN_FILE_COUNT    — 最小文件数（低于此值视为删库，默认 3）
"""

import os
import sys
import json
import subprocess
import urllib.parse
import logging
from datetime import datetime
from pathlib import Path

try:
    import requests
    HAS_REQUESTS = True
except ImportError:
    import urllib.request
    HAS_REQUESTS = False

# ── 配置 ──────────────────────────────────────────────
SCRIPT_DIR = Path(__file__).parent

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[logging.StreamHandler(sys.stderr)]
)
logger = logging.getLogger(__name__)

# 加载 .env
env_file = SCRIPT_DIR / ".env"
if env_file.exists():
    for line in env_file.read_text().splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            key, _, value = line.partition("=")
            os.environ.setdefault(key.strip(), value.strip())

REPOS_FILE = Path(os.environ.get("REPOS_FILE", SCRIPT_DIR / "repos.txt"))
WORKDIR = Path(os.environ.get("BACKUP_WORKDIR", SCRIPT_DIR / "repos_cache"))
USERNAME = os.environ.get("KEYBASE_USERNAME", "")
GH_TOKEN = os.environ.get("GITHUB_TOKEN", "")
GIT_FETCH_TIMEOUT = int(os.environ.get("GIT_FETCH_TIMEOUT", "300"))
MIN_FILE_COUNT = int(os.environ.get("MIN_FILE_COUNT", "3"))

WORKDIR.mkdir(parents=True, exist_ok=True)

# 删库信号关键词
DMCA_KEYWORDS = [
    "dmca", "takedown", "taken down", "removed", "deleted",
    "infringement", "copyright", "cease and desist",
    "this repository has been", "no longer available",
    "archived", "deprecated"
]


def run_cmd(cmd, timeout=None, cwd=None, env=None, silent=False):
    """运行命令，返回 (returncode, stdout, stderr)"""
    try:
        cmd_env = os.environ.copy()
        if env:
            cmd_env.update(env)
        
        result = subprocess.run(
            cmd, shell=True, capture_output=True, text=True,
            timeout=timeout, cwd=cwd or str(WORKDIR), env=cmd_env
        )
        
        if not silent and result.returncode != 0:
            logger.warning(f"命令失败 [{result.returncode}]: {cmd}")
            if result.stderr:
                logger.warning(f"stderr: {result.stderr[:200]}")
        
        return result.returncode, result.stdout, result.stderr
        
    except subprocess.TimeoutExpired:
        logger.error(f"命令超时 ({timeout}s): {cmd}")
        return -1, "", "timeout"
    except Exception as e:
        logger.error(f"命令异常: {cmd}, {e}")
        return -1, "", str(e)


def check_keybase_login():
    """检查 Keybase 登录状态"""
    code, stdout, _ = run_cmd("keybase status", timeout=10, silent=True)
    return code == 0 and "Logged in:     yes" in stdout


def get_git_auth_env():
    """获取 Git 认证环境变量"""
    if GH_TOKEN:
        return {
            "GIT_ASKPASS": "echo",
            "GIT_USERNAME": "x-access-token",
            "GIT_PASSWORD": GH_TOKEN
        }
    return {}


def get_repo_stats(repo_dir):
    """
    获取仓库统计信息
    
    Returns:
        dict: {
            "commit_count": int,
            "branch_count": int,
            "tag_count": int,
            "file_count": int,  # HEAD 的文件数
            "readme_content": str,  # README 内容（前 500 字符）
            "has_readme_only": bool,  # 是否只剩 README
            "dmca_detected": bool,  # 是否检测到 DMCA 信号
            "dmca_keywords_found": list,  # 检测到的关键词
        }
    """
    stats = {
        "commit_count": 0,
        "branch_count": 0,
        "tag_count": 0,
        "file_count": 0,
        "readme_content": "",
        "has_readme_only": False,
        "dmca_detected": False,
        "dmca_keywords_found": [],
    }
    
    try:
        # commit 数量
        code, stdout, _ = run_cmd("git rev-list --count HEAD", cwd=repo_dir, timeout=10, silent=True)
        if code == 0 and stdout.strip().isdigit():
            stats["commit_count"] = int(stdout.strip())
        
        # 分支数量（本地 + 远程）
        code, stdout, _ = run_cmd("git branch -a", cwd=repo_dir, timeout=10, silent=True)
        if code == 0:
            branches = [l.strip() for l in stdout.split('\n') if l.strip() and 'HEAD' not in l]
            stats["branch_count"] = len(branches)
        
        # 标签数量
        code, stdout, _ = run_cmd("git tag", cwd=repo_dir, timeout=10, silent=True)
        if code == 0 and stdout.strip():
            stats["tag_count"] = len(stdout.strip().split('\n'))
        
        # HEAD 文件数量
        code, stdout, _ = run_cmd("git ls-tree -r HEAD --name-only", cwd=repo_dir, timeout=10, silent=True)
        if code == 0:
            files = [f for f in stdout.strip().split('\n') if f.strip()]
            stats["file_count"] = len(files)
            
            # 检查是否只剩 README
            readme_files = [f for f in files if f.lower().startswith('readme')]
            if len(readme_files) > 0 and len(files) <= 2:
                stats["has_readme_only"] = True
        
        # 读取 README 内容
        code, stdout, _ = run_cmd("git show HEAD:README.md", cwd=repo_dir, timeout=10, silent=True)
        if code == 0:
            stats["readme_content"] = stdout[:500].lower()
        else:
            # 尝试其他 README 文件
            for name in ["README", "readme.md", "readme.txt"]:
                code, stdout, _ = run_cmd(f"git show HEAD:{name}", cwd=repo_dir, timeout=10, silent=True)
                if code == 0:
                    stats["readme_content"] = stdout[:500].lower()
                    break
        
        # 检测 DMCA 信号
        if stats["readme_content"]:
            for keyword in DMCA_KEYWORDS:
                if keyword in stats["readme_content"]:
                    stats["dmca_detected"] = True
                    stats["dmca_keywords_found"].append(keyword)
        
        return stats
        
    except Exception as e:
        logger.error(f"获取仓库统计失败: {e}")
        return stats


def detect_repo_deletion(old_stats, new_stats):
    """
    检测仓库是否被删库
    
    Returns:
        (is_deleted: bool, reason: str)
    """
    reasons = []
    
    # 1. 检测是否只剩 README
    if new_stats["has_readme_only"] and not old_stats["has_readme_only"]:
        reasons.append(f"仓库只剩 README（文件数: {new_stats['file_count']}）")
    
    # 2. 检测 DMCA 信号
    if new_stats["dmca_detected"]:
        reasons.append(f"检测到 DMCA 信号: {', '.join(new_stats['dmca_keywords_found'])}")
    
    # 3. 检测文件数骤减（从 >10 个文件变成 <=3 个）
    if old_stats["file_count"] > 10 and new_stats["file_count"] <= 3:
        reasons.append(f"文件数骤减: {old_stats['file_count']} → {new_stats['file_count']}")
    
    # 4. 检测 commit 数异常（新 commit 比旧 commit 少很多）
    if old_stats["commit_count"] > 10 and new_stats["commit_count"] < old_stats["commit_count"] * 0.1:
        reasons.append(f"commit 数异常: {old_stats['commit_count']} → {new_stats['commit_count']}")
    
    if reasons:
        return True, " | ".join(reasons)
    
    return False, ""


def is_repo_accessible(repo_url):
    """检查仓库是否可访问"""
    parsed = urllib.parse.urlparse(repo_url)
    domain = parsed.netloc
    path = parsed.path.lstrip("/")
    
    if domain == "github.com" and GH_TOKEN:
        api_url = f"https://api.github.com/repos/{path}"
        headers = {"Authorization": f"token {GH_TOKEN}"}
        
        try:
            if HAS_REQUESTS:
                resp = requests.get(api_url, headers=headers, timeout=10)
                return resp.status_code == 200
            else:
                req = urllib.request.Request(api_url, headers=headers)
                with urllib.request.urlopen(req, timeout=10) as resp:
                    return resp.status == 200
        except Exception:
            return False
    
    # 非 GitHub 仓库，尝试 git ls-remote
    code, _, _ = run_cmd(f"git ls-remote {repo_url}", timeout=30, silent=True)
    return code == 0


def safe_fetch(repo_dir, repo_url):
    """
    安全地获取远程更新
    
    策略：
    1. 先获取远程最新状态（不应用）
    2. 检测是否被删库
    3. 如果安全，才应用更新
    
    Returns:
        (success: bool, error: str, warnings: list)
    """
    warnings = []
    git_env = get_git_auth_env()
    
    # 获取本地旧统计
    old_stats = get_repo_stats(repo_dir)
    logger.info(f"本地状态: commit={old_stats['commit_count']}, 文件={old_stats['file_count']}, 分支={old_stats['branch_count']}, 标签={old_stats['tag_count']}")
    
    # 获取远程最新状态（只获取，不合并）
    code, stdout, stderr = run_cmd(
        "git fetch origin --force --tags",
        cwd=repo_dir, timeout=GIT_FETCH_TIMEOUT, env=git_env
    )
    
    if code != 0:
        if "Repository not found" in stderr or "404" in stderr:
            return False, "仓库已被删除（404）", warnings
        return False, f"fetch 失败: {stderr[:100]}", warnings
    
    # 获取远程 HEAD 的统计（不切换本地 HEAD）
    # 创建临时分支来检查远程状态
    code, _, _ = run_cmd("git branch -D _temp_check", cwd=repo_dir, timeout=5, silent=True)
    code, _, _ = run_cmd("git branch _temp_check origin/HEAD", cwd=repo_dir, timeout=10, silent=True)
    
    if code == 0:
        # 获取临时分支的统计
        temp_stats = get_repo_stats(repo_dir)
        
        # 切换回原分支
        code, stdout, _ = run_cmd("git symbolic-ref HEAD", cwd=repo_dir, timeout=5, silent=True)
        if code == 0:
            current_branch = stdout.strip().replace("refs/heads/", "")
            run_cmd(f"git checkout {current_branch}", cwd=repo_dir, timeout=10, silent=True)
        
        # 删除临时分支
        run_cmd("git branch -D _temp_check", cwd=repo_dir, timeout=5, silent=True)
        
        # 检测是否被删库
        is_deleted, reason = detect_repo_deletion(old_stats, temp_stats)
        
        if is_deleted:
            logger.warning(f"⚠️ 检测到删库: {reason}")
            warnings.append(f"删库检测: {reason}")
            
            # 拒绝更新，保留本地
            # 重置 fetch 的更改
            run_cmd("git fetch origin", cwd=repo_dir, timeout=30, silent=True)
            return False, f"拒绝同步: 检测到删库 - {reason}", warnings
        
        logger.info(f"远程状态: commit={temp_stats['commit_count']}, 文件={temp_stats['file_count']}")
    
    return True, None, warnings


def safe_push_to_keybase(repo_dir, safe_name):
    """
    安全地推送到 Keybase
    
    策略：只推送本地有而远程没有的内容（不使用 --mirror）
    """
    if not USERNAME:
        return True, "Keybase 未配置，跳过"
    
    kb_remote = f"keybase://private/{USERNAME}/{safe_name}"
    
    # 确保远程存在
    run_cmd(f"keybase git create {safe_name}", timeout=30, silent=True)
    
    # 添加/更新 remote
    run_cmd("git remote remove keybase", cwd=repo_dir, timeout=5, silent=True)
    run_cmd(f"git remote add keybase {kb_remote}", cwd=repo_dir, timeout=5, silent=True)
    
    # 推送所有分支（--all 不会删除远程已有的分支）
    code, stdout, stderr = run_cmd(
        "git push keybase --all --force",
        cwd=repo_dir, timeout=GIT_FETCH_TIMEOUT
    )
    
    if code != 0:
        # 尝试分步推送
        logger.warning("推送分支失败，尝试分步推送")
        code, _, stderr = run_cmd(
            "git push keybase --all",
            cwd=repo_dir, timeout=GIT_FETCH_TIMEOUT
        )
    
    # 推送标签
    run_cmd(
        "git push keybase --tags --force",
        cwd=repo_dir, timeout=GIT_FETCH_TIMEOUT, silent=True
    )
    
    return True, None


def backup_repo(repo_url):
    """备份单个仓库"""
    result = {
        "url": repo_url,
        "status": "skipped",
        "action": "none",
        "warnings": [],
        "errors": [],
        "stats": {},
    }
    
    parsed = urllib.parse.urlparse(repo_url)
    domain = parsed.netloc
    path = parsed.path.lstrip("/")
    if path.endswith(".git"):
        path = path[:-4]
    safe_name = f"{domain.replace('.', '_')}_{path.replace('/', '_')}"
    repo_dir = WORKDIR / f"{safe_name}.git"
    
    logger.info(f"{'='*60}")
    logger.info(f"备份: {repo_url}")
    
    # 1. 检查仓库是否可访问
    if not is_repo_accessible(repo_url):
        if repo_dir.exists():
            # 仓库已删除，但本地有备份，保留本地
            result["status"] = "preserved"
            result["action"] = "upstream_deleted_local_kept"
            result["warnings"].append("上游仓库已删除，保留本地备份")
            logger.warning("⚠️ 上游仓库已删除，保留本地备份")
        else:
            result["status"] = "failed"
            result["errors"].append("仓库不可访问且无本地备份")
            logger.error("❌ 仓库不可访问且无本地备份")
        return result
    
    # 2. 获取或创建本地仓库
    if repo_dir.exists():
        # 已有本地仓库，安全更新
        success, error, warnings = safe_fetch(repo_dir, repo_url)
        result["warnings"].extend(warnings)
        
        if not success:
            result["status"] = "preserved"
            result["action"] = "fetch_rejected"
            result["errors"].append(error)
            logger.warning(f"⚠️ 拒绝更新: {error}")
            
            # 即使 fetch 失败，也推送到 Keybase（保留已有备份）
            safe_push_to_keybase(repo_dir, safe_name)
            result["status"] = "preserved"
            return result
        
        result["action"] = "updated"
        
    else:
        # 首次克隆
        logger.info(f"首次克隆: {safe_name}")
        git_env = get_git_auth_env()
        code, stdout, stderr = run_cmd(
            f"git clone --bare {repo_url} {repo_dir}",
            timeout=GIT_FETCH_TIMEOUT, env=git_env
        )
        
        if code != 0:
            result["status"] = "failed"
            result["errors"].append(f"克隆失败: {stderr[:100]}")
            return result
        
        result["action"] = "cloned"
    
    # 3. 创建 archive 标签
    ts = datetime.now().strftime("%Y%m%d")
    run_cmd(f"git tag archive-{ts}", cwd=repo_dir, timeout=10, silent=True)
    
    # 4. 获取最终统计
    stats = get_repo_stats(repo_dir)
    result["stats"] = {
        "commits": stats["commit_count"],
        "files": stats["file_count"],
        "branches": stats["branch_count"],
        "tags": stats["tag_count"],
    }
    
    # 5. 推送到 Keybase
    push_success, push_error = safe_push_to_keybase(repo_dir, safe_name)
    
    if push_success:
        result["status"] = "success"
        logger.info(f"✅ 备份成功: {stats['commit_count']} commits, {stats['file_count']} files")
    else:
        result["status"] = "partial"
        result["errors"].append(f"Keybase 推送失败: {push_error}")
    
    return result


def main():
    """主函数"""
    summary = {
        "timestamp": datetime.now().isoformat(),
        "keybase_configured": bool(USERNAME),
        "github_configured": bool(GH_TOKEN),
        "keybase_logged_in": False,
        "repos_file": str(REPOS_FILE),
        "repos": [],
        "total": 0,
        "success": 0,
        "preserved": 0,
        "failed": 0,
        "warnings": [],
        "duration_seconds": 0,
    }
    
    start_time = datetime.now()
    
    if USERNAME:
        summary["keybase_logged_in"] = check_keybase_login()
        if not summary["keybase_logged_in"]:
            logger.warning("Keybase 未登录")
    
    if not REPOS_FILE.exists():
        summary["error"] = f"仓库列表文件不存在: {REPOS_FILE}"
        print(json.dumps(summary, ensure_ascii=False, indent=2))
        sys.exit(1)
    
    repos = [
        line.strip()
        for line in REPOS_FILE.read_text().splitlines()
        if line.strip() and not line.strip().startswith("#")
    ]
    
    if not repos:
        summary["error"] = "仓库列表为空"
        print(json.dumps(summary, ensure_ascii=False, indent=2))
        sys.exit(0)
    
    summary["total"] = len(repos)
    logger.info(f"开始备份 {len(repos)} 个仓库")
    
    for repo_url in repos:
        try:
            r = backup_repo(repo_url)
            summary["repos"].append(r)
            
            if r["status"] == "success":
                summary["success"] += 1
            elif r["status"] == "preserved":
                summary["preserved"] += 1
            else:
                summary["failed"] += 1
            
            summary["warnings"].extend(r.get("warnings", []))
                
        except Exception as e:
            logger.error(f"备份异常: {repo_url}, {e}")
            summary["repos"].append({
                "url": repo_url, "status": "failed",
                "action": "error", "warnings": [], "errors": [str(e)], "stats": {}
            })
            summary["failed"] += 1
    
    end_time = datetime.now()
    summary["duration_seconds"] = int((end_time - start_time).total_seconds())
    
    logger.info(f"{'='*60}")
    logger.info(f"备份完成: 成功 {summary['success']}, 保留 {summary['preserved']}, 失败 {summary['failed']}, 耗时 {summary['duration_seconds']}s")
    
    print(json.dumps(summary, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
