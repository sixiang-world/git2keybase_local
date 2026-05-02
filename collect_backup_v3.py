#!/usr/bin/env python3
"""
Git2Keybase 备份收集脚本（带防删库保护）
由 Hermes cron 的 script 参数调用，stdout 会被注入到 prompt 中。

安全特性：
  - 检测上游仓库异常变化（分支/标签大量删除）
  - 暂停同步并报警，防止删库跑路
  - 本地保留历史快照，可回滚

环境变量：
  KEYBASE_USERNAME  — Keybase 用户名
  GITHUB_TOKEN      — GitHub PAT（支持私有库 + 防限流）
  REPOS_FILE        — 仓库列表文件路径（默认同目录 repos.txt）
  BACKUP_WORKDIR    — 备份工作目录（默认 ~/.hermes/scripts/git2keybase/repos_cache）
  MAX_RELEASE_SIZE  — Release 文件最大大小（MB，默认 500）
  GIT_PUSH_TIMEOUT  — Git 推送超时（秒，默认 300）
  MAX_BRANCH_DELETE — 最大允许删除分支数（默认 5）
  MAX_TAG_DELETE    — 最大允许删除标签数（默认 10）
"""

import os
import sys
import json
import subprocess
import urllib.parse
import logging
import tempfile
from datetime import datetime
from pathlib import Path

# 尝试导入 requests
try:
    import requests
    HAS_REQUESTS = True
except ImportError:
    import urllib.request
    HAS_REQUESTS = False

# ── 配置 ──────────────────────────────────────────────
SCRIPT_DIR = Path(__file__).parent

# 设置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[logging.StreamHandler(sys.stderr)]
)
logger = logging.getLogger(__name__)

# 加载 .env 文件
env_file = SCRIPT_DIR / ".env"
if env_file.exists():
    for line in env_file.read_text().splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            key, _, value = line.partition("=")
            os.environ.setdefault(key.strip(), value.strip())

# 配置项
REPOS_FILE = Path(os.environ.get("REPOS_FILE", SCRIPT_DIR / "repos.txt"))
WORKDIR = Path(os.environ.get("BACKUP_WORKDIR", SCRIPT_DIR / "repos_cache"))
USERNAME = os.environ.get("KEYBASE_USERNAME", "")
GH_TOKEN = os.environ.get("GITHUB_TOKEN", "")
MAX_RELEASE_SIZE_MB = int(os.environ.get("MAX_RELEASE_SIZE", "500"))
GIT_PUSH_TIMEOUT = int(os.environ.get("GIT_PUSH_TIMEOUT", "300"))
MAX_BRANCH_DELETE = int(os.environ.get("MAX_BRANCH_DELETE", "5"))
MAX_TAG_DELETE = int(os.environ.get("MAX_TAG_DELETE", "10"))

WORKDIR.mkdir(parents=True, exist_ok=True)


def run_cmd(cmd, timeout=None, check=False, silent_error=False, env=None):
    """运行终端命令"""
    try:
        cmd_env = os.environ.copy()
        if env:
            cmd_env.update(env)
        
        result = subprocess.run(
            cmd, shell=True, capture_output=True, text=True,
            timeout=timeout, cwd=str(WORKDIR), env=cmd_env
        )
        
        if result.returncode != 0:
            if check:
                logger.error(f"命令失败: {cmd}")
                logger.error(f"stderr: {result.stderr}")
                raise subprocess.CalledProcessError(
                    result.returncode, cmd, result.stdout, result.stderr
                )
            elif not silent_error:
                logger.warning(f"命令返回非零: {cmd}")
        
        return result
    except subprocess.TimeoutExpired:
        logger.error(f"命令超时 ({timeout}s): {cmd}")
        raise


def check_keybase_login():
    """检查 Keybase 是否已登录"""
    try:
        result = run_cmd("keybase status", timeout=10, silent_error=True)
        return result.returncode == 0 and "Logged in:     yes" in result.stdout
    except Exception:
        return False


def get_git_url(repo_url):
    """获取带认证的 Git URL（通过环境变量传递 token）"""
    parsed = urllib.parse.urlparse(repo_url)
    domain = parsed.netloc
    
    if domain == "github.com" and GH_TOKEN:
        return repo_url, {
            "GIT_ASKPASS": "echo",
            "GIT_USERNAME": "x-access-token",
            "GIT_PASSWORD": GH_TOKEN
        }
    return repo_url, {}


def get_remote_stats(repo_dir):
    """
    获取远程仓库的分支和标签数量（用于检测异常）
    
    Returns:
        dict: {"branches": int, "tags": int}
    """
    try:
        # 获取远程分支数量
        result = subprocess.run(
            "git branch -r", shell=True, capture_output=True, text=True,
            cwd=str(repo_dir), timeout=30
        )
        branches = len([l for l in result.stdout.strip().split('\n') if l.strip() and 'HEAD' not in l])
        
        # 获取标签数量
        result = subprocess.run(
            "git tag", shell=True, capture_output=True, text=True,
            cwd=str(repo_dir), timeout=30
        )
        tags = len([l for l in result.stdout.strip().split('\n') if l.strip()])
        
        return {"branches": branches, "tags": tags}
    except Exception as e:
        logger.warning(f"获取远程统计失败: {e}")
        return {"branches": 0, "tags": 0}


def detect_dangerous_changes(old_stats, new_stats):
    """
    检测危险的变化（删库跑路保护）
    
    Returns:
        tuple: (is_safe: bool, reason: str)
    """
    branch_diff = old_stats["branches"] - new_stats["branches"]
    tag_diff = old_stats["tags"] - new_stats["tags"]
    
    # 检测分支大量删除
    if branch_diff > MAX_BRANCH_DELETE:
        return False, f"检测到 {branch_diff} 个分支被删除（超过阈值 {MAX_BRANCH_DELETE}），可能是删库！"
    
    # 检测标签大量删除
    if tag_diff > MAX_TAG_DELETE:
        return False, f"检测到 {tag_diff} 个标签被删除（超过阈值 {MAX_TAG_DELETE}），可能是删库！"
    
    # 检测仓库清空
    if new_stats["branches"] == 0 and old_stats["branches"] > 0:
        return False, "仓库分支被清空，可能是删库！"
    
    return True, "变化正常"


def save_safety_snapshot(repo_dir, safe_name):
    """
    保存安全快照（用于回滚）
    
    Returns:
        str: 快照标签名
    """
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    snapshot_tag = f"safety-snapshot-{ts}"
    
    try:
        # 创建快照标签
        subprocess.run(
            f"git tag {snapshot_tag}",
            shell=True, cwd=str(repo_dir), timeout=10,
            capture_output=True, text=True
        )
        logger.info(f"创建安全快照: {snapshot_tag}")
        return snapshot_tag
    except Exception as e:
        logger.warning(f"创建快照失败: {e}")
        return None


def cleanup_old_archive_tags(repo_dir, keep_days=7):
    """清理旧的 archive 标签，只保留最近 N 天"""
    try:
        result = subprocess.run(
            "git tag -l 'archive-*'",
            shell=True, cwd=str(repo_dir), timeout=10,
            capture_output=True, text=True
        )
        
        if result.returncode != 0 or not result.stdout.strip():
            return
        
        tags = result.stdout.strip().split('\n')
        cutoff_date = datetime.now().strftime("%Y%m%d")
        tags_to_delete = []
        
        for tag in tags:
            try:
                date_str = tag.split('-')[1].split('_')[0]
                if date_str < cutoff_date:
                    tags_to_delete.append(tag)
            except (IndexError, ValueError):
                continue
        
        # 只保留最近 N 天
        if len(tags_to_delete) > keep_days:
            tags_to_delete = sorted(tags_to_delete)[:-keep_days]
        
        if tags_to_delete:
            logger.info(f"清理 {len(tags_to_delete)} 个旧标签")
            for tag in tags_to_delete:
                subprocess.run(
                    f"git tag -d {tag}",
                    shell=True, cwd=str(repo_dir), timeout=5,
                    capture_output=True, text=True
                )
    except Exception as e:
        logger.warning(f"清理标签失败: {e}")


def backup_git(repo_url, repo_dir, safe_name):
    """
    备份 Git 仓库（带防删库保护）
    
    Returns:
        dict: {"success": bool, "error": str or None, "warnings": list}
    """
    result = {"success": False, "error": None, "warnings": [], "snapshot": None}
    git_url, git_env = get_git_url(repo_url)
    
    try:
        # 创建 Keybase 仓库（如果不存在）
        if USERNAME:
            run_cmd(f"keybase git create {safe_name}", timeout=30, silent_error=True)
        
        # 获取本地仓库的旧统计
        if repo_dir.exists():
            old_stats = get_remote_stats(repo_dir)
            logger.info(f"本地统计 - 分支: {old_stats['branches']}, 标签: {old_stats['tags']}")
        else:
            old_stats = {"branches": 0, "tags": 0}
        
        # 先获取远程最新状态（不应用）
        logger.info(f"检查上游变化: {safe_name}")
        
        if repo_dir.exists():
            # 保存安全快照
            result["snapshot"] = save_safety_snapshot(repo_dir, safe_name)
            
            # 获取远程最新状态
            fetch_result = subprocess.run(
                "git fetch --all --force --tags",
                shell=True, cwd=str(repo_dir), timeout=GIT_PUSH_TIMEOUT,
                env={**os.environ, **git_env},
                capture_output=True, text=True
            )
            
            if fetch_result.returncode != 0:
                result["error"] = f"fetch 失败: {fetch_result.stderr[:100]}"
                return result
            
            # 获取新的统计
            new_stats = get_remote_stats(repo_dir)
            logger.info(f"远程统计 - 分支: {new_stats['branches']}, 标签: {new_stats['tags']}")
            
            # 检测危险变化
            is_safe, reason = detect_dangerous_changes(old_stats, new_stats)
            
            if not is_safe:
                logger.error(f"⚠️ 安全警告: {reason}")
                result["error"] = f"安全警告: {reason}"
                result["warnings"].append(reason)
                
                # 回滚到快照
                if result["snapshot"]:
                    logger.info(f"回滚到快照: {result['snapshot']}")
                    subprocess.run(
                        f"git reset --hard {result['snapshot']}",
                        shell=True, cwd=str(repo_dir), timeout=30,
                        capture_output=True, text=True
                    )
                return result
            
            result["warnings"].append(f"变化正常: 分支 {old_stats['branches']}→{new_stats['branches']}, 标签 {old_stats['tags']}→{new_stats['tags']}")
            
        else:
            # 首次克隆
            logger.info(f"首次克隆: {safe_name}")
            clone_result = subprocess.run(
                f"git clone --bare {git_url} {repo_dir}",
                shell=True, cwd=str(WORKDIR), timeout=GIT_PUSH_TIMEOUT,
                env={**os.environ, **git_env},
                capture_output=True, text=True
            )
            
            if clone_result.returncode != 0:
                result["error"] = f"克隆失败: {clone_result.stderr[:100]}"
                return result
        
        # 清理旧的 archive 标签
        cleanup_old_archive_tags(repo_dir)
        
        # 创建新的 archive 标签
        ts = datetime.now().strftime("%Y%m%d")
        archive_tag = f"archive-{ts}"
        subprocess.run(
            f"git tag {archive_tag}",
            shell=True, cwd=str(repo_dir), timeout=10,
            capture_output=True, text=True
        )
        
        # 推送到 Keybase
        if USERNAME:
            kb_remote = f"keybase://private/{USERNAME}/{safe_name}"
            subprocess.run("git remote remove keybase", shell=True, cwd=str(repo_dir), timeout=10, capture_output=True)
            subprocess.run(f"git remote add keybase {kb_remote}", shell=True, cwd=str(repo_dir), timeout=10, capture_output=True, text=True)
            
            logger.info(f"推送到 Keybase: {safe_name}")
            
            # 使用 --all 和 --tags 分别推送（不用 --mirror）
            push_all = subprocess.run(
                "git push keybase --all --force",
                shell=True, cwd=str(repo_dir), timeout=GIT_PUSH_TIMEOUT,
                capture_output=True, text=True
            )
            
            if push_all.returncode != 0:
                logger.warning(f"推送分支失败: {push_all.stderr[:100]}")
            
            push_tags = subprocess.run(
                "git push keybase --tags --force",
                shell=True, cwd=str(repo_dir), timeout=GIT_PUSH_TIMEOUT,
                capture_output=True, text=True
            )
            
            if push_tags.returncode != 0:
                logger.warning(f"推送标签失败: {push_tags.stderr[:100]}")
        
        result["success"] = True
        return result
        
    except subprocess.TimeoutExpired:
        result["error"] = f"超时 ({GIT_PUSH_TIMEOUT}s)"
        return result
    except Exception as e:
        result["error"] = str(e)
        return result


def backup_releases(repo_url, safe_name):
    """备份 GitHub Releases（仅 GitHub 仓库）"""
    parsed = urllib.parse.urlparse(repo_url)
    domain = parsed.netloc
    path = parsed.path.lstrip("/")
    
    if domain != "github.com" or not USERNAME:
        return []
    
    if not GH_TOKEN:
        return [{"status": "GitHub Token 未配置"}]
    
    try:
        api_url = f"https://api.github.com/repos/{path}/releases?per_page=100"
        headers = {"Authorization": f"token {GH_TOKEN}"}
        
        if HAS_REQUESTS:
            resp = requests.get(api_url, headers=headers, timeout=10)
            if resp.status_code != 200:
                return [{"status": f"API 返回 {resp.status_code}"}]
            releases = resp.json()
        else:
            req = urllib.request.Request(api_url, headers=headers)
            with urllib.request.urlopen(req, timeout=10) as resp:
                releases = json.loads(resp.read())
        
        if not releases or not isinstance(releases, list):
            return [{"status": "无 releases"}]
        
        top3 = releases[:3]
        results = []
        
        kb_release_dir = f"/keybase/private/{USERNAME}/releases/{safe_name}"
        run_cmd(f"keybase fs mkdir -p {kb_release_dir}", timeout=15, silent_error=True)
        
        for release in top3:
            tag = release.get("tag_name", "unknown")
            assets = release.get("assets", [])
            release_info = {"tag": tag, "assets_count": len(assets), "assets": []}
            
            for asset in assets:
                fname = f"{tag}_{asset.get('name', 'asset')}"
                size_mb = asset.get("size", 0) / 1024 / 1024
                kb_file_path = f"{kb_release_dir}/{fname}"
                
                if size_mb > MAX_RELEASE_SIZE_MB:
                    release_info["assets"].append({
                        "name": fname, "size_mb": round(size_mb, 2),
                        "status": f"跳过（超过 {MAX_RELEASE_SIZE_MB}MB）"
                    })
                    continue
                
                check = run_cmd(f"keybase fs stat {kb_file_path}", timeout=10, silent_error=True)
                if check.returncode == 0:
                    release_info["assets"].append({
                        "name": fname, "size_mb": round(size_mb, 2), "status": "已存在"
                    })
                    continue
                
                download_url = asset.get("browser_download_url")
                if not download_url:
                    release_info["assets"].append({
                        "name": fname, "size_mb": round(size_mb, 2), "status": "无下载链接"
                    })
                    continue
                
                try:
                    with tempfile.NamedTemporaryFile(delete=False, suffix=fname) as tmp:
                        tmp_path = Path(tmp.name)
                        if HAS_REQUESTS:
                            with requests.get(download_url, stream=True, timeout=60) as r:
                                r.raise_for_status()
                                for chunk in r.iter_content(chunk_size=8192):
                                    tmp.write(chunk)
                        else:
                            urllib.request.urlretrieve(download_url, str(tmp_path))
                    
                    run_cmd(f"keybase fs cp {tmp_path} {kb_file_path}", timeout=120, check=True)
                    tmp_path.unlink(missing_ok=True)
                    
                    release_info["assets"].append({
                        "name": fname, "size_mb": round(size_mb, 2), "status": "已备份"
                    })
                except Exception as e:
                    release_info["assets"].append({
                        "name": fname, "size_mb": round(size_mb, 2),
                        "status": f"错误: {str(e)[:50]}"
                    })
                    if 'tmp_path' in locals():
                        tmp_path.unlink(missing_ok=True)
            
            results.append(release_info)
        
        return results
    except Exception as e:
        return [{"status": f"错误: {str(e)[:80]}"}]


def backup_repo(repo_url):
    """备份单个仓库"""
    result = {
        "url": repo_url,
        "git_sync": "skipped",
        "releases": [],
        "errors": [],
        "warnings": [],
        "snapshot": None,
    }
    
    parsed = urllib.parse.urlparse(repo_url)
    domain = parsed.netloc
    path = parsed.path.lstrip("/")
    if path.endswith(".git"):
        path = path[:-4]
    safe_name = f"{domain.replace('.', '_')}_{path.replace('/', '_')}"
    repo_dir = WORKDIR / f"{safe_name}.git"
    
    logger.info(f"开始备份: {repo_url}")
    
    git_result = backup_git(repo_url, repo_dir, safe_name)
    
    if git_result["success"]:
        result["git_sync"] = "success"
        result["snapshot"] = git_result["snapshot"]
        result["warnings"] = git_result["warnings"]
        logger.info(f"Git 备份成功: {safe_name}")
        
        if domain == "github.com":
            result["releases"] = backup_releases(repo_url, safe_name)
    else:
        result["git_sync"] = "failed"
        result["errors"].append(f"git: {git_result['error']}")
        result["warnings"] = git_result.get("warnings", [])
        logger.error(f"Git 备份失败: {safe_name}, 错误: {git_result['error']}")
    
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
        "failed": 0,
        "safety_warnings": [],
        "duration_seconds": 0,
    }
    
    start_time = datetime.now()
    
    if USERNAME:
        summary["keybase_logged_in"] = check_keybase_login()
        if not summary["keybase_logged_in"]:
            logger.warning("Keybase 未登录，将跳过 Keybase 推送")
    
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
            
            if r["git_sync"] == "success":
                summary["success"] += 1
            else:
                summary["failed"] += 1
            
            # 收集安全警告
            if r.get("warnings"):
                summary["safety_warnings"].extend(r["warnings"])
                
        except Exception as e:
            logger.error(f"备份异常: {repo_url}, 错误: {e}")
            summary["repos"].append({
                "url": repo_url, "git_sync": "failed",
                "releases": [], "errors": [str(e)], "warnings": []
            })
            summary["failed"] += 1
    
    end_time = datetime.now()
    summary["duration_seconds"] = int((end_time - start_time).total_seconds())
    
    logger.info(f"备份完成: 成功 {summary['success']}, 失败 {summary['failed']}, 耗时 {summary['duration_seconds']}s")
    
    print(json.dumps(summary, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
