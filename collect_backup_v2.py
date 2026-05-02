#!/usr/bin/env python3
"""
Git2Keybase 备份收集脚本
由 Hermes cron 的 script 参数调用，stdout 会被注入到 prompt 中。

环境变量：
  KEYBASE_USERNAME  — Keybase 用户名
  GITHUB_TOKEN      — GitHub PAT（支持私有库 + 防限流）
  REPOS_FILE        — 仓库列表文件路径（默认同目录 repos.txt）
  BACKUP_WORKDIR    — 备份工作目录（默认 ~/.hermes/scripts/git2keybase/repos_cache）
  MAX_RELEASE_SIZE  — Release 文件最大大小（MB，默认 500）
  GIT_PUSH_TIMEOUT  — Git 推送超时（秒，默认 300）

输出：JSON 格式的备份摘要，供 Hermes Agent 格式化汇报。
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
from concurrent.futures import ThreadPoolExecutor, as_completed

# 尝试导入 requests，如果没有则使用 urllib
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
    handlers=[
        logging.StreamHandler(sys.stderr),  # 日志输出到 stderr，不影响 stdout
    ]
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

# 确保工作目录存在
WORKDIR.mkdir(parents=True, exist_ok=True)


def run_cmd(cmd, timeout=None, check=False, silent_error=False, env=None):
    """
    运行终端命令
    
    Args:
        cmd: 命令字符串
        timeout: 超时秒数
        check: 是否检查返回码
        silent_error: 是否静默错误
        env: 额外的环境变量
    
    Returns:
        subprocess.CompletedProcess 对象
    """
    try:
        # 合并环境变量
        cmd_env = os.environ.copy()
        if env:
            cmd_env.update(env)
        
        result = subprocess.run(
            cmd,
            shell=True,
            capture_output=True,
            text=True,
            timeout=timeout,
            cwd=str(WORKDIR),
            env=cmd_env
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
                logger.warning(f"stderr: {result.stderr}")
        
        return result
        
    except subprocess.TimeoutExpired:
        logger.error(f"命令超时 ({timeout}s): {cmd}")
        raise
    except Exception as e:
        logger.error(f"命令异常: {cmd}, 错误: {e}")
        raise


def check_keybase_login():
    """检查 Keybase 是否已登录"""
    try:
        result = run_cmd("keybase status", timeout=10, silent_error=True)
        if result.returncode == 0 and "Logged in:     yes" in result.stdout:
            return True
        return False
    except Exception:
        return False


def get_git_url(repo_url):
    """获取带认证的 Git URL（不暴露 token 到命令行）"""
    parsed = urllib.parse.urlparse(repo_url)
    domain = parsed.netloc
    path = parsed.path.lstrip("/")
    is_github = domain == "github.com"
    
    if is_github and GH_TOKEN:
        # 返回原始 URL，通过环境变量传递 token
        return repo_url, {
            "GIT_ASKPASS": "echo",
            "GIT_USERNAME": "x-access-token",
            "GIT_PASSWORD": GH_TOKEN
        }
    return repo_url, {}


def backup_git(repo_url, repo_dir, safe_name):
    """
    备份 Git 仓库
    
    Returns:
        (success: bool, error: str or None)
    """
    git_url, git_env = get_git_url(repo_url)
    
    try:
        # 创建 Keybase 仓库（如果不存在）
        if USERNAME:
            run_cmd(
                f"keybase git create {safe_name}",
                timeout=30,
                silent_error=True
            )
        
        # 获取或更新本地仓库
        if repo_dir.exists():
            logger.info(f"更新仓库: {safe_name}")
            run_cmd(
                f"git fetch --all --force --tags",
                timeout=GIT_PUSH_TIMEOUT,
                check=True,
                env=git_env
            )
        else:
            logger.info(f"克隆仓库: {safe_name}")
            run_cmd(
                f"git clone --bare {git_url} {repo_dir}",
                timeout=GIT_PUSH_TIMEOUT,
                check=True,
                env=git_env
            )
        
        # 创建归档标签
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        run_cmd(f"git tag archive-{ts}", timeout=10, silent_error=True)
        
        # 推送到 Keybase
        if USERNAME:
            kb_remote = f"keybase://private/{USERNAME}/{safe_name}"
            run_cmd("git remote remove keybase", timeout=10, silent_error=True)
            run_cmd(
                f"git remote add keybase {kb_remote}",
                timeout=10,
                check=True
            )
            
            # 先尝试 --mirror，失败则分步推送
            logger.info(f"推送到 Keybase: {safe_name}")
            push_result = run_cmd(
                "git push keybase --mirror --force",
                timeout=GIT_PUSH_TIMEOUT,
                silent_error=True
            )
            
            if push_result.returncode != 0:
                logger.warning(f"--mirror 推送失败，尝试分步推送")
                run_cmd(
                    "git push keybase --all --force",
                    timeout=GIT_PUSH_TIMEOUT,
                    check=True
                )
                run_cmd(
                    "git push keybase --tags",
                    timeout=GIT_PUSH_TIMEOUT,
                    check=True
                )
        
        return True, None
        
    except subprocess.TimeoutExpired:
        return False, f"超时 ({GIT_PUSH_TIMEOUT}s)"
    except Exception as e:
        return False, str(e)


def backup_releases(repo_url, safe_name):
    """
    备份 GitHub Releases（仅 GitHub 仓库）
    
    Returns:
        list: releases 备份结果
    """
    parsed = urllib.parse.urlparse(repo_url)
    domain = parsed.netloc
    path = parsed.path.lstrip("/")
    
    if domain != "github.com" or not USERNAME:
        return []
    
    if not GH_TOKEN:
        return [{"status": "GitHub Token 未配置"}]
    
    try:
        # 获取最新 3 个 release
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
        
        # 创建 Keybase FS 目录
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
                
                # 跳过超大文件
                if size_mb > MAX_RELEASE_SIZE_MB:
                    release_info["assets"].append({
                        "name": fname,
                        "size_mb": round(size_mb, 2),
                        "status": f"跳过（超过 {MAX_RELEASE_SIZE_MB}MB）"
                    })
                    continue
                
                # 检查是否已存在
                check = run_cmd(
                    f"keybase fs stat {kb_file_path}",
                    timeout=10,
                    silent_error=True
                )
                
                if check.returncode == 0:
                    release_info["assets"].append({
                        "name": fname,
                        "size_mb": round(size_mb, 2),
                        "status": "已存在"
                    })
                    continue
                
                # 下载并上传
                download_url = asset.get("browser_download_url")
                if not download_url:
                    release_info["assets"].append({
                        "name": fname,
                        "size_mb": round(size_mb, 2),
                        "status": "无下载链接"
                    })
                    continue
                
                try:
                    # 使用临时文件
                    with tempfile.NamedTemporaryFile(delete=False, suffix=fname) as tmp:
                        tmp_path = Path(tmp.name)
                        
                        if HAS_REQUESTS:
                            with requests.get(download_url, stream=True, timeout=60) as r:
                                r.raise_for_status()
                                for chunk in r.iter_content(chunk_size=8192):
                                    tmp.write(chunk)
                        else:
                            urllib.request.urlretrieve(download_url, str(tmp_path))
                    
                    # 上传到 Keybase
                    run_cmd(
                        f"keybase fs cp {tmp_path} {kb_file_path}",
                        timeout=120,
                        check=True
                    )
                    
                    # 清理临时文件
                    tmp_path.unlink(missing_ok=True)
                    
                    release_info["assets"].append({
                        "name": fname,
                        "size_mb": round(size_mb, 2),
                        "status": "已备份"
                    })
                    
                except Exception as e:
                    release_info["assets"].append({
                        "name": fname,
                        "size_mb": round(size_mb, 2),
                        "status": f"错误: {str(e)[:50]}"
                    })
                    # 清理临时文件
                    if 'tmp_path' in locals():
                        tmp_path.unlink(missing_ok=True)
            
            results.append(release_info)
        
        return results
        
    except Exception as e:
        return [{"status": f"错误: {str(e)[:80]}"}]


def backup_repo(repo_url):
    """
    备份单个仓库
    
    Returns:
        dict: 备份结果
    """
    result = {
        "url": repo_url,
        "git_sync": "skipped",
        "releases": [],
        "errors": [],
    }
    
    # 解析 URL
    parsed = urllib.parse.urlparse(repo_url)
    domain = parsed.netloc
    path = parsed.path.lstrip("/")
    if path.endswith(".git"):
        path = path[:-4]
    safe_name = f"{domain.replace('.', '_')}_{path.replace('/', '_')}"
    repo_dir = WORKDIR / f"{safe_name}.git"
    
    logger.info(f"开始备份: {repo_url}")
    
    # Git 备份
    success, error = backup_git(repo_url, repo_dir, safe_name)
    
    if success:
        result["git_sync"] = "success"
        logger.info(f"Git 备份成功: {safe_name}")
    else:
        result["git_sync"] = "failed"
        result["errors"].append(f"git: {error}")
        logger.error(f"Git 备份失败: {safe_name}, 错误: {error}")
    
    # Release 备份（只在 Git 备份成功后进行）
    if success and domain == "github.com":
        result["releases"] = backup_releases(repo_url, safe_name)
    
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
        "duration_seconds": 0,
    }
    
    start_time = datetime.now()
    
    # 检查 Keybase 登录状态
    if USERNAME:
        summary["keybase_logged_in"] = check_keybase_login()
        if not summary["keybase_logged_in"]:
            logger.warning("Keybase 未登录，将跳过 Keybase 推送")
    
    # 读取仓库列表
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
    
    # 逐个备份（串行，避免 git 命令冲突）
    for repo_url in repos:
        try:
            r = backup_repo(repo_url)
            summary["repos"].append(r)
            
            if r["git_sync"] == "success":
                summary["success"] += 1
            else:
                summary["failed"] += 1
                
        except Exception as e:
            logger.error(f"备份异常: {repo_url}, 错误: {e}")
            summary["repos"].append({
                "url": repo_url,
                "git_sync": "failed",
                "releases": [],
                "errors": [str(e)]
            })
            summary["failed"] += 1
    
    # 计算耗时
    end_time = datetime.now()
    summary["duration_seconds"] = int((end_time - start_time).total_seconds())
    
    logger.info(f"备份完成: 成功 {summary['success']}, 失败 {summary['failed']}, 耗时 {summary['duration_seconds']}s")
    
    # 输出 JSON 摘要到 stdout
    print(json.dumps(summary, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
