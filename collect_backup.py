#!/usr/bin/env python3
"""
Git2Keybase 备份收集脚本
由 Hermes cron 的 script 参数调用，stdout 会被注入到 prompt 中。

环境变量：
  KEYBASE_USERNAME  — Keybase 用户名
  GITHUB_TOKEN      — GitHub PAT（支持私有库 + 防限流）
  REPOS_FILE        — 仓库列表文件路径（默认同目录 repos.txt）
  BACKUP_WORKDIR    — 备份工作目录（默认 ~/.hermes/scripts/git2keybase/repos_cache）

输出：JSON 格式的备份摘要，供 Hermes Agent 格式化汇报。
"""

import os
import sys
import json
import subprocess
import urllib.parse
import requests
from datetime import datetime
from pathlib import Path

# ── 配置 ──────────────────────────────────────────────
SCRIPT_DIR = Path(__file__).parent

# 加载 .env 文件
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

# 确保工作目录存在
WORKDIR.mkdir(parents=True, exist_ok=True)


def run_cmd(cmd, check=False, silent_error=False):
    """运行终端命令"""
    try:
        result = subprocess.run(
            cmd, shell=True, capture_output=True, text=True, check=check, cwd=str(WORKDIR)
        )
        if result.returncode != 0 and not check and not silent_error:
            pass  # 静默处理
        return result
    except subprocess.CalledProcessError as e:
        return e


def backup_repo(repo_url):
    """备份单个仓库，返回结果 dict"""
    result = {
        "url": repo_url,
        "git_sync": "skipped",
        "releases": [],
        "errors": [],
    }

    # 1. 解析 URL
    parsed = urllib.parse.urlparse(repo_url)
    domain = parsed.netloc
    path = parsed.path.lstrip("/")
    if path.endswith(".git"):
        path = path[:-4]
    repo_name = path.split("/")[-1] if path else "unknown"
    safe_name = f"{domain.replace('.', '_')}_{path.replace('/', '_')}"
    is_github = domain == "github.com"

    if is_github and GH_TOKEN:
        git_url = f"https://x-access-token:{GH_TOKEN}@github.com/{path}.git"
    else:
        git_url = repo_url

    repo_dir = WORKDIR / f"{safe_name}.git"

    # 2. Git 代码备份
    try:
        run_cmd(f"keybase git create {safe_name} || true", silent_error=True)

        if repo_dir.exists():
            os.chdir(str(repo_dir))
            r = run_cmd(f"git fetch {git_url} '*:*' --force --tags", check=True)
        else:
            run_cmd(f"git clone --bare {git_url} {repo_dir}", check=True)
            os.chdir(str(repo_dir))

        # 防删标签
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        run_cmd(f"git tag archive-{ts}")

        # 推送到 Keybase
        if USERNAME:
            kb_remote = f"keybase://private/{USERNAME}/{safe_name}"
            run_cmd("git remote remove keybase || true", silent_error=True)
            run_cmd(f"git remote add keybase {kb_remote}")

            push_result = run_cmd("git push keybase --mirror --force", silent_error=True)
            if push_result.returncode != 0:
                run_cmd("git push keybase --all --force", silent_error=True)
                run_cmd("git push keybase --tags", silent_error=True)

        os.chdir(str(WORKDIR))
        result["git_sync"] = "success"

    except Exception as e:
        result["git_sync"] = "failed"
        result["errors"].append(f"git: {str(e)}")
        if WORKDIR != Path(os.getcwd()):
            os.chdir(str(WORKDIR))
        return result

    # 3. Release 备份
    if not USERNAME:
        result["releases"] = [{"status": "Keybase 未配置，跳过 Release 备份"}]
        return result

    if is_github:
        api_url = f"https://api.github.com/repos/{path}/releases?per_page=100"
        headers = {"Authorization": f"token {GH_TOKEN}"} if GH_TOKEN else {}
    else:
        api_url = f"https://{domain}/api/v1/repos/{path}/releases?limit=100"
        headers = {}

    try:
        resp = requests.get(api_url, headers=headers, timeout=10)
        if resp.status_code != 200:
            result["releases"] = [{"status": f"API returned {resp.status_code}, skipped"}]
            return result

        releases = resp.json()
        if not releases or not isinstance(releases, list):
            result["releases"] = [{"status": "no releases found"}]
            return result

        top3 = releases[:3]
        keep_tags = [r.get("tag_name", "unknown") for r in top3]

        # 确保 Keybase FS 目录存在
        kb_release_base = f"/keybase/private/{USERNAME}/releases"
        kb_release_dir = f"{kb_release_base}/{safe_name}"
        run_cmd(f"keybase fs mkdir {kb_release_base} || true", silent_error=True)
        run_cmd(f"keybase fs mkdir {kb_release_dir} || true", silent_error=True)

        # 清理旧版本文件
        ls_result = run_cmd(f"keybase fs ls {kb_release_dir}", silent_error=True)
        if ls_result.returncode == 0:
            for f in ls_result.stdout.splitlines():
                f = f.strip()
                if f and not any(f.startswith(f"{t}_") for t in keep_tags):
                    run_cmd(f"keybase fs rm {kb_release_dir}/{f}", silent_error=True)

        for release in top3:
            tag = release.get("tag_name", "unknown")
            assets = release.get("assets", [])
            release_info = {"tag": tag, "assets_count": len(assets), "assets": []}

            for asset in assets:
                fname = f"{tag}_{asset.get('name', 'asset')}"
                size_mb = asset.get("size", 0) / 1024 / 1024
                kb_file_path = f"{kb_release_dir}/{fname}"
                asset_status = "skipped"

                # 检查是否已存在
                check = run_cmd(f"keybase fs stat {kb_file_path}", silent_error=True)
                if check.returncode != 0:
                    download_url = asset.get("browser_download_url")
                    if not download_url:
                        asset_status = "no_download_url"
                    else:
                        try:
                            dl_path = WORKDIR / fname
                            with requests.get(download_url, stream=True, timeout=60) as r:
                                r.raise_for_status()
                                with open(dl_path, "wb") as f:
                                    for chunk in r.iter_content(chunk_size=8192):
                                        f.write(chunk)
                            run_cmd(f"keybase fs cp {dl_path} {kb_file_path}", check=True)
                            dl_path.unlink(missing_ok=True)
                            asset_status = "backed_up"
                        except Exception as e:
                            asset_status = f"error: {str(e)[:80]}"
                else:
                    asset_status = "exists"

                release_info["assets"].append({
                    "name": fname,
                    "size_mb": round(size_mb, 2),
                    "status": asset_status
                })

            result["releases"].append(release_info)

    except Exception as e:
        result["errors"].append(f"releases: {str(e)}")

    return result


def main():
    summary = {
        "timestamp": datetime.now().isoformat(),
        "keybase_configured": bool(USERNAME),
        "github_configured": bool(GH_TOKEN),
        "repos_file": str(REPOS_FILE),
        "repos": [],
        "total": 0,
        "success": 0,
        "failed": 0,
    }

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

    # 逐个备份
    for repo_url in repos:
        r = backup_repo(repo_url)
        summary["repos"].append(r)
        if r["git_sync"] == "success":
            summary["success"] += 1
        else:
            summary["failed"] += 1

    print(json.dumps(summary, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
