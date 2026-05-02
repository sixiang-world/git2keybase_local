#!/usr/bin/env python3
"""
更新备份状态到 JSON 文件，供看板展示
"""

import json
import os
import sys
from datetime import datetime
from pathlib import Path

# 配置
SCRIPT_DIR = Path(__file__).parent
STATUS_FILE = Path("/var/www/dashboard/api/status.json")
BACKUP_LOG_DIR = SCRIPT_DIR / "logs"

# 确保目录存在
STATUS_FILE.parent.mkdir(parents=True, exist_ok=True)
BACKUP_LOG_DIR.mkdir(parents=True, exist_ok=True)


def load_latest_backup_result():
    """加载最新的备份结果"""
    # 查找最新的日志文件
    log_files = sorted(BACKUP_LOG_DIR.glob("backup_*.json"), reverse=True)
    
    if log_files:
        try:
            with open(log_files[0]) as f:
                return json.load(f)
        except Exception as e:
            print(f"Error loading log file: {e}")
    
    # 如果没有日志文件，尝试从 cron output 读取
    cron_output_dir = Path.home() / ".hermes" / "cron" / "output"
    job_id = "a9ddbd9d5f08"
    job_dir = cron_output_dir / job_id
    
    if job_dir.exists():
        output_files = sorted(job_dir.glob("*.md"), reverse=True)
        if output_files:
            # 解析 markdown 文件中的 JSON
            try:
                content = output_files[0].read_text()
                # 查找 JSON 块
                import re
                json_match = re.search(r'```json\n(.*?)\n```', content, re.DOTALL)
                if json_match:
                    return json.loads(json_match.group(1))
            except Exception as e:
                print(f"Error parsing cron output: {e}")
    
    return None


def save_status(data):
    """保存状态到 JSON 文件"""
    # 添加前端需要的字段
    if data:
        data["last_updated"] = datetime.now().isoformat()
    
    with open(STATUS_FILE, 'w') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    
    print(f"Status saved to {STATUS_FILE}")


def main():
    """主函数"""
    # 尝试从命令行参数获取 JSON 数据
    if len(sys.argv) > 1:
        try:
            data = json.loads(sys.argv[1])
            save_status(data)
            return
        except json.JSONDecodeError:
            pass
    
    # 尝试从 stdin 读取
    if not sys.stdin.isatty():
        try:
            data = json.load(sys.stdin)
            save_status(data)
            return
        except json.JSONDecodeError:
            pass
    
    # 尝试加载最新的备份结果
    data = load_latest_backup_result()
    
    if data:
        save_status(data)
    else:
        print("No backup data found")
        # 保存空状态
        save_status({
            "timestamp": datetime.now().isoformat(),
            "keybase_configured": False,
            "github_configured": False,
            "keybase_logged_in": False,
            "total": 0,
            "success": 0,
            "preserved": 0,
            "failed": 0,
            "warnings": ["暂无备份数据"],
            "repos": [],
            "duration_seconds": 0
        })


if __name__ == "__main__":
    main()
