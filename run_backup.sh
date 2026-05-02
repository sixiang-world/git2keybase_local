#!/bin/bash
# Git2Keybase 备份包装脚本
# 运行备份并更新看板

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# 运行备份
echo "开始备份..."
RESULT=$(python3 collect_backup_v4.py 2>&1)

# 提取 JSON 部分
JSON=$(echo "$RESULT" | grep -A 1000 "^{" | head -n -1)

if [ -n "$JSON" ]; then
    # 保存到日志
    mkdir -p logs
    TIMESTAMP=$(date +%Y%m%d_%H%M%S)
    echo "$JSON" > "logs/backup_${TIMESTAMP}.json"
    
    # 更新看板
    echo "$JSON" | python3 update_dashboard.py
    
    echo "备份完成，看板已更新"
else
    echo "备份失败，无法获取结果"
    exit 1
fi
