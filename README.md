# Git2Keybase Local (防删库版)

本地 Hermes Cron 版的 Git 备份脚本，自动备份仓库到 Keybase，带**防删库保护**。

## 防删库策略

使用 **仓库数据大小** 作为安全锚点检测上游删库：
1. 每次备份前测量本地缓存大小 (`du -sb`)
2. `repo_sizes.json` 记录历史大小
3. **当前大小 < 上次的 20% → 判定删库**
4. 判定后：跳过推送 + wxpush 微信告警

## 安全备份

- fetch: `+refs/heads/*:refs/heads/* +refs/tags/*:refs/tags/*`
- push: `--all --tags --force`（不删除远程 ref）
- 每次备份打 archive-YYYYMMDD tag

## 文件说明

| 文件 | 说明 |
|------|------|
| `collect_backup.py` | 主备份脚本 |
| `repos.txt` | 仓库列表（每行一个 URL） |
| `run_backup.sh` | 启动脚本，提取 JSON 并更新看板 |
| `repo_sizes.json` | 大小跟踪数据库（自动创建） |
| `update_dashboard.py` | 看板更新脚本 |
| `repos_cache/` | Git bare 仓库缓存 |

## 配置

环境变量：
- `KEYBASE_USERNAME` — Keybase 用户名
- `GITHUB_TOKEN` — GitHub PAT
- `WXPUSH_API_TOKEN` — wxpush token（可选，删库告警用）
