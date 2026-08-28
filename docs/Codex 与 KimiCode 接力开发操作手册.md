# Codex 与 KimiCode 接力开发操作手册

这份手册用于让 Codex 和 KimiCode 在两个独立目录中开发同一个 Vistamirror 项目，避免互相覆盖代码。

## 目录与分支约定

```text
主项目目录：/Users/sy/my-todo
主分支：main
主项目使用者：你和 Codex

KimiCode 工作目录：/Users/sy/my-todo-kimi
KimiCode 分支：kimi-dev
KimiCode 使用者：KimiCode
```

不要让 KimiCode 直接在 `/Users/sy/my-todo` 中修改代码。

## 第一次创建 KimiCode 工作目录

先在 Mac 打开“终端”，然后按顺序逐行执行下面的命令。

```bash
cd /Users/sy/my-todo
# 进入 Vistamirror 主项目目录。
```

```bash
pwd
# 显示当前所在目录。
# 正常应显示：/Users/sy/my-todo
```

```bash
git status --short --branch
# 查看当前 Git 状态、所在分支和未提交修改。
# 如果看到 data/*、日志或 .DS_Store，不需要把它们提交。
```

```bash
git add runtime backend_modules tests episode-checker
# 把常见代码目录加入待提交列表。
# 不要使用 git add .，避免把 data、日志、Cookie、Token 等运行数据一起提交。
# 如果这次修改了其他代码目录，手动补到命令末尾，例如 scripts。
```

```bash
git status --short
# 再次检查待提交文件。
# 确认其中没有 data/*、*.log、.DS_Store、Cookie、Token、API Key 或密码。
```

```bash
git diff --cached --check
# 检查待提交代码有没有明显的空白符错误。
# 没有输出通常表示检查通过。
```

```bash
git commit -m "保存 KimiCode 开发前基线" -m "保存当前已验证代码，作为 KimiCode 开发前的回退点。"
# 创建本地中文 Git 提交。
# 这一步只保存到本机，不会上传 GitHub。
```

```bash
git log -1 --oneline
# 查看刚刚创建的最新提交。
# 最前面的短编号是提交 hash，以后回退时可以使用。
```

```bash
git branch kimi-dev main
# 从当前 main 创建 KimiCode 专用分支 kimi-dev。
# 如果提示分支已存在，直接跳到下一条命令。
```

```bash
git worktree add /Users/sy/my-todo-kimi kimi-dev
# 创建 KimiCode 专用的独立工作目录。
# 该目录使用 kimi-dev 分支，并与 main 物理隔离。
```

```bash
git worktree list
# 列出所有 Git 工作目录。
# 正常应看到 /Users/sy/my-todo 对应 main，
# 以及 /Users/sy/my-todo-kimi 对应 kimi-dev。
```

## 交给 KimiCode 的提示词

把下面整段复制给 KimiCode：

```text
你正在 Vistamirror 项目的独立工作目录开发。

工作目录：
/Users/sy/my-todo-kimi

工作分支：
kimi-dev

请先执行：
git status --short --branch
git log -1 --oneline

工作规则：
1. 只在 kimi-dev 分支开发，不要切换、修改、合并 main。
2. 不要提交 data/*、日志、.DS_Store、Cookie、Token、API Key、密码。
3. 修改前先阅读相关代码。
4. 修改后运行相关测试。
5. 用中文 Git 提交标题和说明提交改动。
6. 不要执行 git push。
7. 完成后告诉我：提交 hash、改了什么、测试是否通过、仍存在什么问题。
```

## main 更新后，重新同步给 KimiCode

这部分会用 `main` 覆盖 KimiCode 工作目录。**先确认 KimiCode 已经提交它需要保留的改动。**

```bash
cd /Users/sy/my-todo
# 回到主项目目录。
```

```bash
git status --short --branch
# 检查 main 是否还有未提交代码。
# 有代码改动时，先按前面的 git add 和 git commit 步骤提交。
```

```bash
git log -1 --oneline
# 确认 main 当前最新提交。
# KimiCode 接下来将同步到这个版本。
```

```bash
cd /Users/sy/my-todo-kimi
# 进入 KimiCode 的独立工作目录。
```

```bash
git status --short --branch
# 检查 KimiCode 是否有未提交改动。
# 有代码时必须先提交，否则后面的覆盖会删除它们。
```

如果 KimiCode 还有未提交工作，先执行：

```bash
git add <实际修改的文件路径>
# 将 KimiCode 实际改过的文件加入待提交列表。
# 例如：git add backend_modules/example.py tests/test_example.py
```

```bash
git commit -m "保存 KimiCode 当前进度" -m "同步 main 前保存 KimiCode 当前改动。"
# 保存 KimiCode 当前工作，防止覆盖时丢失。
```

然后建立保险分支：

```bash
git branch codex/kimi-dev-before-sync-$(date +%Y%m%d-%H%M%S)
# 为 KimiCode 当前提交创建带时间的保险分支。
# 同步后发现问题时，可从该分支找回旧版本。
```

最后执行覆盖同步：

```bash
git reset --hard main
# 强制让 kimi-dev 与 main 的最新提交一致。
# 警告：所有未提交的 KimiCode 修改都会被删除。
```

```bash
git clean -fd
# 删除 KimiCode 目录中未被 Git 跟踪的文件和文件夹。
# 警告：临时脚本、缓存、生成图片等未跟踪文件会被删除。
```

```bash
git status --short --branch
# 检查 KimiCode 工作目录是否干净。
# 理想结果只显示：## kimi-dev
```

```bash
git diff --exit-code main
# 检查 kimi-dev 是否与 main 完全一致。
# 没有输出表示两边代码相同。
```

## KimiCode 完成后，合并回 main

KimiCode 先完成并提交自己的工作，然后你执行以下步骤。

```bash
cd /Users/sy/my-todo-kimi
# 进入 KimiCode 工作目录。
```

```bash
git status --short --branch
# 确认 KimiCode 没有未提交修改。
```

```bash
git log --oneline main..kimi-dev
# 查看 KimiCode 比 main 多出的提交。
# 有输出表示存在待合并的功能。
```

```bash
cd /Users/sy/my-todo
# 回到主项目目录。
```

```bash
git checkout main
# 切换到主分支 main。
```

```bash
git status --short --branch
# 确认 main 没有未提交代码。
# 如果有，先提交或处理它们，再进行合并。
```

```bash
git merge --no-ff kimi-dev
# 将 KimiCode 的 kimi-dev 分支合并回 main。
# --no-ff 会留下清楚的合并记录，方便以后看 Git 图表。
```

如果 Git 提示有冲突：

```bash
git status
# 查看哪些文件有冲突。
```

```bash
git add <已解决冲突的文件路径>
# 手动处理冲突后，将已解决的文件加入待提交列表。
```

```bash
git commit
# 完成冲突解决后的合并提交。
# Git 会提供默认合并说明，也可以改成中文。
```

合并后验证：

```bash
python3 -m unittest discover -s tests
# 运行项目测试。
# 若本次功能有专用测试，也建议先运行专用测试。
```

```bash
git status --short --branch
# 最后确认 main 工作目录状态。
```

## 回退与查看旧版本

```bash
git log --oneline
# 查看提交历史，找到你想查看或回退的提交 hash。
```

```bash
git switch --detach <提交hash>
# 临时打开某个历史版本查看，不修改 main。
```

```bash
git switch main
# 看完历史版本后，回到 main。
```

```bash
git revert <要撤销的提交hash>
# 安全撤销某个已提交版本。
# Git 会新建一个反向提交，不会删除已有历史。
# 已推送到 GitHub 的版本优先使用这个方式。
```

## 不要随便使用的命令

```bash
git reset --hard <提交hash>
# 这会直接丢弃未提交修改，并改变当前分支位置。
# 只有明确确认不需要当前工作时才使用。
```

## 最常用命令速查

```bash
git branch kimi-dev main
# 第一次创建 KimiCode 分支。

git worktree add /Users/sy/my-todo-kimi kimi-dev
# 第一次创建 KimiCode 独立目录。
```

```bash
cd /Users/sy/my-todo-kimi
# 进入 KimiCode 目录。

git reset --hard main
# 用 main 覆盖 KimiCode 分支。

git clean -fd
# 清理 KimiCode 未跟踪文件。
```

```bash
cd /Users/sy/my-todo
# 回到主项目目录。

git merge --no-ff kimi-dev
# 将 KimiCode 已提交的工作合并回 main。
```
