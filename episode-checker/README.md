# 独立影视缺失集数检测工具

`episode-checker` 是一个不依赖 MoviePilot 的纯 Python CLI。它会通过 TMDB 查询剧集应有季集数，再递归扫描本地视频目录，解析文件名里的季号和集号，最后输出缺失集列表。

它支持两种 TMDB 鉴权方式：

```text
1. 标准独立模式：TMDB_API_KEY（v3 api_key）
2. 兼容 Vistamirror 模式：读取当前项目已保存的 TMDB Bearer Token
```

## 安装

```bash
cd episode-checker
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

设置 TMDB v3 API Key：

```bash
export TMDB_API_KEY="你的_tmdb_api_key"
```

如果你已经在当前 Vistamirror 项目里保存了 TMDB Bearer Token，也可以不设置环境变量，直接在运行时加：

```bash
python main.py --dir "/path/to/动漫/仙逆" --name "仙逆" --use-vistamirror-config
```

注意：只有显式加了 `--use-vistamirror-config`，工具才会去读当前仓库里的保存配置。

## 基础用法

指定目录和剧名：

```bash
python main.py --dir "/path/to/动漫/仙逆" --name "仙逆"
```

不写剧名时，会使用目录名作为剧名：

```bash
python main.py --dir "/path/to/动漫/仙逆"
```

只检查某一季：

```bash
python main.py --dir "/path/to/剧集" --name "某剧" --season 2
```

排查文件名解析问题：

```bash
python main.py --dir "/path/to/动漫/仙逆" --name "仙逆" --verbose

复用当前项目里的 TMDB Bearer Token：

```bash
python main.py --dir "/path/to/动漫/仙逆" --name "仙逆" --use-vistamirror-config
```
```

输出 JSON：

```bash
python main.py --dir "/path/to/动漫/仙逆" --name "仙逆" --json
```

## 支持的文件名格式

默认扫描 `.mkv`、`.mp4`、`.avi`、`.rmvb`、`.ts` 文件，支持常见命名：

```text
仙逆 - S01E01.mkv
仙逆.E01.mkv
仙逆 第01集.mp4
[SUB]仙逆 01.mkv
仙逆_01.mkv
```

没有季号时默认按 `S01` 处理。

## 排除关键词

默认排除：

```text
预告, 特典, 花絮, PV, NCOP, NCED
```

也可以通过命令行覆盖：

```bash
python main.py --dir "/path/to/动漫/仙逆" --name "仙逆" --exclude "预告,特典,SP"
```

## 配置文件

可以使用 JSON 配置文件保存常用参数，但 `TMDB_API_KEY` 仍然只从环境变量读取。

```json
{
  "name": "仙逆",
  "exclude_keywords": ["预告", "特典", "花絮", "PV"],
  "video_extensions": [".mkv", ".mp4", ".ts"],
  "season": 1,
  "verbose": true,
  "use_vistamirror_config": false
}
```

运行：

```bash
python main.py --dir "/path/to/动漫/仙逆" --config config.json
```

命令行参数优先级高于配置文件。

TMDB 鉴权优先级：

```text
1. 如果存在 TMDB_API_KEY，优先走独立 v3 模式
2. 否则如果显式启用 --use-vistamirror-config，则读取当前项目保存的 Bearer Token
3. 两者都没有时直接报错
```

## 输出示例

```text
《仙逆》缺失集数检测结果：
  TMDB ID：123456
  总集数：48集
  已有：35集
  缺失：13集
  S01 缺失：第12集、第18集、第25集
```

多季剧会按季分别输出缺失情况。默认忽略 `Season 0` 特别篇；如果确实要统计特别篇，可以加：

```bash
python main.py --dir "/path/to/剧集" --name "某剧" --include-season-0
```

## 常见问题

### 提示缺少 TMDB_API_KEY

先设置环境变量：

```bash
export TMDB_API_KEY="你的_tmdb_api_key"
```

如果你不想单独申请 v3 key，也可以直接复用当前项目已保存的 TMDB Token：

```bash
python main.py --dir "/path/to/剧集" --use-vistamirror-config
```

### 文件没有被识别

使用 `--verbose` 查看每个文件的解析结果。如果文件名不包含 `S01E01`、`E01`、`第01集` 或结尾裸数字这类信息，第一版可能无法判断集号。

### 同名剧匹配不准

第一版默认取 TMDB 搜索结果中最靠前的候选。遇到同名作品时，建议在 `--name` 里加更精确的标题，或后续扩展为手动指定 TMDB ID。
