# 4 个脚本使用说明

## 1. 背景与整体链路

这 4 个 notebook 围绕两条链路工作：

- 主数据链路：先生产 `chunk` 数据，再基于 `chunk` 数据生产 `embedding` 数据。
- 入库链路：
  - `chunk` 数据可导入 OpenSearch，供关键词 / 条件检索使用。
  - `embedding` 数据可导入 Milvus，供向量检索使用。

推荐执行顺序如下：

1. `生产chunk数据.ipynb`
2. `chunk数据导入到opensearch.ipynb`（如果需要 OpenSearch 检索）
3. `生产embedding数据.ipynb`
4. `embedding数据导入到milvus.ipynb`

## 2. 运行前依赖准备

运行 `生产chunk数据.ipynb` 前，需先准备 `admin_ingest` 依赖库。

### 安装步骤

1. 克隆 `sciverse` 仓库，并切到 `dev` 分支：

```bash
git clone -b dev https://gitlab.shlab.tech/sciverse/sciverse.git
```

2. 进入 `admin-ingest` 目录并安装：

```bash
cd sciverse/agentic-search/python_services/admin-ingest
pip install .
```

### 说明

- `admin_ingest` 安装完成后，再运行本目录下的 notebook。
- 如果本机已经有 `sciverse` 仓库，只需要确认当前分支是 `dev`，然后在 `sciverse/agentic-search/python_services/admin-ingest` 目录重新执行一次 `pip install .`。

## 3. 4 个文件分别做什么

| 文件 | 作用 | 主要输入 | 主要输出 |
| --- | --- | --- | --- |
| `生产chunk数据.ipynb` | 从 StarRocks metadata 和 OA 原始 jsonl 生成 chunk 数据 | StarRocks metadata、OA 原始 jsonl | chunk jsonl、error jsonl、summary、进度目录 |
| `chunk数据导入到opensearch.ipynb` | 把 chunk 数据导入 OpenSearch | chunk jsonl | OpenSearch index |
| `生产embedding数据.ipynb` | 基于 chunk 数据调用 embedding 服务，生成 embedding 数据 | chunk jsonl、chunk summary、embedding 服务配置 | embedding jsonl、error jsonl、summary、进度目录 |
| `embedding数据导入到milvus.ipynb` | 把 embedding 数据转成 Parquet 并 bulk import 到 Milvus | embedding jsonl、Milvus 配置、对象存储路径 | Milvus collection、索引、load 后的 collection |

## 4. `生产chunk数据.ipynb`

### 用途

从 StarRocks 读取 metadata，再根据 metadata 中的真实文档路径读取 OA jsonl，最后生成 chunk 数据。

### 用户需要配置的内容

```python
spark_profile = "prod"  # 可选 "test" 或 "prod"

STARROCKS_CONNECTION = {
    "host": "<starrocks-host>",
    "port": 30030,
    "user": "<starrocks-username>",
    "password": "<starrocks-password>",
    "database": "<starrocks-database>",
    "charset": "utf8mb4",
}

STARROCKS_TABLE = "<metadata-table>"
```

```python
CHUNK_OUTPUT_BASE = "s3://<bucket>/<prefix>"

# notebook 会基于输出根目录派生下面这些路径
CHUNK_OUTPUT_ROOT = f"{CHUNK_OUTPUT_BASE}/oa_chunk_data/<version>"
CHUNK_DATA_ROOT = f"{CHUNK_OUTPUT_ROOT}/data"
ERROR_OUTPUT_ROOT = f"{CHUNK_OUTPUT_ROOT}/error"
SOURCE_PATH_PROGRESS_ROOT = f"{CHUNK_OUTPUT_ROOT}/_source_path_progress"
SUMMARY_PATH = f"{CHUNK_OUTPUT_ROOT}/_SUMMARY.json"
```

### 配置说明

- `STARROCKS_CONNECTION`：StarRocks 连接信息，用户重点关注主机、端口、账号、密码、数据库名。
- `STARROCKS_TABLE`：metadata 表名。如果换表，需要同步修改。
- `CHUNK_OUTPUT_ROOT`：chunk 数据总输出目录。
- `CHUNK_DATA_ROOT`：真正的 chunk 数据目录，后续 `chunk数据导入到opensearch.ipynb` 和 `生产embedding数据.ipynb` 都依赖这里。
- `SOURCE_PATH_PROGRESS_ROOT`：断点续跑目录。不要随意删除；删除后可能导致从头重跑。

### 输入输出关系

- 输入不是手工填写一个原始 jsonl 目录。
- 原始 OA 文档路径来自 metadata 字段 `access_xinghe_repository_processed_path`。
- 运行完成后，后续主要使用：
  - `.../data/`
  - `.../_SUMMARY.json`

## 5. `chunk数据导入到opensearch.ipynb`

### 用途

读取 chunk 数据并写入 OpenSearch index。

### 用户需要配置的内容

```python
INPUT_PATH = "s3://<bucket>/<prefix>/oa_chunk_data/<version>/data/"

OS_HOSTS = [
    {"host": "<opensearch-host>", "port": 80}
]
OS_AUTH = ("<opensearch-username>", "<opensearch-password>")
OS_INDEX = "<opensearch-index-name>"

RECREATE_INDEX = True  # 当前 notebook 默认值；如不想删旧索引，请改成 False
```

### 配置说明

- `INPUT_PATH`：必须指向 `生产chunk数据.ipynb` 产出的 `data/` 目录。
- `OS_HOSTS`：OpenSearch 地址，可以写域名或 IP。
- `OS_AUTH`：OpenSearch 用户名和密码。
- `OS_INDEX`：目标索引名。
- `RECREATE_INDEX`：
  - `False`：复用已有索引。
  - `True`：先删除已有索引，再重新创建并导入。这个操作有破坏性。

### 输出结果

- 这个 notebook 不会再写新的 S3 业务结果目录。
- 结果直接写入 OpenSearch 的 `OS_INDEX`。

## 6. `生产embedding数据.ipynb`

### 用途

读取 chunk 数据，调用 embedding 服务，输出带向量的 embedding 数据。

### 用户需要配置的内容

```python
ENV_PATH = "/path/to/.env"

INPUT_ROOT = "s3://<bucket>/<prefix>/oa_chunk_data/<version>/data/"
INPUT_SUMMARY_PATH = "s3://<bucket>/<prefix>/oa_chunk_data/<version>/_SUMMARY.json"

OUTPUT_ROOT = "s3://<bucket>/<prefix>/oa_chunk_embedding/<version>"
PROGRESS_ROOT = f"{OUTPUT_ROOT}/_progress"
ERROR_ROOT = f"{OUTPUT_ROOT}/error"
SUMMARY_PATH = f"{OUTPUT_ROOT}/_summary.json"
```

需要准备好可用的 `.env` 文件，供 notebook 加载 embedding 相关配置。

### 配置说明

- `ENV_PATH`：`.env` 文件路径，notebook 会优先从这里加载 embedding 配置；`.env` 文件需要放在 `sciverse/agentic-search` 目录下。若使用远程 embedding API，接口地址需要写在 `.env` 的 `EMBEDDING_API_BASE_URL` 字段。
- `INPUT_ROOT`：chunk 数据输入目录，对应第 1 个 notebook 的 `data/`。
- `INPUT_SUMMARY_PATH`：chunk 汇总文件，对应第 1 个 notebook 的 `_SUMMARY.json`。
- `OUTPUT_ROOT`：embedding 数据总输出目录。
- `PROGRESS_ROOT`：断点续跑目录。
- `ERROR_ROOT`：embedding 失败记录目录。
- `SUMMARY_PATH`：embedding 任务汇总文件。

### 运行注意事项

- 默认是续跑模式，会利用 `_progress` 目录继续处理未完成的数据。
- 如果要全量重跑，先确认是否需要清理旧的 `OUTPUT_ROOT` 和 `_progress`。
- 后续 `embedding数据导入到milvus.ipynb` 主要依赖这里的 `.../data/` 输出。

## 7. `embedding数据导入到milvus.ipynb`

### 用途

读取 embedding 数据，先写 Parquet 到对象存储，再通过 Milvus bulk import 导入 collection，最后建索引并 load。

### 用户需要配置的内容

```python
SOURCE_ROOT = [
    "s3://<bucket>/<prefix>/oa_chunk_embedding/<version>/data/"
]
```

```python
MILVUS_URI = "http://<milvus-host>:<port>"
MILVUS_USERNAME = "<milvus-username>"
MILVUS_PASSWORD = "<milvus-password>"
MILVUS_DB_NAME = "<milvus-db-name>"
MILVUS_COLLECTION = "<milvus-collection-name>"
MILVUS_API_KEY = "<milvus-token-or-user:password>"
```

```python
PARQUET_OUTPUT_S3_PREFIX = "s3://<milvus-visible-bucket>/<prefix>"
IMPORT_KEY_PREFIX = "<object-storage-key-prefix>"

RUN_ID = "<unique-run-id>"
IMPORT_PROGRESS_FILE = "./milvus_import_progress_<RUN_ID>.json"
```

```python
RECREATE_COLLECTION = True   # 当前 notebook 默认值
RECREATE_INDEXES_AFTER_IMPORT = True   # 当前 notebook 默认值
```

### 配置说明

- `SOURCE_ROOT`：必须指向 `生产embedding数据.ipynb` 生成的 `data/` 目录。
- `MILVUS_URI`：Milvus HTTP 地址。
- `MILVUS_USERNAME` / `MILVUS_PASSWORD`：Milvus 登录账号密码。
- `MILVUS_DB_NAME`：目标数据库名。
- `MILVUS_COLLECTION`：目标 collection 名。
- `MILVUS_API_KEY`：有些部署方式直接用 token；常见格式是 `user:password`。
- `PARQUET_OUTPUT_S3_PREFIX`：中间 Parquet 文件写到哪里。这个位置必须是 Milvus bulk import 可以访问到的对象存储目录。
- `IMPORT_KEY_PREFIX`：bulk import 使用的对象存储 key 前缀，通常应与 `PARQUET_OUTPUT_S3_PREFIX` 对应。
- `RUN_ID`：本次导入任务的唯一标识，会影响中间输出目录和断点文件名。换一轮新导入，建议换新的 `RUN_ID`。
- `IMPORT_PROGRESS_FILE`：本地断点文件，用于恢复 import 进度。
- `RECREATE_COLLECTION`：如果设为 `True`，已存在的 collection 会被删除后重建，属于破坏性操作。
- `RECREATE_INDEXES_AFTER_IMPORT`：如果设为 `True`，已有索引可能被删除后重建。

### 运行注意事项

- 这个 notebook 会先写一份中间 Parquet 到对象存储，然后 Milvus 再从对象存储拉取导入。
- 如果更换了 `SOURCE_ROOT`、`MILVUS_COLLECTION` 或 import 文件集合，建议同步更换 `RUN_ID`，避免复用旧 checkpoint。
- 如果只想先做小规模验证，可以先看 notebook 里的 smoke test 配置，再决定是否开启。

## 8. 最小执行关系

如果只关心主数据生产链路：

1. 运行 `生产chunk数据.ipynb`
2. 运行 `生产embedding数据.ipynb`

如果还需要构建检索库：

1. 用 `chunk数据导入到opensearch.ipynb` 导入 chunk 检索数据
2. 用 `embedding数据导入到milvus.ipynb` 导入向量检索数据
