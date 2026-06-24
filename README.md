# 5 个脚本使用说明

## 1. 背景与整体链路

这 5 个 notebook 围绕三条链路工作：

- 全文数据链路：从 StarRocks metadata 和 OA 原始 jsonl 生成按 OA / 非 OA 拆分的全文数据。
- 主数据链路：先生产 `chunk` 数据，再基于 `chunk` 数据生产 `embedding` 数据。
- 入库链路：
  - `chunk` 数据可导入 OpenSearch，供关键词 / 条件检索使用。
  - `embedding` 数据可导入 Milvus，供向量检索使用。

推荐执行顺序如下：

1. `生产全文数据.ipynb`
2. `生产chunk数据.ipynb`
3. `chunk数据导入到opensearch.ipynb`（如果需要 OpenSearch 检索）
4. `生产embedding数据.ipynb`
5. `embedding数据导入到milvus.ipynb`

## 2. 运行前依赖准备

当前只有 `生产chunk数据.ipynb` 运行前需先准备 `admin_ingest` 依赖库。

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

## 3. 5 个文件分别做什么

| 文件 | 作用 | 主要输入 | 主要输出 |
| --- | --- | --- | --- |
| `生产chunk数据.ipynb` | 从 StarRocks metadata 和 OA 原始 jsonl 生成 chunk 数据 | StarRocks metadata、OA 原始 jsonl | chunk jsonl、error jsonl、summary、进度目录 |
| `chunk数据导入到opensearch.ipynb` | 把 chunk 数据导入 OpenSearch | chunk jsonl | OpenSearch index |
| `生产embedding数据.ipynb` | 基于 chunk 数据调用 embedding 服务，生成 embedding 数据 | chunk jsonl、chunk summary、embedding 服务配置 | embedding jsonl、error jsonl、summary、进度目录 |
| `embedding数据导入到milvus.ipynb` | 把 embedding 数据转成 Parquet 并 bulk import 到 Milvus | embedding jsonl、Milvus 配置、对象存储路径 | Milvus collection、索引、load 后的 collection |
| `生产全文数据.ipynb` | 从 StarRocks metadata 和 OA 原始 jsonl 生成全文数据，并按 OA / 非 OA 分目录输出 | StarRocks metadata、OA 原始 jsonl | `oa/<sha256>/doc.json`、`others/<sha256>/doc.json`、summary、进度目录、慢路径队列 |

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

## 8. `生产全文数据.ipynb`

### 用途

从 StarRocks 读取 metadata，再根据 metadata 中的真实文档路径读取 OA jsonl，输出按 OA / 非 OA 分目录存放的全文 `doc.json` 数据。

### 用户需要配置的内容

```python
STARROCKS_CONNECTION = {
    "host": "<starrocks-host>",
    "port": 30030,
    "user": "<starrocks-username>",
    "password": "<starrocks-password>",
    "database": "<starrocks-database>",
    "charset": "utf8mb4",
}

STARROCKS_TABLE = "<metadata-table>"

RUN_MODE = "prod"  # "prod" or "test"

PROD_OUTPUT_ROOT = "s3://<bucket>/<prefix>/raw-content/<version>"
TEST_OUTPUT_ROOT = "s3://<bucket>/<prefix>/raw-content_test/<version>"

TEST_ROW_LIMIT = 20
TEST_TARGET_SHA256S = [
    "<sha256-for-test>",
]

OUTPUT_ROOT = PROD_OUTPUT_ROOT if RUN_MODE == "prod" else TEST_OUTPUT_ROOT
OA_ROOT = f"{OUTPUT_ROOT}/oa"
OTHERS_ROOT = f"{OUTPUT_ROOT}/others"
SUMMARY_PATH = f"{OUTPUT_ROOT}/summary.json"
ROW_PROGRESS_ROOT = f"{OUTPUT_ROOT}/_row_progress".replace("s3://", "s3a://", 1)
BUCKET_PROGRESS_ROOT = f"{OUTPUT_ROOT}/_bucket_progress"
```

```python
BATCH_SIZE = 140000 if RUN_MODE == "prod" else TEST_ROW_LIMIT
OVERWRITE_EXISTING_DOCS = False

RUN_MAIN_FLOW = True
RUN_SLOW_PATH = True
```

### 配置说明

- `STARROCKS_CONNECTION`：StarRocks 连接信息，用户重点关注主机、端口、账号、密码、数据库名。
- `STARROCKS_TABLE`：metadata 表名。
- `RUN_MODE`：`prod` 表示正式全量跑；`test` 表示小样本验证模式。
- `PROD_OUTPUT_ROOT` / `TEST_OUTPUT_ROOT`：不同模式下的输出根目录。测试模式建议单独使用测试目录，避免污染正式结果。
- `OA_ROOT`：`access_is_oa = true` 的全文输出目录；单条文档会写成 `oa/<sha256>/doc.json`。
- `OTHERS_ROOT`：非 OA 文档输出目录；单条文档会写成 `others/<sha256>/doc.json`。
- `SUMMARY_PATH`：最终汇总文件，包含 `total_count`、`oa_count`、`non_oa_count`。
- `ROW_PROGRESS_ROOT`：按批次写入的断点目录。notebook 会把这里转成 `s3a://` 供 Spark 访问，通常不需要手动改动派生逻辑。
- `BUCKET_PROGRESS_ROOT`：按 sha bucket 记录游标的断点目录，用于续跑。
- `TEST_ROW_LIMIT` / `TEST_TARGET_SHA256S`：只在 `test` 模式下使用，用于限制测试样本量或固定测试样本。
- `BATCH_SIZE`：主流程每批拉取多少条 metadata。通常保留默认值即可，只有在调优吞吐时才需要修改。
- `OVERWRITE_EXISTING_DOCS`：是否覆盖已存在的 `doc.json`。默认 `False`，已有结果会被跳过。
- `RUN_MAIN_FLOW`：是否执行主流程。要正式开始生产全文数据，这里必须设为 `True`。
- `RUN_SLOW_PATH`：是否继续处理慢路径队列。通常在主流程之后保持 `True`，用于补完大文件 / 慢文件。

### 输入输出关系与运行注意事项

- 输入不是手工填写一个原始 jsonl 目录。
- 原始 OA 文档路径来自 metadata 字段 `access_xinghe_repository_processed_path`。
- 新跑一轮正式任务时，建议：
  - 设 `RUN_MAIN_FLOW = True`
  - 保持 `RUN_SLOW_PATH = True`
  - 确认 `OUTPUT_ROOT` 指向新的或可续跑的目标目录
- notebook 还会在 `OUTPUT_ROOT` 下派生 `_slow_path_queue`、`_oversized_path_queue`、`_row_progress`、`_bucket_progress` 等目录，分别用于慢路径、大文件和断点续跑；不要随意删除，否则可能影响续跑。
- 如果只想做小规模验证，建议切到 `RUN_MODE = "test"`，同时单独配置 `TEST_OUTPUT_ROOT`，并按需调整 `TEST_ROW_LIMIT` 或 `TEST_TARGET_SHA256S`。

## 9. 最小执行关系

如果只关心全文原文链路：

1. 运行 `生产全文数据.ipynb`

如果只关心主数据生产链路：

1. 运行 `生产chunk数据.ipynb`
2. 运行 `生产embedding数据.ipynb`

如果还需要构建检索库：

1. 用 `chunk数据导入到opensearch.ipynb` 导入 chunk 检索数据
2. 用 `embedding数据导入到milvus.ipynb` 导入向量检索数据
