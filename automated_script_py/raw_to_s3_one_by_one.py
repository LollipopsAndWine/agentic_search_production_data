# Auto-generated from automated_script/raw_to_s3_one_by_one.ipynb

# %% [code cell 1]
config = {
    "file_config": {
        "prod_output_root": "",  # 正式输出根目录
        "test_output_root": "",  # 测试输出根目录
    },
    "db_config": {
        "starrocks": {
            "host": "",
            "port": 30030,
            "user": "",
            "password": ""
        }
    },
    "custom_config": {
        "sql": "",
        "run_mode": "prod",  # "prod" 全量模式；"test" 小样本模式
        "run_main_flow": True,  # 是否执行主流程：拉 metadata、读取原文并写出 doc.json
        "run_slow_path": True,  # 是否继续处理慢路径队列：补跑大文件 / 慢文件
        "overwrite_existing_docs": False,  # True 时覆盖已存在 doc.json
        "preview_rows": 5,  # 预览下一批 metadata 条数
        "enable_driver_path_existence_check": False,  # 主流程是否先在 driver 侧判断输出是否已存在
        "disable_boto_response_checksum_validation": True,  # 兼容部分对象存储响应校验
        "batch_size_prod": 120000,  # prod 模式主流程每个 sha bucket 的 metadata 行数上限；None/0 表示由 sql 控制，不额外分批
        "rows_per_partition_prod": 1000,  # prod 模式目标每分区行数
        "target_batch_partitions_prod": 160,  # prod 模式目标分区数
        "max_batch_partitions_prod": 256,  # prod 模式最大分区数
        "sha_bucket_count_prod": 256,  # prod 模式 sha bucket 数
        "max_s3_connections_prod": 64,  # prod 模式 boto 最大连接数
        "target_global_read_concurrency_prod": 320,  # prod 模式目标全局读并发
        "target_global_write_concurrency_prod": 480,  # prod 模式目标全局写并发
        "slow_path_bytes_threshold_prod": 50 * 1024 * 1024,  # prod 模式慢路径字节阈值
        "oversized_bytes_threshold_prod": 512 * 1024 * 1024,  # prod 模式超大文档阈值
        "slow_path_batch_size_prod": 500,  # prod 模式慢路径每批行数
        "slow_path_read_threads": None,  # 慢路径读线程数，None 表示自动
        "slow_path_write_threads": None,  # 慢路径写线程数，None 表示自动
        "slow_path_force_driver_exists_check": True,  # 慢路径是否强制先检查目标文件是否已存在
        "driver_write_max_pending_rows_prod": 256,  # prod 模式触发 driver 直写的最大待处理行数
        "driver_write_max_read_threads_prod": 16,  # prod 模式 driver 直写最大读线程数
        "driver_write_max_write_threads_prod": 32,  # prod 模式 driver 直写最大写线程数
        "min_read_threads_per_partition_prod": 2,  # prod 模式单分区最小读线程数
        "min_write_threads_per_partition_prod": 2,  # prod 模式单分区最小写线程数
        "max_read_threads_per_partition_prod": 6,  # prod 模式单分区最大读线程数
        "max_write_threads_per_partition_prod": 8,  # prod 模式单分区最大写线程数
        "iceberg_path": "s3://lakehouse-iceberg/",
    }
}

# %% [code cell 2]
import os
os.environ["SPARK_USER"] = "renpengli"
os.environ.setdefault("XINGHE_CONF_DIR", "/share/renpengli/conf")
import bz2
import boto3
from collections import defaultdict
import gzip
import heapq
import io
import json
import math
import re
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import urlsplit, urlunsplit
from botocore.client import Config as BotocoreConfig
from pyspark import SparkConf, StorageLevel
from pyspark.sql import SparkSession, Row
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, MapType, ArrayType, LongType
from xinghe.utils.json_util import *
from xinghe.spark import *
from xinghe.s3 import *
from xinghe.s3 import S3LineWriter, delete_s3_object, put_s3_object, read_s3_object, read_s3_row, read_s3_rows
config = json_loads('''${config}''')
custom_config = config["custom_config"]
file_config = config["file_config"]
s3_config = config.get("s3_config", {})
db_config = config.get("db_config", {})
starrocks_config = db_config["starrocks"]
STARROCKS_CONNECTION = {
    **starrocks_config,
    "database": "ads",
    "charset": "utf8mb4",
}
BASE_META_SQL = custom_config["sql"].strip()
if not BASE_META_SQL:
    raise ValueError("custom_config.sql is required")
RUN_MODE = custom_config["run_mode"]
RUN_MAIN_FLOW = custom_config["run_main_flow"]
RUN_SLOW_PATH = custom_config["run_slow_path"]
PROD_OUTPUT_ROOT = file_config["prod_output_root"]
TEST_OUTPUT_ROOT = file_config["test_output_root"]
OVERWRITE_EXISTING_DOCS = custom_config["overwrite_existing_docs"]
PREVIEW_ROWS = int(custom_config["preview_rows"])
ENABLE_DRIVER_PATH_EXISTENCE_CHECK = custom_config["enable_driver_path_existence_check"]
DISABLE_BOTO_RESPONSE_CHECKSUM_VALIDATION = custom_config["disable_boto_response_checksum_validation"]
if RUN_MODE not in {"prod", "test"}:
    raise ValueError(f"RUN_MODE 只支持 'prod' 或 'test'，当前值: {RUN_MODE!r}")
OUTPUT_ROOT = PROD_OUTPUT_ROOT if RUN_MODE == "prod" else TEST_OUTPUT_ROOT
def get_s3_args(path, s3_config):
    bucket = split_s3_path(path)[0]
    s3_args = s3_config.get(bucket, {})
    if not s3_args:
        s3_args = get_s3_config(path)
    print(f"bucket={bucket}, s3_args={s3_args}")
    return bucket, s3_args
write_s3_bucket, write_s3_config = get_s3_args(OUTPUT_ROOT, s3_config)
iceberg_path = custom_config["iceberg_path"]
read_iceberg_bucket, read_iceberg_config = get_s3_args(iceberg_path, s3_config)
OA_ROOT = f"{OUTPUT_ROOT}/oa"
OTHERS_ROOT = f"{OUTPUT_ROOT}/others"
SUMMARY_PATH = f"{OUTPUT_ROOT}/summary.json"
ROW_PROGRESS_ROOT = f"{OUTPUT_ROOT}/_row_progress".replace("s3://", "s3a://", 1)
BUCKET_PROGRESS_ROOT = f"{OUTPUT_ROOT}/_bucket_progress"
BATCH_COMMIT_ROOT = f"{OUTPUT_ROOT}/_batch_commits"
BAD_QUEUE_FILE_ROOT = f"{OUTPUT_ROOT}/_bad_queue_files"
def normalize_optional_positive_int(value):
    if value is None:
        return None
    text = str(value).strip()
    if not text or text.lower() in {"none", "null", "false"}:
        return None
    parsed_value = int(value)
    return parsed_value if parsed_value > 0 else None
BATCH_SIZE = normalize_optional_positive_int(custom_config.get("batch_size_prod"))
ROWS_PER_PARTITION = int(custom_config["rows_per_partition_prod"])
TARGET_BATCH_PARTITIONS = int(custom_config["target_batch_partitions_prod"])
MAX_BATCH_PARTITIONS = int(custom_config["max_batch_partitions_prod"])
SHA_BUCKET_COUNT = int(custom_config["sha_bucket_count_prod"])
MAX_S3_CONNECTIONS = int(custom_config["max_s3_connections_prod"])
TARGET_GLOBAL_READ_CONCURRENCY = int(custom_config["target_global_read_concurrency_prod"])
TARGET_GLOBAL_WRITE_CONCURRENCY = int(custom_config["target_global_write_concurrency_prod"])
SLOW_PATH_QUEUE_ROOT = f"{OUTPUT_ROOT}/_slow_path_queue"
SLOW_PATH_BYTES_THRESHOLD = int(custom_config["slow_path_bytes_threshold_prod"])
OVERSIZED_PATH_QUEUE_ROOT = f"{OUTPUT_ROOT}/_oversized_path_queue"
OVERSIZED_BYTES_THRESHOLD = int(custom_config["oversized_bytes_threshold_prod"])
SLOW_PATH_BATCH_SIZE = int(custom_config["slow_path_batch_size_prod"])
SLOW_PATH_QUEUE_SHARD_ROWS = SLOW_PATH_BATCH_SIZE
SLOW_PATH_TARGET_PARTITIONS = min(TARGET_BATCH_PARTITIONS, 32)
SLOW_PATH_READ_THREADS = custom_config["slow_path_read_threads"]
SLOW_PATH_WRITE_THREADS = custom_config["slow_path_write_threads"]
SLOW_PATH_FORCE_DRIVER_EXISTS_CHECK = custom_config["slow_path_force_driver_exists_check"]
DRIVER_WRITE_MAX_PENDING_ROWS = int(custom_config["driver_write_max_pending_rows_prod"])
DRIVER_WRITE_MAX_READ_THREADS = min(MAX_S3_CONNECTIONS, int(custom_config["driver_write_max_read_threads_prod"]))
DRIVER_WRITE_MAX_WRITE_THREADS = min(MAX_S3_CONNECTIONS, int(custom_config["driver_write_max_write_threads_prod"]))
MIN_READ_THREADS_PER_PARTITION = min(MAX_S3_CONNECTIONS, int(custom_config["min_read_threads_per_partition_prod"]))
MIN_WRITE_THREADS_PER_PARTITION = min(MAX_S3_CONNECTIONS, int(custom_config["min_write_threads_per_partition_prod"]))
MAX_READ_THREADS_PER_PARTITION = min(MAX_S3_CONNECTIONS, int(custom_config["max_read_threads_per_partition_prod"]))
MAX_WRITE_THREADS_PER_PARTITION = min(MAX_S3_CONNECTIONS, int(custom_config["max_write_threads_per_partition_prod"]))

REAL_DOC_FIELDS = [
    "track_id", "sha256", "process_status", "processed_path", "origin_url", "origin_path",
    "file_format", "file_type", "content_type", "content_length", "page_cnt", "is_broken",
    "obtain_timestamp", "language", "title", "author", "abstract", "category", "major",
    "major_2", "major_3", "source", "db_source", "subject", "doi", "keyword", "isbn",
    "issn", "issn_p", "issn_e", "magazine", "pub_time", "volume", "area", "grade_class",
    "grade", "data_date", "dt", "start_ts", "content_list", "ali_pdf_path", "theme",
    "sub_path", "page_index_range", "content", "lang", "model_name", "model_version",
    "model_process_ts", "pred_major", "pred_major2", "pred_major3", "ext", "fail_msg",
    "end_ts", "cost_sec", "id", "doc_loc", "doc_id"
]
ROW_PROGRESS_SCHEMA = T.StructType([
    T.StructField("source_sha256", T.StringType(), True),
    T.StructField("processed_path", T.StringType(), True),
    T.StructField("access_is_oa", T.StringType(), True),
    T.StructField("batch_index", T.LongType(), True),
    T.StructField("completed_at", T.StringType(), True),
])

# %% [code cell 3]
spark_config = {
    "spark.driver.memory": "8g",
    "spark.driver.maxResultSize": "2g",
    "spark.executor.memory": "12g",
    "spark.executor.memoryOverhead": "4g",
    "spark.executor.cores": "4",
    "spark.executor.instances": "250",
    "spark.network.timeout": "800s",
    "spark.kubernetes.executor.limit.cores": "8",  # spark.kubernetes.executor.limit.cores 是对cpu的限制
    "spark.default.parallelism": "4000",
    "spark.ui.enabled": "false",
    "spark.ui.retainedJobs": "20",
    "spark.ui.retainedStages": "20",
    "spark.ui.retainedTasks": "1000",
    "spark.sql.ui.retainedExecutions": "20",
    "spark.speculation.multiplier": "1.5",
    "spark.speculation.quantile": "0.90",
    "spark.speculation.minTaskRuntime": "60000",
    #sparksql 配置
    "spark.sql.sources.partitionOverwriteMode":"dynamic",
    "spark.sql.parquet.compression.codec":"snappy",
    "spark.sql.files.maxRecordsPerFile": 50000,
    "spark.sql.adaptive.enabled":"true",
    "spark.sql.shuffle.partitions":"4096",
    "spark.sql.adaptive.coalescePartitions.enabled":"true",
    "spark.sql.adaptive.advisoryPartitionSizeInBytes":"256MB",
    "spark.sql.autoBroadcastJoinThreshold":"-1",
    "spark.sql.adaptive.shuffle.targetPostShuffleInputSize":"67108864",
    "spark.dynamicAllocation.minExecutors": "50",
    "spark.dynamicAllocation.initialExecutors": "100",
    "spark.dynamicAllocation.maxExecutors": "400",
    # spark 其他配置
    "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
    "spark.speculation": "true",
    "spark.executorEnv.SPARK_USER": "renpengli",
    # hadoop s3a 配置
    "spark.hadoop.fs.s3a.impl":"org.apache.hadoop.fs.s3a.S3AFileSystem",
    "spark.hadoop.fs.s3a.connection.ssl.enabled": "false",
    "spark.hadoop.fs.s3a.path.style.access": "true",
    "spark.hadoop.fs.s3a.endpoint": write_s3_config["endpoint"],
    "spark.hadoop.fs.s3a.access.key": write_s3_config["ak"],
    "spark.hadoop.fs.s3a.secret.key": write_s3_config["sk"],
    "spark.hadoop.fs.s3a.multiobjectdelete.enable":"false",
    "spark.hadoop.fs.s3a.directory.marker.retention":"keep",
    "spark.hadoop.fs.s3a.fast.upload":"true",
    "spark.hadoop.fs.s3a.connection.maximum":"1000",
    "spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version":"2",
    # kubernetes 配置
    "spark.kubernetes.executor.deleteOnTermination":"false",
    "spark.submit.deployMode": "cluster",
    "spark.kubernetes.namespace": "dataops-paas",
    "spark.kubernetes.authenticate.driver.serviceAccountName": "spark-default",
    "spark.kubernetes.container.image.pullPolicy": "Always",
    "spark.kubernetes.container.image": "registry.sensetime.com/hadoop/dataops/apache/spark:3.5.7-data-platform",
    # iceberg 配置
    "spark.sql.defaultCatalog": "iceberg-dataops",
    "spark.sql.catalog.iceberg-dataops.uri": "thrift://pjlab-dataproducer-hive-metastore.pjlab-dataproducer.svc.cluster.local:9083",
    "spark.sql.catalog.iceberg-dataops.warehouse": "s3a://lakehouse-iceberg/",
    "spark.sql.catalog.iceberg-dataops": "org.apache.iceberg.spark.SparkCatalog",
    "spark.sql.catalog.iceberg-dataops.type": "hive",
    "spark.sql.catalog.iceberg-dataops.hadoop.fs.s3a.aws.credentials.provider": "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
    "spark.sql.catalog.iceberg-dataops.hadoop.fs.s3a.access.key": read_iceberg_config["ak"],
    "spark.sql.catalog.iceberg-dataops.hadoop.fs.s3a.secret.key": read_iceberg_config["sk"],
    "spark.sql.catalog.iceberg-dataops.hadoop.fs.s3a.endpoint": read_iceberg_config["endpoint"],
    "spark.sql.catalog.iceberg-dataops.hadoop.fs.s3a.fast.upload": "true",
    "spark.sql.catalog.iceberg-dataops.hadoop.fs.s3a.path.style.access": "true",
    "spark.sql.catalog.iceberg-dataops.hadoop.fs.s3a.connection.ssl.enabled": "false",
    "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",  # 创建bloom索引必须得参数，需iceberg版本大于2.0
    # event log 配置
    "spark.eventLog.enabled": "true",
    "spark.eventLog.dir": "/spark/eventLog",
    # Magic Committer（推荐 - 不需要临时目录，性能最佳）
    # 参考: https://hadoop.apache.org/docs/current/hadoop-aws/tools/hadoop-aws/committers.html
    "spark.jars":"/share/spark-jars/spark-hadoop-cloud_2.12-3.5.7.jar",
    "spark.hadoop.fs.s3a.committer.name": "magic",
    "spark.hadoop.fs.s3a.committer.magic.enabled": "true" ,
    "spark.hadoop.mapreduce.outputcommitter.factory.scheme.s3a": "org.apache.hadoop.fs.s3a.commit.S3ACommitterFactory",
    "spark.sql.sources.commitProtocolClass": "org.apache.spark.internal.io.cloud.PathOutputCommitProtocol",
    "spark.sql.parquet.output.committer.class": "org.apache.spark.internal.io.cloud.BindingParquetOutputCommitter",
    "spark.kubernetes.executor.podTemplateFile": "/share/renpengli/pod-template/pod-template-dolphin.yaml",
    "spark.kubernetes.executor.label.queue": "root.datacenter.data-producer.default",
    "spark.driver.host": os.environ.get("POD_IP"),
    "spark.submit.deployMode": "client",
}   

conf = SparkConf() 
conf.setAll(list(spark_config.items()))

# 初始化Spark
master = "k8s://https://{kubernetes_service_host}:{kubernetes_service_port}".format(kubernetes_service_host = os.environ.get("KUBERNETES_SERVICE_HOST"), 
        kubernetes_service_port = os.environ.get("KUBERNETES_SERVICE_PORT"))
tt = int(time.time())
spark = SparkSession.builder.master(master).config(conf=conf).appName(f"raw_to_s3_one_by_one_{tt}").getOrCreate()
sc = spark.sparkContext
sc.setLogLevel("ERROR")
print(spark)

# %% [code cell 4]
def ensure_s3a_path(path):
    if path.startswith("s3://"):
        return "s3a://" + path[len("s3://"):]
    return path
def strip_bytes_suffix(path):
    parts = urlsplit(path)
    return urlunsplit((parts.scheme, parts.netloc, parts.path, "", ""))
def normalize_bool_text(value):
    return "true" if str(value).strip().lower() == "true" else "false"
def quote_sql(value):
    return "'" + str(value).replace("\\", "\\\\").replace("'", "\\'") + "'"
def log_warning(message):
    print(f"[WARN] {message}")
def parse_bytes_length_from_processed_path(processed_path):
    text = str(processed_path or "").strip()
    if "?bytes=" not in text:
        return None
    try:
        _, bytes_part = text.split("?bytes=", 1)
        _, length_str = bytes_part.split(",", 1)
        return int(length_str)
    except Exception:
        return None
def classify_processed_path(processed_path):
    text = str(processed_path or "").strip()
    if "?bytes=" in text:
        byte_len = parse_bytes_length_from_processed_path(text)
        if byte_len is None:
            return "bytes"
        return f"bytes:{int(byte_len / (1024 * 1024))}MB"
    if text.endswith(".jsonl.gz"):
        return "jsonl.gz"
    if text.endswith(".jsonl.bz2"):
        return "jsonl.bz2"
    if text.endswith(".jsonl"):
        return "jsonl"
    if text.endswith(".gz"):
        return "gz"
    if text.endswith(".bz2"):
        return "bz2"
    return "other"
def summarize_processed_path_types(meta_rows):
    summary = defaultdict(int)
    for meta_row in meta_rows:
        summary[classify_processed_path(meta_row.get("processed_path"))] += 1
    return dict(sorted(summary.items(), key=lambda item: (-item[1], item[0])))
def summarize_result_rows(result_rows, top_n=5):
    top_n = max(1, int(top_n or 1))
    written_rows = [row for row in result_rows if row.get("status") == "written"]
    sorted_rows = sorted(
        written_rows,
        key=lambda row: (
            float(row.get("read_seconds") or 0.0) + float(row.get("write_seconds") or 0.0),
            float(row.get("read_seconds") or 0.0),
            float(row.get("write_seconds") or 0.0),
        ),
        reverse=True,
    )
    return [
        {
            "source_sha256": row.get("source_sha256"),
            "processed_path": row.get("processed_path"),
            "path_type": classify_processed_path(row.get("processed_path")),
            "read_seconds": float(row.get("read_seconds") or 0.0),
            "write_seconds": float(row.get("write_seconds") or 0.0),
            "total_seconds": float(row.get("read_seconds") or 0.0) + float(row.get("write_seconds") or 0.0),
        }
        for row in sorted_rows[:top_n]
    ]
def should_scan_processed_path_by_sha(processed_path):
    text = str(processed_path or "").strip()
    return ("?bytes=" not in text) and text.endswith((".jsonl", ".jsonl.gz", ".jsonl.bz2"))
def build_boto_s3_config(disable_response_checksum_validation=False):
    config_kwargs = {
        "s3": {"addressing_style": "path"},
        "retries": {"max_attempts": 8, "mode": "standard"},
        "connect_timeout": 600,
        "read_timeout": 600,
        "max_pool_connections": MAX_S3_CONNECTIONS,
    }
    if disable_response_checksum_validation:
        config_kwargs["request_checksum_calculation"] = "when_required"
        config_kwargs["response_checksum_validation"] = "when_required"
    try:
        return BotocoreConfig(**config_kwargs)
    except TypeError:
        config_kwargs.pop("request_checksum_calculation", None)
        config_kwargs.pop("response_checksum_validation", None)
        try:
            return BotocoreConfig(**config_kwargs)
        except TypeError:
            config_kwargs["retries"] = {"max_attempts": 8}
            return BotocoreConfig(**config_kwargs)
def get_boto_s3_client(path, outside=False, disable_response_checksum_validation=False):
    s3_config = get_s3_config(path, outside)
    return boto3.client(
        "s3",
        aws_access_key_id=s3_config["ak"],
        aws_secret_access_key=s3_config["sk"],
        endpoint_url=s3_config["endpoint"],
        config=build_boto_s3_config(disable_response_checksum_validation=disable_response_checksum_validation),
    )
def get_boto_s3_client_from_args(s3_args, disable_response_checksum_validation=False):
    return boto3.client(
        "s3",
        aws_access_key_id=s3_args["ak"],
        aws_secret_access_key=s3_args["sk"],
        endpoint_url=s3_args["endpoint"],
        config=build_boto_s3_config(disable_response_checksum_validation=disable_response_checksum_validation),
    )
class ClientCache:
    def __init__(self, disable_response_checksum_validation=DISABLE_BOTO_RESPONSE_CHECKSUM_VALIDATION):
        self.cache = {}
        self.disable_response_checksum_validation = disable_response_checksum_validation
    def get_client(self, path):
        bucket, _ = split_s3_path(path)
        cache_key = (bucket, self.disable_response_checksum_validation)
        if not bucket:
            return get_boto_s3_client(
                path,
                disable_response_checksum_validation=self.disable_response_checksum_validation,
            )
        if cache_key not in self.cache:
            if bucket == write_s3_bucket:
                self.cache[cache_key] = boto3.client(
                    "s3",
                    aws_access_key_id=write_s3_config["ak"],
                    aws_secret_access_key=write_s3_config["sk"],
                    endpoint_url=write_s3_config["endpoint"],
                    config=build_boto_s3_config(disable_response_checksum_validation=self.disable_response_checksum_validation),
                )
            else:
                self.cache[cache_key] = get_boto_s3_client(
                    path,
                    disable_response_checksum_validation=self.disable_response_checksum_validation,
                )
        return self.cache[cache_key]
def starrocks_query(sql):
    rows = spark.sql(sql).collect()
    return [row.asDict(recursive=True) for row in rows]
def hdfs_path_exists(path):
    jvm = spark._jvm
    hconf = spark._jsc.hadoopConfiguration()
    path_obj = jvm.org.apache.hadoop.fs.Path(ensure_s3a_path(path))
    return path_obj.getFileSystem(hconf).exists(path_obj)
def hdfs_list_files(path):
    jvm = spark._jvm
    hconf = spark._jsc.hadoopConfiguration()
    path_obj = jvm.org.apache.hadoop.fs.Path(ensure_s3a_path(path))
    fs = path_obj.getFileSystem(hconf)
    if not fs.exists(path_obj):
        return []
    result = []
    for status in fs.listStatus(path_obj):
        name = status.getPath().getName()
        if status.isFile() and not name.startswith("_") and not name.startswith("."):
            result.append(str(status.getPath()))
    return sorted(result)
def target_path_for_sha(target_root, sha256):
    return f"{target_root.rstrip('/')}/{sha256}/doc.json"
def target_relative_path_for_result_row(result_row):
    target_path = str(result_row["target_path"])
    if target_path.startswith(f"{OA_ROOT}/"):
        return OA_ROOT, target_path[len(f"{OA_ROOT}/"):]
    if target_path.startswith(f"{OTHERS_ROOT}/"):
        return OTHERS_ROOT, target_path[len(f"{OTHERS_ROOT}/"):]
    raise ValueError(f"unexpected target_path={target_path}")
def progress_output_path(batch_index):
    root = ROW_PROGRESS_ROOT.replace("s3a://", "s3://", 1).rstrip("/")
    return f"{root}/batch-{int(batch_index):08d}-{uuid.uuid4().hex}.jsonl"
def bucket_progress_path(bucket_id):
    return f"{BUCKET_PROGRESS_ROOT.rstrip('/')}/bucket-{int(bucket_id):04d}.json"
def parse_progress_batch_index(path):
    name = str(path).rsplit("/", 1)[-1]
    if not (name.startswith("batch-") and name.endswith(".jsonl")):
        return None
    parts = name[:-len(".jsonl")].split("-")
    if len(parts) < 3:
        return None
    try:
        return int(parts[1])
    except (TypeError, ValueError):
        return None
def load_row_progress_file_infos():
    if not hdfs_path_exists(ROW_PROGRESS_ROOT):
        return []
    files = hdfs_list_files(ROW_PROGRESS_ROOT)
    return [
        {"path": path, "batch_index": parse_progress_batch_index(path)}
        for path in files
    ]
def load_row_progress_df(progress_files=None):
    files = progress_files if progress_files is not None else [item["path"] for item in load_row_progress_file_infos()]
    if not files:
        return None
    return spark.read.schema(ROW_PROGRESS_SCHEMA).json(files).where(F.col("source_sha256").isNotNull() & (F.trim(F.col("source_sha256")) != ""))
def load_row_progress_summary():
    progress_file_infos = load_row_progress_file_infos()
    progress_files = [item["path"] for item in progress_file_infos]
    progress_df = load_row_progress_df(progress_files)
    if progress_df is None:
        return {
            "completed_source_sha256_count": 0,
            "completed_batch_index": 0,
            "last_completed_source_sha256": None,
            "oa_count": 0,
            "non_oa_count": 0,
        }
    if progress_df.first() is None:
        return {
            "completed_source_sha256_count": 0,
            "completed_batch_index": 0,
            "last_completed_source_sha256": None,
            "oa_count": 0,
            "non_oa_count": 0,
        }
    latest_df = (
        progress_df
        .groupBy("source_sha256")
        .agg(
            F.max(F.coalesce(F.col("batch_index"), F.lit(0))).alias("batch_index"),
            F.max(F.coalesce(F.col("access_is_oa"), F.lit("false"))).alias("access_is_oa"),
        )
    )
    summary_row = latest_df.agg(
        F.count("*").alias("completed_source_sha256_count"),
        F.max("batch_index").alias("completed_batch_index"),
        F.max("source_sha256").alias("last_completed_source_sha256"),
        F.sum(F.when(F.col("access_is_oa") == F.lit("true"), F.lit(1)).otherwise(F.lit(0))).alias("oa_count"),
    ).first()
    completed_batch_index = int(summary_row["completed_batch_index"] or 0)
    if completed_batch_index > 0:
        latest_batch_files = [item["path"] for item in progress_file_infos if item["batch_index"] == completed_batch_index]
        latest_batch_df = load_row_progress_df(latest_batch_files)
        latest_sha_row = latest_batch_df.agg(F.max("source_sha256").alias("last_completed_source_sha256")).first() if latest_batch_df is not None else None
        last_completed_source_sha256 = latest_sha_row["last_completed_source_sha256"] if latest_sha_row is not None else None
    else:
        last_completed_source_sha256 = summary_row["last_completed_source_sha256"]
    total_count = int(summary_row["completed_source_sha256_count"] or 0)
    oa_count = int(summary_row["oa_count"] or 0)
    return {
        "completed_source_sha256_count": total_count,
        "completed_batch_index": int(completed_batch_index or 0),
        "last_completed_source_sha256": last_completed_source_sha256,
        "oa_count": oa_count,
        "non_oa_count": int(total_count - oa_count),
    }
def write_row_progress(batch_index, progress_rows):
    completed_at = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    output_path = progress_output_path(batch_index)
    client = ClientCache().get_client(output_path)
    with S3LineWriter(output_path, client=client) as writer:
        for progress_row in progress_rows:
            if isinstance(progress_row, dict):
                source_sha256 = progress_row["source_sha256"]
                processed_path = progress_row["processed_path"]
                access_is_oa = progress_row["access_is_oa"]
            else:
                source_sha256, processed_path, access_is_oa = progress_row
            writer.write(json.dumps({
                "source_sha256": source_sha256,
                "processed_path": processed_path,
                "access_is_oa": access_is_oa,
                "batch_index": int(batch_index),
                "completed_at": completed_at,
            }, ensure_ascii=False, separators=(",", ":")))
    return completed_at
def load_bucket_progress(bucket_id):
    path = bucket_progress_path(bucket_id)
    client = ClientCache().get_client(path)
    try:
        stream = read_s3_object(path, client=client)
    except Exception as exc:
        log_warning(f"load_bucket_progress failed, fallback to defaults: bucket_id={bucket_id}, error={type(exc).__name__}: {exc}")
        return {
            "bucket_id": int(bucket_id),
            "last_sort_key": None,
            "last_source_sha256": None,
            "completed_batch_index": 0,
            "total_count": None,
            "pending_count": None,
            "initialized": False,
        }
    with stream:
        payload = stream.read()
    text = payload.decode("utf-8") if isinstance(payload, bytes) else str(payload)
    item = json.loads(text)
    return {
        "bucket_id": int(item.get("bucket_id") or bucket_id),
        "last_sort_key": item.get("last_sort_key"),
        "last_source_sha256": item.get("last_source_sha256"),
        "completed_batch_index": int(item.get("completed_batch_index") or 0),
        "total_count": None if item.get("total_count") is None else int(item.get("total_count") or 0),
        "pending_count": None if item.get("pending_count") is None else int(item.get("pending_count") or 0),
        "initialized": bool(item.get("initialized", item.get("pending_count") is not None)),
    }
def write_bucket_progress(bucket_id, last_sort_key, last_source_sha256, completed_batch_index, total_count=None, pending_count=None, initialized=None):
    path = bucket_progress_path(bucket_id)
    client = ClientCache().get_client(path)
    payload = {
        "bucket_id": int(bucket_id),
        "last_sort_key": None if last_sort_key is None else int(last_sort_key),
        "last_source_sha256": last_source_sha256,
        "completed_batch_index": int(completed_batch_index or 0),
        "total_count": None if total_count is None else int(total_count),
        "pending_count": None if pending_count is None else int(pending_count),
        "initialized": bool(initialized) if initialized is not None else (pending_count is not None),
        "updated_at": time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()),
    }
    put_s3_object(path, json.dumps(payload, ensure_ascii=False, indent=2).encode("utf-8"), client=client)
def batch_commit_output_path(batch_index, bucket_id):
    root = BATCH_COMMIT_ROOT.rstrip("/")
    return f"{root}/batch-{int(batch_index):08d}-bucket-{int(bucket_id):04d}-{uuid.uuid4().hex}.json"
def parse_batch_commit_file_info(path):
    name = str(path).rsplit("/", 1)[-1]
    match = re.match(r"^batch-(\d+)-bucket-(\d+)-[0-9a-f]{32}\.json$", name)
    if not match:
        return None
    return {
        "path": path,
        "batch_index": int(match.group(1)),
        "bucket_id": int(match.group(2)),
    }
def load_batch_commit_records():
    if not hdfs_path_exists(BATCH_COMMIT_ROOT):
        return []
    records = []
    for s3a_path in hdfs_list_files(BATCH_COMMIT_ROOT):
        s3_path = s3a_path.replace("s3a://", "s3://", 1)
        file_info = parse_batch_commit_file_info(s3_path)
        if file_info is None:
            continue
        try:
            stream = read_s3_object(s3_path, client=ClientCache().get_client(s3_path))
            with stream:
                payload = stream.read()
            text = payload.decode("utf-8") if isinstance(payload, bytes) else str(payload)
            item = json.loads(text)
            records.append({
                "path": s3_path,
                "bucket_id": int(item.get("bucket_id") or file_info["bucket_id"]),
                "batch_index": int(item.get("batch_index") or file_info["batch_index"]),
                "last_sort_key": item.get("last_sort_key"),
                "last_source_sha256": item.get("last_source_sha256"),
                "processed_count": int(item.get("processed_count") or 0),
                "completed_at": item.get("completed_at"),
            })
        except Exception as exc:
            log_warning(f"skip invalid batch commit file: path={s3_path}, error={type(exc).__name__}: {exc}")
    return records
def write_batch_commit(batch_index, bucket_id, last_sort_key, last_source_sha256, processed_count, completed_at):
    path = batch_commit_output_path(batch_index, bucket_id)
    payload = {
        "bucket_id": int(bucket_id),
        "batch_index": int(batch_index),
        "last_sort_key": None if last_sort_key is None else int(last_sort_key),
        "last_source_sha256": last_source_sha256,
        "processed_count": int(processed_count or 0),
        "completed_at": completed_at,
        "updated_at": time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()),
    }
    put_s3_object(
        path,
        json.dumps(payload, ensure_ascii=False, indent=2).encode("utf-8"),
        client=ClientCache().get_client(path),
    )
    return path
def recover_bucket_progress_from_batch_commits(bucket_progresses):
    commit_records = load_batch_commit_records()
    if not commit_records:
        print("batch-commit-recovery: no commit records")
        return
    latest_by_bucket = {}
    for record in commit_records:
        bucket_id = int(record["bucket_id"])
        current = latest_by_bucket.get(bucket_id)
        if current is None or int(record["batch_index"]) > int(current["batch_index"]):
            latest_by_bucket[bucket_id] = record
    recovered_count = 0
    for item in bucket_progresses:
        bucket_id = int(item["bucket_id"])
        latest = latest_by_bucket.get(bucket_id)
        if latest is None:
            continue
        if int(latest["batch_index"]) <= int(item.get("completed_batch_index") or 0):
            continue
        item["last_sort_key"] = latest.get("last_sort_key")
        item["last_source_sha256"] = latest.get("last_source_sha256")
        item["completed_batch_index"] = int(latest["batch_index"])
        # Reset pending counters for a safe re-initialization in this run.
        item["total_count"] = None
        item["pending_count"] = None
        item["initialized"] = False
        write_bucket_progress(
            bucket_id,
            item.get("last_sort_key"),
            item.get("last_source_sha256"),
            item.get("completed_batch_index"),
            total_count=None,
            pending_count=None,
            initialized=False,
        )
        recovered_count += 1
    print(f"batch-commit-recovery: recovered_buckets={recovered_count}, commit_records={len(commit_records)}")
def queue_shard_path(queue_root):
    return f"{queue_root.rstrip('/')}/shard-{uuid.uuid4().hex}.jsonl"
def queue_bad_file_path(queue_root, source_path):
    queue_name = str(queue_root).rstrip("/").rsplit("/", 1)[-1] or "queue"
    file_name = str(source_path).rsplit("/", 1)[-1] or "unknown"
    return f"{BAD_QUEUE_FILE_ROOT.rstrip('/')}/{queue_name}/{file_name}.{uuid.uuid4().hex}.bad"
def quarantine_queue_file(queue_root, bad_file):
    if isinstance(bad_file, dict):
        source_path = bad_file.get("path")
        reason = bad_file.get("error")
    else:
        source_path = bad_file
        reason = None
    if not source_path:
        return None
    target_path = queue_bad_file_path(queue_root, source_path)
    source_client = ClientCache().get_client(source_path)
    target_client = ClientCache().get_client(target_path)
    stream = read_s3_object(source_path, client=source_client)
    with stream:
        payload = stream.read()
    put_s3_object(target_path, payload, client=target_client)
    delete_s3_object(source_path, dry_run=False, client=source_client)
    if reason:
        log_warning(f"queue file quarantined: path={source_path}, target={target_path}, reason={reason}")
    else:
        log_warning(f"queue file quarantined: path={source_path}, target={target_path}")
    return target_path
def append_queue_rows(queue_root, rows, rows_per_shard=None):
    if not rows:
        return 0
    rows = list(rows)
    rows_per_shard = None if rows_per_shard is None else max(1, int(rows_per_shard))
    client_cache = ClientCache()
    total_count = 0
    start = 0
    while start < len(rows):
        shard_rows = rows[start:] if rows_per_shard is None else rows[start:start + rows_per_shard]
        output_path = queue_shard_path(queue_root)
        client = client_cache.get_client(output_path)
        lines = [json.dumps(row, ensure_ascii=False, separators=(",", ":")) for row in shard_rows]
        put_s3_object(output_path, ("\n".join(lines) + "\n").encode("utf-8"), client=client)
        total_count += len(shard_rows)
        if rows_per_shard is None:
            break
        start += rows_per_shard
    return total_count
def load_queue_file_rows(s3_path, client=None):
    if client is None:
        client = ClientCache().get_client(s3_path)
    stream = read_s3_object(s3_path, client=client)
    with stream:
        payload = stream.read()
    text = payload.decode("utf-8") if isinstance(payload, bytes) else str(payload)
    shard_rows = []
    for line in text.splitlines():
        line = line.strip()
        if not line:
            continue
        shard_rows.append(json.loads(line))
    return shard_rows
def rebalance_queue_file(queue_root, s3_path, shard_rows, rows_per_shard):
    rewritten_rows = append_queue_rows(queue_root, shard_rows, rows_per_shard=rows_per_shard)
    try:
        client = ClientCache().get_client(s3_path)
        delete_s3_object(s3_path, dry_run=False, client=client)
    except Exception as exc:
        log_warning(f"rebalance_queue_file delete failed: path={s3_path}, error={type(exc).__name__}: {exc}")
    return rewritten_rows
def load_queue_rows(queue_root, limit_rows, preserve_file_boundaries=False, rows_per_shard=None):
    files = hdfs_list_files(queue_root.replace("s3://", "s3a://", 1))
    rows = []
    consumed_files = []
    remainder_rows = []
    bad_files = []
    rebalanced_files = []
    for s3a_path in files:
        if len(rows) >= int(limit_rows):
            break
        s3_path = s3a_path.replace("s3a://", "s3://", 1)
        client = ClientCache().get_client(s3_path)
        try:
            shard_rows = load_queue_file_rows(s3_path, client=client)
        except Exception as exc:
            bad_files.append({
                "path": s3_path,
                "error": f"{type(exc).__name__}: {exc}",
            })
            log_warning(f"load_queue_file_rows failed: path={s3_path}, error={type(exc).__name__}: {exc}")
            continue
        if preserve_file_boundaries and rows_per_shard and len(shard_rows) > int(rows_per_shard):
            rebalanced_files.append({
                "path": s3_path,
                "row_count": len(shard_rows),
                "rewritten_rows": rebalance_queue_file(queue_root, s3_path, shard_rows, rows_per_shard),
            })
            continue
        remaining_capacity = int(limit_rows) - len(rows)
        if preserve_file_boundaries and rows and len(shard_rows) > remaining_capacity:
            break
        consumed_files.append(s3_path)
        if remaining_capacity > 0:
            rows.extend(shard_rows[:remaining_capacity])
            remainder_rows.extend(shard_rows[remaining_capacity:])
        else:
            remainder_rows.extend(shard_rows)
        if len(rows) >= int(limit_rows):
            break
    return rows, consumed_files, remainder_rows, bad_files, rebalanced_files
def rewrite_queue_rows(queue_root, consumed_files, remaining_rows, bad_files=None, rows_per_shard=None):
    appended_remaining_rows = 0
    if remaining_rows:
        appended_remaining_rows = append_queue_rows(queue_root, remaining_rows, rows_per_shard=rows_per_shard)
    for path in consumed_files:
        try:
            client = ClientCache().get_client(path)
            delete_s3_object(path, dry_run=False, client=client)
        except Exception as exc:
            log_warning(f"rewrite_queue_rows delete consumed file failed: path={path}, error={type(exc).__name__}: {exc}")
    quarantined_bad_files = 0
    for bad_file in (bad_files or []):
        try:
            quarantined_path = quarantine_queue_file(queue_root, bad_file)
            if quarantined_path:
                quarantined_bad_files += 1
        except Exception as exc:
            source_path = bad_file.get("path") if isinstance(bad_file, dict) else bad_file
            log_warning(f"rewrite_queue_rows quarantine bad file failed: path={source_path}, error={type(exc).__name__}: {exc}")
    return appended_remaining_rows, quarantined_bad_files
def count_queue_files(queue_root):
    return len(hdfs_list_files(queue_root.replace("s3://", "s3a://", 1)))
def format_seconds_compact(total_seconds):
    total_seconds = max(0, int(round(float(total_seconds or 0.0))))
    hours, remainder = divmod(total_seconds, 3600)
    minutes, seconds = divmod(remainder, 60)
    if hours > 0:
        return f"{hours}h{minutes:02d}m{seconds:02d}s"
    if minutes > 0:
        return f"{minutes}m{seconds:02d}s"
    return f"{seconds}s"
def estimate_remaining_batches_by_queue_files(remaining_queue_files, completed_batches, consumed_queue_files):
    remaining_queue_files = max(0, int(remaining_queue_files or 0))
    completed_batches = max(0, int(completed_batches or 0))
    consumed_queue_files = max(0, int(consumed_queue_files or 0))
    if remaining_queue_files == 0:
        return 0
    if completed_batches == 0 or consumed_queue_files == 0:
        return remaining_queue_files
    avg_queue_files_per_batch = float(consumed_queue_files) / float(completed_batches)
    if avg_queue_files_per_batch <= 0:
        return remaining_queue_files
    return int(math.ceil(float(remaining_queue_files) / avg_queue_files_per_batch))
def append_slow_path_rows(rows):
    return append_queue_rows(SLOW_PATH_QUEUE_ROOT, rows, rows_per_shard=SLOW_PATH_QUEUE_SHARD_ROWS)
def load_slow_path_rows(limit_rows=SLOW_PATH_BATCH_SIZE):
    all_bad_files = []
    all_rebalanced_files = []
    while True:
        rows, consumed_files, remainder_rows, bad_files, rebalanced_files = load_queue_rows(
            SLOW_PATH_QUEUE_ROOT,
            limit_rows,
            preserve_file_boundaries=True,
            rows_per_shard=SLOW_PATH_QUEUE_SHARD_ROWS,
        )
        all_bad_files.extend(bad_files)
        all_rebalanced_files.extend(rebalanced_files)
        if rows or not rebalanced_files:
            return rows, consumed_files, remainder_rows, all_bad_files, all_rebalanced_files
def rewrite_slow_path_rows(consumed_files, remaining_rows, bad_files=None):
    return rewrite_queue_rows(
        SLOW_PATH_QUEUE_ROOT,
        consumed_files,
        remaining_rows,
        bad_files,
        rows_per_shard=SLOW_PATH_QUEUE_SHARD_ROWS,
    )
def append_oversized_path_rows(rows):
    return append_queue_rows(OVERSIZED_PATH_QUEUE_ROOT, rows)
def parse_content_list_value(value):
    if value is None:
        return None
    if isinstance(value, (list, dict)):
        return value
    if not isinstance(value, str):
        return value
    text = value.strip()
    if not text:
        return value
    try:
        parsed = json.loads(text)
    except (TypeError, ValueError):
        return value
    while isinstance(parsed, str):
        stripped = parsed.strip()
        if not stripped:
            return parsed
        if not (stripped.startswith("[") or stripped.startswith("{") or stripped.startswith('"')):
            return parsed
        try:
            reparsed = json.loads(stripped)
        except (TypeError, ValueError):
            return parsed
        if reparsed == parsed:
            return parsed
        parsed = reparsed
    return parsed
def should_repair_content_list(content_list, raw_content_list):
    if raw_content_list is None:
        return False
    if isinstance(content_list, list):
        return False
    if isinstance(content_list, dict):
        return True
    if isinstance(content_list, str):
        stripped = content_list.strip()
        return stripped.startswith("[") or stripped.startswith("{") or stripped.startswith('"')
    return content_list is None
def normalize_content_list_for_payload(doc):
    raw_content_list = doc.get("content_list")
    content_list = doc.get("content_list")
    if isinstance(content_list, list):
        normalized = []
        for item in content_list:
            if hasattr(item, "asDict"):
                normalized.append(item.asDict(recursive=False))
            else:
                normalized.append(item)
        return normalized
    if should_repair_content_list(content_list, raw_content_list):
        repaired = parse_content_list_value(raw_content_list)
        if repaired is not None:
            return repaired
    if content_list is not None:
        return parse_content_list_value(content_list)
    return content_list
def decode_real_doc(processed_path, client=None):
    raw_text = None
    doc = None
    if "?bytes=" in processed_path:
        base_path, bytes_param = processed_path.split("?bytes=", 1)
        stream = read_s3_object(base_path, bytes_param, client=client)
        with stream:
            raw_value = stream.read()
        if base_path.endswith(".gz"):
            raw_value = gzip.GzipFile(fileobj=io.BytesIO(raw_value)).read()
        elif base_path.endswith(".bz2"):
            raw_value = bz2.BZ2File(io.BytesIO(raw_value)).read()
        raw_text = raw_value.decode("utf-8") if isinstance(raw_value, bytes) else str(raw_value)
    else:
        if processed_path.endswith((".jsonl", ".jsonl.gz", ".jsonl.bz2")):
            row = read_s3_row(processed_path, client=client)
            if row is None:
                raise ValueError(f"read_s3_row returned None for {processed_path}")
            raw_value = row.value
            raw_text = raw_value.decode("utf-8") if isinstance(raw_value, bytes) else str(raw_value)
        else:
            stream = read_s3_object(processed_path, client=client)
            with stream:
                raw_value = stream.read()
            if processed_path.endswith(".gz"):
                raw_value = gzip.GzipFile(fileobj=io.BytesIO(raw_value)).read()
            elif processed_path.endswith(".bz2"):
                raw_value = bz2.BZ2File(io.BytesIO(raw_value)).read()
            raw_text = raw_value.decode("utf-8") if isinstance(raw_value, bytes) else str(raw_value)
    raw_text = raw_text.strip()
    if not raw_text:
        raise ValueError(f"empty content for {processed_path}")
    try:
        doc = json.loads(raw_text)
    except (TypeError, ValueError):
        if "?bytes=" in processed_path:
            raise
        if processed_path.endswith((".jsonl", ".jsonl.gz", ".jsonl.bz2")):
            raise
        row = read_s3_row(processed_path, client=client)
        if row is None:
            raise ValueError(f"read_s3_row returned None for {processed_path}")
        raw_value = row.value
        raw_text = raw_value.decode("utf-8") if isinstance(raw_value, bytes) else str(raw_value)
        raw_text = raw_text.strip()
        if not raw_text:
            raise ValueError(f"empty fallback row content for {processed_path}")
        doc = json.loads(raw_text)
    doc["content_list"] = normalize_content_list_for_payload(doc)
    doc["doc_loc"] = doc.get("doc_loc") or processed_path
    doc["processed_path"] = doc.get("processed_path") or doc["doc_loc"]
    return doc
def extract_doc_sha256_candidates(doc):
    candidates = []
    for key in ("sha256", "doc_id", "id"):
        value = doc.get(key)
        if isinstance(value, str):
            value = value.strip().lower()
            if len(value) == 64 and all(ch in "0123456789abcdef" for ch in value):
                candidates.append(value)
    return candidates
def extract_doc_sha256_candidates_from_text(raw_text):
    text = str(raw_text or "")
    seen = set()
    candidates = []
    for match in re.finditer(r'"(?:sha256|doc_id|id)"\s*:\s*"([0-9a-fA-F]{64})"', text):
        candidate = match.group(1).strip().lower()
        if candidate not in seen:
            seen.add(candidate)
            candidates.append(candidate)
    return candidates
def load_real_docs_for_path(processed_path, expected_sha256s, client=None):
    expected_sha256s = {str(item).strip().lower() for item in expected_sha256s if str(item).strip()}
    if not expected_sha256s:
        return {}
    if "?bytes=" in processed_path:
        doc = decode_real_doc(processed_path, client=client)
        docs_by_sha = {}
        for candidate in extract_doc_sha256_candidates(doc):
            docs_by_sha[candidate] = doc
        if not docs_by_sha:
            fallback_sha = normalize_sha256(next(iter(expected_sha256s)))
            docs_by_sha[fallback_sha] = doc
        return {sha: docs_by_sha[sha] for sha in docs_by_sha if sha in expected_sha256s}
    docs_by_sha = {}
    for row in read_s3_rows(processed_path, use_stream=True, client=client):
        raw_value = row.value
        raw_text = raw_value.decode("utf-8") if isinstance(raw_value, bytes) else str(raw_value)
        raw_text = raw_text.strip()
        if not raw_text:
            continue
        text_candidates = extract_doc_sha256_candidates_from_text(raw_text)
        matched_candidates = [candidate for candidate in text_candidates if candidate in expected_sha256s and candidate not in docs_by_sha]
        if not matched_candidates:
            continue
        try:
            doc = json.loads(raw_text)
        except MemoryError as exc:
            raise MemoryError(
                f"json.loads OOM for processed_path={processed_path}, row_loc={getattr(row, 'loc', None)}, raw_text_len={len(raw_text)}, expected_sha_count={len(expected_sha256s)}"
            ) from exc
        doc["content_list"] = normalize_content_list_for_payload(doc)
        doc["doc_loc"] = doc.get("doc_loc") or row.loc or processed_path
        doc["processed_path"] = doc.get("processed_path") or processed_path
        for candidate in extract_doc_sha256_candidates(doc):
            if candidate in expected_sha256s and candidate not in docs_by_sha:
                docs_by_sha[candidate] = doc
        if len(docs_by_sha) >= len(expected_sha256s):
            break
    return docs_by_sha
def build_output_payload(meta_row, real_doc):
    payload = {field: real_doc.get(field) for field in REAL_DOC_FIELDS}
    for field in ("doi", "title", "author", "language", "abstract", "origin_url", "origin_path", "model_name", "model_version"):
        if meta_row.get(field):
            payload[field] = meta_row[field]
    sha256 = (meta_row.get("source_sha256") or real_doc.get("sha256") or real_doc.get("doc_id") or real_doc.get("id") or "").strip()
    if not sha256:
        raise ValueError(f"missing sha256 for processed_path={meta_row['processed_path']}")
    payload["sha256"] = sha256
    payload["doc_id"] = payload.get("doc_id") or real_doc.get("doc_id") or real_doc.get("id") or sha256
    payload["doc_loc"] = payload.get("doc_loc") or meta_row["processed_path"]
    payload["processed_path"] = meta_row["processed_path"]
    return payload
def normalize_sha256(value):
    normalized = (value or "").strip().lower()
    if len(normalized) != 64 or any(ch not in "0123456789abcdef" for ch in normalized):
        raise ValueError(f"invalid sha256={value!r}")
    return normalized
def build_skipped_existing_result(meta_row, source_sha256, target_path):
    return {
        "processed_path": meta_row["processed_path"],
        "source_sha256": source_sha256,
        "sha256": source_sha256,
        "access_is_oa": meta_row["access_is_oa"],
        "target_path": target_path,
        "status": "skipped_existing",
        "read_seconds": 0.0,
        "write_seconds": 0.0,
    }
def split_existing_meta_rows(meta_rows, force_driver_exists_check=None):
    driver_exists_check = ENABLE_DRIVER_PATH_EXISTENCE_CHECK if force_driver_exists_check is None else bool(force_driver_exists_check)
    if OVERWRITE_EXISTING_DOCS or not driver_exists_check:
        return list(meta_rows), []
    pending_rows = []
    existence_checks = []
    for meta_row in meta_rows:
        source_sha256 = (meta_row.get("source_sha256") or "").strip().lower()
        if not source_sha256:
            pending_rows.append(meta_row)
            continue
        target_root = OA_ROOT if meta_row["access_is_oa"] == "true" else OTHERS_ROOT
        target_path = target_path_for_sha(target_root, source_sha256)
        existence_checks.append((meta_row, source_sha256, target_path))
    if not existence_checks:
        return pending_rows, []
    skipped_rows = []
    max_workers = 32
    def check_one(item):
        meta_row, source_sha256, target_path = item
        return meta_row, source_sha256, target_path, hdfs_path_exists(target_path)
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        for meta_row, source_sha256, target_path, exists_flag in executor.map(check_one, existence_checks):
            if exists_flag:
                skipped_rows.append(build_skipped_existing_result(meta_row, source_sha256, target_path))
            else:
                pending_rows.append(meta_row)
    return pending_rows, skipped_rows
def dedup_meta_rows_by_source_sha(meta_rows):
    dedup = {}
    duplicated_sha256s = set()
    for meta_row in meta_rows:
        source_sha256 = (meta_row.get("source_sha256") or "").strip().lower()
        if not source_sha256:
            continue
        if source_sha256 in dedup:
            duplicated_sha256s.add(source_sha256)
            continue
        dedup[source_sha256] = meta_row
    deduped_rows = [dedup[key] for key in sorted(dedup.keys(), key=lambda sha: dedup[sha]["source_sha256"])]
    return deduped_rows, duplicated_sha256s
def build_partition_batches(meta_rows, target_rows_per_partition=ROWS_PER_PARTITION, target_partition_count=TARGET_BATCH_PARTITIONS, max_partition_count=MAX_BATCH_PARTITIONS):
    total_rows = len(meta_rows)
    target_rows_per_partition = max(1, int(target_rows_per_partition or 1))
    target_partition_count = max(1, int(target_partition_count or 1))
    max_partition_count = max(1, int(max_partition_count or target_partition_count))
    if not meta_rows:
        return {
            "batches": [],
            "partition_count": 0,
            "target_rows_per_partition": target_rows_per_partition,
            "target_partition_count": target_partition_count,
            "rows_based_partition_count": 0,
            "max_partitions_by_read_budget": 0,
            "max_partitions_by_write_budget": 0,
            "unique_path_group_count": 0,
            "max_bucket_size": 0,
            "min_bucket_size": 0,
            "avg_bucket_size": 0.0,
        }
    groups = []
    current_path = None
    current_group = []
    for meta_row in meta_rows:
        processed_path = meta_row["processed_path"]
        if current_path is None or processed_path == current_path:
            current_group.append(meta_row)
            current_path = processed_path
            continue
        groups.append(current_group)
        current_group = [meta_row]
        current_path = processed_path
    if current_group:
        groups.append(current_group)
    unique_path_group_count = len(groups)
    rows_based_partition_count = max(1, int(math.ceil(total_rows / float(target_rows_per_partition))))
    max_partitions_by_read_budget = max(1, int(TARGET_GLOBAL_READ_CONCURRENCY // max(1, MIN_READ_THREADS_PER_PARTITION))) if TARGET_GLOBAL_READ_CONCURRENCY > 0 else max_partition_count
    max_partitions_by_write_budget = max(1, int(TARGET_GLOBAL_WRITE_CONCURRENCY // max(1, MIN_WRITE_THREADS_PER_PARTITION))) if TARGET_GLOBAL_WRITE_CONCURRENCY > 0 else max_partition_count
    bucket_count = min(
        max_partition_count,
        target_partition_count,
        rows_based_partition_count,
        unique_path_group_count,
        max_partitions_by_read_budget,
        max_partitions_by_write_budget,
    )
    bucket_count = max(1, int(bucket_count or 1))
    buckets = [[] for _ in range(bucket_count)]
    bucket_sizes = [0 for _ in range(bucket_count)]
    bucket_heap = [(0, idx) for idx in range(bucket_count)]
    heapq.heapify(bucket_heap)
    for group in sorted(groups, key=len, reverse=True):
        _, bucket_idx = heapq.heappop(bucket_heap)
        buckets[bucket_idx].extend(group)
        bucket_sizes[bucket_idx] += len(group)
        heapq.heappush(bucket_heap, (bucket_sizes[bucket_idx], bucket_idx))
    batches = [bucket for bucket in buckets if bucket]
    non_empty_bucket_sizes = [size for size in bucket_sizes if size > 0]
    return {
        "batches": batches,
        "partition_count": len(batches),
        "target_rows_per_partition": target_rows_per_partition,
        "target_partition_count": target_partition_count,
        "rows_based_partition_count": rows_based_partition_count,
        "max_partitions_by_read_budget": max_partitions_by_read_budget,
        "max_partitions_by_write_budget": max_partitions_by_write_budget,
        "unique_path_group_count": unique_path_group_count,
        "max_bucket_size": max(non_empty_bucket_sizes) if non_empty_bucket_sizes else 0,
        "min_bucket_size": min(non_empty_bucket_sizes) if non_empty_bucket_sizes else 0,
        "avg_bucket_size": (float(total_rows) / float(len(batches))) if batches else 0.0,
    }
def summarize_partition_work(meta_rows):
    grouped_counts = defaultdict(int)
    for meta_row in meta_rows:
        grouped_counts[meta_row["processed_path"]] += 1
    direct_row_count = 0
    scan_group_count = 0
    for processed_path, row_count in grouped_counts.items():
        if should_scan_processed_path_by_sha(processed_path):
            scan_group_count += 1
        elif ("?bytes=" in processed_path) or (row_count == 1):
            direct_row_count += row_count
        else:
            scan_group_count += 1
    return {
        "row_count": len(meta_rows),
        "unique_processed_path_count": len(grouped_counts),
        "direct_row_count": direct_row_count,
        "scan_group_count": scan_group_count,
        "read_task_count": direct_row_count + scan_group_count,
        "write_task_count": len(meta_rows),
    }
def resolve_partition_threading(partition_count, row_count=None, read_task_count=None, write_task_count=None, forced_read_threads=None, forced_write_threads=None):
    partition_count = max(1, int(partition_count or 1))
    row_count = max(1, int(row_count or 1))
    read_task_count = max(1, int(read_task_count or row_count))
    write_task_count = max(1, int(write_task_count or row_count))
    if forced_read_threads is not None:
        read_threads = max(1, int(forced_read_threads))
    else:
        read_threads = max(MIN_READ_THREADS_PER_PARTITION, int(math.ceil(TARGET_GLOBAL_READ_CONCURRENCY / float(partition_count))))
    if forced_write_threads is not None:
        write_threads = max(1, int(forced_write_threads))
    else:
        write_threads = max(MIN_WRITE_THREADS_PER_PARTITION, int(math.ceil(TARGET_GLOBAL_WRITE_CONCURRENCY / float(partition_count))))
    return {
        "read_threads": min(read_threads, read_task_count, MAX_READ_THREADS_PER_PARTITION, MAX_S3_CONNECTIONS),
        "write_threads": min(write_threads, write_task_count, MAX_WRITE_THREADS_PER_PARTITION, MAX_S3_CONNECTIONS),
    }
def clamp_thread_plan(thread_plan, row_count, max_read_threads=None, max_write_threads=None, read_task_count=None, write_task_count=None):
    row_count = max(1, int(row_count or 1))
    read_task_count = max(1, int(read_task_count or row_count))
    write_task_count = max(1, int(write_task_count or row_count))
    max_read_threads = max(1, int(max_read_threads or MAX_READ_THREADS_PER_PARTITION))
    max_write_threads = max(1, int(max_write_threads or MAX_WRITE_THREADS_PER_PARTITION))
    read_threads = max(1, int((thread_plan or {}).get("read_threads") or 1))
    write_threads = max(1, int((thread_plan or {}).get("write_threads") or 1))
    return {
        "read_threads": min(read_threads, row_count, read_task_count, max_read_threads, MAX_S3_CONNECTIONS),
        "write_threads": min(write_threads, row_count, write_task_count, max_write_threads, MAX_S3_CONNECTIONS),
        "max_in_flight_writes": min(
            max(1, write_threads * 2),
            row_count,
            write_task_count,
            max(1, max_write_threads * 2),
            max(1, MAX_S3_CONNECTIONS * 2),
        ),
    }
def summarize_thread_plans(thread_plans):
    if not thread_plans:
        return {
            "read_min": 0,
            "read_max": 0,
            "read_avg": 0.0,
            "write_min": 0,
            "write_max": 0,
            "write_avg": 0.0,
        }
    read_values = [int(item.get("read_threads") or 1) for item in thread_plans]
    write_values = [int(item.get("write_threads") or 1) for item in thread_plans]
    return {
        "read_min": min(read_values),
        "read_max": max(read_values),
        "read_avg": float(sum(read_values)) / float(len(read_values)),
        "write_min": min(write_values),
        "write_max": max(write_values),
        "write_avg": float(sum(write_values)) / float(len(write_values)),
    }
def flush_write_futures(write_futures, result_rows, wait_for_all=False):
    remaining_futures = []
    for future in write_futures:
        if wait_for_all or future.done():
            result_rows.append(future.result())
        else:
            remaining_futures.append(future)
    return remaining_futures
def process_meta_partition_bundle(item):
    rows, thread_plan, batch_index = item
    return process_meta_partition(rows, thread_plan, batch_index)
def process_meta_partition(rows, thread_plan=None, batch_index=None):
    client_cache = ClientCache()
    read_threads = int((thread_plan or {}).get("read_threads") or 1)
    write_threads = int((thread_plan or {}).get("write_threads") or 1)
    max_in_flight_writes = max(1, int((thread_plan or {}).get("max_in_flight_writes") or write_threads))
    def write_payload(meta_row, real_doc, read_seconds):
        source_sha256 = (meta_row.get("source_sha256") or "").strip().lower()
        target_root = OA_ROOT if meta_row["access_is_oa"] == "true" else OTHERS_ROOT
        payload = build_output_payload(meta_row, real_doc)
        payload["sha256"] = normalize_sha256(payload["sha256"])
        target_path = target_path_for_sha(target_root, payload["sha256"])
        client = client_cache.get_client(target_path)
        write_started_at = time.perf_counter()
        try:
            put_s3_object(
                target_path,
                json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8"),
                client=client,
            )
        except Exception as exc:
            raise RuntimeError(
                f"put_s3_object failed: processed_path={meta_row['processed_path']}, "
                f"sha256={payload['sha256']}, target_path={target_path}, error={type(exc).__name__}: {exc}"
            ) from exc
        write_seconds = time.perf_counter() - write_started_at
        return {
            "processed_path": meta_row["processed_path"],
            "source_sha256": payload["sha256"],
            "sha256": payload["sha256"],
            "access_is_oa": meta_row["access_is_oa"],
            "target_path": target_path,
            "status": "written",
            "read_seconds": read_seconds,
            "write_seconds": write_seconds,
            "total_seconds": read_seconds + write_seconds,
        }
    def read_meta_row(meta_row):
        read_client = client_cache.get_client(meta_row["processed_path"])
        read_started_at = time.perf_counter()
        try:
            real_doc = decode_real_doc(meta_row["processed_path"], client=read_client)
        except Exception as exc:
            byte_len = parse_bytes_length_from_processed_path(meta_row.get("processed_path"))
            raise RuntimeError(
                f"decode_real_doc failed: processed_path={meta_row.get('processed_path')}, "
                f"source_sha256={meta_row.get('source_sha256')}, byte_length={byte_len}, "
                f"error={type(exc).__name__}: {exc}"
            ) from exc
        read_seconds = time.perf_counter() - read_started_at
        return meta_row, real_doc, read_seconds
    def scan_path_group(group_item):
        processed_path, path_rows = group_item
        source_sha256s = [(row.get("source_sha256") or "").strip().lower() for row in path_rows]
        read_client = client_cache.get_client(processed_path)
        read_started_at = time.perf_counter()
        docs_by_sha = load_real_docs_for_path(processed_path, source_sha256s, client=read_client)
        read_seconds = time.perf_counter() - read_started_at
        missing_sha256s = [sha for sha in source_sha256s if sha not in docs_by_sha]
        if missing_sha256s:
            raise RuntimeError(
                f"missing docs after scanning processed_path={processed_path}, missing_count={len(missing_sha256s)}, "
                f"sample_missing={missing_sha256s[:5]}"
            )
        per_row_read_seconds = read_seconds / max(1, len(path_rows))
        return [
            (meta_row, docs_by_sha[(meta_row.get("source_sha256") or "").strip().lower()], per_row_read_seconds)
            for meta_row in path_rows
        ]
    def read_direct_task(meta_row):
        meta_row, real_doc, read_seconds = read_meta_row(meta_row)
        return [(meta_row, real_doc, read_seconds)]
    rows = list(rows)
    partition_started_at = time.perf_counter()
    result_rows = []
    grouped_rows = defaultdict(list)
    for meta_row in rows:
        grouped_rows[meta_row["processed_path"]].append(meta_row)
    direct_rows = []
    grouped_scan_groups = []
    for processed_path, path_rows in grouped_rows.items():
        if should_scan_processed_path_by_sha(processed_path):
            grouped_scan_groups.append((processed_path, path_rows))
        elif ("?bytes=" in processed_path) or (len(path_rows) == 1):
            direct_rows.extend(path_rows)
        else:
            grouped_scan_groups.append((processed_path, path_rows))
    if direct_rows or grouped_scan_groups:
        scan_futures = []
        write_futures = []
        with ThreadPoolExecutor(max_workers=read_threads) as read_executor, ThreadPoolExecutor(max_workers=write_threads) as write_executor:
            for meta_row in direct_rows:
                scan_futures.append(read_executor.submit(read_direct_task, meta_row))
            for group_item in grouped_scan_groups:
                scan_futures.append(read_executor.submit(scan_path_group, group_item))
            for future in as_completed(scan_futures):
                for meta_row, real_doc, per_row_read_seconds in future.result():
                    write_futures.append(write_executor.submit(write_payload, meta_row, real_doc, per_row_read_seconds))
                    if len(write_futures) >= max_in_flight_writes:
                        write_futures = flush_write_futures(write_futures, result_rows)
                        while len(write_futures) >= max_in_flight_writes:
                            done_future = next(as_completed(write_futures))
                            result_rows.append(done_future.result())
                            write_futures.remove(done_future)
            write_futures = flush_write_futures(write_futures, result_rows, wait_for_all=True)
    partition_elapsed = time.perf_counter() - partition_started_at
    processed_sha256s = [row["source_sha256"] for row in result_rows]
    progress_rows = [
        (
            row["source_sha256"],
            row["processed_path"],
            row["access_is_oa"],
        )
        for row in result_rows
    ]
    return {
        "written_count": sum(1 for row in result_rows if row["status"] == "written"),
        "processed_count": len(processed_sha256s),
        "partition_elapsed": partition_elapsed,
        "checkpoint_elapsed": 0.0,
        "total_read_seconds": sum(float(row.get("read_seconds") or 0.0) for row in result_rows),
        "total_write_seconds": sum(float(row.get("write_seconds") or 0.0) for row in result_rows),
        "slowest_result_rows": summarize_result_rows(result_rows, top_n=3),
        "completed_at": None,
        "progress_rows": progress_rows,
    }
def get_meta_cache_df():
    meta_df = globals().get("meta_cache_df")
    if meta_df is None:
        raise RuntimeError("请先运行『prepare meta cache』单元格，构建并缓存全量 metadata")
    return meta_df
def build_full_meta_source_df():
    return spark.sql(BASE_META_SQL)
def prepare_meta_cache():
    global meta_cache_df, meta_cache_bucket_total_counts, meta_cache_total_count
    if globals().get("meta_cache_df") is not None:
        try:
            meta_cache_df.unpersist()
        except Exception as exc:
            log_warning(f"meta_cache_df.unpersist failed, continue: error={type(exc).__name__}: {exc}")
    started_at = time.perf_counter()
    source_df = build_full_meta_source_df()
    source_planned_elapsed = time.perf_counter() - started_at
    print(f"meta-cache: source spark.sql planned, elapsed={source_planned_elapsed:.2f}s")
    transform_started_at = time.perf_counter()
    meta_cache_df = (
        source_df
        .withColumn("processed_path", F.trim(F.col("processed_path")))
        .withColumn("source_sha256", F.lower(F.trim(F.col("source_sha256"))))
        .where(F.col("processed_path").isNotNull() & (F.col("processed_path") != ""))
        .where(F.col("source_sha256").isNotNull() & (F.col("source_sha256") != ""))
        .withColumn(
            "access_is_oa",
            F.when(F.lower(F.trim(F.coalesce(F.col("access_is_oa").cast("string"), F.lit("")))) == F.lit("true"), F.lit("true")).otherwise(F.lit("false")),
        )
        .dropDuplicates(["source_sha256"])
        .withColumn("bucket_id", (F.abs(F.crc32(F.col("source_sha256"))) % F.lit(int(SHA_BUCKET_COUNT))).cast("int"))
        .select(
            "metadata_type",
            "doi",
            "isbn13",
            "title",
            "author",
            "language",
            "origin_url",
            "origin_path",
            "processed_path",
            "source_sha256",
            "model_name",
            "model_version",
            "access_is_oa",
            "abstract",
            "bucket_id",
        )
        .repartition(int(SHA_BUCKET_COUNT), "bucket_id")
        .sortWithinPartitions("source_sha256")
        .persist(StorageLevel.MEMORY_AND_DISK)
    )
    transform_elapsed = time.perf_counter() - transform_started_at
    print(f"meta-cache: transform planned, elapsed={transform_elapsed:.2f}s")
    materialize_started_at = time.perf_counter()
    bucket_rows = meta_cache_df.groupBy("bucket_id").agg(F.count("*").alias("total_count")).collect()
    meta_cache_bucket_total_counts = {int(row["bucket_id"]): int(row["total_count"] or 0) for row in bucket_rows}
    for bucket_id in range(SHA_BUCKET_COUNT):
        meta_cache_bucket_total_counts.setdefault(int(bucket_id), 0)
    meta_cache_total_count = int(sum(meta_cache_bucket_total_counts.values()))
    materialize_elapsed = time.perf_counter() - materialize_started_at
    print(f"meta-cache: materialized + bucket counts ready, rows={meta_cache_total_count}, buckets={len(meta_cache_bucket_total_counts)}, elapsed={materialize_elapsed:.2f}s")
    print(f"meta-cache: total elapsed={time.perf_counter() - started_at:.2f}s")
    return meta_cache_df, meta_cache_bucket_total_counts, meta_cache_total_count
def fetch_meta_batch(bucket_id, last_source_sha256=None, limit_rows=BATCH_SIZE):
    fetch_started_at = time.perf_counter()
    meta_df = get_meta_cache_df().where(F.col("bucket_id") == F.lit(int(bucket_id)))
    if last_source_sha256:
        meta_df = meta_df.where(F.col("source_sha256") > F.lit(str(last_source_sha256).strip().lower()))
    limit_rows = normalize_optional_positive_int(limit_rows)
    if limit_rows is not None:
        meta_df = meta_df.orderBy("source_sha256").limit(limit_rows)
    rows = [row.asDict(recursive=True) for row in meta_df.collect()]
    fetch_elapsed = time.perf_counter() - fetch_started_at
    print(
        f"  缓存批次获取完成: rows={len(rows)}, total={fetch_elapsed:.2f}s, "
        f"bucket_id={bucket_id}, last_source_sha256={last_source_sha256}, "
        f"limit_rows={limit_rows if limit_rows is not None else 'disabled'}"
    )
    return rows
def count_pending_rows_by_bucket(bucket_progresses):
    if not bucket_progresses:
        return {}
    started_at = time.perf_counter()
    print(f"pending-count: start, buckets={len(bucket_progresses)}")
    progress_rows = [
        (
            int(item["bucket_id"]),
            (str(item.get("last_source_sha256") or "").strip().lower() or None),
        )
        for item in bucket_progresses
    ]
    progress_build_elapsed = time.perf_counter() - started_at
    print(f"pending-count: progress rows prepared, rows={len(progress_rows)}, elapsed={progress_build_elapsed:.2f}s")
    progress_df = spark.createDataFrame(
        progress_rows,
        T.StructType([
            T.StructField("bucket_id", T.IntegerType(), False),
            T.StructField("last_source_sha256", T.StringType(), True),
        ]),
    )
    print(f"pending-count: progress_df ready, rows={len(progress_rows)}")
    cache_df = get_meta_cache_df()
    count_collect_started_at = time.perf_counter()
    count_rows = (
        progress_df.alias("p")
        .join(
            cache_df.alias("m"),
            (F.col("p.bucket_id") == F.col("m.bucket_id"))
            & (
                F.col("p.last_source_sha256").isNull()
                | (F.col("m.source_sha256") > F.col("p.last_source_sha256"))
            ),
            "left",
        )
        .groupBy(F.col("p.bucket_id").alias("bucket_id"))
        .agg(F.count(F.col("m.source_sha256")).alias("total_count"))
        .collect()
    )
    count_collect_elapsed = time.perf_counter() - count_collect_started_at
    print(f"pending-count: cache join/groupBy/collect done, buckets={len(count_rows)}, elapsed={count_collect_elapsed:.2f}s")
    counts = {int(row["bucket_id"]): int(row["total_count"] or 0) for row in count_rows}
    for item in bucket_progresses:
        counts.setdefault(int(item["bucket_id"]), 0)
    total_elapsed = time.perf_counter() - started_at
    print(f"pending-count: done, total_pending={sum(counts.values())}, total_elapsed={total_elapsed:.2f}s")
    return counts
def initialize_bucket_pending_counts(bucket_progresses):
    stale_zero_items = [
        item for item in bucket_progresses
        if item.get("initialized")
        and int(item.get("pending_count") or 0) <= 0
        and not str(item.get("last_source_sha256") or "").strip()
        and int(item.get("completed_batch_index") or 0) <= 0
        and int(meta_cache_bucket_total_counts.get(int(item["bucket_id"]), 0)) > 0
    ]
    if stale_zero_items:
        print(f"pending-count-init: found stale zero-pending buckets={len(stale_zero_items)}, will reinitialize")
        for item in stale_zero_items:
            item["initialized"] = False
            item["pending_count"] = None
            item["total_count"] = None
    missing_items = [item for item in bucket_progresses if not item.get("initialized")]
    if not missing_items:
        print("pending-count-init: all bucket pending counts already initialized")
        return {int(item["bucket_id"]): int(item.get("pending_count") or 0) for item in bucket_progresses}
    print(f"pending-count-init: initializing buckets={len(missing_items)}")
    counts = {}
    no_cursor_items = [item for item in missing_items if not str(item.get("last_source_sha256") or "").strip()]
    if no_cursor_items:
        counts.update({
            int(item["bucket_id"]): int(meta_cache_bucket_total_counts.get(int(item["bucket_id"]), 0))
            for item in no_cursor_items
        })
        print(f"pending-count-init: reused cached bucket totals for buckets={len(no_cursor_items)}")
    cursor_items = [item for item in missing_items if str(item.get("last_source_sha256") or "").strip()]
    if cursor_items:
        counts.update(count_pending_rows_by_bucket(cursor_items))
    for item in bucket_progresses:
        bucket_id = int(item["bucket_id"])
        if item in missing_items:
            pending_count = int(counts.get(bucket_id, 0))
            item["total_count"] = pending_count
            item["pending_count"] = pending_count
            item["initialized"] = True
            write_bucket_progress(
                bucket_id,
                item.get("last_sort_key"),
                item.get("last_source_sha256"),
                item.get("completed_batch_index"),
                total_count=pending_count,
                pending_count=pending_count,
                initialized=True,
            )
    return {int(item["bucket_id"]): int(item.get("pending_count") or 0) for item in bucket_progresses}
def write_batch(batch_index, meta_rows, enable_slow_path_split=True, target_partitions=TARGET_BATCH_PARTITIONS, rows_per_partition=ROWS_PER_PARTITION, forced_read_threads=None, forced_write_threads=None, force_driver_exists_check=None, bucket_id=None):
    driver_exists_check = ENABLE_DRIVER_PATH_EXISTENCE_CHECK if force_driver_exists_check is None else bool(force_driver_exists_check)
    print(
        f"  批前过滤开始: batch_index={batch_index}, metadata_rows={len(meta_rows)}, "
        f"driver_exists_check={driver_exists_check}"
    )
    dedup_started_at = time.perf_counter()
    deduped_meta_rows, duplicated_sha256s = dedup_meta_rows_by_source_sha(meta_rows)
    dedup_elapsed = time.perf_counter() - dedup_started_at
    if duplicated_sha256s:
        print(
            f"  批内去重完成: before={len(meta_rows)}, after={len(deduped_meta_rows)}, "
            f"duplicated_sha256s={len(duplicated_sha256s)}, 耗时={dedup_elapsed:.2f}s"
        )
    meta_rows = deduped_meta_rows
    split_started_at = time.perf_counter()
    pending_meta_rows, skipped_existing_rows = split_existing_meta_rows(meta_rows, force_driver_exists_check=driver_exists_check)
    split_elapsed = time.perf_counter() - split_started_at
    print(
        f"  批前过滤完成: pending_rows={len(pending_meta_rows)}, skipped_existing_rows={len(skipped_existing_rows)}, "
        f"耗时={split_elapsed:.2f}s"
    )
    oversized_path_rows = []
    oversized_path_candidates = [
        row for row in pending_meta_rows
        if (parse_bytes_length_from_processed_path(row.get("processed_path")) or 0) >= int(OVERSIZED_BYTES_THRESHOLD)
    ]
    if oversized_path_candidates:
        oversized_path_rows = oversized_path_candidates
        oversized_sha_set = {row["source_sha256"] for row in oversized_path_rows}
        pending_meta_rows = [row for row in pending_meta_rows if row["source_sha256"] not in oversized_sha_set]
        appended_oversized_rows = append_oversized_path_rows(oversized_path_rows)
        print(
            f"  超大文档拆分: oversized_rows={len(oversized_path_rows)}, appended_to_queue={appended_oversized_rows}, "
            f"remaining_rows={len(pending_meta_rows)}"
        )
    slow_path_rows = []
    if enable_slow_path_split:
        def is_large_bytes_path(processed_path):
            if "?bytes=" not in processed_path:
                return False
            try:
                _, bytes_part = processed_path.split("?bytes=", 1)
                _, length_str = bytes_part.split(",", 1)
                return int(length_str) >= int(SLOW_PATH_BYTES_THRESHOLD)
            except Exception as exc:
                log_warning(f"is_large_bytes_path parse failed: processed_path={processed_path}, error={type(exc).__name__}: {exc}")
                return False
        slow_path_candidates = [
            row for row in pending_meta_rows
            if ((("?bytes=" not in row["processed_path"]) and row["processed_path"].endswith((".jsonl", ".jsonl.gz", ".jsonl.bz2"))) or is_large_bytes_path(row["processed_path"]))
        ]
        if slow_path_candidates:
            slow_path_rows = slow_path_candidates
            slow_sha_set = {row["source_sha256"] for row in slow_path_rows}
            pending_meta_rows = [row for row in pending_meta_rows if row["source_sha256"] not in slow_sha_set]
            appended_slow_rows = append_slow_path_rows(slow_path_rows)
            print(
                f"  慢路径拆分: slow_path_rows={len(slow_path_rows)}, appended_to_queue={appended_slow_rows}, "
                f"remaining_fast_rows={len(pending_meta_rows)}"
            )
    execution_mode = "skip"
    pending_path_type_summary = summarize_processed_path_types(pending_meta_rows)
    if pending_path_type_summary:
        print(f"  待写路径类型分布: {pending_path_type_summary}")
    if pending_meta_rows:
        pending_meta_rows = sorted(pending_meta_rows, key=lambda row: (row["processed_path"], row["source_sha256"]))
        pending_row_count = len(pending_meta_rows)
        unique_processed_path_count = len({row["processed_path"] for row in pending_meta_rows})
        if pending_row_count <= int(DRIVER_WRITE_MAX_PENDING_ROWS):
            partition_count = 1
            partition_work = summarize_partition_work(pending_meta_rows)
            thread_plan = clamp_thread_plan(
                resolve_partition_threading(
                    partition_count,
                    row_count=partition_work["row_count"],
                    read_task_count=partition_work["read_task_count"],
                    write_task_count=partition_work["write_task_count"],
                    forced_read_threads=forced_read_threads,
                    forced_write_threads=forced_write_threads,
                ),
                pending_row_count,
                DRIVER_WRITE_MAX_READ_THREADS,
                DRIVER_WRITE_MAX_WRITE_THREADS,
                read_task_count=partition_work["read_task_count"],
                write_task_count=partition_work["write_task_count"],
            )
            print(
                f"  Driver 直写开始: pending_rows={pending_row_count}, unique_processed_paths={unique_processed_path_count}, "
                f"read_threads={thread_plan['read_threads']}, write_threads={thread_plan['write_threads']}"
            )
            spark_started_at = time.perf_counter()
            partition_summaries = [process_meta_partition(pending_meta_rows, thread_plan, batch_index)] if pending_meta_rows else []
            spark_elapsed = time.perf_counter() - spark_started_at
            execution_mode = "driver"
            print(f"  Driver 直写完成: partition_summaries={len(partition_summaries)}, 耗时={spark_elapsed:.2f}s")
        else:
            partition_plan = build_partition_batches(pending_meta_rows, rows_per_partition, target_partitions, MAX_BATCH_PARTITIONS)
            partition_batches = partition_plan["batches"]
            partition_count = len(partition_batches)
            partition_inputs = []
            thread_plans = []
            for batch in partition_batches:
                partition_work = summarize_partition_work(batch)
                thread_plan = clamp_thread_plan(
                    resolve_partition_threading(
                        partition_count,
                        row_count=partition_work["row_count"],
                        read_task_count=partition_work["read_task_count"],
                        write_task_count=partition_work["write_task_count"],
                        forced_read_threads=forced_read_threads,
                        forced_write_threads=forced_write_threads,
                    ),
                    partition_work["row_count"],
                    read_task_count=partition_work["read_task_count"],
                    write_task_count=partition_work["write_task_count"],
                )
                partition_inputs.append((batch, thread_plan, batch_index))
                thread_plans.append(thread_plan)
            thread_summary = summarize_thread_plans(thread_plans)
            print(
                f"  Spark 写入开始: pending_rows={pending_row_count}, partition_count={partition_count}, "
                f"rows_per_partition={partition_plan['target_rows_per_partition']}, target_partition_count={partition_plan['target_partition_count']}, "
                f"rows_based_partition_count={partition_plan['rows_based_partition_count']}, unique_processed_paths={unique_processed_path_count}, "
                f"unique_path_groups={partition_plan['unique_path_group_count']}, "
                f"bucket_min={partition_plan['min_bucket_size']}, bucket_avg={partition_plan['avg_bucket_size']:.1f}, bucket_max={partition_plan['max_bucket_size']}, "
                f"read_threads={thread_summary['read_min']}~{thread_summary['read_max']} (avg={thread_summary['read_avg']:.1f}), "
                f"write_threads={thread_summary['write_min']}~{thread_summary['write_max']} (avg={thread_summary['write_avg']:.1f})"
            )
            spark_started_at = time.perf_counter()
            partition_summaries = (
                spark.sparkContext.parallelize(partition_inputs, partition_count)
                .map(process_meta_partition_bundle)
                .collect()
            )
            spark_elapsed = time.perf_counter() - spark_started_at
            execution_mode = "spark"
            print(f"  Spark 写入完成: partition_summaries={len(partition_summaries)}, 耗时={spark_elapsed:.2f}s")
    else:
        partition_count = 0
        result_rows = []
        spark_elapsed = 0.0
        print("  Spark 写入跳过: pending_rows=0")
    skipped_existing_count = len(skipped_existing_rows)
    skipped_progress_rows = [
        (
            row["source_sha256"],
            row["processed_path"],
            row["access_is_oa"],
        )
        for row in skipped_existing_rows
    ]
    if execution_mode == "skip":
        partition_summaries = []
    written_count = sum(int(item.get("written_count") or 0) for item in partition_summaries)
    processed_count = skipped_existing_count + sum(int(item.get("processed_count") or 0) for item in partition_summaries)
    total_read_seconds = sum(float(item.get("total_read_seconds") or 0.0) for item in partition_summaries)
    total_write_seconds = sum(float(item.get("total_write_seconds") or 0.0) for item in partition_summaries)
    progress_rows = list(skipped_progress_rows)
    for item in partition_summaries:
        progress_rows.extend(item.get("progress_rows") or [])
    checkpoint_started_at = time.perf_counter()
    completed_at = write_row_progress(batch_index, progress_rows) if progress_rows else None
    checkpoint_elapsed = time.perf_counter() - checkpoint_started_at
    if progress_rows:
        print(
            f"  checkpoint 批量写入完成: source_sha256s={len(progress_rows)}, "
            f"written={written_count}, skipped_existing={skipped_existing_count}, 耗时={checkpoint_elapsed:.2f}s"
        )
    batch_commit_path = None
    if bucket_id is not None and progress_rows:
        batch_commit_path = write_batch_commit(
            batch_index,
            bucket_id,
            None,
            meta_rows[-1]["source_sha256"] if meta_rows else None,
            len(progress_rows),
            completed_at,
        )
        print(f"  batch commit 写入完成: path={batch_commit_path}")
    slowest_result_rows = []
    for item in partition_summaries:
        slowest_result_rows.extend(item.get("slowest_result_rows") or [])
    if slowest_result_rows:
        slowest_result_rows = sorted(slowest_result_rows, key=lambda row: (float(row.get("total_seconds") or 0.0), float(row.get("read_seconds") or 0.0), float(row.get("write_seconds") or 0.0)), reverse=True)[:5]
        print("  最慢写入样本:")
        for idx, item in enumerate(slowest_result_rows, start=1):
            print(
                f"    {idx}. sha256={item['source_sha256']}, path_type={item['path_type']}, "
                f"read={item['read_seconds']:.2f}s, write={item['write_seconds']:.2f}s, total={item['total_seconds']:.2f}s, "
                f"processed_path={item['processed_path']}"
            )
    total_batch_elapsed = dedup_elapsed + split_elapsed + spark_elapsed + checkpoint_elapsed
    print(f"  批次阶段耗时汇总: dedup={dedup_elapsed:.2f}s, prefilter={split_elapsed:.2f}s, process={spark_elapsed:.2f}s, checkpoint(聚合)={checkpoint_elapsed:.2f}s, approx_total={total_batch_elapsed:.2f}s")
    return {
        "written_count": written_count,
        "skipped_existing_count": skipped_existing_count,
        "processed_count": processed_count,
        "execution_mode": execution_mode,
        "partition_count": partition_count,
        "split_elapsed": split_elapsed,
        "dedup_elapsed": dedup_elapsed,
        "spark_elapsed": spark_elapsed,
        "checkpoint_elapsed": checkpoint_elapsed,
        "total_read_seconds": total_read_seconds,
        "total_write_seconds": total_write_seconds,
        "pending_path_type_summary": pending_path_type_summary,
        "slowest_result_rows": slowest_result_rows,
        "slow_path_count": len(slow_path_rows),
        "oversized_path_count": len(oversized_path_rows),
        "last_sort_key": None,
        "completed_at": completed_at,
        "last_source_sha256": meta_rows[-1]["source_sha256"] if meta_rows else None,
        "batch_commit_path": batch_commit_path,
    }

# %% [code cell 5]
meta_cache_started_at = time.perf_counter()
meta_cache_df, meta_cache_bucket_total_counts, meta_cache_total_count = prepare_meta_cache()
progress_summary = load_row_progress_summary()
completed_source_sha256_count = progress_summary["completed_source_sha256_count"]
bucket_progresses = [load_bucket_progress(bucket_id) for bucket_id in range(SHA_BUCKET_COUNT)]
recover_bucket_progress_from_batch_commits(bucket_progresses)
completed_batch_index = max(
    int(progress_summary.get("completed_batch_index") or 0),
    max((item["completed_batch_index"] for item in bucket_progresses), default=0),
)
bucket_pending_counts = initialize_bucket_pending_counts(bucket_progresses)
pending_row_count = sum(int(bucket_pending_counts.get(int(item["bucket_id"]), 0)) for item in bucket_progresses)
if BATCH_SIZE is None:
    pending_batch_count = sum(
        1
        for item in bucket_progresses
        if int(bucket_pending_counts.get(int(item["bucket_id"]), 0)) > 0
    )
else:
    pending_batch_count = sum(
        (int(bucket_pending_counts.get(int(item["bucket_id"]), 0)) + BATCH_SIZE - 1) // BATCH_SIZE
        for item in bucket_progresses
        if int(bucket_pending_counts.get(int(item["bucket_id"]), 0)) > 0
    )
estimated_total_batch_count = completed_batch_index + pending_batch_count
print(f"meta-cache summary: total_rows={meta_cache_total_count}")
print(f"已完成 source_sha256 数 = {completed_source_sha256_count}")
print(f"已完成全局批次号 = {completed_batch_index}")
print(f"待处理 metadata 行数 = {pending_row_count}")
print(f"预计待处理批次数(不含慢路径) = {pending_batch_count}")
print(f"预计续跑后的全局总批次数(不含慢路径) = {estimated_total_batch_count}")
print(f"prepare meta cache total elapsed = {time.perf_counter() - meta_cache_started_at:.2f}s")

# %% [code cell 6]
if globals().get("meta_cache_df") is None:
    raise RuntimeError("请先运行『prepare meta cache』单元格，构建并缓存全量 metadata")
progress_summary = load_row_progress_summary()
completed_source_sha256_count = progress_summary["completed_source_sha256_count"]
completed_batch_index = int(progress_summary.get("completed_batch_index") or 0)
global_batch_index = completed_batch_index
total_written_count = 0
total_skipped_existing_count = 0
total_processed_count = 0
print(f"主流程启动: completed_source_sha256_count={completed_source_sha256_count}, completed_batch_index={completed_batch_index}")
if RUN_MAIN_FLOW:
    bucket_progress_by_id = {int(item["bucket_id"]): item for item in bucket_progresses}
    for bucket_id in range(SHA_BUCKET_COUNT):
        bucket_progress = bucket_progress_by_id.get(int(bucket_id))
        if bucket_progress is None:
            bucket_progress = load_bucket_progress(bucket_id)
            bucket_progress_by_id[int(bucket_id)] = bucket_progress
        bucket_pending_count = int(bucket_pending_counts.get(int(bucket_id), bucket_progress.get("pending_count") or 0))
        bucket_progress["pending_count"] = bucket_pending_count
        if bucket_progress.get("initialized") and bucket_pending_count <= 0:
            print(f"bucket_id={bucket_id} 已完成，跳过主流程")
            continue
        cursor_sort_key = bucket_progress["last_sort_key"]
        cursor_sha256 = bucket_progress["last_source_sha256"]
        while True:
            fetch_started_at = time.perf_counter()
            batch_rows = fetch_meta_batch(bucket_id, cursor_sha256, limit_rows=BATCH_SIZE)
            print(f"批次 metadata 获取完成: bucket_id={bucket_id}, rows={len(batch_rows)}, elapsed={time.perf_counter() - fetch_started_at:.2f}s")
            if not batch_rows:
                break
            global_batch_index += 1
            print(f"批次 {global_batch_index}: bucket_id={bucket_id}, 本批 metadata 行数 = {len(batch_rows)}")
            started_at = time.perf_counter()
            batch_result = write_batch(global_batch_index, batch_rows, bucket_id=bucket_id)
            elapsed = time.perf_counter() - started_at
            total_written_count += batch_result["written_count"]
            total_skipped_existing_count += batch_result["skipped_existing_count"]
            total_processed_count += batch_result["processed_count"]
            cursor_sort_key = batch_result["last_sort_key"]
            cursor_sha256 = batch_result["last_source_sha256"]
            processed_count_for_pending = len(batch_rows)
            current_pending_count = bucket_progress.get("pending_count")
            current_total_count = bucket_progress.get("total_count")
            next_pending_count = None if current_pending_count is None else max(0, int(current_pending_count) - processed_count_for_pending)
            write_bucket_progress(
                bucket_id,
                cursor_sort_key,
                cursor_sha256,
                global_batch_index,
                total_count=current_total_count,
                pending_count=next_pending_count,
                initialized=(next_pending_count is not None),
            )
            bucket_progress["last_sort_key"] = cursor_sort_key
            bucket_progress["last_source_sha256"] = cursor_sha256
            bucket_progress["completed_batch_index"] = global_batch_index
            if current_total_count is not None:
                bucket_progress["total_count"] = int(current_total_count)
            bucket_progress["pending_count"] = next_pending_count
            bucket_progress["initialized"] = (next_pending_count is not None)
            avg_read_seconds = (batch_result["total_read_seconds"] / batch_result["written_count"]) if batch_result["written_count"] else 0.0
            avg_write_seconds = (batch_result["total_write_seconds"] / batch_result["written_count"]) if batch_result["written_count"] else 0.0
            effective_read_concurrency = (batch_result["total_read_seconds"] / elapsed) if elapsed > 0 else 0.0
            effective_write_concurrency = (batch_result["total_write_seconds"] / elapsed) if elapsed > 0 else 0.0
            print(
                f"  执行模式 = {batch_result['execution_mode']}，实际分区数 = {batch_result['partition_count']}，写入 = {batch_result['written_count']}，已存在跳过 = {batch_result['skipped_existing_count']}，"
                f"checkpoint = {batch_result['processed_count']}，read耗时汇总 = {batch_result['total_read_seconds']:.2f}s，"
                f"write耗时汇总 = {batch_result['total_write_seconds']:.2f}s，平均单条read = {avg_read_seconds:.3f}s，"
                f"平均单条write = {avg_write_seconds:.3f}s，等效read并发 = {effective_read_concurrency:.2f}，"
                f"等效write并发 = {effective_write_concurrency:.2f}，completed_at = {batch_result['completed_at']}，批次总耗时 = {elapsed:.2f}s"
            )
            print(
                f"  批次详细耗时: dedup={batch_result['dedup_elapsed']:.2f}s, prefilter={batch_result['split_elapsed']:.2f}s, "
                f"process={batch_result['spark_elapsed']:.2f}s, checkpoint={batch_result['checkpoint_elapsed']:.2f}s"
            )
            if RUN_MODE == "test" or BATCH_SIZE is None:
                break
        if RUN_MODE == "test":
            break
print(f"主流程完成，新增写入 = {total_written_count}")
print(f"主流程已存在跳过 = {total_skipped_existing_count}")
print(f"主流程新增 checkpoint 行数 = {total_processed_count}")
print(f"最新全局批次号 = {global_batch_index}")
if RUN_SLOW_PATH:
    slow_batch_index = global_batch_index
    slow_completed_batches = 0
    slow_total_input_rows = 0
    slow_total_written_rows = 0
    slow_total_elapsed_seconds = 0.0
    slow_total_consumed_files = 0
    while True:
        slow_rows, consumed_files, remainder_rows, bad_files, rebalanced_files = load_slow_path_rows(SLOW_PATH_BATCH_SIZE)
        if not slow_rows:
            print("慢路径队列为空")
            break
        if rebalanced_files:
            print(f"慢路径队列重分片: files={len(rebalanced_files)}")
        slow_batch_index += 1
        print(f"慢路径批次 {slow_batch_index}: rows={len(slow_rows)}")
        sample_slow_sha256 = next((str(row.get("source_sha256") or "").strip().lower() for row in slow_rows if str(row.get("source_sha256") or "").strip()), None)
        if sample_slow_sha256:
            print(f"慢路径批次样例 sha256: {sample_slow_sha256}")
        slow_started_at = time.perf_counter()
        slow_result = write_batch(
            slow_batch_index,
            slow_rows,
            enable_slow_path_split=False,
            target_partitions=SLOW_PATH_TARGET_PARTITIONS,
            rows_per_partition=max(1, int(math.ceil(len(slow_rows) / float(SLOW_PATH_TARGET_PARTITIONS)))),
            forced_read_threads=SLOW_PATH_READ_THREADS,
            forced_write_threads=SLOW_PATH_WRITE_THREADS,
            force_driver_exists_check=SLOW_PATH_FORCE_DRIVER_EXISTS_CHECK,
        )
        slow_elapsed = time.perf_counter() - slow_started_at
        total_written_count += slow_result["written_count"]
        total_skipped_existing_count += slow_result["skipped_existing_count"]
        total_processed_count += slow_result["processed_count"]
        appended_remainder_rows, quarantined_bad_files = rewrite_slow_path_rows(consumed_files, remainder_rows, bad_files)
        remaining_slow_queue_files = count_queue_files(SLOW_PATH_QUEUE_ROOT)
        slow_completed_batches += 1
        slow_total_input_rows += len(slow_rows)
        slow_total_written_rows += slow_result["written_count"]
        slow_total_elapsed_seconds += slow_elapsed
        slow_total_consumed_files += len(consumed_files)
        avg_slow_batch_rows = float(slow_total_input_rows) / float(slow_completed_batches)
        avg_slow_batch_written = float(slow_total_written_rows) / float(slow_completed_batches)
        avg_slow_batch_elapsed = float(slow_total_elapsed_seconds) / float(slow_completed_batches)
        avg_slow_queue_files_per_batch = float(slow_total_consumed_files) / float(slow_completed_batches)
        estimated_remaining_batches = estimate_remaining_batches_by_queue_files(
            remaining_slow_queue_files,
            slow_completed_batches,
            slow_total_consumed_files,
        )
        estimated_remaining_eta_seconds = estimated_remaining_batches * avg_slow_batch_elapsed
        print(
            f"慢路径批次完成: execution_mode={slow_result['execution_mode']}, written={slow_result['written_count']}, skipped_existing={slow_result['skipped_existing_count']}, "
            f"remainder_rows={len(remainder_rows)}, appended_remainder_rows={appended_remainder_rows}, bad_files={len(bad_files)}, "
            f"quarantined_bad_files={quarantined_bad_files}, elapsed={slow_elapsed:.2f}s"
        )
        print(
            f"慢路径剩余估算: queue_files={remaining_slow_queue_files}, estimated_batches={estimated_remaining_batches}, "
            f"eta={format_seconds_compact(estimated_remaining_eta_seconds)}, avg_batch_rows={avg_slow_batch_rows:.1f}, "
            f"avg_written={avg_slow_batch_written:.1f}, avg_elapsed={avg_slow_batch_elapsed:.2f}s, "
            f"avg_queue_files_per_batch={avg_slow_queue_files_per_batch:.2f}"
        )
print(f"超大文档队列 shard 数量 = {count_queue_files(OVERSIZED_PATH_QUEUE_ROOT)}")

# %% [code cell 7]
final_progress_summary = load_row_progress_summary()
total_count = int(final_progress_summary["completed_source_sha256_count"])
oa_count = int(final_progress_summary["oa_count"])

summary_payload = {
    "total_count": int(total_count),
    "oa_count": int(oa_count),
    "non_oa_count": int(total_count - oa_count),
}
put_s3_object(SUMMARY_PATH, json.dumps(summary_payload, ensure_ascii=False, indent=2).encode("utf-8"))
print(summary_payload)

# %% [code cell 8]

spark.stop()

