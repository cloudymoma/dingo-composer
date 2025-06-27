import datetime
import pendulum

from airflow.models.dag import DAG
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateBatchOperator,
)
from airflow.utils.trigger_rule import TriggerRule

# --- Common Configuration ---
# You can use Airflow Variables or retrieve the project_id dynamically
# from the execution context via '{{ var.value.gcp_project }}'
PROJECT_ID = "du-hast-mich"
REGION = "us-central1"
BUCKET_NAME = "dingoproc"
GCS_FATJAR_PATH = f"gs://{BUCKET_NAME}/jars/gcptest-assembly-0.1.0.jar"
PHS_CLUSTER_NAME = "dingohist"
PHS_RESOURCE_NAME = (
    f"projects/{PROJECT_ID}/regions/{REGION}/clusters/{PHS_CLUSTER_NAME}"
)
RUNTIME_VERSION = "1.2"

# --- Base Properties for Spark Jobs ---
# These properties are common across multiple jobs
BASE_PROPERTIES = {
    "spark.dynamicAllocation.enabled": "true",
    "spark.dynamicAllocation.initialExecutors": "2",
    "spark.dynamicAllocation.minExecutors": "2",
    "spark.dynamicAllocation.maxExecutors": "100",
    "spark.sql.adaptive.enabled": "true",
    "spark.dataproc.enhanced.optimizer.enabled": "true",
    "dataproc.profiling.enabled": "true",
    "dataproc.profiling.name": "dingoserverless",
}

# --- Batch Configurations ---
# We define a dictionary for each job from your script

# 1. Configuration for __run_serverless_standard
standard_spark_properties = BASE_PROPERTIES.copy()
standard_spark_properties.update({
    "spark.executor.cores": "4",
    "spark.executor.memory": "25g",
    "spark.executor.memoryOverhead": "4g",
    "spark.driver.cores": "4",
    "spark.driver.memory": "25g",
    "spark.driver.memoryOverhead": "4g",
    "spark.dataproc.driver.compute.tier": "standard",
    "spark.dataproc.executor.compute.tier": "standard",
    "spark.dataproc.driver.disk.tier": "standard",
    "spark.dataproc.driver.disk.size": "500g",
    "spark.dataproc.executor.disk.tier": "standard",
    "spark.dataproc.executor.disk.size": "500g",
})

BATCH_STANDARD_CONFIG = {
    "spark_batch": {
        "main_class": "GcpTest",
        "jar_file_uris": [GCS_FATJAR_PATH],
        "args": ["--spark.driver.log.level=INFO"],
    },
    "environment_config": {
        "execution_config": {
            "subnetwork_uri": "default",
            "service_account": "bindiego@du-hast-mich.iam.gserviceaccount.com"
        },
        "peripherals_config": {
            "spark_history_server_config": {
                "dataproc_cluster": PHS_RESOURCE_NAME,
            },
        },
    },
    "runtime_config": {
        "version": RUNTIME_VERSION,
        "properties": standard_spark_properties
    },
}

# 2. Configuration for __run_serverless (Premium)
premium_spark_properties = BASE_PROPERTIES.copy()
premium_spark_properties.update({
    "spark.executor.cores": "4",
    "spark.executor.memory": "25g",
    "spark.executor.memoryOverhead": "4g",
    "spark.driver.cores": "4",
    "spark.driver.memory": "25g",
    "spark.driver.memoryOverhead": "4g",
    "spark.dataproc.driver.compute.tier": "premium",
    "spark.dataproc.executor.compute.tier": "premium",
    "spark.dataproc.driver.disk.tier": "premium",
    "spark.dataproc.driver.disk.size": "375g",
    "spark.dataproc.executor.disk.tier": "premium",
    "spark.dataproc.executor.disk.size": "375g",
})

BATCH_PREMIUM_CONFIG = {
    "spark_batch": {
        "main_class": "GcpTest",
        "jar_file_uris": [GCS_FATJAR_PATH],
        "args": ["--spark.driver.log.level=INFO"],
    },
    "environment_config": {
        "execution_config": {
            "subnetwork_uri": "default",
            "service_account": "bindiego@du-hast-mich.iam.gserviceaccount.com"
        },
        "peripherals_config": {
            "spark_history_server_config": {
                "dataproc_cluster": PHS_RESOURCE_NAME,
            },
        },
    },
    "runtime_config": {
        "version": RUNTIME_VERSION,
        "properties": premium_spark_properties
    },
}

# 3. Configuration for __nqe (Native Query Engine)
nqe_spark_properties = BASE_PROPERTIES.copy()
nqe_spark_properties.update({
    "spark.executor.cores": "4",
    "spark.executor.memory": "5g",
    "spark.executor.memoryOverhead": "4g",
    "spark.memory.offHeap.size": "20g",
    "spark.driver.cores": "4",
    "spark.driver.memory": "25g",
    "spark.driver.memoryOverhead": "4g",
    "spark.dataproc.driver.compute.tier": "premium",
    "spark.dataproc.executor.compute.tier": "premium",
    "spark.dataproc.driver.disk.tier": "premium",
    "spark.dataproc.driver.disk.size": "375g",
    "spark.dataproc.executor.disk.tier": "premium",
    "spark.dataproc.executor.disk.size": "375g",
    "spark.dataproc.runtimeEngine": "native", # Key property for NQE
})

BATCH_NQE_CONFIG = {
    "spark_batch": {
        "main_class": "GcpTest",
        "jar_file_uris": [GCS_FATJAR_PATH],
        "args": ["--spark.driver.log.level=INFO"],
    },
    "environment_config": {
        "execution_config": {
            "subnetwork_uri": "default",
            "service_account": "bindiego@du-hast-mich.iam.gserviceaccount.com"
        },
        "peripherals_config": {
            "spark_history_server_config": {
                "dataproc_cluster": PHS_RESOURCE_NAME,
            },
        },
    },
    "runtime_config": {
        "version": RUNTIME_VERSION,
        "properties": nqe_spark_properties
    },
}

with DAG(
    dag_id="dataproc_sequential_orchestration_dag",
    #start_date=datetime.datetime(2025, 6, 25),
    start_date=pendulum.datetime(2025, 6, 25, tz="Asia/Shanghai"),
    schedule_interval="0 1 * * *",
    catchup=False,
    tags=["dataproc", "serverless", "timezone"],
) as dag:
    # TASK 1: Run __run_serverless_standard in a sync/blocking mode
    # - retries=1: Retry once if it fails.
    # - deferrable=False (default): The task waits for job completion.
    run_serverless_standard = DataprocCreateBatchOperator(
        task_id="run_serverless_standard_sync",
        project_id=PROJECT_ID,
        region=REGION,
        # Use Jinja templating for a unique batch ID per run
        batch_id="standard-{{ ts_nodash | lower}}",
        batch=BATCH_STANDARD_CONFIG,
        retries=1,
        retry_delay=datetime.timedelta(seconds=5),
    )

    # TASK 2: Run __run_serverless in an async/non-blocking mode
    # - retries=1: Retry submission once if it fails.
    # - deferrable=True: Submits the job and moves on (non-blocking).
    # - trigger_rule="all_done": Runs after Task 1 finishes, regardless of its success or failure.
    run_serverless_premium = DataprocCreateBatchOperator(
        task_id="run_serverless_premium_async",
        project_id=PROJECT_ID,
        region=REGION,
        batch_id="premium-{{ ts_nodash | lower}}",
        batch=BATCH_PREMIUM_CONFIG,
        retries=1,
        retry_delay=datetime.timedelta(seconds=5),
        deferrable=True, # Make it non-blocking
        trigger_rule=TriggerRule.ALL_DONE,
    )

    # TASK 3: Run __nqe in an async/non-blocking mode
    # - retries=0: Do not retry if submission fails.
    # - deferrable=True: Submits the job and moves on (non-blocking).
    # - trigger_rule="all_done": Runs after Task 2 finishes.
    # - The DAG will fail if this task fails (default behavior with no retries).
    run_nqe = DataprocCreateBatchOperator(
        task_id="run_nqe_async",
        project_id=PROJECT_ID,
        region=REGION,
        batch_id="nqe-{{ ts_nodash | lower}}",
        batch=BATCH_NQE_CONFIG,
        retries=0, # No retries
        deferrable=True, # Make it non-blocking
        trigger_rule=TriggerRule.ALL_DONE,
    )

    # Define the task sequence
    run_serverless_standard >> run_serverless_premium >> run_nqe
