from typing import List

from dagster import (
    In,
    Nothing,
    Out,
    ResourceDefinition,
    RetryPolicy,
    RunRequest,
    ScheduleDefinition,
    SkipReason,
    graph,
    op,
    sensor,
    static_partitioned_config,
)
from project.resources import mock_s3_resource, redis_resource, s3_resource
from project.sensors import get_s3_keys
from project.types import Aggregation, Stock


# from WEEK 2
@op(
    required_resource_keys={"s3"},
    config_schema={"s3_key": str},
    out={"stocks": Out(dagster_type=List[Stock])},
    tags={"kind": "s3"},
    description="Get a list of stocks from an S3 file",
)
def get_s3_data(context):
    s3_key = context.op_config["s3_key"]    
    data = context.resources.s3.get_data(s3_key)    
    output = list()
    for row in data:
        stock = Stock.from_list(row)
        output.append(stock)
    return output 


# from WEEK 1
@op(
    ins={"stocks" : In(dagster_type=List[Stock])},
    out={"aggregation": Out(dagster_type=Aggregation)},
    description="Given a list of stocks return the Aggregation with the greatest price",
)
def process_data(stocks):
    highest_stock = max(stocks, key=lambda s:s.high) 
    return Aggregation(date=highest_stock.date, high=highest_stock.high)


# from WEEK 2
@op(
    required_resource_keys={"redis"},
    ins={"aggregation": In(dagster_type=Aggregation)},
    tags={"kind":"redis"},
    description="Upload an Aggregation to Redis",
)
def put_redis_data(context, aggregation):
    context.resources.redis.put_data(str(aggregation.date), str(aggregation.high))


# from WEEK 1 
@graph
def week_3_pipeline():
    A = get_s3_data()
    B = process_data(A)
    C = put_redis_data(B)


local = {
    "ops": {"get_s3_data": {"config": {"s3_key": "prefix/stock_9.csv"}}},
}


docker = {
    "resources": {
        "s3": {
            "config": {
                "bucket": "dagster",
                "access_key": "test",
                "secret_key": "test",
                "endpoint_url": "http://localstack:4566",
            }
        },
        "redis": {
            "config": {
                "host": "redis",
                "port": 6379,
            }
        },
    },
    "ops": {"get_s3_data": {"config": {"s3_key": "prefix/stock_9.csv"}}},
}


# Static Partitioning by month , format of ingested files `prefix/stock_{month}.csv`
KEYS = [str(_) for _ in range(1,11)]
@static_partitioned_config(partition_keys=KEYS)
def docker_config(partition_key: str):
    return {
            "resources": {
                    "s3": {
                        "config": {
                            "bucket": "dagster",
                            "access_key": "test",
                            "secret_key": "test",
                            "endpoint_url": "http://localstack:4566",
                        }
                    },
                    "redis": {
                        "config": {
                            "host": "redis",
                            "port": 6379,
                        }
                    },
                },
            "ops": {"get_s3_data": {"config": {"s3_key": f"prefix/stock_{partition_key}.csv"}}},
            }


local_week_3_pipeline = week_3_pipeline.to_job(
    name="local_week_3_pipeline",
    config=local,
    resource_defs={
        "s3": mock_s3_resource,
        "redis": ResourceDefinition.mock_resource(),
    },
)

docker_week_3_pipeline = week_3_pipeline.to_job(
    name="docker_week_3_pipeline",
    config=docker_config,
    resource_defs={
        "s3": s3_resource,
        "redis": redis_resource,
    },
    op_retry_policy=RetryPolicy(max_retries=10, delay=1)        # retry policy for Redis
)


#  run `local_week_3_pipeline` every 15 mins
local_week_3_schedule = ScheduleDefinition(job=local_week_3_pipeline, cron_schedule="*/15 * * * *")

# run `docker_week_3_pipeline` every hour
docker_week_3_schedule = ScheduleDefinition(job=docker_week_3_pipeline, cron_schedule="0 * * * *")


# check `dagster` bucket with prefix `prefix` 
# helper fn `get_s3_keys()` to check for new files
@sensor(
    job=docker_week_3_pipeline,
    minimum_interval_seconds=30
)
def docker_week_3_sensor(context):
    new_files = get_s3_keys(
        bucket="dagster",
        prefix="prefix",
        endpoint_url="http://host.docker.internal:4566"
    )
    if not new_files:
        yield SkipReason("No new s3 files found in bucket.")
        return
    for new_file in new_files:
        yield RunRequest(
            run_key=new_file,
            run_config={    # use Dockerized config from @72
                "resources": { 
                    "s3": {
                        "config": {
                            "bucket": "dagster",
                            "access_key": "test",
                            "secret_key": "test",
                            "endpoint_url": "http://localstack:4566" 
                        }
                    },
                    "redis": {
                        "config": {
                            "host": "redis",
                            "port": 6379,
                        }
                    }
            },
            "ops": {"get_s3_data": {"config": {"s3_key": new_file}}},         # run op on each new_file
            }
        )
