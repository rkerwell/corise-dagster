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
    asset,
    with_resources
)
from project.resources import redis_resource, s3_resource
from project.types import Aggregation, Stock


@asset(
    required_resource_keys={"s3"},
    config_schema={"s3_key": str},
    group_name="corise",
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


@asset(
    group_name="corise",
    description="Given a list of stocks return the Aggregation with the greatest price",
)
def process_data(stocks):
    highest_stock = max(stocks, key=lambda s:s.high) 
    return Aggregation(date=highest_stock.date, high=highest_stock.high)


@asset(
    required_resource_keys={"redis"},
    group_name="corise",
    description="Upload an Aggregation to Redis",
)
def put_redis_data(context, aggregation):
    context.resources.redis.put_data(str(aggregation.date), str(aggregation.high))


get_s3_data_docker, process_data_docker, put_redis_data_docker = with_resources(
    definitions=[get_s3_data,process_data, put_redis_data],
    resource_defs={"s3": s3_resource, "redis": redis_resource},
    resource_config_by_key={
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
            }
)
