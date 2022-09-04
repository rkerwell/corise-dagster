from typing import List

from dagster import In, Nothing, Out, ResourceDefinition, graph, op
from dagster_ucr.project.types import Aggregation, Stock
from dagster_ucr.resources import mock_s3_resource, redis_resource, s3_resource

# Use S3 resource and `get_data()` method to fetch rows from s3 bucket
# Config provided in the job definition 
@op(
    required_resource_keys={"s3"},
    config_schema={"s3_key": str},
    out={"stocks": Out(dagster_type=List[Stock])},
    tags={"kind": "s3"},
    description="Get a list of stocks from an S3 file",
)
def get_s3_data(context):
    s3_key = context.op_config["s3_key"]    # define s3 key
    data = context.resources.s3.get_data(s3_key)    # fetch rows/data using key
    output = list()
    for row in data:
        stock = Stock.from_list(row)
        output.append(stock)
    return output      

# from Week 1
@op(
    ins={"stocks" : In(dagster_type=List[Stock])},
    out={"aggregation": Out(dagster_type=Aggregation)},
    description="Given a list of stocks return the Aggregation with the greatest price",
)
def process_data(stocks):
    highest_stock = max(stocks, key=lambda s:s.high)  # find max stock using `high` attribute of each element
    return Aggregation(date=highest_stock.date, high=highest_stock.high)


# Use Redis resource and `put_data()` method to load data as string only
# Config provided in the job definition 
@op(
    required_resource_keys={"redis"},
    ins={"aggregation": In(dagster_type=Aggregation)},
    tags={"kind":"redis"},
    description="Upload an Aggregation to Redis",
)
def put_redis_data(context, aggregation):
    context.resources.redis.put_data(str(aggregation.date), str(aggregation.high))
    

# from Week 1 
@graph
def week_2_pipeline():
    A = get_s3_data()
    B = process_data(A)
    C = put_redis_data(B)


local = {
    "ops": {"get_s3_data": {"config": {"s3_key": "prefix/stock.csv"}}},
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
    "ops": {"get_s3_data": {"config": {"s3_key": "prefix/stock.csv"}}},
}

local_week_2_pipeline = week_2_pipeline.to_job(
    name="local_week_2_pipeline",
    config=local,
    resource_defs={"s3": mock_s3_resource, "redis": ResourceDefinition.mock_resource()},
)

docker_week_2_pipeline = week_2_pipeline.to_job(
    name="docker_week_2_pipeline",
    config=docker,
    resource_defs={"s3": s3_resource, "redis": redis_resource},
)
