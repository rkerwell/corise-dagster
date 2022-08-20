import csv
from datetime import datetime
from typing import List

from dagster import In, Nothing, Out, job, op, usable_as_dagster_type
from pydantic import BaseModel


@usable_as_dagster_type(description="Stock data")
class Stock(BaseModel):
    date: datetime
    close: float
    volume: int
    open: float
    high: float
    low: float

    @classmethod
    def from_list(cls, input_list: list):
        """Do not worry about this class method for now"""
        return cls(
            date=datetime.strptime(input_list[0], "%Y/%m/%d"),
            close=float(input_list[1]),
            volume=int(float(input_list[2])),
            open=float(input_list[3]),
            high=float(input_list[4]),
            low=float(input_list[5]),
        )


@usable_as_dagster_type(description="Aggregation of stock data")
class Aggregation(BaseModel):
    date: datetime
    high: float


@op(
    config_schema={"s3_key": str},
    out={"stocks": Out(dagster_type=List[Stock])},
    tags={"kind": "s3"},
    description="Get a list of stocks from an S3 file",
)
def get_s3_data(context):
    output = list()
    with open(context.op_config["s3_key"]) as csvfile:
        reader = csv.reader(csvfile)
        for row in reader:
            stock = Stock.from_list(row)
            output.append(stock)
    return output


## Data Contract ->
# Input = Stocks data of type List
# Ouput = Custom data type Aggregation with greatest `high` & corresponding `date`
@op(
    ins={"stocks" : In(dagster_type=List[Stock])},
    out={"aggregation": Out(dagster_type=Aggregation)},
    description="Given a list of stocks return the Aggregation with the greatest price",
)
def process_data(stocks):
    highest_stock = max(stocks, key=lambda s:s.high)  # find max stock using `high` attribute of each element
    return Aggregation(date=highest_stock.date, high=highest_stock.high)


## Data Contract ->
# Input = highest stock price of type Aggregation
# Output = Nothing 
@op( 
    ins={"aggregation": In(dagster_type=Aggregation)},
    out=Out(dagster_type=Nothing),
    tags={"kind":"redis"},
    description="Upload an Aggregation to Redis",
)
def put_redis_data(aggregation):
    pass

## DAG dependencies
@job
def week_1_pipeline():
    A = get_s3_data()
    B = process_data(A)
    C = put_redis_data(B)