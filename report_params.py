from dataclasses import dataclass, field
from marshmallow_dataclass import class_schema, Optional
import yaml


@dataclass()
class PeriodParams:
    date_from: str
    date_to: str


@dataclass()
class GroupingParams:
    receipt_date: bool = field(default=False)
    region: bool = field(default=False)
    channel: bool = field(default=False)


@dataclass()
class ReportParams:
    product_name_path: str
    period: PeriodParams
    group_by: GroupingParams
    kkt_category_filters: Optional[str]

ReportParamsSchema = class_schema(ReportParams)


def read_report_params(path: str) -> ReportParams:
    with open(path, "r") as input_stream:
        schema = ReportParamsSchema()
        return schema.load(yaml.safe_load(input_stream))
