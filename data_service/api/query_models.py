import re
from typing import Optional

from pydantic.main import BaseModel


class InputTimePeriodQuery(BaseModel):
    dataStructureName: str
    version: str
    startDate: int
    stopDate: int
    population: Optional[list]
    include_attributes: Optional[bool] = False


class InputTimeQuery(BaseModel):
    dataStructureName: str
    version: str
    date: int
    population: Optional[list]
    include_attributes: Optional[bool] = False


class InputFixedQuery(BaseModel):
    dataStructureName: str
    version: str
    population: Optional[list]
    include_attributes: Optional[bool] = False


class QueryValidator:
    def validate(query: BaseModel):
        pattern = re.compile(r"^([0-9]+)\.([0-9]+)\.([0-9]+)\.([0-9]+)$")
        if not pattern.match(query.version):
            raise Exception("==> version {} is not a valid semantic version.".format(query.version))
