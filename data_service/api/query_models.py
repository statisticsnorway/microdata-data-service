import re
from typing import Optional

from pydantic.main import BaseModel


class InputTimePeriodQuery(BaseModel):
    dataStructureName: str
    version: str
    startDate: int
    stopDate: int
    population: Optional[dict]
    include_attributes: Optional[bool] = False


class InputTimeQuery(BaseModel):
    dataStructureName: str
    version: str
    date: int
    population: Optional[dict]
    include_attributes: Optional[bool] = False


class InputFixedQuery(BaseModel):
    dataStructureName: str
    version: str
    population: Optional[dict]
    include_attributes: Optional[bool] = False


class QueryValidator:
    def validate(query: BaseModel):
        if query.dataStructureName is None or len(query.dataStructureName) == 0:
            raise Exception('==> dataStructureName is required.')
        if query.version is None or len(query.version) == 0:
            raise Exception('==> version is required.')
        pattern = re.compile(r"^([0-9]+)\.([0-9]+)\.([0-9]+)\.([0-9]+)$")
        if not pattern.match(query.version):
            raise Exception('==> version {version} is not a valid semantic version.')


