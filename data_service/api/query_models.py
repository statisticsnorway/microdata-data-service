import re
from typing import Optional

from pydantic import BaseModel, validator


class InputTimePeriodQuery(BaseModel):
    dataStructureName: str
    version: str
    startDate: int
    stopDate: int
    population: Optional[list]
    include_attributes: Optional[bool] = False

    @validator('version')
    def check_for_sem_ver(cls, v):
        if not validate(v):
            raise ValueError("==> version {} is not a valid semantic version.".format(v))


class InputTimeQuery(BaseModel):
    dataStructureName: str
    version: str
    date: int
    population: Optional[list]
    include_attributes: Optional[bool] = False

    @validator('version')
    def check_for_sem_ver(cls, v):
        if not validate(v):
            raise ValueError("==> version {} is not a valid semantic version.".format(v))


class InputFixedQuery(BaseModel):
    dataStructureName: str
    version: str
    population: Optional[list]
    include_attributes: Optional[bool] = False

    @validator('version')
    def check_for_sem_ver(cls, v):
        if not validate(v):
            raise ValueError("==> version {} is not a valid semantic version.".format(v))


def validate(version: str = False):
    pattern = re.compile(r"^([0-9]+)\.([0-9]+)\.([0-9]+)\.([0-9]+)$")
    if pattern.match(version):
        return True
