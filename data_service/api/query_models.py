import re
from typing import Optional

from pydantic import BaseModel, validator


class InputQuery(BaseModel):
    dataStructureName: str
    version: str
    population: Optional[list]
    include_attributes: Optional[bool] = False

    @validator('version')
    def check_for_sem_ver(cls, version):
        pattern = re.compile(r"^([0-9]+)\.([0-9]+)\.([0-9]+)\.([0-9]+)$")
        if not pattern.match(version):
            raise ValueError("==> version {} is not a valid semantic version.".format(version))
        return version


class InputTimePeriodQuery(InputQuery):
    startDate: int
    stopDate: int


class InputTimeQuery(InputQuery):
    date: int


class InputFixedQuery(InputQuery):
    pass
