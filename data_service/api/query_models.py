import copy
import re
from typing import Optional

from pydantic import BaseModel, validator


class InputQuery(BaseModel):
    dataStructureName: str
    version: str
    population: Optional[list[int]]
    includeAttributes: Optional[bool] = False

    @validator('version')
    def check_for_sem_ver(cls, version):  # pylint: disable=no-self-argument
        pattern = re.compile(r"^([0-9]+)\.([0-9]+)\.([0-9]+)\.([0-9]+)$")
        if not pattern.match(version):
            raise ValueError(
                f"'{version}' is not a valid semantic version."
            )
        return version

    def __str__(self) -> str:
        temp: InputQuery = copy.deepcopy(self)
        if temp.population:
            temp.population = f"<length: {len(temp.population)}>"
        return super(InputQuery, temp).__str__()


class InputTimePeriodQuery(InputQuery):
    startDate: int
    stopDate: int


class InputTimeQuery(InputQuery):
    date: int


class InputFixedQuery(InputQuery):
    pass
