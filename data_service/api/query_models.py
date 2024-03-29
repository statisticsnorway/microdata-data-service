import copy
import re
from typing import Optional

from pydantic import BaseModel, field_validator


class InputQuery(BaseModel):
    dataStructureName: str
    version: str
    population: Optional[list] = None
    includeAttributes: Optional[bool] = False

    @field_validator("version")
    @classmethod
    def check_for_sem_ver(cls, version):  # pylint: disable=no-self-argument
        pattern = re.compile(r"^([0-9]+)\.([0-9]+)\.([0-9]+)\.([0-9]+)$")
        if not pattern.match(version):
            raise ValueError(f"'{version}' is not a valid semantic version.")
        return version

    def get_file_version(self) -> str:
        version_numbers = self.version.split(".")
        return f"{version_numbers[0]}_{version_numbers[1]}"

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
