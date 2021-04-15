from typing import Optional

from pydantic.main import BaseModel


class InputQuery(BaseModel):
    dataStructureName: str
    version: str
    startDate: str
    stopDate: str
    values: Optional[list]
    population: Optional[dict]
    interval_filter: Optional[str]
    include_attributes: Optional[bool] = False
