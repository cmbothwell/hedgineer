from datetime import date
from typing import Any

from pydantic import BaseModel

type AuditFact = tuple[int, str, Any, date]
type AuditTrail = list[AuditFact]
type Header = list[str]
type SMData = list[tuple]
type ColumnIndex = dict[str, int]
type AttributePair = tuple[str, Any]
type FlatFactSet = tuple[int, date, list[AttributePair]]


class SecurityMaster(BaseModel):
    header: Header
    data: SMData
    col_index: ColumnIndex

    @classmethod
    def from_tuple(cls, t: tuple[Header, SMData, ColumnIndex]):
        return cls(header=t[0], data=t[1], col_index=t[2])

    def to_tuple(self):
        return (self.header, self.data, self.col_index)


type TableData = list[tuple]


class JoinedPositions(BaseModel):
    header: Header
    data: TableData

    @classmethod
    def from_tuple(cls, t: tuple[Header, TableData]):
        return cls(header=t[0], data=t[1])

    def to_tuple(self):
        return (self.header, self.data)
