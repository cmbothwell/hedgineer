from datetime import date
from typing import Any

from pydantic import BaseModel

# mypy complains about this Python 3.12 feature
type AuditFact = tuple[int, str, Any, date]  # type: ignore
type AuditTrail = list[AuditFact]  # type: ignore
type Header = list[str]  # type: ignore
type SMData = list[tuple]  # type: ignore
type ColumnIndex = dict[str, int]  # type: ignore
type AttributePair = tuple[str, Any]  # type: ignore
type FlatFactSet = tuple[int, date, list[AttributePair]]  # type: ignore
type TableData = list[tuple]  # type: ignore


class SecurityMaster(BaseModel):
    header: Header
    data: SMData
    col_index: ColumnIndex

    @classmethod
    def from_tuple(cls, t: tuple[Header, SMData, ColumnIndex]):
        return cls(header=t[0], data=t[1], col_index=t[2])

    def to_tuple(self):
        return (self.header, self.data, self.col_index)


class JoinedPositions(BaseModel):
    header: Header
    data: TableData

    @classmethod
    def from_tuple(cls, t: tuple[Header, TableData]):
        return cls(header=t[0], data=t[1])

    def to_tuple(self):
        return (self.header, self.data)
