# coding: utf8
from abc import ABC
from dataclasses import dataclass, field
from ._types_def import DataType, GeneralValueTypes
from ._util_func import to_string


@dataclass
class Column(object):
    name: str
    data_type: DataType
    primary_key: bool = False
    nullable: bool = True
    unique: bool = False
    has_default: bool = False
    default: GeneralValueTypes = 0

    secure: bool = False

    def __post_init__(self):
        if self.secure is True and self.data_type != DataType.BLOB:
            raise ValueError("Only BLOB data can be secured")

    def __str__(self):
        head = f"{self.name} {self.data_type.value}"
        if self.primary_key:
            head = f"{head} PRIMARY KEY"
        if not self.nullable:
            head = f"{head} NOT NULL"
        if self.unique:
            head = f"{head} UNIQUE"
        if self.has_default:
            head = f"{head} DEFAULT {to_string(self.default)}"
        return head

    __repr__ = __str__


@dataclass
class Table(ABC):
    table: str = ""

    all: list[Column] = field(default_factory=list)

    def __post_init__(self):
        if len(self.table) == 0:
            raise ValueError("table name must be set")
        for i in self.__dir__():
            a = getattr(self, i)
            if isinstance(a, Column):
                self.all.append(a)
