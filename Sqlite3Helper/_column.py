# coding: utf8
from __future__ import annotations
from abc import ABC
from enum import StrEnum
from dataclasses import dataclass, field

from ._types_def import DataType, GeneralValueTypes
from ._util_func import to_string


@dataclass
class Column(object):
    col_name: str  # 内部使用名称时用 name 而不要用 col_name
    data_type: DataType
    primary_key: bool = False
    nullable: bool = True
    unique: bool = False
    has_default: bool = False
    default: GeneralValueTypes = 0

    secure: bool = False
    table: Table = None

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

    @property
    def name(self):
        if self.table is not None and len(self.table.table_as) != 0:
            return f"{self.table.table_as}.{self.col_name}"

        return self.col_name

    @name.setter
    def name(self, value: str):
        self.col_name = value


@dataclass
class Table(ABC):
    table: str = ""
    table_as: str = ""

    all: list[Column] = field(default_factory=list)

    def __post_init__(self):
        if len(self.table) == 0:
            raise ValueError("table cannot be empty")
        for i in self.__dir__():
            a = getattr(self, i)
            if isinstance(a, Column):
                self.all.append(a)

        # 只要是在 Table 内定义的 Column 都会有 table 属性
        for c in self.all:
            c.table = self

    def as_(self, alias: str) -> Table:
        self.table_as = alias
        self.table = f"{self.table} {alias}"
        return self

    def reset_as(self):
        self.table = self.table.removesuffix(f" {self.table_as}")
        self.table_as = ""


class JoinType(StrEnum):
    INNER_JOIN = "INNER JOIN"
    LEFT_JOIN = "LEFT JOIN"


# def join(join_type: JoinType, left: Column, right: Column)
