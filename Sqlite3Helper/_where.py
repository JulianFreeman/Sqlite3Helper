# coding: utf8
from __future__ import annotations
import os
import time
import sqlite3
from enum import Enum
from ._crypto import NotRandomFernet
from ._types_def import (
    GeneralValueTypes, BlobType,
)
from ._util_func import to_string, implicitly_convert
from ._column import Column


class Expression(object):

    def __init__(self, expr: str):
        self._expr = expr

    def __str__(self):
        return self._expr

    def and_(self, expression: Expression):
        return Expression(f"{self._expr} AND {expression}")

    def or_(self, expression: Expression, high_priority: bool = False):
        statement = f"{self._expr} OR {expression}"
        if high_priority:
            statement = f"({statement})"
        return Expression(statement)

    def exists(self, not_: bool = False):
        mark = "EXISTS"
        if not_:
            mark = "NOT EXISTS"
        return Expression(f"{mark} ({self._expr})")


class Operand(object):

    def __init__(
            self,
            column: Column | str,
            key: bytes = None,
            fix_time: int = None,
            fix_iv: bytes = None,
    ):
        self._column = column
        self._key = key
        self._fix_time = fix_time
        self._fix_iv = fix_iv
        self._name = column.name if isinstance(column, Column) else column

    def _try_encrypt(self, value: GeneralValueTypes) -> GeneralValueTypes:
        if isinstance(self._column, Column):
            # 这里主要为了转换 BlobType
            value = implicitly_convert(self._column.data_type, value)
            if self._key is not None and self._column.secure and isinstance(value, BlobType):
                fix_time = self._fix_time if self._fix_time is not None else int(time.time())
                fix_iv = self._fix_iv if self._fix_iv is not None else os.urandom(16)
                value = value.encrypt(NotRandomFernet(self._key, fix_time, fix_iv))

        return value

    def equal_to(self, value: GeneralValueTypes, not_: bool = False):
        value = self._try_encrypt(value)
        op = "!=" if not_ else "="
        return Expression(f"{self._name} {op} {to_string(value)}")

    # 上面的相等比较可能会用在字符串或者二进制数据上，所以进行隐式转换并尝试加密
    # 对于不等比较一般只用于数字，差别不大，所以不进行隐式转换

    def less_than(self, value: GeneralValueTypes):
        return Expression(f"{self._name} < {to_string(value)}")

    def greater_than(self, value: GeneralValueTypes):
        return Expression(f"{self._name} > {to_string(value)}")

    def less_equal(self, value: GeneralValueTypes):
        return Expression(f"{self._name} <= {to_string(value)}")

    def greater_equal(self, value: GeneralValueTypes):
        return Expression(f"{self._name} >= {to_string(value)}")

    def between(self, minimum: GeneralValueTypes, maximum: GeneralValueTypes, not_: bool = False):
        mark = "BETWEEN"
        if not_:
            mark = "NOT BETWEEN"
        return Expression(f"{self._name} {mark} {to_string(minimum)} AND {to_string(maximum)}")

    def in_(self, values: list[GeneralValueTypes], not_: bool = False):
        # in 也算是相等比较的一种，所以也给隐私转换并尝试加密了
        values_str = ", ".join([to_string(self._try_encrypt(value)) for value in values])
        mark = "IN"
        if not_:
            mark = "NOT IN"
        return Expression(f"{self._name} {mark} ({values_str})")

    def like(self, regx: str, escape: str = "", not_: bool = False):
        head = "LIKE"
        if not_:
            head = "NOT LIKE"
        body = f"{head} {to_string(regx)}"
        if len(escape) != 0:
            body = f"{body} ESCAPE {to_string(escape)}"
        return Expression(f"{self._name} {body}")

    def is_null(self, not_: bool = False):
        mark = "IS NULL"
        if not_:
            mark = "IS NOT NULL"
        return Expression(f"{self._name} {mark}")

    def glob(self, regx: str):
        return Expression(f"{self._name} GLOB {to_string(regx)}")


class SortOption(Enum):
    NONE = ""
    ASC = "ASC"
    DESC = "DESC"


class NullOption(Enum):
    NONE = ""
    NULLS_FIRST = "NULLS FIRST"
    NULLS_LAST = "NULLS LAST"


def order(
        column: Column | str | int,
        sort_option: SortOption = SortOption.NONE,
        null_option: NullOption = NullOption.NONE
) -> str:
    name = column.name if isinstance(column, Column) else str(column)
    if sort_option != SortOption.NONE:
        name = f"{name} {sort_option.value}"
    if sqlite3.sqlite_version_info >= (3, 30, 0):
        if null_option != NullOption.NONE:
            name = f"{name} {null_option.value}"

    return name
