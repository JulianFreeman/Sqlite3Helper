# coding: utf8
from __future__ import annotations

import os
import sqlite3
import time
from dataclasses import dataclass
from enum import StrEnum
from os import PathLike

from cryptography.fernet import Fernet, InvalidToken


__version__ = "2.2.0"
__version_info__ = tuple(map(int, __version__.split(".")))


class DataType(StrEnum):
    NULL = "NULL"
    INTEGER = "INTEGER"
    REAL = "REAL"
    TEXT = "TEXT"
    BLOB = "BLOB"


class NullType(object):

    def __str__(self):
        return "NULL"


class BlobType(object):

    def __init__(self, data: bytes = b""):
        self.data = data

    def __str__(self):
        return f"X'{self.data.hex()}'"


class NotRandomFernet(Fernet):
    """固定下来每次相同的 key 的加密结果相同，方便条件查询"""

    def __init__(self, key: bytes | str, fix_time: int, fix_iv: bytes, backend=None):
        super().__init__(key, backend)
        self._fix_time = fix_time
        self._fix_iv = fix_iv

    def encrypt(self, data: bytes) -> bytes:
        return self._encrypt_from_parts(data, self._fix_time, self._fix_iv)


def _encrypt_blob(blob: BlobType, fernet: NotRandomFernet) -> BlobType:
    if fernet is None:
        raise ValueError("Key is not set")
    return BlobType(fernet.encrypt(blob.data))


def _get_type(data_type: DataType) -> type:
    if data_type == DataType.NULL:
        return NullType
    if data_type == DataType.INTEGER:
        return int
    if data_type == DataType.REAL:
        return float
    if data_type == DataType.TEXT:
        return str
    if data_type == DataType.BLOB:
        return BlobType
    raise TypeError(f"Data type {data_type} is not supported")


def _get_data_type(type_: type) -> DataType:
    if type_ is NullType:
        return DataType.NULL
    if type_ is int:
        return DataType.INTEGER
    if type_ is float:
        return DataType.REAL
    if type_ is str:
        return DataType.TEXT
    if type_ is BlobType:
        return DataType.BLOB
    raise TypeError(f"Data type {type_} is not supported")


def _to_string(value):
    # 如果传入的类型不是 text 会直接返回原值
    if type(value) is str:
        if not (value.startswith("'") or value.endswith("'")):
            value = f"'{value}'"
    return str(value)


@dataclass
class Column(object):
    name: str
    data_type: DataType
    primary_key: bool = False
    nullable: bool = True
    unique: bool = False
    has_default: bool = False
    default: NullType | int | float | str | BlobType = 0

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
            head = f"{head} DEFAULT {_to_string(self.default)}"
        return head

    __repr__ = __str__


class Expression(object):

    def __init__(self, expr: str):
        self._expr = expr

    def __str__(self):
        return self._expr

    def and_(self, expression: Expression):
        return Expression(f"{self._expr} AND {expression}")

    def or_(self, expression: Expression):
        return Expression(f"{self._expr} OR {expression}")

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

    def equal_to(self, value, not_: bool = False):
        if isinstance(self._column, Column):
            if self._column.data_type == DataType.BLOB and type(value) is str:
                value = BlobType(value.encode("utf-8"))
            # 这里不能换成 elif
            if self._key is not None and self._column.secure and isinstance(value, BlobType):
                fix_time = self._fix_time if self._fix_time is not None else int(time.time())
                fix_iv = self._fix_iv if self._fix_iv is not None else os.urandom(16)
                try:
                    value = _encrypt_blob(value, NotRandomFernet(self._key, fix_time, fix_iv))
                except ValueError:
                    pass
        op = "!=" if not_ else "="
        return Expression(f"{self._name} {op} {_to_string(value)}")

    def less_than(self, value):
        return Expression(f"{self._name} < {_to_string(value)}")

    def greater_than(self, value):
        return Expression(f"{self._name} > {_to_string(value)}")

    def less_equal(self, value):
        return Expression(f"{self._name} <= {_to_string(value)}")

    def greater_equal(self, value):
        return Expression(f"{self._name} >= {_to_string(value)}")

    def between(self, minimum, maximum, not_: bool = False):
        mark = "BETWEEN"
        if not_:
            mark = "NOT BETWEEN"
        return Expression(f"{self._name} {mark} {_to_string(minimum)} AND {_to_string(maximum)}")

    def in_(self, values: list | str, not_: bool = False):
        if isinstance(values, list):
            values = ", ".join([_to_string(value) for value in values])
        mark = "IN"
        if not_:
            mark = "NOT IN"
        return Expression(f"{self._name} {mark} ({values})")

    def like(self, regx: str, escape: str = "", not_: bool = False):
        head = "LIKE"
        if not_:
            head = "NOT LIKE"
        body = f"{head} {_to_string(regx)}"
        if len(escape) != 0:
            body = f"{body} ESCAPE {_to_string(escape)}"
        return Expression(f"{self._name} {body}")

    def is_null(self, not_: bool = False):
        mark = "IS NULL"
        if not_:
            mark = "IS NOT NULL"
        return Expression(f"{self._name} {mark}")

    def glob(self, regx: str):
        return Expression(f"{self._name} GLOB {_to_string(regx)}")


class SortOption(StrEnum):
    NONE = ""
    ASC = "ASC"
    DESC = "DESC"


class NullOption(StrEnum):
    NONE = ""
    NULLS_FIRST = "NULLS FIRST"
    NULLS_LAST = "NULLS LAST"


def order(column: Column | str | int,
          sort_option: SortOption = SortOption.NONE,
          null_option: NullOption = NullOption.NONE) -> str:
    name = column.name if isinstance(column, Column) else str(column)
    if sort_option != SortOption.NONE:
        name = f"{name} {sort_option.value}"
    if sqlite3.sqlite_version_info >= (3, 30, 0):
        if null_option != NullOption.NONE:
            name = f"{name} {null_option.value}"
    return name


class Sqlite3Worker(object):

    def __init__(
            self,
            db_name: str | PathLike[str] = ":memory:",
            key: bytes = None,
            fix_time: int = None,
            fix_iv: bytes = None,
    ):
        self._db_name = db_name
        self._conn = sqlite3.connect(db_name)
        self._cursor = self._conn.cursor()
        self._is_closed = False
        self._fernet = None
        if key is not None:
            fix_time = fix_time if fix_time is not None else int(time.time())
            fix_iv = fix_iv if fix_iv is not None else os.urandom(16)
            try:
                self._fernet = NotRandomFernet(key, fix_time, fix_iv)
            except ValueError:
                pass

    def __del__(self):
        self.close()

    @property
    def db_name(self) -> str:
        return self._db_name

    def close(self):
        if self._is_closed is False:
            self._cursor.close()
            self._conn.close()
            self._is_closed = True

    def commit(self):
        self._conn.commit()

    def create_table(self, table_name: str, columns: list[Column],
                     if_not_exists: bool = False, schema_name: str = "",
                     *, execute: bool = True) -> str:
        if table_name.startswith("sqlite_"):
            raise ValueError("Table name must not start with 'sqlite_')")

        columns_str = ", ".join([str(col) for col in columns])
        head = "CREATE TABLE"
        if if_not_exists:
            head = f"{head} IF NOT EXISTS"
        name = table_name
        if len(schema_name) != 0:
            name = f"{schema_name}.{name}"

        statement = f"{head} {name} ({columns_str});"

        if execute:
            self._cursor.execute(statement)
        return statement

    def drop_table(self, table_name: str, if_exists: bool = False,
                   schema_name: str = "", *, execute: bool = True) -> str:
        head = "DROP TABLE"
        if if_exists:
            head = f"{head} IF EXISTS"
        name = table_name
        if len(schema_name) != 0:
            name = f"{schema_name}.{name}"

        statement = f"{head} {name};"

        if execute:
            self._cursor.execute(statement)
        return statement

    def rename_table(self, table_name: str, new_name: str, *, execute: bool = True) -> str:
        head = "ALTER TABLE"
        statement = f"{head} {table_name} RENAME TO {new_name};"
        if execute:
            self._cursor.execute(statement)
        return statement

    def add_column(self, table_name: str, column: Column, *, execute: bool = True) -> str:
        if column.primary_key or column.unique:
            raise ValueError("The new column cannot have primary key or unique")
        if not column.nullable:
            if not column.has_default:
                raise ValueError("If the new column is not null, it must have default value")
            if column.default is NullType:
                raise ValueError("If the new column is not null, its default value must not be NULL")

        head = "ALTER TABLE"
        statement = f"{head} {table_name} ADD COLUMN {str(column)};"
        if execute:
            self._cursor.execute(statement)
        return statement

    def rename_column(self, table_name: str, column_name: str,
                      new_name: str, *, execute: bool = True) -> str:
        if sqlite3.sqlite_version_info < (3, 25, 0):
            raise ValueError("SQLite under 3.25.0 does not support rename column")

        head = "ALTER TABLE"
        statement = f"{head} {table_name} RENAME COLUMN {column_name} TO {new_name};"
        if execute:
            self._cursor.execute(statement)
        return statement

    def show_tables(self) -> list[str]:
        cond = Operand("type").equal_to("table").and_(Operand("name").like("sqlite_%", not_=True))
        _, tables = self.select("sqlite_schema", ["name"], where=cond)
        return [table[0] for table in tables]

    @staticmethod
    def _columns_to_string(columns: list[Column | str]) -> str:
        columns_str_ls = []
        for column in columns:
            if isinstance(column, Column):
                columns_str_ls.append(column.name)
            elif isinstance(column, str):
                columns_str_ls.append(column)
            else:
                raise ValueError(f"Column must be str or Column object, found {type(column)}")
        return ", ".join(columns_str_ls)

    def insert_into(self, table_name: str, columns: list[Column | str],
                    values: list[list[NullType | str | int | float | BlobType]],
                    *, execute: bool = True, commit: bool = True) -> str:
        col_count = len(columns)
        columns_str = self._columns_to_string(columns)

        values_str_ls = []
        for value_row in values:
            if len(value_row) != col_count:
                raise ValueError(f"Length of values must be {col_count}")

            value_row_str_ls = []
            for i in range(col_count):
                column = columns[i]
                value = value_row[i]
                if isinstance(column, Column):
                    col_type = _get_type(column.data_type)
                    val_type = type(value)
                    # 支持将 int 隐式转为 float
                    if val_type is int and col_type is float:
                        pass
                    # 支持将 NULL 值插入任意类型的列，除了 NOT NULL 限制的
                    elif val_type is NullType and column.nullable is True:
                        pass
                    # 其他类型不匹配
                    elif val_type is not col_type:
                        raise ValueError(f"The {i + 1}(th) type of value must be {col_type},"
                                         f" because the column type is {column.data_type},"
                                         f" found {val_type}")
                    # 如果加密
                    if column.secure and val_type is not NullType:
                        value = _to_string(_encrypt_blob(value, self._fernet))

                value_row_str_ls.append(_to_string(value))

            values_str_ls.append(f"({', '.join(value_row_str_ls)})")

        values_str = ", ".join(values_str_ls)

        head = "INSERT INTO"
        statement = f"{head} {table_name} ({columns_str}) VALUES {values_str};"
        if execute:
            self._cursor.execute(statement)
            if commit:
                self._conn.commit()
        return statement

    @staticmethod
    def _join_where_order_limit(body: str,
                                where: Expression, order_by: list[str] | str,
                                limit: int, offset: int) -> str:
        if where is not None:
            body = f"{body} WHERE {where}"
        if order_by is not None:
            if not isinstance(order_by, list):
                order_by = [order_by]
            body = f"{body} ORDER BY {', '.join(order_by)}"
        if limit is not None:
            body = f"{body} LIMIT {limit}"
            if offset is not None:
                body = f"{body} OFFSET {offset}"
        return body

    def select(self, table_name: str, columns: list[Column | str], distinct: bool = False,
               where: Expression = None,
               order_by: list[str] | str = None,
               limit: int = None, offset: int = None,
               *, execute: bool = True) -> tuple[str, list[list]]:
        if len(columns) == 0:
            columns_str = "*"
        else:
            columns_str = self._columns_to_string(columns)

        head = "SELECT"
        if distinct:
            head = f"{head} DISTINCT"
        body = f"{head} {columns_str} FROM {table_name}"
        body = self._join_where_order_limit(body, where, order_by, limit, offset)

        statement = f"{body};"
        if execute:
            self._cursor.execute(statement)
            rows = self._cursor.fetchall()
            rows = [list(row) for row in rows]  # 将每行转成列表，方便替换解密数据
            # 下面的整个循环都是为了找到需要解密的数据尝试解密
            for i in range(len(columns)):
                column = columns[i]
                if isinstance(column, Column) and column.secure:
                    for row in rows:
                        # 如果是加密的 BLOB 但是值不为 NULL 才解密
                        if row[i] is not None and self._fernet is not None:
                            # 不管是key错误还是密文错误，都是 InvalidToken，貌似没法区分
                            # 因此如果有的数据不是加密过的，应该跳过，不应该影响之后的密文解密，
                            # 因此这里还是得继续循环下去
                            try:
                                row[i] = self._fernet.decrypt(row[i])
                            except InvalidToken:
                                pass

            return statement, rows
        else:
            return statement, []

    def delete_from(self, table_name: str, where: Expression = None,
                    *, execute: bool = True, commit: bool = True) -> str:
        head = "DELETE FROM"
        body = f"{head} {table_name}"
        if where is not None:
            body = f"{body} WHERE {where}"

        statement = f"{body};"
        if execute:
            self._cursor.execute(statement)
            if commit:
                self._conn.commit()
        return statement

    def update(self, table_name: str, new_values: list[tuple[Column | str, NullType | int | float | str | BlobType]],
               where: Expression = None,
               *, execute: bool = True, commit: bool = True) -> str:
        new_values_str_ls = []
        for column, value in new_values:
            if isinstance(column, Column):
                # 支持将 NULL 值填入任意类型的列，除了 NOT NULL 限制的
                if type(value) is NullType and column.nullable is True:
                    pass
                elif _get_data_type(type(value)) != column.data_type:
                    raise ValueError(f"Type of {column.name} must be {column.data_type}, found {type(value)}")

                if column.secure and type(value) is not NullType:
                    value = _encrypt_blob(value, self._fernet)

                name = column.name
            else:
                name = column

            new_values_str_ls.append(f"{name} = {_to_string(value)}")

        head = f"UPDATE {table_name}"
        body = f"{head} SET {', '.join(new_values_str_ls)}"
        if where is not None:
            body = f"{body} WHERE {where}"

        statement = f"{body};"
        if execute:
            self._cursor.execute(statement)
            if commit:
                self._conn.commit()
        return statement
