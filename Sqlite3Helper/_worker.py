# coding: utf8
from __future__ import annotations

import os
import sqlite3
import time
from os import PathLike
from types import NoneType
from cryptography.fernet import InvalidToken

from ._crypto import NotRandomFernet
from ._types_def import (
    DataType, GeneralValueTypes,
    NullType, BlobType,
)
from ._util_func import to_string, implicitly_convert
from ._column import Column
from ._where import Operand, Expression


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

    def _execute(self, statement: str):
        try:
            self._cursor.execute(statement)
        except sqlite3.Error as e:
            raise sqlite3.Error(f"Error name: {e.sqlite_errorname};\nError statement: {statement}")

    @staticmethod
    def _check_data_type(data_type: DataType, allow_null: bool, value: GeneralValueTypes) -> bool:
        allow_types = []
        if data_type == DataType.NULL:
            pass
        elif data_type == DataType.INTEGER:
            allow_types.extend([int, ])
        elif data_type == DataType.REAL:
            allow_types.extend([int, float])
        elif data_type == DataType.TEXT:
            allow_types.extend([str, ])
        elif data_type == DataType.BLOB:
            allow_types.extend([str, bytes, BlobType])

        if allow_null:
            allow_types.extend([NoneType, NullType])

        return isinstance(value, tuple(allow_types))

    @staticmethod
    def _is_null(value: GeneralValueTypes) -> bool:
        return isinstance(value, (NoneType, NullType))

    def _try_encrypt(self, column: Column, value: GeneralValueTypes) -> GeneralValueTypes:
        # 如果有 secure，则这里的类型要么是 BlobType，要么是 NULL
        # 尽管判断不是 NULL 也可以，但是为了更严谨些，还是判断 BlobType 吧
        if column.secure and isinstance(value, BlobType):
            value = value.encrypt(self._fernet)

        return value

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
            self._execute(statement)
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
            self._execute(statement)
        return statement

    def rename_table(self, table_name: str, new_name: str, *, execute: bool = True) -> str:
        head = "ALTER TABLE"
        statement = f"{head} {table_name} RENAME TO {new_name};"
        if execute:
            self._execute(statement)
        return statement

    def add_column(self, table_name: str, column: Column, *, execute: bool = True) -> str:
        if column.primary_key or column.unique:
            raise ValueError("The new column cannot have primary key or unique")
        if not column.nullable:
            if not column.has_default:
                raise ValueError("If the new column is not null, it must have default value")
            if self._is_null(column.default):
                raise ValueError("If the new column is not null, its default value must not be NULL")

        head = "ALTER TABLE"
        statement = f"{head} {table_name} ADD COLUMN {str(column)};"
        if execute:
            self._execute(statement)
        return statement

    def rename_column(self, table_name: str, column_name: str,
                      new_name: str, *, execute: bool = True) -> str:
        if sqlite3.sqlite_version_info < (3, 25, 0):
            raise ValueError("SQLite under 3.25.0 does not support rename column")

        head = "ALTER TABLE"
        statement = f"{head} {table_name} RENAME COLUMN {column_name} TO {new_name};"
        if execute:
            self._execute(statement)
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
                    values: list[list[GeneralValueTypes]],
                    *, execute: bool = True, commit: bool = True) -> str:
        col_count = len(columns)
        columns_str = self._columns_to_string(columns)

        values_str_ls = []
        for value_row in values:
            if len(value_row) != col_count:
                raise ValueError(f"Length of values must be {col_count}")

            value_row_str_ls = []
            for column, value in zip(columns, value_row):
                if isinstance(column, Column):
                    if not self._check_data_type(column.data_type, column.nullable, value):
                        raise ValueError(f"Type of {column.name} must be {column.data_type}, found {type(value)}")
                    value = self._try_encrypt(column, implicitly_convert(column.data_type, value))

                value_row_str_ls.append(to_string(value))

            values_str_ls.append(f"({', '.join(value_row_str_ls)})")

        values_str = ", ".join(values_str_ls)

        head = "INSERT INTO"
        statement = f"{head} {table_name} ({columns_str}) VALUES {values_str};"
        if execute:
            self._execute(statement)
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
            self._execute(statement)
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
            self._execute(statement)
            if commit:
                self._conn.commit()
        return statement

    def update(self, table_name: str, new_values: list[tuple[Column | str, GeneralValueTypes]],
               where: Expression = None,
               *, execute: bool = True, commit: bool = True) -> str:
        new_values_str_ls = []
        for column, value in new_values:
            if isinstance(column, Column):
                if not self._check_data_type(column.data_type, column.nullable, value):
                    raise ValueError(f"Type of {column.name} must be {column.data_type}, found {type(value)}")
                value = self._try_encrypt(column, implicitly_convert(column.data_type, value))

                name = column.name
            else:
                name = column

            new_values_str_ls.append(f"{name} = {to_string(value)}")

        head = f"UPDATE {table_name}"
        body = f"{head} SET {', '.join(new_values_str_ls)}"
        if where is not None:
            body = f"{body} WHERE {where}"

        statement = f"{body};"
        if execute:
            self._execute(statement)
            if commit:
                self._conn.commit()
        return statement
