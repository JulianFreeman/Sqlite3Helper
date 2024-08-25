# coding: utf8
from __future__ import annotations
from enum import Enum
from ._crypto import NotRandomFernet


class DataType(Enum):
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
        self._data = data

    def __str__(self):
        return f"X'{self._data.hex()}'"

    def encrypt(self, fernet: NotRandomFernet) -> BlobType:
        if fernet is None:
            raise ValueError("Key is not set")
        return BlobType(fernet.encrypt(self._data))


GeneralValueTypes = None | NullType | int | float | str | bytes | BlobType
SpecialValueTypes = NullType | int | float | str | BlobType
