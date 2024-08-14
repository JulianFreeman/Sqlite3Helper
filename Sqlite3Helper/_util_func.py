# coding: utf8
from ._types_def import (
    DataType, GeneralValueTypes, SpecialValueTypes,
    NullType, BlobType,
)


def implicitly_convert(data_type: DataType, value: GeneralValueTypes) -> SpecialValueTypes:
    if data_type == DataType.NULL and value is None:
        return NullType()
    if data_type == DataType.REAL and isinstance(value, int):
        return float(value)
    if data_type == DataType.BLOB:
        if isinstance(value, str):
            return BlobType(value.encode("utf-8"))
        if isinstance(value, bytes):
            return BlobType(value)

    return value


def to_string(value: GeneralValueTypes):
    # 有的时候用此函数之前没有隐式转换，
    # 因此还是要判断一下 None
    if value is None:
        value = NullType()
    elif isinstance(value, str):
        # 只要开头或者结尾任意一个字符不是单引号
        if not (value.startswith("'") and value.endswith("'")):
            # 把单引号换为两个单引号转义
            value = value.replace("'", "''")
            value = f"'{value}'"
    elif isinstance(value, bytes):
        value = BlobType(value)

    return str(value)
