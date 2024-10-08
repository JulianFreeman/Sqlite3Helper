# coding: utf8
from Sqlite3Helper import *
from dataclasses import dataclass


@dataclass
class PersonsCol(Table):
    table: str = "persons"  # 这个地方必须写 table: str 如果只写 table 就不行，但是下面的 Column 不能标注类型

    person_id = Column("person_id", DataType.INTEGER, primary_key=True)
    name = Column("name", DataType.TEXT, nullable=False)
    age = Column("age", DataType.INTEGER)
    salary = Column("salary", DataType.REAL)
    address = Column("address", DataType.TEXT, has_default=True, default="Earth")
    data = Column("data", DataType.BLOB)
    secure_data = Column("secure_data", DataType.BLOB, secure=True)


P = PersonsCol()

key, ti, iv = generate_key_and_stuff()
sqh = Sqlite3Worker("demo.db", key=key, fix_time=ti, fix_iv=iv)
sqh.drop_table(P.table, if_exists=True)
sqh.create_table(P.table, P.all, if_not_exists=True)
sqh.insert_into(P.table, [
    P.name, P.age, P.salary, P.secure_data,
], [
    ["John", 24, 1300.0, "I am genius"],
    ["Karl", 35, 2634.52, "Not so secret"],
    ["Liz", 12, 1000.5, "Hehe"],
])

_, results = sqh.select(P.table, [P.person_id, P.name, P.age, P.secure_data],
                        where=Operand(P.age).greater_than(20))
print(results)

sqh.update(P.table, [
    (P.salary, 1800.32),
    (P.secure_data, BlobType(b"Not hehe")),
], where=Operand(P.name).equal_to("Liz"))

_, results = sqh.select(P.table, [P.name, P.salary, P.secure_data],
                        where=Operand(P.salary).between(1500, 2500))
print(results)
