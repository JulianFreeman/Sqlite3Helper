# coding: utf8
from Sqlite3Helper import *
from dataclasses import dataclass


@dataclass
class PersonsCol(Table):
    table: str = "persons"
    person_id = Column("person_id", DataType.INTEGER, primary_key=True)
    name = Column("name", DataType.TEXT, nullable=False)
    age = Column("age", DataType.INTEGER)
    salary = Column("salary", DataType.REAL)
    address = Column("address", DataType.TEXT, has_default=True, default="Earth")
    data = Column("data", DataType.BLOB)


P = PersonsCol()

sqh = Sqlite3Worker("demo.db")
sqh.drop_table(P.table, if_exists=True)
sqh.create_table(P.table, P.all, if_not_exists=True)
sqh.insert_into(P.table, [
    P.name, P.age, P.salary, P.data,
], [
    ["John", 24, 1300.0, "I am genius"],
    ["Karl", 35, 2634.52, "Not so secret"],
    ["Liz", 12, 1000.5, "Hehe"],
])

_, results = sqh.select(P.table, [P.person_id, P.name, P.age, P.data],
                        where=Operand(P.age).greater_than(20))
print(results)

sqh.update(P.table, [
    (P.salary, 1800.32),
    (P.data, BlobType(b"Not hehe")),
], where=Operand(P.name).equal_to("Liz"))

_, results = sqh.select(P.table, [P.name, P.salary, P.data],
                        where=Operand(P.salary).between(1500, 2500))
print(results)
