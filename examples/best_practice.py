# coding: utf8
from Sqlite3Helper import *
from dataclasses import dataclass, field


@dataclass
class PersonsCol(object):
    person_id = Column("person_id", DataType.INTEGER, primary_key=True)
    name = Column("name", DataType.TEXT, nullable=False)
    age = Column("age", DataType.INTEGER)
    salary = Column("salary", DataType.REAL)
    address = Column("address", DataType.TEXT, has_default=True, default="Earth")
    data = Column("data", DataType.BLOB)
    secure_data = Column("secure_data", DataType.BLOB, secure=True)

    all: list[Column] = field(default_factory=list)

    def __post_init__(self):
        self.all.extend([
            self.person_id, self.name, self.age,
            self.salary, self.address,
            self.data, self.secure_data,
        ])


P = PersonsCol()

key, ti, iv = generate_key_and_stuff()
sqh = Sqlite3Worker("demo.db", key=key, fix_time=ti, fix_iv=iv)
sqh.drop_table("persons", if_exists=True)
sqh.create_table("persons", P.all, if_not_exists=True)
sqh.insert_into("persons", [
    P.name, P.age, P.salary, P.secure_data,
], [
    ["John", 24, 1300.0, "I am genius"],
    ["Karl", 35, 2634.52, "Not so secret"],
    ["Liz", 12, 1000.5, "Hehe"],
])

_, results = sqh.select("persons", [P.person_id, P.name, P.age, P.secure_data],
                        where=Operand(P.age).greater_than(20))
print(results)

sqh.update("persons", [
    (P.salary, 1800.32),
    (P.secure_data, BlobType(b"Not hehe")),
], where=Operand(P.name).equal_to("Liz"))

_, results = sqh.select("persons", [P.name, P.salary, P.secure_data],
                        where=Operand(P.salary).between(1500, 2500))
print(results)
