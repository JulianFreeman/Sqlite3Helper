# coding: utf8
import unittest
from unittest import TestCase
from dataclasses import dataclass
from Sqlite3Helper import Column, Table, DataType


@dataclass
class PersonTable(Table):
    table: str = "person"
    name = Column("name", DataType.TEXT, primary_key=True)
    age = Column("age", DataType.INTEGER)


class TabelTestCase(TestCase):

    def setUp(self):
        self.p = PersonTable()

    def test_main(self):
        print(self.p.all)
        print(self.p.name.table)


if __name__ == '__main__':
    unittest.main()
