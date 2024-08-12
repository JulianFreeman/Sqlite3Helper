# coding: utf8
import unittest
from unittest import TestCase
from Sqlite3Helper import (
    _to_string, Column, DataType,
    NullType, BlobType, _NotRandomFernet,
    Sqlite3Worker, Operand, Expression,

)


class BlobTypeTestCase(TestCase):

    def setUp(self):
        self.key = b'a5ohpollt_86HP8zgL3v4ad7pBFvDEW7gWWJqWIBkX8='
        self.time = 1723392234
        self.iv = b'\x1a\xf86\xf0\xfb"\xf2\xab\x83\xccW\xd8=zqY'
        self.b = BlobType(b"hello")

        self.sqh = Sqlite3Worker(key=self.key, fix_time=self.time, fix_iv=self.iv)
        self.name = Column("name", DataType.TEXT, nullable=False, has_default=True, default="John")
        self.secure_data = Column("secure_data", DataType.BLOB, secure=True)

    def test_encrypt(self):
        _fernet = _NotRandomFernet(self.key, self.time, self.iv)
        _expected = ("X'674141414141426d754f44714776673238507369387175447a46"
                     "665950587078575a2d75466d5139736f42485f5f516d3562306354"
                     "494f3144446d723648526c656a7335416c6957334a324b4f647230"
                     "7030502d72727630397932436d67786c65673d3d'")
        self.assertEqual(str(self.b.encrypt(_fernet)), _expected)
        self.assertRaises(ValueError, self.b.encrypt, None)

    def test_blobtype(self):
        self.assertEqual(str(self.b), "X'68656c6c6f'")

    def test_insert_and_update(self):
        i1 = self.sqh.insert_into("demo", [self.secure_data], [["Hello"]], execute=False)
        self.assertEqual(i1, "INSERT INTO demo (secure_data) VALUES (X'674141414141426d754f447147766732385073693"
                             "87175447a4666595058707857654749353766634a543354734b506f506d722d546d4a74545f3170645"
                             "36134486b516d374b4b2d70754a325556654c39767a6e306c64536235616f6e57524743673d3d');")
        i2 = self.sqh.insert_into("demo", [self.secure_data], [[None]], execute=False)
        self.assertEqual(i2, "INSERT INTO demo (secure_data) VALUES (NULL);")

        cond = Operand(self.name).equal_to("John")
        u1 = self.sqh.update("demo", [(self.secure_data, b"world")], where=cond, execute=False)
        self.assertEqual(u1, "UPDATE demo SET secure_data = X'674141414141426d754f44714776673238507369387175447a4"
                             "6665950587078575a4935507a5737366b50347948584335346579723853396c794f36724b7a6c757050"
                             "6c743455386935396258414e6b624f5858722d3051705f723645444c6253413d3d' WHERE name = 'John';")
        u2 = self.sqh.update("demo", [(self.secure_data, NullType())], where=cond, execute=False)
        self.assertEqual(u2, "UPDATE demo SET secure_data = NULL WHERE name = 'John';")


class ColumnTestCase(TestCase):

    def test_output(self):
        self.assertEqual(str(Column("name", DataType.TEXT)), "name TEXT")
        self.assertEqual(str(Column("name", DataType.TEXT, primary_key=True)),
                         "name TEXT PRIMARY KEY")
        self.assertEqual(str(Column("name", DataType.TEXT, nullable=False)),
                         "name TEXT NOT NULL")
        self.assertEqual(str(Column("name", DataType.TEXT, unique=True)),
                         "name TEXT UNIQUE")
        self.assertEqual(str(Column("name", DataType.TEXT, has_default=True, default="John")),
                         "name TEXT DEFAULT 'John'")
        self.assertRaises(ValueError, Column, "name", DataType.TEXT, secure=True)


class CRUDTestCase(TestCase):

    def setUp(self):
        self.sqh = Sqlite3Worker()
        self.name = Column("name", DataType.TEXT, nullable=False, has_default=True, default="John")
        self.age = Column("age", DataType.INTEGER)
        self.salary = Column("salary", DataType.REAL)
        self.data = Column("data", DataType.BLOB)
        self.secure_data = Column("secure_data", DataType.BLOB, secure=True)
        self.null_value = Column("null_value", DataType.NULL)

    def test_create_table(self):
        c1 = self.sqh.create_table("demo", [
            self.name, self.age, self.salary, self.data, self.secure_data, self.null_value
        ], if_not_exists=True, execute=False)
        self.assertEqual(c1, "CREATE TABLE IF NOT EXISTS demo (name TEXT NOT NULL DEFAULT 'John', age INTEGER, "
                             "salary REAL, data BLOB, secure_data BLOB, null_value NULL);")

    def test_insert(self):
        i1 = self.sqh.insert_into("demo", [self.null_value], [[None]], execute=False)
        i2 = self.sqh.insert_into("demo", [self.null_value], [[NullType()]], execute=False)
        self.assertEqual(i1, "INSERT INTO demo (null_value) VALUES (NULL);")
        self.assertEqual(i2, "INSERT INTO demo (null_value) VALUES (NULL);")

        i3 = self.sqh.insert_into("demo", [self.age], [[23]], execute=False)
        self.assertEqual(i3, "INSERT INTO demo (age) VALUES (23);")
        self.assertRaises(ValueError, self.sqh.insert_into, "demo", [self.age], [[25.0]], execute=False)

        i4 = self.sqh.insert_into("demo", [self.salary], [[1000]], execute=False)
        i5 = self.sqh.insert_into("demo", [self.salary], [[1596.5]], execute=False)
        self.assertEqual(i4, "INSERT INTO demo (salary) VALUES (1000.0);")
        self.assertEqual(i5, "INSERT INTO demo (salary) VALUES (1596.5);")

        i6 = self.sqh.insert_into("demo", [self.name], [["John"]], execute=False)
        i7 = self.sqh.insert_into("demo", [self.name], [["O'liver"]], execute=False)
        i8 = self.sqh.insert_into("demo", [self.name], [["'Karl'"]], execute=False)
        i9 = self.sqh.insert_into("demo", [self.name], [["'Liz"]], execute=False)
        self.assertEqual(i6, "INSERT INTO demo (name) VALUES ('John');")
        self.assertEqual(i7, "INSERT INTO demo (name) VALUES ('O''liver');")
        self.assertEqual(i8, "INSERT INTO demo (name) VALUES ('Karl');")
        self.assertEqual(i9, "INSERT INTO demo (name) VALUES ('''Liz');")

        i10 = self.sqh.insert_into("demo", [self.data], [["hello"]], execute=False)
        i11 = self.sqh.insert_into("demo", [self.data], [[b"hello"]], execute=False)
        i12 = self.sqh.insert_into("demo", [self.data], [[BlobType(b"hello")]], execute=False)
        self.assertEqual(i10, "INSERT INTO demo (data) VALUES (X'68656c6c6f');")
        self.assertEqual(i11, "INSERT INTO demo (data) VALUES (X'68656c6c6f');")
        self.assertEqual(i12, "INSERT INTO demo (data) VALUES (X'68656c6c6f');")

        i13 = self.sqh.insert_into("demo", [
            self.name, self.age, self.salary, self.data
        ], [
            ["Kath", 34, 4352.21, b"world"]
        ], execute=False)
        self.assertEqual(i13, "INSERT INTO demo (name, age, salary, data) VALUES ('Kath', 34, 4352.21, X'776f726c64');")

        # 可以是 NULL 的字段可以插入 None
        i14 = self.sqh.insert_into("demo", [self.age], [[None]], execute=False)
        self.assertEqual(i14, "INSERT INTO demo (age) VALUES (NULL);")
        # 不可以是 NULL 的字段插入 None 会报错
        self.assertRaises(ValueError, self.sqh.insert_into, "demo", [self.name], [[NullType()]], execute=False)

    def test_update(self):
        cond = Operand(self.name).equal_to("John")
        u1 = self.sqh.update("demo", [(self.age, 24)], where=cond, execute=False)
        self.assertEqual(u1, "UPDATE demo SET age = 24 WHERE name = 'John';")
        u2 = self.sqh.update("demo", [(self.data, "world")], where=cond, execute=False)
        self.assertEqual(u2, "UPDATE demo SET data = X'776f726c64' WHERE name = 'John';")

        # 可以是 NULL 的字段可以更新为 None
        u3 = self.sqh.update("demo", [(self.age, NullType())], where=cond, execute=False)
        self.assertEqual(u3, "UPDATE demo SET age = NULL WHERE name = 'John';")
        # 不可以是 NULL 的字段更新为 None 会报错
        self.assertRaises(ValueError, self.sqh.update, "demo", [(self.name, None)], where=cond, execute=False)


class OperandTestCase(TestCase):

    def setUp(self):
        self.key = b'a5ohpollt_86HP8zgL3v4ad7pBFvDEW7gWWJqWIBkX8='
        self.time = 1723392234
        self.iv = b'\x1a\xf86\xf0\xfb"\xf2\xab\x83\xccW\xd8=zqY'
        self.b = BlobType(b"hello")

    def test_simple(self):
        col_name = Column("name", DataType.TEXT)
        o1 = Operand(col_name).less_than(10)
        self.assertEqual(str(o1), "name < 10")
        o2 = Operand(col_name).equal_to("John")
        self.assertEqual(str(o2), "name = 'John'")

        col_data = Column("data", DataType.BLOB, secure=True)
        o3 = Operand(col_data, self.key, self.time, self.iv).equal_to("John")
        self.assertEqual(str(o3), "data = X'674141414141426d754f44714776673238507369387175447a"
                                  "46665950587078575a32565554713964714837763350576862577667395369796d3"
                                  "85a3869766455636f456473357a56435855566c724a776e66415842696564374443"
                                  "7746594832673d3d'")
        o4 = Operand(col_data).equal_to("John")
        self.assertEqual(str(o4), "data = X'4a6f686e'")

        o5 = Operand(col_data).in_(["John", 10, self.b])
        self.assertEqual(str(o5), "data IN (X'4a6f686e', 10, X'68656c6c6f')")


class TestMain(TestCase):

    def test_datatype_output(self):
        self.assertEqual(DataType.NULL.value, "NULL")
        self.assertEqual(DataType.INTEGER.value, "INTEGER")
        self.assertEqual(DataType.REAL.value, "REAL")
        self.assertEqual(DataType.TEXT.value, "TEXT")
        self.assertEqual(DataType.BLOB.value, "BLOB")

    def test_to_string(self):
        self.assertEqual(_to_string("Hello"), "'Hello'")
        self.assertEqual(_to_string("'Hello'"), "'Hello'")
        self.assertEqual(_to_string("'Hello"), "'''Hello'")
        self.assertEqual(_to_string("Hello'"), "'Hello'''")
        self.assertEqual(_to_string("He'llo"), "'He''llo'")
        self.assertEqual(_to_string("He'll'o"), "'He''ll''o'")

        self.assertEqual(_to_string(None), "NULL")
        self.assertEqual(_to_string(NullType()), "NULL")
        self.assertEqual(_to_string(BlobType(b"hello")), "X'68656c6c6f'")
        self.assertEqual(_to_string(1), "1")
        self.assertEqual(_to_string(1.0), "1.0")

    def test_expression(self):
        e1 = Expression("A")
        e2 = Expression("B")
        p1 = e1.and_(e2)
        self.assertEqual(str(p1), "A AND B")
        p2 = e1.or_(e2)
        self.assertEqual(str(p2), "A OR B")
        p3 = e1.or_(e2, high_priority=True)
        self.assertEqual(str(p3), "(A OR B)")
        p4 = e1.exists()
        self.assertEqual(str(p4), "EXISTS (A)")
        p5 = e1.exists(not_=True)
        self.assertEqual(str(p5), "NOT EXISTS (A)")


if __name__ == '__main__':
    unittest.main()
