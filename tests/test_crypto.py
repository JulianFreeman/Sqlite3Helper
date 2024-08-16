# coding: utf8
from Sqlite3Helper import generate_key_and_stuff, Sqlite3Worker

key, _, _ = generate_key_and_stuff()
print(key)

sqh = Sqlite3Worker()
