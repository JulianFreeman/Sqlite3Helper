# coding: utf8
import os
import time
from cryptography.fernet import Fernet


def generate_key_and_stuff():
    key = Fernet.generate_key()
    fix_time = int(time.time())
    fix_iv = os.urandom(16)
    return key, fix_time, fix_iv


class NotRandomFernet(Fernet):
    """固定下来每次相同的 key 的加密结果相同，方便条件查询"""

    def __init__(self, key: bytes | str, fix_time: int, fix_iv: bytes, backend=None):
        super().__init__(key, backend)
        self._fix_time = fix_time
        self._fix_iv = fix_iv

    def encrypt(self, data: bytes) -> bytes:
        return self._encrypt_from_parts(data, self._fix_time, self._fix_iv)
