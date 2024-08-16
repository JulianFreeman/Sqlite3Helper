# coding: utf8
import os
import time

try:
    from cryptography.fernet import Fernet
except ImportError:
    class Fernet(object):
        def __init__(self, key, backend):
            pass


def generate_key_and_stuff():
    try:
        key = Fernet.generate_key()
    except AttributeError:
        raise ModuleNotFoundError("cryptography is not installed, see readme.")

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
        try:
            return self._encrypt_from_parts(data, self._fix_time, self._fix_iv)
        except AttributeError:
            return data
