import base64
import os
from cryptography.fernet import Fernet
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC
from typing import Union, Type
import ast


class Cryptography(object):

    @staticmethod
    def generate_key(password: Union[str, bytes], encoding='utf-8'):
        if isinstance(password, str):
            password = bytes(password, encoding)
        salt = os.urandom(16)
        kdf = PBKDF2HMAC(
            algorithm=hashes.SHA256(),
            length=32,
            salt=salt,
            iterations=100000
        )
        key = base64.urlsafe_b64decode(kdf.derive(password))
        return key

    @staticmethod
    def encode(data, encoding: str = 'utf-8'):
        return str(data).encode(encoding)

    @staticmethod
    def decode(b: bytes, encoding: str = 'utf-8', as_type: Type = None, eval: bool = False):
        decoded = b.decode(encoding)
        if eval:
            decoded = ast.literal_eval(b.decode(encoding))
        if as_type:
            decoded = as_type(decoded)
        return decoded

    @classmethod
    def encrypt(cls, key, data, encoding):
        f = Fernet(key)
        return f.encrypt(cls.encode(data, encoding=encoding))

    @classmethod
    def decrypt(cls, key, msg, as_type: Type = None, eval: bool = False):
        f = Fernet(key)
        return cls.decode(f.decrypt(msg), as_type=as_type, eval=eval)
