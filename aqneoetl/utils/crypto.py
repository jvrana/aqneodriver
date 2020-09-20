import ast
import base64
import os
from typing import Any
from typing import Type
from typing import Union

from cryptography.fernet import Fernet
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC


class Cryptography:
    """Methods for encrypting data."""

    UTF8 = "utf-8"
    DEFAULT_ENCODING = UTF8

    @staticmethod
    def generate_key(password: Union[str, bytes], encoding=DEFAULT_ENCODING) -> bytes:
        if isinstance(password, str):
            password = password.encode(encoding)
        salt = os.urandom(16)
        kdf = PBKDF2HMAC(
            algorithm=hashes.SHA256(), length=32, salt=salt, iterations=100000
        )
        key = base64.urlsafe_b64encode(kdf.derive(password))
        return key

    @staticmethod
    def encode(data, encoding: str = DEFAULT_ENCODING) -> bytes:
        return str(data).encode(encoding)

    @staticmethod
    def decode(
        b: bytes,
        encoding: str = DEFAULT_ENCODING,
        as_type: Type = None,
        literal_eval: bool = False,
    ) -> Any:
        decoded = b.decode(encoding)
        if literal_eval:
            decoded = ast.literal_eval(b.decode(encoding))
        if as_type:
            decoded = as_type(decoded)
        return decoded

    @classmethod
    def encrypt(cls, key: bytes, data, encoding: str = DEFAULT_ENCODING) -> bytes:
        f = Fernet(key)
        return f.encrypt(cls.encode(data, encoding=encoding))

    @classmethod
    def decrypt(
        cls, key: bytes, msg: bytes, as_type: Type = None, literal_eval: bool = False
    ) -> Any:
        f = Fernet(key)
        return cls.decode(f.decrypt(msg), as_type=as_type, literal_eval=literal_eval)
