import pytest
from cryptography.fernet import InvalidToken

from aqneoetl.crypto import Cryptography


def test_key_gen():
    key = Cryptography.generate_key("password")
    print(key)
    assert key
    key2 = Cryptography.generate_key("password")
    assert key != key2


def test_encrypt():
    key = Cryptography.generate_key("mypass")
    key2 = Cryptography.generate_key("mypass")
    txt = "this is my text"

    encrypted = Cryptography.encrypt(key, txt)
    assert encrypted != txt

    decrypted = Cryptography.decrypt(key, encrypted)
    assert decrypted == txt

    with pytest.raises(InvalidToken):
        decrypted2 = Cryptography.decrypt(key2, encrypted)


def test_encrypt_dict_decrypt_as_str():
    key = Cryptography.generate_key("mypass")
    data = {1: 2}

    encrypted = Cryptography.encrypt(key, data)
    decrypted = Cryptography.decrypt(key, encrypted)
    assert isinstance(decrypted, str)
    assert decrypted == str(data)


def test_encrypt_dict_decrypt_as_data():
    key = Cryptography.generate_key("mypass")
    data = {1: 2}

    encrypted = Cryptography.encrypt(key, data)
    decrypted_data = Cryptography.decrypt(key, encrypted, literal_eval=True)
    assert isinstance(decrypted_data, dict)
    assert decrypted_data == data
