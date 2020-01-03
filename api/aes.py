# Copyright (c) 2017-2020, Carl Cheung
# All rights reserved.

from Crypto.Cipher import AES
from binascii import b2a_hex, a2b_hex


class AESEncryption:
    """
    AES encryption. Encrypt a string-like plaintext into ciphertext
    """

    def __init__(self, key, encrypt_length=16):
        # key length have to be 16
        # if shorter than 16, '0' will be padded at the end until length reach the nearest multiple of 16
        # if longer than 16, the end will be cut into length of 16
        if len(key) < 16:
            remainder = len(key) % 16
            if remainder != 0:
                for i in range(16 - len(key)):
                    key += '0'
        elif len(key) > 16:
            key = key[:16]

        self.key = key.encode()
        self.encrypt_length = encrypt_length
        assert encrypt_length in [16, 24,
                                  32], 'encrypt_length should be 16（AES-128）/24（AES-192）/32（AES-256）bytes length, default 16'

        self.mode = AES.MODE_CBC

    def encrypt(self, binary: bytes):
        cryptor = AES.new(self.key, self.mode, self.key)
        count = len(binary)
        add = self.encrypt_length - (count % self.encrypt_length)
        binary = binary + (b'\0' * add)
        self.ciphertext = cryptor.encrypt(binary)
        # unifying text encoding in 'UTF-8'
        return b2a_hex(self.ciphertext)

    def decrypt(self, binary: bytes):
        cryptor = AES.new(self.key, self.mode, self.key)
        plain_text = cryptor.decrypt(a2b_hex(binary))
        return plain_text.rstrip(b'\0')


if __name__ == '__main__':
    cipher = AESEncryption('123avx', encrypt_length=16)  # init
    e = cipher.encrypt(b'sdjfokdsdas129u`1.\';23131902-')
    d = cipher.decrypt(e)
    print(e, d)
    e = cipher.encrypt('但是我拒绝'.encode())
    d = cipher.decrypt(e)
    print(e, d)
