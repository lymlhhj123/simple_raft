# coding: utf-8

import struct


class Storage(object):

    def __init__(self, encoding="utf-8"):

        self.encoding = encoding

    def put_int8(self, k, v):
        """

        :param k:
        :param v:
        :return:
        """
        assert isinstance(v, int)

        v = struct.pack("!", v)
        self.put(k, v)

    def put_uint8(self, k, v):
        """

        :param k:
        :param v:
        :return:
        """

    def put_int16(self, k, v):
        """

        :param k:
        :param v:
        :return:
        """

    def put_uint16(self, k, v):
        """

        :param k:
        :param v:
        :return:
        """

    def put_int32(self, k, v):
        """

        :param k:
        :param v:
        :return:
        """

    def put_uint32(self, k, v):
        """

        :param k:
        :param v:
        :return:
        """

    def put_int64(self, k, v):
        """

        :param k:
        :param v:
        :return:
        """

    def put_uint64(self, k, v):
        """

        :param k:
        :param v:
        :return:
        """

    def put(self, k, v):
        """

        :param k:
        :param v:
        :return:
        """
        if isinstance(k, str):
            k = k.encode(self.encoding)

        if isinstance(v, str):
            v = v.encode(self.encoding)

        assert isinstance(k, bytes) and isinstance(v, bytes)

    def get_int8(self, k, default=0):
        """

        :param k:
        :param default:
        :return:
        """

    def get_uint8(self, k, default=0):
        """

        :param k:
        :param default:
        :return:
        """

    def get_int16(self, k, default=0):
        """

        :param k:
        :param default:
        :return:
        """

    def get_uint16(self, k, default=0):
        """

        :param k:
        :param default:
        :return:
        """

    def get_int32(self, k, default=0):
        """

        :param k:
        :param default:
        :return:
        """

    def get_uint32(self, k, default=0):
        """

        :param k:
        :param default:
        :return:
        """

    def get_int64(self, k, default=0):
        """

        :param k:
        :param default:
        :return:
        """

    def get_uint64(self, k, default=0):
        """

        :param k:
        :param default:
        :return:
        """

    def get(self, k, default=None):
        """

        :param k:
        :param default:
        :return:
        """

    def delete(self, k):
        """

        :param k:
        :return:
        """