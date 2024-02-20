# coding: utf-8


class StableStore(object):

    async def put_int(self, k, v):

        pass

    async def put(self, k, v):
        """put k=v in the store, k and v must be str"""
        pass

    async def get_int(self, k, default=0):

        pass

    async def get(self, k, default=None):
        """get v where key=k, if not exist, return default"""
        pass

    async def delete(self, k):
        """delete k in the store"""
        pass
