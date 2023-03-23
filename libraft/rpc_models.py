# coding: utf-8


class Rpc(object):

    REQUEST_VOTE = 1
    REQUEST_VOTE_ACK = 2
    APPEND_ENTRIES = 3
    APPEND_ENTRIES_ACK = 4

    def __init__(self, op, data, mem_id):

        self.op = op
        self.data = data
        # 节点的id，表示rpc消息的来源
        self.mem_id = mem_id

    def as_json(self):
        """

        :return:
        """
        return {
            "op": self.op,
            "data": self.data,
            "mem_id": self.mem_id
        }

    @classmethod
    def from_json(cls, py_json):
        """

        :param py_json:
        :return:
        """
        return cls(**py_json)


class VoteRpc(Rpc):
    
    def __init__(self, data, mem_id):
        
        super(VoteRpc, self).__init__(self.REQUEST_VOTE, data, mem_id)


class VoteAckRpc(Rpc):

    def __init__(self, data, mem_id):
        
        super(VoteAckRpc, self).__init__(self.REQUEST_VOTE_ACK, data, mem_id)
        

class AppendEntriesRpc(Rpc):
    
    def __init__(self, data, mem_id):
        
        super(AppendEntriesRpc, self).__init__(self.APPEND_ENTRIES, data, mem_id)
        
        
class AppendEntriesAckRpc(Rpc):
    
    def __init__(self, data, mem_id):
        
        super(AppendEntriesAckRpc, self).__init__(self.APPEND_ENTRIES_ACK, data, mem_id)
