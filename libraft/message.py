# coding: utf-8

REQUEST_VOTE = 1
REQUEST_VOTE_ACK = 2
APPEND_ENTRIES = 3
APPEND_ENTRIES_ACK = 4


class Message(object):

    def __init__(self, op, data, node_id):

        self.op = op
        self.data = data
        self.node_id = node_id

    @classmethod
    def from_json(cls, py_json):
        """

        :param py_json:
        :return:
        """
        return cls(**py_json)

    def as_json(self):
        """

        :return:
        """
        return {
            "op": self.op,
            "data": self.data,
            "node_id": self.node_id,
        }

    def __getitem__(self, item):
        """

        :param item:
        :return:
        """
        return self.data.get(item, None)


class RequestVoteMessage(Message):
    
    def __init__(self, data, node_id):
        
        super(RequestVoteMessage, self).__init__(REQUEST_VOTE, data, node_id)


class RequestVoteAckMessage(Message):

    def __init__(self, data, node_id):
        
        super(RequestVoteAckMessage, self).__init__(REQUEST_VOTE_ACK, data, node_id)
        

class AppendEntriesMessage(Message):
    
    def __init__(self, data, node_id):
        
        super(AppendEntriesMessage, self).__init__(APPEND_ENTRIES, data, node_id)
        
        
class AppendEntriesAckMessage(Message):
    
    def __init__(self, data, node_id):
        
        super(AppendEntriesAckMessage, self).__init__(APPEND_ENTRIES_ACK, data, node_id)
