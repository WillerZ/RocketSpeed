import socket

class HardcodedEnv:
    def __init__(self, hosts):
        self.hosts_ = hosts

    def __is_me(self, host):
        return socket.getfqdn() == host

    def is_orchestrator(self):
        return self.__is_me(self.hosts_.get('orchestrator', ''))

    def hosts(self):
        return self.hosts_.get('runners', [])

# very simple single box setup that runs clients and servers locally.
# As this is the only runner, it is the leader.
class SingleBoxEnv(HardcodedEnv):
    def __init__(self):
        localhost = socket.getfqdn()
        super().__init__({
            'runners': [localhost, localhost], # at least one server and client
            'orchestrator': localhost
        })
