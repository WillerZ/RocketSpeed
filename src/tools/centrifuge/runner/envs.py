import socket

class HardcodedEnv:
    def __init__(self, hosts):
        self.hosts_ = hosts

    def __is_me(self, host):
        return socket.getfqdn() == host

    def is_client(self):
        return any([self.__is_me(x) for x in self.hosts_.get('clients', [])])

    def is_server(self):
        return any([self.__is_me(x) for x in self.hosts_.get('clients', [])])

    def is_orchestrator(self):
        return self.__is_me(self.hosts_.get('orchestrator', ''))

    def hosts(self):
        return self.hosts_

# very simple single box setup that runs clients and servers locally.
# As this is the only runner, it is the leader.
class SingleBoxEnv(HardcodedEnv):
    def __init__(self):
        localhost = socket.getfqdn()
        super().__init__({
            'clients': [localhost],
            'servers': [localhost],
            'orchestrator': localhost
        })
