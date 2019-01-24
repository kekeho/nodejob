import socket


class Runner:
    def __init__(self, address_dict: dict):
        """Initialize method
        Arg:
            address_dict: {'adress': port}
                        like {'127.0.0.1': 8000, '127.0.0.2': 8080}
        """
        self.address_dict = address_dict
        self.clients = [socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                        for x in address_dict]

    def run(self, function, iterator):
        run_list = list(iterator)
        [client.connect(worker)
         for client, worker in zip(self.clients, self.address_dict.items())]
        [client.send(b"START") for client in self.clients]


def f():
    pass


if __name__ == "__main__":
    runner = Runner({'127.0.0.1': 49155})
    runner.run(f, range(10))
