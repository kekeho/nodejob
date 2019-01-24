import socket
import socketserver
import pickle
import types
import time


class NodeJobResult:
    """Result iterator class"""
    def __init__(self, non_ordered_list: list):
        self._list = non_ordered_list
        self._x = 0
        self._y = 0
        self._x_max = len(non_ordered_list)

    def __iter__(self):
        return self

    def __next__(self):
        if self._x == self._x_max:
            self._x = 0
            self._y += 1
        try:
            return_val = self._list[self._x][self._y]
        except IndexError:
            raise StopIteration()

        self._x += 1
        return return_val


class Master:
    def __init__(self, address_list: list):
        """Initialize method
        Arg:
            address_dict: {'adress': port}
                        like {'127.0.0.1': 8000, '127.0.0.2': 8080}
        """
        self.address_list = address_list
        self.clients = [socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                        for x in address_list]

    def run(self, function: types.FunctionType, iterator):
        clients_count = len(self.clients)

        # dividing　list(iterator) equally
        data_blocks = [list(iterator)[i::clients_count]
                       for i in range(clients_count)]

        pickled_function = pickle.dumps(function)

        # Connect to worker
        [client.connect(worker)
         for client, worker in zip(self.clients, self.address_list)]

        # Send function-object to worker
        # map(lambda x: x.sendall(pickled_function), self.clients)
        [client.send(pickled_function) for client in self.clients]
        time.sleep(0.001)
        # Send data blocks to worker
        [client.send(pickle.dumps(data_block))
         for client, data_block in zip(self.clients, data_blocks)]

        returns = [pickle.loads(client.recv(2**30)) for client in self.clients]
        return NodeJobResult(returns)


class Worker():
    """nodejob worker object"""
    class Handler(socketserver.StreamRequestHandler):
        """TCP server handler"""
        def handle(self):
            count = 0
            while True:
                data = self.request.recv(2**30)
                if len(data) == 0:
                    break

                if count == 0:
                    function = pickle.loads(data)
                    count += 1
                elif count == 1:
                    print(data)
                    data_block = pickle.loads(data)
                    returns = self._work(function, data_block)
                    self.request.send(pickle.dumps(returns))
                else:
                    break

            self.request.close()

        def _work(self, function: types.FunctionType, data_bloc: list):
            """execute function"""
            returns = []
            for data in data_bloc:
                returns.append(function(data))

            return returns

    def __init__(self, host: str, port: int):
        self.HOST = host
        self.PORT = port
        while True:
            server = socketserver.TCPServer((self.HOST, self.PORT),
                                            self.Handler)
            print('work on', server.socket.getsockname())
            server.serve_forever()
