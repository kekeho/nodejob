import socket
import pickle
import types
import time
import multiprocessing as mp


class NodeJobResult:
    """Result iterator class"""
    def __init__(self, non_ordered_list: list):
        self._list = non_ordered_list
        self._x = 0
        self._y = 0
        self._y_max = len(non_ordered_list)

    def __iter__(self):
        return self

    def __next__(self):
        if self._y == self._y_max:
            self._y = 0
            self._x += 1
        try:
            return_val = self._list[self._y][self._x]
        except IndexError:
            raise StopIteration()

        self._y += 1
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
        [client.close() for client in self.clients]

        # Send data blocks to worker
        self.clients = [socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                        for x in self.address_list]
        [client.connect(worker)
         for client, worker in zip(self.clients, self.address_list)]
        [client.send(pickle.dumps(data_block))
         for client, data_block in zip(self.clients, data_blocks)]

        [client.close() for client in self.clients]

        self.clients = [socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                        for x in self.address_list]
        [client.connect(worker)
         for client, worker in zip(self.clients, self.address_list)]

        returns = []
        for client in self.clients:
            data = b''
            while True:
                buffer = client.recv(2**30)
                if len(buffer) == 0:
                    break
                data += buffer

            returns.append(pickle.loads(data))

        return NodeJobResult(returns)


class Worker():
    """nodejob worker object"""

    def __init__(self, host: str, port: int):
        self.HOST = host
        self.PORT = port

        self.server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server.bind((self.HOST, self.PORT))

        self.master = None  # master address

        self.function = None
        self.data_block = None
        self.results = None

    def wait_forever(self):
        while True:
            self.server.listen()
            self.socket, self.master = self.server.accept()

            if self.function and self.data_block:
                self.__run()
                self.__send()
                self.__clean()
                continue

            data = b''
            while True:
                buffer = self.socket.recv(2**30)
                if len(buffer) == 0:
                    break

                data += buffer

            if self.function is None:
                self.function = pickle.loads(data)
            else:
                self.data_block = pickle.loads(data)

    def __run(self):
        print(f'Running {self.function}')
        start_time = time.time()
        pool = mp.Pool(mp.cpu_count())
        self.results = pool.map(self.function, self.data_block)
        pool.close()
        print(f'Done at {time.time() - start_time}s')

    def __send(self):
        self.socket.send(pickle.dumps(self.results))

    def __clean(self):
        self.function = None
        self.data_block = None
        self.results = None
        self.socket.close()
