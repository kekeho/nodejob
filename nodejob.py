import socket
import socketserver
import pickle
import types
import time


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
        print(pickled_function)

        # Connect to worker
        [client.connect(worker)
         for client, worker in zip(self.clients, self.address_list)]

        # Send function-object to worker
        [client.send(pickled_function) for client in self.clients]
        # time break
        time.sleep(0.001)
        # Send data blocks to worker
        [client.send(pickle.dumps(data_block))
         for client, data_block in zip(self.clients, data_blocks)]


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
                    print('function', function)
                    count += 1
                elif count == 1:
                    data_block = pickle.loads(data)
                    print('data', data_block)
                    self.__work(function, data_block)
                    count += 1
                else:
                    break

            self.request.close()
        
        def __work(self, function: types.FunctionType, data_bloc: list):
            """execute function"""
            for data in data_bloc:
                function(data)

    def __init__(self, host: str, port: int):
        self.HOST = host
        self.PORT = port
        server = socketserver.TCPServer((self.HOST, self.PORT), self.Handler)
        print('work on', server.socket.getsockname())
        server.serve_forever()
