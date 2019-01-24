import socketserver


class Handler(socketserver.StreamRequestHandler):
    """TCP server handler"""
    def handle(self):
        while True:
            data = self.request.recv(1024)
            if len(data) == 0:
                break
            print(data)  # debug
        self.request.close()


if __name__ == "__main__":
    HOST = '127.0.0.1'
    PORT = 49155
    # Boot server
    server = socketserver.TCPServer((HOST, PORT), Handler)
    print('work on', server.socket.getsockname())
    server.serve_forever()
