#!/usr/bin/env python
# -*- encoding: utf-8 -*-


import asyncore
import socket


class EchoHandler(asyncore.dispatcher_with_send):
    def handle_read(self):
        data = self.recv(8192)
        print(data)
        response = []
        next_line = '\r\n'
        start_line = 'HTTP/1.1 200 OK' + next_line
        head = 'server: asyncore' + next_line
        empty_line = next_line * 2
        body = '<html><body>' + data + '</body></html>'
        response.extend([start_line, head, empty_line, body])
        response = ''.join(response) 
        if data:
            self.send(response)


class EchoServer(asyncore.dispatcher):
    def __init__(self, host, port):
        asyncore.dispatcher.__init__(self)
        self.create_socket(socket.AF_INET, socket.SOCK_STREAM)
        self.set_reuse_addr()
        self.bind((host, port))
        self.listen(5)


    def handle_accept(self):
        pair = self.accept()
        if pair is not None:
            sock, addr = pair
            print("Incoming connection from %s" % repr(addr))
            handler = EchoHandler(sock)

if __name__ == '__main__':
    server = EchoServer('', 8080)
    asyncore.loop()
