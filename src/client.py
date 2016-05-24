# -*- coding: utf-8 -*-

import sys
import socket

HOST = '127.0.0.1'
PORT = 23333


def main():
    if len(sys.argv) == 1:
        host, port = HOST, PORT
    elif len(sys.argv) == 2:
        host, port = sys.argv[1], PORT
    elif len(sys.argv) == 3:
        host = sys.argv[1]
        port = sys.argv[2]
    client = socket.create_connection((host, port))
    client.sendall('PING')
    msg = client.recv(2048)
    print msg


if __name__ == '__main__':
    main()
