# -*- coding: utf-8 -*-

import sys
import socket
import sqlite3


HOST = '0.0.0.0'
PORT = 23333
MAX_CONNECTIONS = 5


def invalid_command(params):
    return 'Invalid command'


def ping_command(params):
    return 'Pong'


def read_command(params):
    pass


commands = {
    'PING': ping_command,
    'READ': read_command,
}


def loop(host, port):
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.bind((host, port))
    server.listen(MAX_CONNECTIONS)
    print 'listen %s:%s' % (host, port)

    while True:
        connection, address = server.accept()
        connection.settimeout(60)

        print 'Connected from %s' % str(address)

        msg = connection.recv(2048)

        if msg is not None:
            split_msg = msg.split(' ', 1)
            if len(split_msg) > 1:
                command, params = split_msg
            else:
                command = split_msg[0]
                params = None

            command_handler = commands.get(command, invalid_command)
            result = command_handler(params)

            if result is not None:
                connection.send('OK\r\n%s' % result)

        connection.close()


def main():
    if len(sys.argv) == 1:
        host, port = HOST, PORT
    elif len(sys.argv) == 2:
        host = sys.argv[1]
        port = PORT
    elif len(sys.argv) == 3:
        host = sys.argv[1]
        port = sys.argv[2]

    loop(host, port)


if __name__ == '__main__':
    main()
