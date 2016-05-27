# -*- coding: utf-8 -*-

import os
import sys
import socket
import base64
import sqlite3


HOST = '0.0.0.0'
PORT = 23333
MAX_CONNECTIONS = 1
SEGMENT_SZIE = 1024
DB_PATH = './dbs/sync.db'


db = sqlite3.connect(DB_PATH)
cursor = db.cursor()


def invalid_command(params):
    return 'Invalid command'


def ping_command(params):
    return 'Pong'


def last_command(params):
    cursor.execute('SELECT id FROM images ORDER BY ID DESC LIMIT 1')
    rs = cursor.fetchone()
    if rs:
        return str(rs[0])
    else:
        return None

def sync_command(params):
    id_ = params
    cursor.execute('SELECT id, data FROM images WHERE id > ? ORDER BY ID LIMIT 1', (id_,))
    data = cursor.fetchone()
    img = base64.b64encode(data[1])
    packet = '{} {}'.format(data[0], img)
    if data is None:
        return None
    return packet


def shutdown(params):
    raise IOError()


class Server(object):

    commands = {
        'PING': ping_command,
        'LAST': last_command,
        'SYNC': sync_command,
        'SHUTDOWN': shutdown,
    }

    def __init__(self, host, port):
        self.host = host
        self.port = port
        self.server = None

    def run(self):
        self.server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server.bind((self.host, self.port))
        self.server.listen(MAX_CONNECTIONS)
        print 'listen %s:%s' % (self.host, self.port)

        while True:
            connection, address = self.server.accept()

            print 'Connected from %s' % str(address)

            while True:
                msg = connection.recv(SEGMENT_SZIE)

                if msg is not None:
                    split_msg = msg.split(' ', 1)
                    if len(split_msg) > 1:
                        command, params = split_msg
                    else:
                        command = split_msg[0]
                        params = None

                    # print command
                    if command == 'CLOSE':
                        break
                    command_handler = self.commands.get(command, invalid_command)
                    result = command_handler(params)

                    if result is not None:
                        connection.send(result + '\r\n\r\n')

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

    server = Server(host, port)
    server.run()


if __name__ == '__main__':
    main()
