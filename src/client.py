# -*- coding: utf-8 -*-

import sys
import socket
import base64
import sqlite3

HOST = '127.0.0.1'
PORT = 23333
SEGMENT_SZIE = 1024
DB_PATH = '/Users/shonenada/Projects/toys/img-server/db/1.db'

db = sqlite3.connect(DB_PATH)
cursor = db.cursor()


class Client(object):

    def __init__(self, host, port):
        self.host = host
        self.port = port
        self.client = None
        self.connect()

    def connect(self):
        self.client = socket.create_connection((self.host, self.port))

    def ping(self):
        self.client.sendall('PING')
        return self.client.recv(SEGMENT_SZIE)

    def get_last(self):
        cursor.execute('SELECT id FROM images ORDER BY ID DESC LIMIT 1')
        rs = cursor.fetchone()
        if rs:
            return rs[0]
        else:
            return None

    def last(self):
        self.client.sendall('LAST')
        return self.client.recv(SEGMENT_SZIE)

    def sync_from_server(self, id_):
        self.client.sendall('SYNC %s' % id_)
        data = []
        tmp = ''
        while True:
            tmp = self.client.recv(4)
            if tmp == '\r\n\r\n':
                break
            data.append(tmp)
        return ''.join(data)

    def close(self):
        self.client.sendall('CLOSE')
        self.client.close()

    def shutdown(self):
        self.client.sendall('SHUTDOWN')

    def sync(self):
        last = self.last()
        if last == 'None':
            print 'last is None'
            return
        this_last = self.get_last()
        if this_last is None:
            this_last = 0

        if int(this_last) < int(last):
            self.sync_once(this_last)

    def sync_once(self, last_id):
        data = self.sync_from_server(last_id)
        id_, b64img = data.split(' ', 1)
        print 'Inserting %s' % id_, len(b64img)
        img = base64.b64decode(b64img)
        cursor.execute('INSERT INTO images(id, data) VALUES(?,?)',
                       [id_, buffer(img)])
        db.commit()
        self.sync()


def main():
    if len(sys.argv) == 1:
        host, port = HOST, PORT
    elif len(sys.argv) == 2:
        host, port = sys.argv[1], PORT
    elif len(sys.argv) == 3:
        host = sys.argv[1]
        port = sys.argv[2]

    client = Client(host, port)

    client.sync()

    client.close()


if __name__ == '__main__':
    main()
