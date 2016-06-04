# -*- coding: utf-8 -*-

import sys
import socket
import base64
import sqlite3

HOST = '127.0.0.1'
PORT = 23333
SEGMENT_SZIE = 1024
DB_PATH = '/Users/shonenada/Projects/toys/img-server/db/1.db'

END = '\r\n\r\n'

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

    def get_last(self, tbl):
        cursor.execute('SELECT id FROM %s ORDER BY ID DESC LIMIT 1' % tbl)
        rs = cursor.fetchone()
        if rs:
            return rs[0]
        else:
            return None

    def last(self, tbl):
        self.client.sendall('LAST %s' % tbl)
        return self.client.recv(SEGMENT_SZIE)

    def get_tables(self):
        self.client.sendall('SCHEMA')
        data = self.client.recv(SEGMENT_SZIE).strip()
        return data.split()

    def get_columns(self, table):
        self.client.sendall('COLUMNS %s' % table)
        data = self.client.recv(SEGMENT_SZIE).strip()
        return data.split()

    def sync_from_server(self, id_):
        self.client.sendall('SYNC %s' % id_)
        return self._recv()

    def _recv(self):
        data = []
        tmp = ''
        while True:
            tmp = self.client.recv(4)
            if END in tmp:
                break
            data.append(tmp)
            if len(data) > 1:
                last_pair = ''.join(data[-2:-1])
                if END in last_pair:
                    data[-2] = last_pair[:last_pair.find(END)]
                    data.pop()
                    break
        return ''.join(data)

    def close(self):
        self.client.sendall('CLOSE')
        self.client.close()

    def shutdown(self):
        self.client.sendall('SHUTDOWN')

    def sync(self):
        tbls = self.get_tables()
        for tbl in tbls:
            last = self.last(tbl)
            if last == 'None':
                print 'last is None'
                return
            this_last = self.get_last(tbl)
            if this_last is None:
                this_last = 0

            if int(this_last) < int(last):
                columns = self.get_columns(tbl)
                self.sync_once(tbl, columns, this_last)

    def sync_once(self, tbl, columns, last_id):
        data = self.sync_from_server(last_id)
        id_, b64img = data.split(' ', 1)
        print 'Inserting %s' % id_, len(b64img)
        img = base64.b64decode(b64img)
        sql = self.build_sql(tbl, columns)
        cursor.execute(sql, [id_, buffer(img)])
        db.commit()
        self.sync()

    def build_sql(self, tbl, columns):
        sql = ('INSERT INTO %s(%s) VALUES(?,?)' %
               (tbl, ', '.join(columns)))
        return sql

    def build_data(self, data):
        pass


def main():
    shutdown = False
    if len(sys.argv) == 1:
        host, port = HOST, PORT
    elif len(sys.argv) == 2:
        host, port = sys.argv[1], PORT
    elif len(sys.argv) == 3:
        host = sys.argv[1]
        port = sys.argv[2]
        if port == 'shutdown':
            port = PORT
            shutdown = True
    elif len(sys.argv) == 4:
        host = sys.argv[1]
        port = sys.argv[2]
        shutdown = True

    client = Client(host, port)

    '''
    if shutdown:
        client.shutdown()
    else:
        client.sync()
    '''
    for tbl in client.get_tables():
        print client.get_columns(tbl)
    client.shutdown()
    client.close()


if __name__ == '__main__':
    main()
