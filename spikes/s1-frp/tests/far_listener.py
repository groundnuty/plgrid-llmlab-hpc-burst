#!/usr/bin/env python3
"""A far end for the job tests: a loopback listener that is silent, sends the wrong line, or closes at once.

    far_listener.py MODE PORT        MODE: silent | wrong | closed
    far_listener.py unix PATH        a unix-domain socket that sends UNIX-TOKEN and closes
"""
import select
import socket
import sys


def main_unix(path):
    u = socket.socket(socket.AF_UNIX)
    u.bind(path)
    u.listen(8)
    while True:
        conn, _ = u.accept()
        conn.sendall(b"UNIX-TOKEN\n")
        conn.close()


def main(mode, port):
    srv = socket.socket()
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.bind(("127.0.0.1", port))
    srv.listen(8)
    held = []
    while True:
        ready, _, _ = select.select([srv], [], [], 0.5)
        if not ready:
            continue
        conn, _ = srv.accept()
        if mode == "wrong":
            conn.sendall(b"HELLO\n")
            conn.close()
        elif mode == "closed":
            conn.close()
        else:
            held.append(conn)


if __name__ == "__main__":
    if sys.argv[1] == "unix":
        main_unix(sys.argv[2])
    else:
        main(sys.argv[1], int(sys.argv[2]))
