"""Tests of s2b_listeners.py against sockets this test opens itself.

Run: python3 -I spikes/s2b/py/test_s2b_listeners.py
"""
import contextlib
import io
import os
import socket
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import s2b_listeners  # noqa: E402


def listed(pid):
    buf = io.StringIO()
    with contextlib.redirect_stdout(buf):
        s2b_listeners.main([str(pid)])
    return buf.getvalue().split("\n")


class ListenersTest(unittest.TestCase):
    def test_loopback_v4_listener_found(self):
        s = socket.socket()
        s.bind(("127.0.0.1", 0))
        s.listen()
        port = s.getsockname()[1]
        try:
            self.assertIn(f"{os.getpid()} 127.0.0.1:{port}", listed(os.getpid()))
        finally:
            s.close()

    def test_wildcard_v6_listener_found(self):
        try:
            s = socket.socket(socket.AF_INET6)
            s.bind(("::", 0))
        except OSError:
            self.skipTest("no IPv6")
        s.listen()
        port = s.getsockname()[1]
        try:
            self.assertIn(f"{os.getpid()} [::]:{port}", listed(os.getpid()))
        finally:
            s.close()

    def test_connected_socket_is_not_a_listener(self):
        srv = socket.socket()
        srv.bind(("127.0.0.1", 0))
        srv.listen()
        c = socket.create_connection(srv.getsockname())
        port = srv.getsockname()[1]
        srv.close()  # only the connected client socket remains
        try:
            self.assertFalse([x for x in listed(os.getpid()) if x.endswith(f":{port}")])
        finally:
            c.close()


if __name__ == "__main__":
    unittest.main()
