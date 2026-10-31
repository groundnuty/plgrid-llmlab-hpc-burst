"""Print the TCP listening sockets of the given process IDs, read from /proc (no ss or netstat needed).

usage: python3 -I s2b_listeners.py PID...   prints "PID ADDRESS:PORT" per listening socket
"""
import os
import socket
import struct
import sys


def listening_inodes():
    """Map socket inode -> 'address:port' for every TCP socket in LISTEN state (state 0A)."""
    out = {}
    for path, fam in (("/proc/net/tcp", socket.AF_INET), ("/proc/net/tcp6", socket.AF_INET6)):
        try:
            lines = open(path).read().splitlines()[1:]
        except OSError:
            continue
        for line in lines:
            f = line.split()
            if len(f) < 10 or f[3] != "0A":
                continue
            hexaddr, hexport = f[1].split(":")
            raw = bytes.fromhex(hexaddr)
            if fam == socket.AF_INET:
                addr = socket.inet_ntop(fam, struct.pack("<I", struct.unpack(">I", raw)[0]))
            else:  # four little-endian 32-bit words
                addr = "[" + socket.inet_ntop(fam, b"".join(raw[i:i + 4][::-1] for i in range(0, 16, 4))) + "]"
            out[f[9]] = f"{addr}:{int(hexport, 16)}"
    return out


def main(pids):
    socks = listening_inodes()
    for pid in pids:
        try:
            fds = os.listdir(f"/proc/{pid}/fd")
        except OSError:
            continue
        for fd in fds:
            try:
                link = os.readlink(f"/proc/{pid}/fd/{fd}")
            except OSError:
                continue
            if link.startswith("socket:[") and link[8:-1] in socks:
                print(pid, socks[link[8:-1]])


if __name__ == "__main__":
    main(sys.argv[1:])
