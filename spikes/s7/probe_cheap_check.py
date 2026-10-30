#!/usr/bin/env python3
"""What a size-and-mtime check at start can and cannot see, measured on the real filesystem.

Creates a 4 KiB file in the directory given as argument, then changes it in place four
ways and records, for each, which of size, mtime, ctime and inode changed and whether the
content hash changed. Python 3.6 compatible. Writes only inside the given directory.

Cases:
  same-second   flip one byte at once, leave the mtime alone (filesystem decides)
  restored      flip one byte, then set mtime back with utime (what cp -p or rsync -t do)
  later         flip one byte after 2.5 s, leave the mtime alone
  replaced      write the same size under a new name, copy the old mtime, rename over
"""
import hashlib
import json
import os
import shutil
import sys
import time


def snap(path):
    st = os.stat(path)
    with open(path, "rb") as f:
        digest = hashlib.sha256(f.read()).hexdigest()
    return {"size": st.st_size, "mtime_ns": st.st_mtime_ns, "ctime_ns": st.st_ctime_ns,
            "ino": st.st_ino, "sha256": digest}


def flip_byte(path):
    with open(path, "r+b") as f:
        f.seek(100)
        b = f.read(1)
        f.seek(100)
        f.write(bytes([b[0] ^ 0xFF]))
        f.flush()
        os.fsync(f.fileno())


def compare(before, after):
    return {
        "size_changed": before["size"] != after["size"],
        "mtime_changed": before["mtime_ns"] != after["mtime_ns"],
        "ctime_changed": before["ctime_ns"] != after["ctime_ns"],
        "ino_changed": before["ino"] != after["ino"],
        "content_changed": before["sha256"] != after["sha256"],
        "mtime_delta_ns": after["mtime_ns"] - before["mtime_ns"],
    }


def fresh(directory, name):
    path = os.path.join(directory, name)
    with open(path, "wb") as f:
        f.write(os.urandom(4096))
        f.flush()
        os.fsync(f.fileno())
    return path


def main():
    directory = sys.argv[1]
    os.makedirs(directory)  # refuses an existing directory: only files made here are removed below
    results = {}

    path = fresh(directory, "same-second")
    before = snap(path)
    flip_byte(path)
    results["same-second"] = compare(before, snap(path))

    path = fresh(directory, "restored")
    before = snap(path)
    time.sleep(1.2)
    flip_byte(path)
    os.utime(path, ns=(before["mtime_ns"], before["mtime_ns"]))
    results["restored"] = compare(before, snap(path))

    path = fresh(directory, "later")
    before = snap(path)
    time.sleep(2.5)
    flip_byte(path)
    results["later"] = compare(before, snap(path))

    path = fresh(directory, "replaced")
    before = snap(path)
    time.sleep(1.2)
    tmp = path + ".new"
    with open(tmp, "wb") as f:
        f.write(os.urandom(4096))
        f.flush()
        os.fsync(f.fileno())
    os.utime(tmp, ns=(before["mtime_ns"], before["mtime_ns"]))
    os.rename(tmp, path)
    results["replaced"] = compare(before, snap(path))

    print(json.dumps(results, indent=1, sort_keys=True))
    shutil.rmtree(directory)  # the directory was created by this run


if __name__ == "__main__":
    main()
