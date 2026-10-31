"""Loads kit modules from S1_KIT_DIR (default: the directory above tests/), so the same tests can run
against an older commit or a mutated copy of the kit."""
import importlib.util
import os

KIT = os.environ.get("S1_KIT_DIR") or os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def load(name):
    path = os.path.join(KIT, name + ".py")
    spec = importlib.util.spec_from_file_location("kit_" + name, path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def path(name):
    return os.path.join(KIT, name)


_PORTS_HANDED_OUT = set()


def free_port():
    """A port in 29400-29499 that nothing listens on right now, on 127.0.0.1. The range is the one
    the shared VM leaves to spikes. The scan starts at a random offset and skips ports already handed
    out in this process, so two servers of one test never collide. Never returns 0."""
    import random
    import socket
    first = random.randrange(100)
    for i in range(100):
        port = 29400 + (first + i) % 100
        if port in _PORTS_HANDED_OUT:
            continue
        probe = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            probe.bind(("127.0.0.1", port))
        except OSError:
            continue
        finally:
            probe.close()
        _PORTS_HANDED_OUT.add(port)
        return port
    raise RuntimeError("no free port left in 29400-29499")
