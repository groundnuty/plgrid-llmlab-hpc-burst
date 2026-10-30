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
