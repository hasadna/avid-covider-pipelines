import logging
import subprocess
import os
from dataflows import Flow, load, update_resource


def subprocess_call_log(*args, **kwargs):
    with subprocess.Popen(*args, **kwargs, stdout=subprocess.PIPE, stderr=subprocess.STDOUT) as proc:
        for line in iter(proc.stdout.readline, b''):
            logging.info(line.decode().rstrip())
        proc.wait()
        return proc.returncode


def load_if_exists(load_source, name, not_exists_rows, *args, **kwargs):
    if os.path.exists(load_source):
        return Flow(load(load_source, name, *args, **kwargs))
    else:
        return Flow(iter(not_exists_rows), update_resource(-1, name=name))
