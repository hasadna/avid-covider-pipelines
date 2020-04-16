import logging
import subprocess
import os
from dataflows import Flow, load, update_resource


def subprocess_call_log(*args, log_file=None, **kwargs):
    if log_file:
        log_file = open(log_file, 'w')
    try:
        with subprocess.Popen(*args, **kwargs, stdout=subprocess.PIPE, stderr=subprocess.STDOUT) as proc:
            for line in iter(proc.stdout.readline, b''):
                line = line.decode().rstrip()
                logging.info(line)
                if log_file:
                    log_file.write(line + "\n")
            proc.wait()
            return proc.returncode
    finally:
        if log_file:
            log_file.close()


def load_if_exists(load_source, name, not_exists_rows, *args, **kwargs):
    if os.path.exists(load_source):
        return Flow(load(load_source, name, *args, **kwargs))
    else:
        return Flow(iter(not_exists_rows), update_resource(-1, name=name))
