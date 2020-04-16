import logging
import subprocess
from html import escape


def subprocess_call_log(*args, **kwargs):
    with subprocess.Popen(*args, **kwargs, stdout=subprocess.PIPE, stderr=subprocess.STDOUT) as proc:
        for line in iter(proc.stdout.readline, b''):
            # html escape is needed due to bug in dpp dashboard
            logging.info(escape(line.decode().rstrip()))
        proc.wait()
        return proc.returncode
