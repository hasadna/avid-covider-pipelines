import logging
import subprocess


def subprocess_call_log(*args, **kwargs):
    with subprocess.Popen(*args, **kwargs, stdout=subprocess.PIPE, stderr=subprocess.STDOUT) as proc:
        for line in iter(proc.stdout.readline, b''):
            logging.info(line.decode().rstrip())
        proc.wait()
        return proc.returncode
