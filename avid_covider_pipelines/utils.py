import logging
import subprocess
import os
import stat
from dataflows import Flow, load, update_resource, sort_rows, printer
from dataflows.processors.dumpers.to_path import PathDumper
import datetime


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


class dump_to_path(PathDumper):

    def write_file_to_output(self, filename, path):
        path = super(dump_to_path, self).write_file_to_output(filename, path)
        os.chmod(path, stat.S_IRUSR | stat.S_IWUSR | stat.S_IRGRP | stat.S_IROTH)
        return path


def keep_last_runs_history(output_dir, run_callback):
    run_row = {'start_time': datetime.datetime.now()}
    last_run_row = Flow(
        load_if_exists('%s/last_run/datapackage.json' % output_dir, 'last_run', [{}])
    ).results()[0][0][0]
    run_row = run_callback(last_run_row, run_row)
    if run_row:
        Flow(
            iter([run_row]),
            update_resource(-1, name='last_run', path='last_run.csv', **{'dpp:streaming': True}),
            dump_to_path('%s/last_run' % output_dir)
        ).process()

    def _get_runs_history():
        if os.path.exists('%s/runs_history/datapackage.json' % output_dir):
            for resource in Flow(
                load('%s/runs_history/datapackage.json' % output_dir),
            ).datastream().res_iter:
                yield from resource
        if run_row:
            yield run_row

    Flow(
        _get_runs_history(),
        update_resource(-1, name='runs_history', path='runs_history', **{'dpp:streaming': True}),
        dump_to_path('%s/runs_history' % output_dir)
    ).process()

    return Flow(
        load('%s/runs_history/datapackage.json' % output_dir),
        sort_rows('{start_time}', reverse=True),
        printer(num_rows=10)
    )
