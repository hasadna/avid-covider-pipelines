from dataflows import Flow, update_resource, printer, dump_to_path
from common import utils
from glob import glob
import logging
import sys
import os
import hashlib


HASH_BLOCKSIZE = 65536


def get_hash(path):
    hasher = hashlib.sha256()
    with open(path, 'rb') as f:
        buf = f.read(HASH_BLOCKSIZE)
        while len(buf) > 0:
            hasher.update(buf)
            buf = f.read(HASH_BLOCKSIZE)
    return hasher.hexdigest()


def get_updated_files(mtimes, sizes, hashes):
    num_updated = 0
    for path in glob('../COVID19-ISRAEL/**', recursive=True):
        if os.path.isfile(path):
            if path not in mtimes or (mtimes[path] != os.path.getmtime(path) and (sizes[path] != os.path.getsize(path) or hashes[path] != get_hash(path))):
                yield {'path': path.replace('../COVID19-ISRAEL/', '')}
                num_updated += 1
    logging.info('number of updated files: ' + str(num_updated))


def flow(parameters, *_):
    logging.info('Running COVID19-ISRAEL module %s' % parameters['module'])
    mtimes = {}
    sizes = {}
    hashes = {}
    for path in glob('../COVID19-ISRAEL/**', recursive=True):
        if os.path.isfile(path):
            mtimes[path] = os.path.getmtime(path)
            sizes[path] = os.path.getsize(path)
            hashes[path] = get_hash(path)
    if utils.subprocess_call_log(['python', '-u', '-m', parameters['module']], cwd='../COVID19-ISRAEL') != 0:
        raise Exception('Failed to run module %s' % parameters['module'])
    return Flow(
        get_updated_files(mtimes, sizes, hashes),
        update_resource(-1, name='covid19_israel_updated_files', path='covid19_israel_updated_files.csv', **{'dpp:streaming': True}),
        printer(num_rows=999),
        dump_to_path('data/run_covid19_israel/last_updated_files/%s' % parameters['module'])
    )


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print('usage:\n  %s <MODULE_NAME>\n' % sys.argv[0])
        print('for example:\n  %s src.utils.get_raw_data' % sys.argv[0])
        exit(1)
    logging.basicConfig(level=logging.DEBUG)
    flow({'module': sys.argv[1]}).process()
