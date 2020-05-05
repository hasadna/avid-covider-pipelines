from dataflows import Flow, update_resource, load
import logging
from avid_covider_pipelines.utils import dump_to_path, get_hash
import os
from corona_data_collector import questionare_versions
from collections import defaultdict
from corona_data_collector.DBToFileWriter import convert_values, collect_row
from corona_data_collector.config import answer_titles
import csv
import datetime
import atexit
import json
from distutils.version import LooseVersion
import tempfile
import stat
import shutil


def store_destination_output_package(destination_output, csv_temp_files):
    logging.info("Storing destination output package")
    os.makedirs(destination_output, exist_ok=True)
    logging.info("Writing to destination_output dir: " + destination_output)
    last_package = {}
    if os.path.exists(os.path.join(destination_output, "datapackage.json")):

        def _load_last_package(row):
            last_package[row['name']] = row
            yield row

        Flow(
            load(os.path.join(destination_output, "datapackage.json")),
            _load_last_package
        ).process()

    def _files_list():
        for temp_filepath, name in csv_temp_files.items():
            target_filepath = os.path.join(destination_output, name)
            shutil.move(temp_filepath, target_filepath)
            os.chmod(target_filepath, stat.S_IRUSR | stat.S_IWUSR | stat.S_IRGRP | stat.S_IROTH)
            size = os.path.getsize(target_filepath)
            hash = get_hash(target_filepath)
            last_row = last_package.get(name)
            if last_row and hash == last_row.get('hash') and size == last_row['size']:
                mtime = last_row['mtime']
            else:
                mtime = datetime.datetime.fromtimestamp(os.path.getmtime(target_filepath))
            yield {"name": name, "size": size, "mtime": mtime, "hash": hash}

    Flow(
        _files_list(),
        update_resource(-1, name='files_list', path='files_list.csv'),
        dump_to_path(destination_output),
    ).process()


def flow(parameters, *_):
    stats = defaultdict(int)
    output_keys = []
    for k in sorted(answer_titles.keys()):
        output_keys.append(answer_titles[k])
    cur_csv = {
        'day': None,
        'month': None,
        'year': None,
        'filename': None,
        'file': None,
        'writer': None
    }
    csv_temp_files = {}
    last_questionare_version = questionare_versions.get_last_version()

    def _close_csv():
        if cur_csv['file']:
            cur_csv['file'].close()
            cur_csv['writer'] = None

    def _close_all_csv():
        _close_csv()
        for filename in csv_temp_files:
            if os.path.exists(filename):
                os.unlink(filename)

    atexit.register(_close_all_csv)

    def _filter_questionnare_versions(row):
        if not row.get("version") or not json.loads(row['version']):
            if not parameters.get("unsupported"):
                stats["rows_with_invalid_version"] += 1
            return False
        is_supported_version = questionare_versions.is_supported_version(json.loads(row['version']))
        if parameters.get("unsupported"):
            if is_supported_version:
                return False
            else:
                stats["rows_included"] += 1
                return True
        elif is_supported_version:
            stats['rows_supported_version'] += 1
            return True
        else:
            stats['rows_not_supported_version'] += 1
            return False

    def _get_row(row):
        # print(row)
        try:
            data_dict = {k: json.loads(v) for k, v in row.items()
                         if k not in ["__id", "__created", "lat", "lng", "address_street_accurate", "workplace_lat", "workplace_lng", "workplace_street_accurate"]}
        except TypeError:
            logging.info(row)
            raise
        stats["v__" + data_dict["version"]] += 1
        data_dict["id"] = row["__id"]
        data_dict["created"] = row["__created"].isoformat()
        data_dict["lat"] = row["lat"]
        data_dict["lng"] = row["lng"]
        data_dict["address_street_accurate"] = row["address_street_accurate"]
        data_dict["workplace_lat"] = row.get("workplace_lat")
        data_dict["workplace_lng"] = row.get("workplace_lng")
        data_dict["workplace_street_accurate"] = row.get("workplace_street_accurate")
        # print(data_dict)
        fixed_row = convert_values(data_dict, stats)
        if fixed_row is None:
            return None
        # print(fixed_row)
        if parameters.get("unsupported"):
            force_version = last_questionare_version
        else:
            force_version = None
        collected_row = collect_row(fixed_row, return_array=True, force_version=force_version)
        if len(collected_row) != len(answer_titles):
            raise Exception('skipped row: %s' % collected_row)
        output_row = dict(zip(output_keys, collected_row))
        output_row['lat'] = str(row['lat'])
        output_row['lng'] = str(row['lng'])
        output_row['address_street_accurate'] = str(row['address_street_accurate'])
        output_row['workplace_lat'] = str(row.get('workplace_lat', ""))
        output_row['workplace_lng'] = str(row.get('workplace_lng', ""))
        output_row['workplace_street_accurate'] = str(row.get('workplace_street_accurate', ""))
        return {"output_row": output_row, "created": row['__created']}

    def _dump_row(row):
        if row is None:
            return None
        day, month, year = row['created'].day, row['created'].month, row['created'].year
        if not cur_csv['writer'] or cur_csv['day'] != day or cur_csv['month'] != month or cur_csv['year'] != year:
            if cur_csv['writer']:
                cur_csv['file'].close()
            cur_csv['day'], cur_csv['month'], cur_csv['year'] = day, month, year
            cur_csv['file'] = tempfile.NamedTemporaryFile("w", delete=False)
            cur_csv['filename'] = cur_csv['file'].name
            logging.info("Writing to temp file for %s_%s_%s" % (day, month, year))
            csv_temp_files[cur_csv['filename']] = "corona_bot_answers_%s_%s_%s_with_coords.csv" % (day, month, year)
            cur_csv['writer'] = csv.DictWriter(cur_csv['file'], output_keys + ["lat", "lng", "address_street_accurate"] + ["workplace_lat", "workplace_lng", "workplace_street_accurate"])
            cur_csv['writer'].writeheader()
        cur_csv['writer'].writerow(row['output_row'])

    def _process_rows(rows):
        if rows.res.name == "db_data":
            for row in rows:
                yield row
                if _filter_questionnare_versions(row):
                    _dump_row(_get_row(row))
            _close_csv()
            logging.info('--- num rows with invalid values to convert (unsupported=%s) ---' % parameters.get("unsupported", False))
            total_invalid_values = 0
            for k in list(stats.keys()):
                if k.startswith('invalid_values_to_convert_'):
                    v = stats.pop(k)
                    logging.info("%s = %s : %s" % (*k.replace('invalid_values_to_convert_', '').split("__"), str(v)))
                    total_invalid_values += int(v)
            logging.info("--- version stats (unsupported=%s) ---" % parameters.get("unsupported", False))
            version_stats = {}
            for k in list(stats.keys()):
                if k.startswith("v__"):
                    version_stats[k.replace("v__", "")] = stats.pop(k)
            for version in sorted(map(LooseVersion, version_stats.keys())):
                logging.info("%s = %s" % (version, version_stats[str(version)]))
            logging.info('--- additional stats  (unsupported=%s) ---' % parameters.get("unsupported", False))
            for k, v in stats.items():
                logging.info("%s = %s" % (k, v))
            store_destination_output_package(parameters['destination_output'], csv_temp_files)
            if total_invalid_values > 10000:
                raise Exception("Too many invalid values rows: " + str(total_invalid_values))
        else:
            yield from rows

    flow_args = []
    if parameters.get('load'):
        flow_args += [
            load(os.path.join(parameters['load'], 'datapackage.json'))
        ]
    flow_args += [
        _process_rows
    ]
    return Flow(*flow_args)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    flow({
        'load': 'data/corona_data_collector/add_gps_coordinates',
        "destination_output": "data/corona_data_collector/destination_output"
    }).process()
