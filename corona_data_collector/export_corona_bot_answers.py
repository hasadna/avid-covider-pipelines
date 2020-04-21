from dataflows import Flow, update_resource, load, filter_rows
import logging
from avid_covider_pipelines.utils import dump_to_path
import os
from corona_data_collector.questionare_versions import questionare_versions
from collections import defaultdict
from corona_data_collector.DBToFileWriter import convert_values, collect_row
from corona_data_collector.config import answer_titles


def export_corona_bot_answers(parameters):
    stats = defaultdict(int)

    def _filter_questionnare_versions(row):
        if row['data']['version'] in questionare_versions.keys():
            stats['rows_supported_version'] += 1
            return True
        else:
            stats['rows_not_supported_version'] += 1
            return False

    def _get_row(row):
        # print(row)
        data_dict = row['data']
        data_dict['id'] = row['id']
        data_dict['created'] = row['created'].isoformat()
        # print(data_dict)
        fixed_row = convert_values(data_dict, stats)
        # print(fixed_row)
        collected_row = collect_row(fixed_row, return_array=True)
        if len(collected_row) != len(answer_titles):
            raise Exception('skipped row: %s' % collected_row)
        keys = []
        for k in sorted(answer_titles.keys()):
            keys.append(answer_titles[k])
        output_row = dict(zip(keys, collected_row))
        output_row['lat'] = row['lat']
        output_row['lng'] = row['lng']
        output_row['address_street_accurate'] = row['address_street_accurate']
        return output_row

    for resource in Flow(
        load(os.path.join(parameters['load'], 'datapackage.json')),
        filter_rows(_filter_questionnare_versions)
    ).datastream().res_iter:
        for row in resource:
            yield _get_row(row)
    logging.info('--- num rows with known invalid values to convert ---')
    for k in list(stats.keys()):
        v = stats.pop(k)
        if k.startswith('invalid_values_to_convert_'):
            logging.info("%s = %s : %s" % (*k.replace('invalid_values_to_convert_', '').split("__"), str(v)))
    logging.info('--- additional stats ---')
    for k, v in stats.items():
        logging.info("%s = %s" % (k, v))


def flow(parameters, *_):
    return Flow(
        export_corona_bot_answers(parameters),
        update_resource(-1, name="corona_bot_answers", path="corona_bot_answers.csv", **{"dpp:streaming": True}),
        dump_to_path(parameters['dump_to_path'])
    )


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    flow({
        'load': 'data/corona_data_collector/add_gps_coordinates',
        'dump_to_path': 'data/corona_data_collector/export_corona_bot_answers'
    }).process()
