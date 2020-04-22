from dataflows import Flow, update_resource, load, add_field, filter_rows
import logging
from avid_covider_pipelines.utils import dump_to_path
import kvfile
import os
from corona_data_collector.gps_generator import get_coords_from_web
import json
from collections import defaultdict
import atexit


def get_coords(stats, kv, street, city, is_street=True):
    key = '%s_%s' % (street, city)
    try:
        value = kv.get(key)
        stats['got_value_from_cache' + ('_is_street' if is_street else '_not_is_street')] += 1
        return value
    except KeyError:
        pass
    logging.info('Getting coords from web: "%s" "%s"' % (street, city))
    stats['getting_coords_from_web' + ('_is_street' if is_street else '_not_is_street')] += 1
    lat, lng, accurate = get_coords_from_web(street, city)
    value = [float(lat), float(lng), int(accurate)]
    if value[0] in [0, -1]:
        stats['invalid_coords_from_web' + ('_is_street' if is_street else '_not_is_street')] += 1
        if is_street and street != city:
            stats['invalid_coords_from_web_trying_is_street'] += 1
            value = get_coords(stats, kv, city, city, is_street=False)
    else:
        stats['valid_coords_from_web' + ('_is_street' if is_street else '_not_is_street')] += 1
    kv.set(key, value)
    return value


def load_cache_from_package(parameters, stats, kv):
    logging.info("Loading coordinates cache from datapackage " + parameters['gps_datapackage_path'])
    for resource in Flow(load(os.path.join(parameters['gps_datapackage_path'], "datapackage.json"), strip=False)).datastream().res_iter:
        for row in resource:
            stats['loaded_from_package_cache'] += 1
            kv.set(row['k'], row['v'])


def load_cache_from_json(parameters, stats, kv):
    logging.info("Loading coordinates cache from json")
    with open(parameters['gps_data']) as f:
        gps_data = json.load(f)
    for k, v in gps_data.items():
        try:
            value = [float(v["lat"]), float(v["lng"]), int(v["street_accurate"])]
        except ValueError:
            value = None
        if value:
            stats['loaded_from_json_cache'] += 1
            kv.set(str(k), value)
        else:
            stats['invalid_data_in_json_cache'] += 1


def save_cache(parameters, kv):
    logging.info('Saving cache to ' + parameters['gps_datapackage_path'])
    Flow(
        ({"k": k, "v": v} for k, v in kv.items()),
        update_resource(-1, name="gps_data", path="gps_data.csv", **{"dpp:streaming": True}),
        dump_to_path(parameters['gps_datapackage_path'])
    ).process()


def add_gps_coordinates(stats, kv, parameters):
    logging.info('adding gps coordinates')

    def _add_gps_coordinates(rows):
        for row in rows:
            lat, lng, accurate = get_coords(stats, kv, row['data']['street'], row['data']['city_town'])
            yield {
                **row,
                "lat": lat,
                "lng": lng,
                "address_street_accurate": accurate
            }
        logging.info(str(dict(stats)))

    flow_args = []
    if parameters.get('load'):
        flow_args += [
            load(os.path.join(parameters['load'], 'datapackage.json'))
        ]
    if parameters.get('min_id'):
        flow_args += [
            filter_rows(lambda row: row['id'] >= parameters['min_id'])
        ]
    flow_args += [
        filter_rows(lambda row: isinstance(row['data'], dict) and 'street' in row['data'] and 'city_town' in row['data']),
        add_field('lat', 'number', 0, -1),
        add_field('lng', 'number', 0, -1),
        add_field('address_street_accurate', 'number', 0, -1),
        _add_gps_coordinates,
        update_resource(-1, name="db_data_with_coords", path="db_data_with_coords.csv", **{"dpp:streaming": True}),
    ]
    if parameters.get('dump_to_path'):
        flow_args += [
            dump_to_path(parameters['dump_to_path'])
        ]
    return Flow(*flow_args)


def flow(parameters, *_):
    if kvfile.kvfile.db_kind != 'LevelDB':
        raise Exception("Please install levelDB package, otherwise this flow is extremely slow")
    stats = defaultdict(int)
    kv = kvfile.KVFile()
    atexit.register(save_cache, parameters, kv)
    if os.path.exists(os.path.join(parameters['gps_datapackage_path'], "datapackage.json")):
        load_cache_from_package(parameters, stats, kv)
    elif parameters.get('gps_data'):
        load_cache_from_json(parameters, stats, kv)
    logging.info('cache loaded successfully')
    return add_gps_coordinates(stats, kv, parameters)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    flow({
        'min_id': 600000,
        'load': 'data/corona_data_collector/load_from_db',
        'dump_to_path': 'data/corona_data_collector/add_gps_coordinates',
        'gps_data': 'data/corona_data_collector/gps_data.json'
    }).process()
