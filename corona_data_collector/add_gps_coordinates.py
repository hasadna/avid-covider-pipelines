from dataflows import Flow, update_resource, load, add_field
import logging
from avid_covider_pipelines.utils import dump_to_path
import kvfile
import os
from corona_data_collector.gps_generator import get_coords_from_web
import json
from collections import defaultdict
import atexit


def get_coords(stats, kv, inputs, is_street=True, get_coords_callback=None):
    if get_coords_callback is None:
        get_coords_callback = get_coords_from_web
    street, city = inputs.get("street"), inputs.get("city")
    if not street and not city:
        return 0, 0, 0
    elif not street:
        is_street = False
        street = ""
    elif not city:
        city = ""
    key = '%s_%s' % (street, city)
    try:
        value = kv.get(key)
        stats['got_value_from_cache' + ('_is_street' if is_street else '_not_is_street')] += 1
        return value
    except KeyError:
        pass
    logging.info('Getting coords from web: "%s" "%s"' % (street, city))
    stats['getting_coords_from_web' + ('_is_street' if is_street else '_not_is_street')] += 1
    lat, lng, accurate = get_coords_callback(street, city)
    value = [float(lat), float(lng), int(accurate)]
    if value[0] in [0, -1] or value[1] in [0, -1]:
        stats['invalid_coords_from_web' + ('_is_street' if is_street else '_not_is_street')] += 1
        if is_street and street != city:
            stats['invalid_coords_from_web_trying_is_street'] += 1
            value = get_coords(stats, kv, {"street": city, "city": city}, is_street=False, get_coords_callback=get_coords_callback)
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
    if parameters.get("gps_datapackage_path"):
        logging.info('Saving cache to ' + parameters['gps_datapackage_path'])
        Flow(
            ({"k": k, "v": v} for k, v in kv.items()),
            update_resource(-1, name="gps_data", path="gps_data.csv", **{"dpp:streaming": True}),
            dump_to_path(parameters['gps_datapackage_path'])
        ).process()


def add_gps_coordinates(stats, kv, parameters):
    logging.info('adding gps coordinates')

    def _add_gps_coordinates(rows):
        logging.info("resource name = " + rows.res.name)
        if rows.res.name == "db_data":
            source = "db"
        else:
            source = rows.res.name.split("__")[0]
        fields = parameters["source_fields"][source]
        workplace_fields = parameters.get("workplace_source_fields", {}).get(source)
        if workplace_fields and source != "db":
            raise Exception("sorry, wokrplace_fields is only supported for db source")
        for row in rows:
            inputs = {}
            workplace_inputs = {}
            for k, v in row.items():
                input = fields.get(k.strip())
                if input and v and v.strip():
                    if input in inputs:
                        logging.warning("duplicate input %s, %s: %s" % (source, input, row))
                    elif source == "db":
                        inputs[input] = json.loads(v)
                    else:
                        inputs[input] = v
                if workplace_fields:
                    input = workplace_fields.get(k.strip())
                    if input and v and v.strip():
                        if input in workplace_inputs:
                            logging.warning("duplicate workplace_input %s, %s: %s" % (source, input, row))
                        elif source == "db":
                            workplace_inputs[input] = json.loads(v)
                        else:
                            workplace_inputs[input] = v
            lat, lng, accurate = get_coords(stats, kv, inputs, get_coords_callback=parameters.get("get-coords-callback"))
            if workplace_fields:
                workplace_lat, workplace_lng, workplace_accurate = get_coords(stats, kv, workplace_inputs, get_coords_callback=parameters.get("get-coords-callback"))
            yield {
                **row,
                "lat": str(lat),
                "lng": str(lng),
                **({"address_street_accurate": str(accurate)} if source == "db" else {}),
                **({
                    "workplace_lat": str(workplace_lat),
                    "workplace_lng": str(workplace_lng),
                    **({"workplace_street_accurate": str(workplace_accurate)} if source == "db" else {}),
                } if workplace_fields else {}),
            }
        logging.info(str(dict(stats)))

    flow_args = []
    if parameters.get('load_db_data'):
        flow_args += [
            load(os.path.join(parameters['load_db_data'], 'datapackage.json'))
        ]
    if parameters.get('load_gdrive_data'):
        flow_args += [
            load(os.path.join(parameters['load_gdrive_data'], 'datapackage.json'))
        ]
    flow_args += [
        add_field('lat', 'string', default="0"),
        add_field('lng', 'string', default="0"),
        add_field('address_street_accurate', 'string', default="0", resources="db_data"),
        add_field('workplace_lat', 'string', default="0", resources="db_data"),
        add_field('workplace_lng', 'string', default="0", resources="db_data"),
        add_field('workplace_street_accurate', 'string', default="0", resources="db_data"),
        _add_gps_coordinates,
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
    if parameters.get("gps_datapackage_path") and os.path.exists(os.path.join(parameters['gps_datapackage_path'], "datapackage.json")):
        load_cache_from_package(parameters, stats, kv)
    elif parameters.get('gps_data'):
        load_cache_from_json(parameters, stats, kv)
    logging.info('cache loaded successfully')
    return add_gps_coordinates(stats, kv, parameters)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    flow({
        "load_db_data": "data/corona_data_collector/load_from_db",
        "load_gdrive_data": "data/download_gdrive_files/data",
        "source_fields": {
            "db": {
                "street": "street",
                "city_town": "city",
            },
            "google": {
                "Street": "street",
                "Город проживания": "street",
                "City": "city",
                "Улица": "city",
            },
            "hebrew_google": {
                "עיר / ישוב מגורים": "city",
                "עיר / יישוב מגורים": "city",
                "רחוב מגורים": "street",
            },
            "maccabi": {
                "yishuv": "city",
            }
        },
        'dump_to_path': 'data/corona_data_collector/add_gps_coordinates',
        # 'gps_data': 'data/corona_data_collector/gps_data.json'
        "gps_datapackage_path": "data/corona_data_collector/add_gps_coordinates/gps_data",
    }).process()
